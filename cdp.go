package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
)

func scrapeCDP(ctx, allocCtx context.Context, req TaskRequest, captureHub *extensionCaptureHub, emit func(StreamEvent)) (string, []string, []string, []m3u8Capture, error) {
	tabCtx, tabCancel := chromedp.NewContext(allocCtx)
	defer tabCancel()

	// Forward cancellation from the request context (ctx) to tabCtx
	go func() {
		select {
		case <-ctx.Done():
			tabCancel()
		case <-tabCtx.Done():
		}
	}()

	var (
		m3u8URLs []string
		allURLs  []string
		captures []m3u8Capture
		mu       sync.Mutex
	)
	bodyScanner := newResponseBodyScanner()

	trackURL := func(u string) {
		u = normalizeCapturedURL(u, req.URL)
		if u == "" {
			return
		}
		mu.Lock()
		allURLs = append(allURLs, u)
		isMedia := isMediaCandidateURL(u)
		if isMedia {
			m3u8URLs = append(m3u8URLs, u)
		}
		mu.Unlock()
		if emit != nil {
			emit(StreamEvent{Type: "url", URL: u, IsMedia: isMedia})
		}
	}

	trackCapture := func(c m3u8Capture) {
		mu.Lock()
		captures = append(captures, c)
		mu.Unlock()
		if emit != nil {
			emit(StreamEvent{Type: "url", URL: c.URL, IsMedia: true})
		}
	}
	trackText := func(text, baseURL string) {
		for _, u := range extractCandidateURLs(text, baseURL) {
			trackURL(u)
		}
	}

	unregister := captureHub.register("", req.URL, trackURL)
	defer unregister()

	var attachChild func(context.Context, target.ID)
	attachChild = func(parent context.Context, tid target.ID) {
		cctx, _ := chromedp.NewContext(parent, chromedp.WithTargetID(tid))
		ch := bodyScanner.listen(cctx, trackURL, trackText, trackCapture)
		chromedp.ListenTarget(cctx, func(ev interface{}) {
			ch(ev)
			if e, ok := ev.(*target.EventAttachedToTarget); ok {
				go attachChild(cctx, e.TargetInfo.TargetID)
			}
		})
		go chromedp.Run(cctx,
			target.SetAutoAttach(true, false).WithFlatten(true),
			network.Enable().WithMaxTotalBufferSize(100*1024*1024).WithMaxResourceBufferSize(maxResponseBodyScanBytes),
		)
	}

	mainHandler := bodyScanner.listen(tabCtx, trackURL, trackText, trackCapture)
	chromedp.ListenTarget(tabCtx, func(ev interface{}) {
		mainHandler(ev)
		if e, ok := ev.(*target.EventAttachedToTarget); ok {
			t := string(e.TargetInfo.Type)
			if t == "iframe" || t == "page" || t == "service_worker" || t == "worker" {
				go attachChild(tabCtx, e.TargetInfo.TargetID)
			}
		}
	})

	var actions []chromedp.Action
	if req.UserAgent != "" {
		actions = append(actions, emulation.SetUserAgentOverride(req.UserAgent))
	}
	if len(req.Headers) > 0 {
		headers := make(network.Headers)
		for k, v := range req.Headers {
			headers[k] = v
		}
		actions = append(actions, network.SetExtraHTTPHeaders(headers))
	}
	actions = append(actions,
		target.SetAutoAttach(true, false).WithFlatten(true),
		network.Enable().WithMaxTotalBufferSize(100 * 1024 * 1024).WithMaxResourceBufferSize(maxResponseBodyScanBytes),
		chromedp.ActionFunc(func(ctx context.Context) error {
			if _, err := page.AddScriptToEvaluateOnNewDocument(stealthScript).Do(ctx); err != nil {
				return err
			}
			if req.IsHLSScrape {
				if _, err := page.AddScriptToEvaluateOnNewDocument(hlsScraperInjectedScript).Do(ctx); err != nil {
					return err
				}
			}
			return nil
		}),
		chromedp.Navigate(req.URL),
	)
	if len(req.LocalStorage) > 0 {
		actions = append(actions, chromedp.ActionFunc(func(ctx context.Context) error {
			for k, v := range req.LocalStorage {
				km, _ := json.Marshal(k)
				vm, _ := json.Marshal(v)
				if err := chromedp.Evaluate(fmt.Sprintf("localStorage.setItem(%s,%s)", km, vm), nil).Do(ctx); err != nil {
					return err
				}
			}
			return nil
		}), chromedp.Reload())
	}
	var htmlContent string
	actions = append(actions, chromedp.WaitReady("body", chromedp.ByQuery))
	for _, a := range req.Actions {
		actions = append(actions, buildBrowserAction(a))
	}
	actions = append(actions,
		chromedp.Sleep(time.Duration(req.WaitMs)*time.Millisecond),
		chromedp.OuterHTML("html", &htmlContent),
	)

	runErr := chromedp.Run(tabCtx, actions...)
	trackText(htmlContent, req.URL)
	scanResponseBodies(bodyScanner, trackText)

	mu.Lock()
	m3u8s := dedupe(append([]string(nil), m3u8URLs...))
	all := dedupe(append([]string(nil), allURLs...))
	caps := append([]m3u8Capture(nil), captures...)
	mu.Unlock()

	if runErr != nil {
		return "", m3u8s, all, caps, fmt.Errorf("chromedp: %w", runErr)
	}
	return htmlContent, m3u8s, all, caps, nil
}

type responseMeta struct {
	url               string
	resourceType      network.ResourceType
	mime              string
	encodedDataLength float64
	requestHeaders    map[string]string
}

type responseBodyJob struct {
	ctx       context.Context
	requestID network.RequestID
	baseURL   string
}

type responseBodyScanner struct {
	mu        sync.Mutex
	responses map[network.RequestID]responseMeta
	jobs      []responseBodyJob
}

func newResponseBodyScanner() *responseBodyScanner {
	return &responseBodyScanner{responses: make(map[network.RequestID]responseMeta)}
}

func (s *responseBodyScanner) listen(
	ctx context.Context,
	trackURL func(string),
	trackText func(string, string),
	trackCapture func(m3u8Capture),
) func(interface{}) {
	return func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventRequestWillBeSent:
			if e.Request != nil {
				trackURL(e.Request.URL)
				reqHdrs := make(map[string]string, len(e.Request.Headers))
				for k, v := range e.Request.Headers {
					if sv, ok := v.(string); ok {
						reqHdrs[k] = sv
					}
				}
				s.remember(e.RequestID, responseMeta{
					url:            e.Request.URL,
					resourceType:   e.Type,
					requestHeaders: reqHdrs,
				})
			}
			if e.RedirectResponse != nil {
				trackURL(e.RedirectResponse.URL)
			}
			if e.Initiator != nil {
				trackURL(e.Initiator.URL)
			}
		case *network.EventResponseReceived:
			if e.Response == nil {
				return
			}
			trackURL(e.Response.URL)
			for _, h := range e.Response.Headers {
				if v, ok := h.(string); ok {
					trackText(v, e.Response.URL)
				}
			}
			respHdrs := make(map[string]string, len(e.Response.Headers))
			for k, v := range e.Response.Headers {
				if sv, ok := v.(string); ok {
					respHdrs[k] = sv
				}
			}
			s.mu.Lock()
			ex := s.responses[e.RequestID]
			ex.url = e.Response.URL
			ex.resourceType = e.Type
			ex.mime = e.Response.MimeType
			s.mu.Unlock()

			if isM3U8URL(e.Response.URL) {
				s.mu.Lock()
				reqHdrs := ex.requestHeaders
				s.mu.Unlock()
				trackCapture(m3u8Capture{
					URL:             e.Response.URL,
					Status:          int(e.Response.Status),
					RequestHeaders:  reqHdrs,
					ResponseHeaders: respHdrs,
				})
			}
			s.remember(e.RequestID, responseMeta{
				url:          e.Response.URL,
				resourceType: e.Type,
				mime:         e.Response.MimeType,
			})
		case *network.EventLoadingFinished:
			s.finish(ctx, e.RequestID, e.EncodedDataLength)
		case *network.EventWebSocketCreated:
			trackURL(e.URL)
		case *network.EventWebSocketFrameReceived:
			if e.Response != nil {
				trackText(e.Response.PayloadData, "")
			}
		case *network.EventWebTransportCreated:
			trackURL(e.URL)
		}
	}
}

func (s *responseBodyScanner) remember(id network.RequestID, meta responseMeta) {
	s.mu.Lock()
	ex := s.responses[id]
	if meta.url != "" {
		ex.url = meta.url
	}
	if meta.resourceType != "" {
		ex.resourceType = meta.resourceType
	}
	if meta.mime != "" {
		ex.mime = meta.mime
	}
	if meta.requestHeaders != nil {
		ex.requestHeaders = meta.requestHeaders
	}
	s.responses[id] = ex
	s.mu.Unlock()
}

func (s *responseBodyScanner) finish(ctx context.Context, id network.RequestID, size float64) {
	s.mu.Lock()
	meta, ok := s.responses[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	meta.encodedDataLength = size
	delete(s.responses, id)
	if !shouldInspectResponseBody(meta) {
		s.mu.Unlock()
		return
	}
	s.jobs = append(s.jobs, responseBodyJob{ctx: ctx, requestID: id, baseURL: meta.url})
	s.mu.Unlock()
}

func (s *responseBodyScanner) drain() []responseBodyJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := s.jobs
	s.jobs = nil
	return jobs
}

func scanResponseBodies(scanner *responseBodyScanner, trackText func(string, string)) {
	deadline := time.Now().Add(6 * time.Second)
	jobs := scanner.drain()
	if len(jobs) == 0 {
		return
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, job := range jobs {
		if time.Now().After(deadline) {
			break
		}
		wg.Add(1)
		go func(job responseBodyJob) {
			defer wg.Done()
			bctx, cancel := context.WithTimeout(job.ctx, time.Until(deadline))
			defer cancel()
			var body []byte
			err := chromedp.Run(bctx, chromedp.ActionFunc(func(ctx context.Context) error {
				var e error
				body, e = network.GetResponseBody(job.requestID).Do(ctx)
				return e
			}))
			if err == nil && len(body) > 0 {
				mu.Lock()
				trackText(string(body), job.baseURL)
				mu.Unlock()
			}
		}(job)
	}
	wg.Wait()
}

func shouldInspectResponseBody(meta responseMeta) bool {
	if meta.url != "" && isMediaCandidateURL(meta.url) {
		return true
	}
	if meta.encodedDataLength > maxResponseBodyScanBytes {
		return false
	}
	mime := strings.ToLower(meta.mime)
	matched := false
	for _, sub := range []string{"json", "javascript", "mpegurl", "dash+xml", "text", "html", "xml"} {
		if strings.Contains(mime, sub) {
			matched = true
			break
		}
	}
	if matched {
		return true
	}
	switch meta.resourceType {
	case network.ResourceTypeDocument, network.ResourceTypeScript,
		network.ResourceTypeXHR, network.ResourceTypeFetch,
		network.ResourceTypeManifest, network.ResourceTypeMedia:
		return true
	}
	return false
}
