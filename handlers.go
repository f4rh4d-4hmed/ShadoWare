package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Semaphore chan struct{}

func NewSemaphore(n int) Semaphore {
	return make(Semaphore, n)
}

func (s Semaphore) Acquire() {
	s <- struct{}{}
}

func (s Semaphore) Release() {
	<-s
}

func (s Semaphore) TryAcquire() bool {
	select {
	case s <- struct{}{}:
		return true
	default:
		return false
	}
}

func handleExecute(bd *BrowserDaemon, sem Semaphore, cfg Config, captureHub *extensionCaptureHub, jobHub *extensionJobHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		var req TaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := validateRequest(req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
		defer cancel()

		if !sem.TryAcquire() {
			writeError(w, "Server busy, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer sem.Release()

		if req.Stream {
			handleExecuteStream(w, r, ctx, req, captureHub, jobHub)
			return
		}

		content, m3u8s, allURLs, captures, err := scrapeExtension(ctx, req, captureHub, jobHub, nil)

		resp := TaskResponse{Content: content, M3u8URLs: m3u8s}
		if req.Debug {
			resp.AllURLs = allURLs
		}
		if req.Debug || req.IncludeHeaders {
			if len(captures) > 0 {
				resp.M3u8Headers = captureSliceToHeaders(captures)
			} else {
				resp.M3u8Headers = collectM3U8Headers(ctx, m3u8s)
			}
		}
		if err != nil {
			resp.Error = err.Error()
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func handleExecuteStream(w http.ResponseWriter, r *http.Request, ctx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	events := make(chan StreamEvent, 2048)
	emit := func(ev StreamEvent) {
		select {
		case events <- ev:
		default:
			log.Println("[stream] event buffer full, dropping event for URL:", ev.URL)
		}
	}
	results := make(chan scrapeResult, 1)

	go func() {
		var res scrapeResult
		res.content, res.m3u8s, res.allURLs, res.captures, res.err = scrapeExtension(ctx, req, captureHub, jobHub, emit)
		results <- res
	}()

	writeEvent := func(ev StreamEvent) bool {
		if err := json.NewEncoder(w).Encode(ev); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	for {
		select {
		case ev := <-events:
			if !writeEvent(ev) {
				return
			}
		case res := <-results:
			for {
				select {
				case ev := <-events:
					if !writeEvent(ev) {
						return
					}
				default:
					resp := TaskResponse{Content: res.content, M3u8URLs: res.m3u8s}
					if req.Debug {
						resp.AllURLs = res.allURLs
					}
					if req.Debug || req.IncludeHeaders {
						if len(res.captures) > 0 {
							resp.M3u8Headers = captureSliceToHeaders(res.captures)
						} else {
							resp.M3u8Headers = collectM3U8Headers(ctx, res.m3u8s)
						}
					}
					if res.err != nil {
						resp.Error = res.err.Error()
					}
					writeEvent(StreamEvent{Type: "done", Response: &resp})
					return
				}
			}
		case <-r.Context().Done():
			return
		}
	}
}

func handleScrapeHLS(bd *BrowserDaemon, sem Semaphore, cfg Config, captureHub *extensionCaptureHub, jobHub *extensionJobHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeHLSScrapeError(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		var req HLSScrapeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHLSScrapeError(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.URL == "" {
			writeHLSScrapeError(w, "url is required", http.StatusBadRequest)
			return
		}
		parsed, err := url.ParseRequestURI(req.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			writeHLSScrapeError(w, "url must be a valid http/https URL", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
		defer cancel()

		if !sem.TryAcquire() {
			writeHLSScrapeError(w, "Server busy, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer sem.Release()

		androidChromeUA := "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"

		taskReq := TaskRequest{
			URL:            req.URL,
			WaitMs:         req.WaitMs,
			LocalStorage:   req.LocalStorage,
			Headers:        req.Headers,
			IncludeHeaders: true,
			UserAgent:      androidChromeUA,
			IsHLSScrape:    true,
		}

		_, m3u8s, _, captures, err := scrapeExtension(ctx, taskReq, captureHub, jobHub, nil)
		if err != nil {
			writeHLSScrapeError(w, "Scraping failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if len(m3u8s) == 0 {
			writeHLSScrapeError(w, "No HLS streams found", http.StatusNotFound)
			return
		}

		log.Printf("[HLSScraper] Found %d candidate HLS URLs: %v", len(m3u8s), m3u8s)

		var playableURL string
		var finalQualities []HLSQuality
		var finalHeaders map[string]string

		for _, candidate := range m3u8s {
			var reqHeaders map[string]string
			for _, capEntry := range captures {
				if capEntry.URL == candidate {
					reqHeaders = capEntry.RequestHeaders
					break
				}
			}
			if reqHeaders == nil {
				reqHeaders = make(map[string]string)
			}
			if _, ok := reqHeaders["User-Agent"]; !ok {
				reqHeaders["User-Agent"] = androidChromeUA
			}

			log.Printf("[HLSScraper] Validating candidate HLS manifest: %s", candidate)
			qualities, playErr := validateAndParseHLS(ctx, candidate, reqHeaders)
			if playErr == nil {
				log.Printf("[HLSScraper] Candidate is valid! Found %d stream qualities.", len(qualities))
				playableURL = candidate
				finalQualities = qualities
				finalHeaders = reqHeaders
				break
			}
			log.Printf("[HLSScraper] Candidate validation failed: %v", playErr)
		}

		if playableURL == "" {
			writeHLSScrapeError(w, "Found HLS links but none were playable", http.StatusNotFound)
			return
		}

		resp := HLSScrapeResponse{
			PlayableURL: playableURL,
			Qualities:   finalQualities,
			Headers:     finalHeaders,
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func handleExtensionCapture(hub *extensionCaptureHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var ev extensionCaptureEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		hub.capture(ev)
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleExtensionCommand(hub *extensionJobHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		job, ok := hub.next()
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func handleExtensionResult(hub *extensionJobHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var result extensionJobResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !hub.complete(result) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func handleVersion(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(Version))
}

func handleConfigGet(cfg Config, bd *BrowserDaemon) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := bd.Status()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"port":         cfg.Port,
			"headless":     cfg.Headless,
			"max_tabs":     cfg.MaxTabs,
			"timeout":      cfg.Timeout.String(),
			"browser":      status["browser"],
			"browser_path": status["path"],
			"status":       status["status"],
			"pid":          status["pid"],
			"uptime":       status["uptime"],
		})
	}
}

func captureSliceToHeaders(caps []m3u8Capture) []CapturedURLHeader {
	deduped := make(map[string]m3u8Capture)
	for _, c := range caps {
		if _, already := deduped[c.URL]; !already {
			deduped[c.URL] = c
		}
	}
	out := make([]CapturedURLHeader, 0, len(deduped))
	for _, c := range deduped {
		out = append(out, CapturedURLHeader{
			URL:             c.URL,
			Status:          c.Status,
			RequestHeaders:  c.RequestHeaders,
			ResponseHeaders: c.ResponseHeaders,
		})
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, msg string, code int) {
	writeJSON(w, code, TaskResponse{Error: msg})
}

func writeHLSScrapeError(w http.ResponseWriter, msg string, code int) {
	writeJSON(w, code, HLSScrapeResponse{Error: msg})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/extension-") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s — %s", r.Method, r.URL.Path, time.Since(start))
	})
}
