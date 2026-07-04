package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"
)

func handleTabList(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, pool.List())
	}
}

func handleTabCreate(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
			writeError(w, "url is required", http.StatusBadRequest)
			return
		}
		tab, err := pool.Open(req.URL)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, tab.info())
	}
}

func handleTabGet(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t, ok := pool.Get(r.PathValue("id"))
		if !ok {
			writeError(w, "tab not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, t.info())
	}
}

func handleTabClose(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !pool.Close(r.PathValue("id")) {
			writeError(w, "tab not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleTabNavigate(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
			writeError(w, "url is required", http.StatusBadRequest)
			return
		}
		if err := pool.Navigate(r.PathValue("id"), req.URL); err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		t, _ := pool.Get(r.PathValue("id"))
		writeJSON(w, http.StatusOK, t.info())
	}
}

func handleTabActions(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Actions []BrowserAction `json:"actions"`
			WaitMs  int             `json:"wait_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		html, err := pool.RunActions(r.PathValue("id"), req.Actions, req.WaitMs)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"content": html})
	}
}

func handleTabSnapshot(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		html, m3u8s, urls, caps, err := pool.Snapshot(r.PathValue("id"))
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := TaskResponse{
			Content:  html,
			M3u8URLs: m3u8s,
			AllURLs:  urls,
		}
		if len(caps) > 0 {
			resp.M3u8Headers = captureSliceToHeaders(caps)
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func handleTabEvaluate(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Script string `json:"script"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Script) == "" {
			writeError(w, "script is required", http.StatusBadRequest)
			return
		}
		result, err := pool.Evaluate(r.PathValue("id"), req.Script)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"result": result})
	}
}

func handleTabClearURLs(pool *TabPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !pool.ClearURLs(r.PathValue("id")) {
			writeError(w, "tab not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleBrowserStatus(bm *BrowserManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, bm.Status())
	}
}

func handleBrowserRestart(bm *BrowserManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bm.Restart()
		writeJSON(w, http.StatusOK, map[string]string{"status": "restarted"})
	}
}

func handleConfigGet(cfg Config, browserPath, browserName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"port":         cfg.Port,
			"headless":     cfg.Headless,
			"max_tabs":     cfg.MaxTabs,
			"timeout":      cfg.Timeout.String(),
			"mode":         cfg.Mode,
			"browser_name": browserName,
			"browser_path": browserPath,
		})
	}
}

func handleExecute(bm *BrowserManager, sem *semaphore.Weighted, cfg Config, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string) http.HandlerFunc {
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
		mode := strings.ToLower(req.Mode)
		if mode == "" {
			mode = cfg.Mode
		}

		ctx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
		defer cancel()

		if !sem.TryAcquire(1) {
			writeError(w, "Server busy, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer sem.Release(1)

		if req.Stream {
			handleExecuteStream(w, r, ctx, bm.AllocCtx(), req, mode, captureHub, jobHub, browserPath, extensionDir)
			return
		}

		var (
			content  string
			m3u8s    []string
			allURLs  []string
			captures []m3u8Capture
			err      error
		)
		if mode == "cdp" {
			content, m3u8s, allURLs, captures, err = scrapeCDP(ctx, bm.AllocCtx(), req, captureHub, nil)
		} else {
			content, m3u8s, allURLs, captures, err = scrapeExtension(ctx, req, captureHub, jobHub, browserPath, extensionDir, nil)
		}

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

func handleExecuteStream(w http.ResponseWriter, r *http.Request, ctx context.Context, allocCtx context.Context, req TaskRequest, mode string, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	events := make(chan StreamEvent, 256)
	emit := func(ev StreamEvent) {
		select {
		case events <- ev:
		default:
		}
	}
	results := make(chan scrapeResult, 1)

	go func() {
		var res scrapeResult
		if mode == "cdp" {
			res.content, res.m3u8s, res.allURLs, res.captures, res.err = scrapeCDP(ctx, allocCtx, req, captureHub, emit)
		} else {
			res.content, res.m3u8s, res.allURLs, res.captures, res.err = scrapeExtension(ctx, req, captureHub, jobHub, browserPath, extensionDir, emit)
		}
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
		log.Printf("[ExtensionCapture] Job: %s, URL: %s", ev.JobID, ev.URL)
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
		log.Printf("[ExtensionResult] Job: %s, M3u8s: %v, AllURLs: %v, Error: %s", result.JobID, result.M3u8URLs, result.AllURLs, result.Error)
		if !hub.complete(result) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, msg string, code int) {
	writeJSON(w, code, TaskResponse{Error: msg})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, DELETE, OPTIONS")
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
