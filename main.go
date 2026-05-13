package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	"golang.org/x/sync/semaphore"
)

// ─── Config ───────────────────────────────────────────────────────────────────

// Config holds all startup options, populated from CLI flags.
// Every field has a sensible default so zero-config works out of the box.
type Config struct {
	Port     string        // e.g. ":8080" or "0.0.0.0:9000"
	Headless bool          // run browser headless
	MaxTabs  int           // max concurrent one-shot scrapes
	Browser  string        // explicit browser path; auto-detected if empty
	Timeout  time.Duration // per-request deadline
	Mode     string        // default scrape mode: "extension" | "cdp"
}

func parseConfig() Config {
	var cfg Config
	flag.StringVar(&cfg.Port, "port", ":8080", "HTTP listen address (e.g. :8080 or 0.0.0.0:9000)")
	flag.BoolVar(&cfg.Headless, "headless", false, "Run browser headless (set true for production)")
	flag.IntVar(&cfg.MaxTabs, "max-tabs", 5, "Max concurrent browser tabs for one-shot /execute")
	flag.StringVar(&cfg.Browser, "browser", "", "Override browser executable path (auto-detected if empty)")
	flag.DurationVar(&cfg.Timeout, "timeout", 120*time.Second, "Per-request timeout")
	flag.StringVar(&cfg.Mode, "mode", "extension", "Default scrape mode: extension | cdp")
	flag.Parse()

	if !strings.HasPrefix(cfg.Port, ":") && !strings.Contains(cfg.Port, ":") {
		cfg.Port = ":" + cfg.Port
	}
	cfg.Mode = strings.ToLower(cfg.Mode)
	return cfg
}

// ─── Core types ───────────────────────────────────────────────────────────────

type TaskRequest struct {
	URL            string            `json:"url"`
	Mode           string            `json:"mode,omitempty"`
	WaitMs         int               `json:"wait_ms"`
	LocalStorage   map[string]string `json:"local_storage,omitempty"`
	Actions        []BrowserAction   `json:"actions,omitempty"`
	Debug          bool              `json:"debug,omitempty"`
	IncludeHeaders bool              `json:"include_headers,omitempty"`
	Stream         bool              `json:"stream,omitempty"`
}

type BrowserAction struct {
	Type     string  `json:"type"`
	Selector string  `json:"selector,omitempty"`
	Script   string  `json:"script,omitempty"`
	Text     string  `json:"text,omitempty"`
	X        float64 `json:"x,omitempty"`
	Y        float64 `json:"y,omitempty"`
	DeltaX   float64 `json:"delta_x,omitempty"`
	DeltaY   float64 `json:"delta_y,omitempty"`
	WaitMs   int     `json:"wait_ms,omitempty"`
}

type TaskResponse struct {
	Content     string              `json:"content"`
	M3u8URLs    []string            `json:"m3u8_urls,omitempty"`
	AllURLs     []string            `json:"all_urls,omitempty"`
	M3u8Headers []CapturedURLHeader `json:"m3u8_headers,omitempty"`
	Error       string              `json:"error,omitempty"`
}

type CapturedURLHeader struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Status  int               `json:"status,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Error   string            `json:"error,omitempty"`
}

type StreamEvent struct {
	Type     string        `json:"type"`
	URL      string        `json:"url,omitempty"`
	IsMedia  bool          `json:"is_media,omitempty"`
	Response *TaskResponse `json:"response,omitempty"`
	Error    string        `json:"error,omitempty"`
}

// ─── Tab pool ─────────────────────────────────────────────────────────────────

// Tab is a persistent browser tab controlled by the caller over the REST API.
type Tab struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"` // loading | ready | error
	CreatedAt time.Time `json:"created_at"`
	ErrMsg    string    `json:"error,omitempty"`

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	m3u8s  []string
	urls   []string
}

func (t *Tab) info() map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return map[string]interface{}{
		"id":         t.ID,
		"url":        t.URL,
		"status":     t.Status,
		"created_at": t.CreatedAt,
		"error":      t.ErrMsg,
		"url_count":  len(t.urls),
		"m3u8_count": len(t.m3u8s),
	}
}

// TabPool manages long-lived CDP tabs that external callers can control.
type TabPool struct {
	mu   sync.RWMutex
	tabs map[string]*Tab
	bm   *BrowserManager
}

func newTabPool(bm *BrowserManager) *TabPool {
	return &TabPool{tabs: make(map[string]*Tab), bm: bm}
}

func (p *TabPool) Open(reqURL string) (*Tab, error) {
	allocCtx := p.bm.AllocCtx()
	tabCtx, tabCancel := chromedp.NewContext(allocCtx)

	tab := &Tab{
		ID:        newID(),
		URL:       reqURL,
		Status:    "loading",
		CreatedAt: time.Now(),
		ctx:       tabCtx,
		cancel:    tabCancel,
	}

	trackURL := func(u string) {
		u = normalizeCapturedURL(u, reqURL)
		if u == "" {
			return
		}
		tab.mu.Lock()
		tab.urls = append(tab.urls, u)
		if isMediaCandidateURL(u) {
			tab.m3u8s = append(tab.m3u8s, u)
		}
		tab.mu.Unlock()
	}
	trackText := func(text, base string) {
		for _, u := range extractCandidateURLs(text, base) {
			trackURL(u)
		}
	}

	scanner := newResponseBodyScanner()
	mainHandler := scanner.listen(tabCtx, trackURL, trackText)
	var attachChild func(context.Context, target.ID)
	attachChild = func(parent context.Context, tid target.ID) {
		cctx, _ := chromedp.NewContext(parent, chromedp.WithTargetID(tid))
		ch := scanner.listen(cctx, trackURL, trackText)
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
	chromedp.ListenTarget(tabCtx, func(ev interface{}) {
		mainHandler(ev)
		if e, ok := ev.(*target.EventAttachedToTarget); ok {
			t := string(e.TargetInfo.Type)
			if t == "iframe" || t == "page" || t == "service_worker" || t == "worker" {
				go attachChild(tabCtx, e.TargetInfo.TargetID)
			}
		}
	})

	p.mu.Lock()
	p.tabs[tab.ID] = tab
	p.mu.Unlock()

	// Navigate asynchronously so the caller gets the tab ID immediately.
	go func() {
		err := chromedp.Run(tabCtx,
			target.SetAutoAttach(true, false).WithFlatten(true),
			network.Enable().WithMaxTotalBufferSize(100*1024*1024),
			chromedp.ActionFunc(func(ctx context.Context) error {
				_, err := page.AddScriptToEvaluateOnNewDocument(stealthScript).Do(ctx)
				return err
			}),
			chromedp.Navigate(reqURL),
			chromedp.WaitReady("body", chromedp.ByQuery),
		)
		tab.mu.Lock()
		if err != nil {
			tab.Status = "error"
			tab.ErrMsg = err.Error()
		} else {
			tab.Status = "ready"
		}
		tab.mu.Unlock()
	}()

	return tab, nil
}

func (p *TabPool) Get(id string) (*Tab, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	t, ok := p.tabs[id]
	return t, ok
}

func (p *TabPool) Close(id string) bool {
	p.mu.Lock()
	t, ok := p.tabs[id]
	if ok {
		delete(p.tabs, id)
	}
	p.mu.Unlock()
	if ok {
		t.cancel()
	}
	return ok
}

func (p *TabPool) CloseAll() {
	p.mu.Lock()
	tabs := make([]*Tab, 0, len(p.tabs))
	for _, t := range p.tabs {
		tabs = append(tabs, t)
	}
	p.tabs = make(map[string]*Tab)
	p.mu.Unlock()
	for _, t := range tabs {
		t.cancel()
	}
}

func (p *TabPool) List() []map[string]interface{} {
	p.mu.RLock()
	tabs := make([]*Tab, 0, len(p.tabs))
	for _, t := range p.tabs {
		tabs = append(tabs, t)
	}
	p.mu.RUnlock()
	sort.Slice(tabs, func(i, j int) bool {
		return tabs[i].CreatedAt.Before(tabs[j].CreatedAt)
	})
	out := make([]map[string]interface{}, len(tabs))
	for i, t := range tabs {
		out[i] = t.info()
	}
	return out
}

func (p *TabPool) Navigate(id, newURL string) error {
	t, ok := p.Get(id)
	if !ok {
		return errors.New("tab not found")
	}
	t.mu.Lock()
	t.Status = "loading"
	t.URL = newURL
	t.mu.Unlock()

	err := chromedp.Run(t.ctx,
		chromedp.Navigate(newURL),
		chromedp.WaitReady("body", chromedp.ByQuery),
	)
	t.mu.Lock()
	if err != nil {
		t.Status = "error"
		t.ErrMsg = err.Error()
	} else {
		t.Status = "ready"
		t.ErrMsg = ""
	}
	t.mu.Unlock()
	return err
}

func (p *TabPool) RunActions(id string, actions []BrowserAction, waitMs int) (string, error) {
	t, ok := p.Get(id)
	if !ok {
		return "", errors.New("tab not found")
	}
	cdpActions := make([]chromedp.Action, 0, len(actions)+2)
	for _, a := range actions {
		cdpActions = append(cdpActions, buildBrowserAction(a))
	}
	if waitMs > 0 {
		cdpActions = append(cdpActions, chromedp.Sleep(time.Duration(waitMs)*time.Millisecond))
	}
	var html string
	cdpActions = append(cdpActions, chromedp.OuterHTML("html", &html))
	return html, chromedp.Run(t.ctx, cdpActions...)
}

func (p *TabPool) Snapshot(id string) (string, []string, []string, error) {
	t, ok := p.Get(id)
	if !ok {
		return "", nil, nil, errors.New("tab not found")
	}
	var html string
	if err := chromedp.Run(t.ctx, chromedp.OuterHTML("html", &html)); err != nil {
		return "", nil, nil, err
	}
	t.mu.Lock()
	m3u8s := dedupe(append([]string(nil), t.m3u8s...))
	urls := dedupe(append([]string(nil), t.urls...))
	t.mu.Unlock()
	return html, m3u8s, urls, nil
}

func (p *TabPool) Evaluate(id, script string) (interface{}, error) {
	t, ok := p.Get(id)
	if !ok {
		return nil, errors.New("tab not found")
	}
	var result interface{}
	err := chromedp.Run(t.ctx, chromedp.Evaluate(script, &result))
	return result, err
}

func (p *TabPool) ClearURLs(id string) bool {
	t, ok := p.Get(id)
	if !ok {
		return false
	}
	t.mu.Lock()
	t.m3u8s = nil
	t.urls = nil
	t.mu.Unlock()
	return true
}

// ─── Browser Manager ──────────────────────────────────────────────────────────

// BrowserManager owns the allocator context and can restart the browser.
type BrowserManager struct {
	mu          sync.RWMutex
	allocCtx    context.Context
	allocCancel context.CancelFunc
	opts        []chromedp.ExecAllocatorOption
	pool        *TabPool
	browserPath string
	browserName string
	startedAt   time.Time
}

func newBrowserManager(browserPath, browserName string, opts []chromedp.ExecAllocatorOption) *BrowserManager {
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	bm := &BrowserManager{
		allocCtx:    allocCtx,
		allocCancel: allocCancel,
		opts:        opts,
		browserPath: browserPath,
		browserName: browserName,
		startedAt:   time.Now(),
	}
	return bm
}

func (bm *BrowserManager) AllocCtx() context.Context {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.allocCtx
}

func (bm *BrowserManager) Restart() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Close all persistent tabs before killing the browser.
	if bm.pool != nil {
		bm.pool.CloseAll()
	}
	bm.allocCancel()

	bm.allocCtx, bm.allocCancel = chromedp.NewExecAllocator(context.Background(), bm.opts...)
	bm.startedAt = time.Now()
	log.Println("Browser restarted")
}

func (bm *BrowserManager) Status() map[string]interface{} {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return map[string]interface{}{
		"browser":    bm.browserName,
		"path":       bm.browserPath,
		"started_at": bm.startedAt,
		"uptime":     time.Since(bm.startedAt).String(),
	}
}

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	maxHeaderProbes        = 8
	headerProbeTimeout     = 12 * time.Second
	maxResponseBodyScanBytes = 2 * 1024 * 1024
)

var (
	nextExtensionJobID atomic.Uint64
	mediaURLPattern    = regexp.MustCompile(`(?i)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*(?:\.m3u8|m3u8|\.mpd|/playlist|/master|manifest)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*`)
)

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := parseConfig()

	browserPath := cfg.Browser
	var browserName string
	if browserPath == "" {
		browserPath, browserName = findBrowser()
		if browserPath == "" {
			log.Fatal("No Chromium-based browser found. Install Chrome, Edge, or Brave, or pass -browser=<path>")
		}
	} else {
		browserName = "custom"
	}
	log.Printf("Using browser: %s (%s)", browserName, browserPath)

	captureExtensionDir, err := ensureCaptureExtension(cfg.Port)
	if err != nil {
		log.Fatalf("Failed to prepare capture extension: %v", err)
	}
	log.Printf("Capture extension ready at %s", captureExtensionDir)

	opts := buildAllocatorOptions(browserPath, captureExtensionDir, cfg)
	bm := newBrowserManager(browserPath, browserName, opts)

	captureHub := newExtensionCaptureHub()
	jobHub := newExtensionJobHub()
	pool := newTabPool(bm)
	bm.pool = pool

	sem := semaphore.NewWeighted(int64(cfg.MaxTabs))

	mux := http.NewServeMux()

	// ── One-shot scrape (backward-compatible) ──
	mux.HandleFunc("POST /execute", handleExecute(bm, sem, cfg, captureHub, jobHub, browserPath, captureExtensionDir))

	// ── Persistent tab control ──
	mux.HandleFunc("GET /tabs", handleTabList(pool))
	mux.HandleFunc("POST /tabs", handleTabCreate(pool))
	mux.HandleFunc("GET /tabs/{id}", handleTabGet(pool))
	mux.HandleFunc("DELETE /tabs/{id}", handleTabClose(pool))
	mux.HandleFunc("POST /tabs/{id}/navigate", handleTabNavigate(pool))
	mux.HandleFunc("POST /tabs/{id}/actions", handleTabActions(pool))
	mux.HandleFunc("GET /tabs/{id}/snapshot", handleTabSnapshot(pool))
	mux.HandleFunc("POST /tabs/{id}/evaluate", handleTabEvaluate(pool))
	mux.HandleFunc("DELETE /tabs/{id}/urls", handleTabClearURLs(pool))

	// ── Browser control ──
	mux.HandleFunc("GET /browser", handleBrowserStatus(bm))
	mux.HandleFunc("POST /browser/restart", handleBrowserRestart(bm))

	// ── Config ──
	mux.HandleFunc("GET /config", handleConfigGet(cfg, browserPath, browserName))

	// ── Extension bridge (internal; used by the injected extension) ──
	mux.HandleFunc("GET /extension-command", handleExtensionCommand(jobHub))
	mux.HandleFunc("POST /extension-capture", handleExtensionCapture(captureHub))
	mux.HandleFunc("POST /extension-result", handleExtensionResult(jobHub))

	// ── Misc ──
	mux.HandleFunc("GET /health", handleHealth)

	timeout := cfg.Timeout
	srv := &http.Server{
		Addr:         cfg.Port,
		Handler:      withCORS(withLogging(mux)),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: timeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdown := func(reason string) {
		log.Printf("Shutting down: %s", reason)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Forced shutdown: %v", err)
		}
	}

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		sig := <-quit
		shutdown(fmt.Sprintf("signal %s received", sig))
	}()

	startParentWatchdog(shutdown)

	fmt.Printf("Shadoware active on http://localhost%s  (parent PID: %d)\n", cfg.Port, os.Getppid())
	fmt.Printf("Mode: %s | Headless: %v | MaxTabs: %d | Timeout: %s\n",
		cfg.Mode, cfg.Headless, cfg.MaxTabs, cfg.Timeout)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}

func buildAllocatorOptions(browserPath, extensionDir string, cfg Config) []chromedp.ExecAllocatorOption {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(browserPath),
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.DisableGPU,
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-extensions", false),
		chromedp.Flag("disable-extensions-except", extensionDir),
		chromedp.Flag("load-extension", extensionDir),
		chromedp.Flag("disable-plugins", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("safebrowsing-disable-auto-update", true),
		chromedp.Flag("js-flags", "--max-old-space-size=128"),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
	)
	if cfg.Headless {
		opts = append(opts, chromedp.Flag("headless", true))
	} else {
		opts = append(opts, chromedp.Flag("headless", false))
	}
	return opts
}

// ─── Tab HTTP handlers ────────────────────────────────────────────────────────

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
		html, m3u8s, urls, err := pool.Snapshot(r.PathValue("id"))
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, TaskResponse{
			Content:  html,
			M3u8URLs: m3u8s,
			AllURLs:  urls,
		})
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

// ─── Browser HTTP handlers ────────────────────────────────────────────────────

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

// ─── One-shot execute handler ─────────────────────────────────────────────────

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
		// Per-request mode overrides the server default.
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
			content string
			m3u8s   []string
			allURLs []string
			err     error
		)
		if mode == "cdp" {
			content, m3u8s, allURLs, err = scrapeCDP(ctx, bm.AllocCtx(), req, captureHub, nil)
		} else {
			content, m3u8s, allURLs, err = scrapeExtension(ctx, req, captureHub, jobHub, browserPath, extensionDir, nil)
		}

		resp := TaskResponse{Content: content, M3u8URLs: m3u8s}
		if req.Debug {
			resp.AllURLs = allURLs
		}
		if req.Debug || req.IncludeHeaders {
			resp.M3u8Headers = collectM3U8Headers(ctx, m3u8s)
		}
		if err != nil {
			resp.Error = err.Error()
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

type scrapeResult struct {
	content string
	m3u8s   []string
	allURLs []string
	err     error
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
			res.content, res.m3u8s, res.allURLs, res.err = scrapeCDP(ctx, allocCtx, req, captureHub, emit)
		} else {
			res.content, res.m3u8s, res.allURLs, res.err = scrapeExtension(ctx, req, captureHub, jobHub, browserPath, extensionDir, emit)
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
						resp.M3u8Headers = collectM3U8Headers(ctx, res.m3u8s)
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

// ─── Extension bridge ─────────────────────────────────────────────────────────

type extensionCaptureEvent struct {
	JobID     string `json:"job_id,omitempty"`
	URL       string `json:"url"`
	TabID     int    `json:"tab_id"`
	FrameID   int    `json:"frame_id"`
	RequestID string `json:"request_id"`
	Type      string `json:"type"`
	Initiator string `json:"initiator,omitempty"`
}

type extensionCaptureSession struct {
	jobID    string
	startURL string
	hasTabID bool
	tabID    int
	trackURL func(string)
}

type extensionCaptureHub struct {
	mu       sync.Mutex
	sessions map[*extensionCaptureSession]struct{}
}

type extensionJob struct {
	JobID        string            `json:"job_id"`
	URL          string            `json:"url"`
	WaitMs       int               `json:"wait_ms"`
	LocalStorage map[string]string `json:"local_storage,omitempty"`
	Actions      []BrowserAction   `json:"actions,omitempty"`
	CloseTab     bool              `json:"close_tab"`
}

type extensionJobResult struct {
	JobID   string `json:"job_id"`
	Content string `json:"content"`
	Error   string `json:"error,omitempty"`
}

type extensionJobHub struct {
	mu      sync.Mutex
	queue   []*extensionJob
	results map[string]chan extensionJobResult
}

func newExtensionCaptureHub() *extensionCaptureHub {
	return &extensionCaptureHub{sessions: make(map[*extensionCaptureSession]struct{})}
}

func newExtensionJobHub() *extensionJobHub {
	return &extensionJobHub{results: make(map[string]chan extensionJobResult)}
}

func (h *extensionJobHub) enqueue(job *extensionJob) <-chan extensionJobResult {
	ch := make(chan extensionJobResult, 1)
	h.mu.Lock()
	h.queue = append(h.queue, job)
	h.results[job.JobID] = ch
	h.mu.Unlock()
	return ch
}

func (h *extensionJobHub) next() (*extensionJob, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.queue) == 0 {
		return nil, false
	}
	job := h.queue[0]
	h.queue = h.queue[1:]
	return job, true
}

func (h *extensionJobHub) complete(result extensionJobResult) bool {
	h.mu.Lock()
	ch := h.results[result.JobID]
	delete(h.results, result.JobID)
	h.mu.Unlock()
	if ch == nil {
		return false
	}
	ch <- result
	return true
}

func (h *extensionCaptureHub) register(jobID, startURL string, trackURL func(string)) func() {
	s := &extensionCaptureSession{
		jobID:    jobID,
		startURL: normalizeCapturedURL(startURL, ""),
		trackURL: trackURL,
	}
	h.mu.Lock()
	h.sessions[s] = struct{}{}
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		delete(h.sessions, s)
		h.mu.Unlock()
	}
}

func (h *extensionCaptureHub) capture(ev extensionCaptureEvent) {
	if ev.URL == "" || ev.TabID < 0 {
		return
	}
	normalized := normalizeCapturedURL(ev.URL, "")
	var trackers []func(string)
	h.mu.Lock()
	for s := range h.sessions {
		if s.jobID != "" {
			if s.jobID == ev.JobID {
				trackers = append(trackers, s.trackURL)
			}
			continue
		}
		if s.hasTabID {
			if s.tabID == ev.TabID {
				trackers = append(trackers, s.trackURL)
			}
			continue
		}
		if ev.Type == "main_frame" && sameCapturedURL(normalized, s.startURL) {
			s.hasTabID = true
			s.tabID = ev.TabID
			trackers = append(trackers, s.trackURL)
		}
	}
	h.mu.Unlock()
	for _, fn := range trackers {
		fn(ev.URL)
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

// ─── Extension browser ────────────────────────────────────────────────────────

type extensionBrowserProcess struct {
	cmd        *exec.Cmd
	profileDir string
}

func ensureCaptureExtension(serverAddr string) (string, error) {
	dir := filepath.Join(os.TempDir(), "shadoware-capture-extension")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	apiBase := "http://127.0.0.1" + serverAddr
	manifest := `{
  "manifest_version": 3,
  "name": "ShadoWare Capture",
  "version": "1.0.0",
  "permissions": ["webRequest", "tabs", "scripting"],
  "host_permissions": ["<all_urls>"],
  "background": { "service_worker": "background.js" }
}`
	background := fmt.Sprintf(`const API_BASE = %q;
const CAPTURE = API_BASE + "/extension-capture";
const COMMAND = API_BASE + "/extension-command";
const RESULT  = API_BASE + "/extension-result";
const tabJobs = new Map();
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function post(url, body) {
  await fetch(url, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify(body) });
}

chrome.webRequest.onBeforeRequest.addListener((d) => {
  if (!d.url || d.url.startsWith(CAPTURE)) return;
  const jobId = tabJobs.get(d.tabId);
  if (!jobId) return;
  post(CAPTURE, { job_id: jobId, url: d.url, tab_id: d.tabId, frame_id: d.frameId,
                  request_id: d.requestId, type: d.type, initiator: d.initiator||"" }).catch(()=>{});
}, { urls: ["<all_urls>"] });

function waitComplete(tabId, ms=30000) {
  return new Promise(resolve => {
    let done=false;
    const finish=()=>{ if(done)return; done=true; chrome.tabs.onUpdated.removeListener(l); resolve(); };
    const l=(id,i)=>{ if(id===tabId&&i.status==="complete") finish(); };
    chrome.tabs.onUpdated.addListener(l);
    chrome.tabs.get(tabId).then(t=>{ if(t.status==="complete") finish(); }).catch(finish);
    setTimeout(finish, ms);
  });
}

async function runAction(tabId, a) {
  const type = (a.type||"").toLowerCase();
  if (type==="wait"||type==="sleep") { await sleep(a.wait_ms||0); return; }
  if (type==="wait_ready") {
    await chrome.scripting.executeScript({ target:{tabId}, args:[a.selector,a.wait_ms||10000],
      func:(sel,ms)=>{ const s=Date.now(); return new Promise(r=>{ const i=setInterval(()=>{ if(document.querySelector(sel)||Date.now()-s>ms){clearInterval(i);r();} },100); }); } });
    return;
  }
  if (type==="click"||type==="double_click") {
    await chrome.scripting.executeScript({ target:{tabId}, args:[a.selector||"",a.x||0,a.y||0,type==="double_click"],
      func:(sel,x,y,dbl)=>{ const el=sel?document.querySelector(sel):document.elementFromPoint(x,y); if(!el)return;
        ["mousedown","mouseup","click"].forEach(n=>el.dispatchEvent(new MouseEvent(n,{bubbles:true,cancelable:true,view:window})));
        if(dbl) el.dispatchEvent(new MouseEvent("dblclick",{bubbles:true,cancelable:true,view:window})); } });
    return;
  }
  if (type==="scroll") {
    await chrome.scripting.executeScript({ target:{tabId}, args:[a.delta_x||0,a.delta_y||0],
      func:(dx,dy)=>window.scrollBy(dx,dy) });
    return;
  }
  if (type==="send_keys"||type==="type") {
    await chrome.scripting.executeScript({ target:{tabId}, args:[a.selector,a.text||""],
      func:(sel,txt)=>{ const el=document.querySelector(sel); if(!el)return; el.focus(); el.value=txt;
        el.dispatchEvent(new Event("input",{bubbles:true})); el.dispatchEvent(new Event("change",{bubbles:true})); } });
    return;
  }
  if (type==="evaluate"||type==="eval") {
    await chrome.scripting.executeScript({ target:{tabId}, args:[a.script||""], world:"MAIN",
      func:(s)=>(0,eval)(s) });
  }
}

async function runJob(job) {
  let tab;
  try {
    tab = await chrome.tabs.create({ url: job.url, active: true });
    tabJobs.set(tab.id, job.job_id);
    await waitComplete(tab.id);
    if (job.local_storage && Object.keys(job.local_storage).length) {
      await chrome.scripting.executeScript({ target:{tabId:tab.id}, args:[job.local_storage],
        func:(items)=>{ for(const[k,v] of Object.entries(items)) localStorage.setItem(k,v); } });
      await chrome.tabs.reload(tab.id);
      await waitComplete(tab.id);
    }
    for (const action of job.actions||[]) await runAction(tab.id, action);
    if (job.wait_ms) await sleep(job.wait_ms);
    const frames = await chrome.scripting.executeScript({ target:{tabId:tab.id,allFrames:true},
      func:()=>document.documentElement.outerHTML });
    const content = frames.map(f=>f.result||"").join("\n");
    await post(RESULT, { job_id: job.job_id, content });
  } catch(e) {
    await post(RESULT, { job_id: job.job_id, content: "", error: e&&e.message?e.message:String(e) }).catch(()=>{});
  } finally {
    if (tab&&tab.id!==undefined) {
      tabJobs.delete(tab.id);
      if (job.close_tab!==false) chrome.tabs.remove(tab.id).catch(()=>{});
    }
  }
}

async function poll() {
  for(;;) {
    try {
      const r = await fetch(COMMAND);
      if (r.status===200) await runJob(await r.json());
      else await sleep(500);
    } catch(_) { await sleep(1000); }
  }
}
poll();
`, apiBase)

	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "background.js"), []byte(background), 0644); err != nil {
		return "", err
	}
	return dir, nil
}

func launchExtensionBrowser(browserPath, extensionDir, jobID string) (*extensionBrowserProcess, error) {
	profileDir, err := os.MkdirTemp("", "shadoware-profile-"+safeFilePart(jobID)+"-")
	if err != nil {
		return nil, err
	}
	args := []string{
		"--user-data-dir=" + profileDir,
		"--headless=new",
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-gpu",
		"--disable-blink-features=AutomationControlled",
		"--disable-extensions-except=" + extensionDir,
		"--load-extension=" + extensionDir,
		"--window-size=1365,768",
		"--mute-audio",
		"about:blank",
	}
	cmd := exec.Command(browserPath, args...)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(profileDir)
		return nil, err
	}
	return &extensionBrowserProcess{cmd: cmd, profileDir: profileDir}, nil
}

func closeExtensionBrowser(b *extensionBrowserProcess) {
	if b == nil {
		return
	}
	if b.cmd != nil && b.cmd.Process != nil {
		if runtime.GOOS == "windows" {
			_ = exec.Command("taskkill", "/PID", strconv.Itoa(b.cmd.Process.Pid), "/T", "/F").Run()
		} else {
			_ = b.cmd.Process.Kill()
		}
		done := make(chan struct{})
		go func() { _ = b.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	if b.profileDir != "" && strings.HasPrefix(filepath.Base(b.profileDir), "shadoware-profile-") {
		_ = os.RemoveAll(b.profileDir)
	}
}

// ─── Scrape modes ─────────────────────────────────────────────────────────────

func scrapeExtension(ctx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string, emit func(StreamEvent)) (string, []string, []string, error) {
	var (
		m3u8URLs []string
		allURLs  []string
		mu       sync.Mutex
	)

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
	trackText := func(text, baseURL string) {
		for _, u := range extractCandidateURLs(text, baseURL) {
			trackURL(u)
		}
	}

	jobID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), nextExtensionJobID.Add(1))

	browser, err := launchExtensionBrowser(browserPath, extensionDir, jobID)
	if err != nil {
		return "", nil, nil, err
	}
	defer closeExtensionBrowser(browser)

	// BUG FIX 2: give the browser's service worker time to start polling.
	time.Sleep(1500 * time.Millisecond)

	// BUG FIX 1: unregister AFTER trackText so the final HTML scan is captured.
	unregister := captureHub.register(jobID, req.URL, trackURL)

	resultCh := jobHub.enqueue(&extensionJob{
		JobID:        jobID,
		URL:          req.URL,
		WaitMs:       req.WaitMs,
		LocalStorage: req.LocalStorage,
		Actions:      req.Actions,
		CloseTab:     true,
	})

	var result extensionJobResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		unregister()
		return "", dedupe(m3u8URLs), dedupe(allURLs), ctx.Err()
	}

	// Scan the returned HTML for embedded URLs before unregistering.
	trackText(result.Content, req.URL)
	unregister() // FIX: called explicitly after trackText, not via defer

	mu.Lock()
	m3u8s := dedupe(append([]string(nil), m3u8URLs...))
	all := dedupe(append([]string(nil), allURLs...))
	mu.Unlock()

	if result.Error != "" {
		return result.Content, m3u8s, all, errors.New(result.Error)
	}
	return result.Content, m3u8s, all, nil
}

func scrapeCDP(ctx, allocCtx context.Context, req TaskRequest, captureHub *extensionCaptureHub, emit func(StreamEvent)) (string, []string, []string, error) {
	tabCtx, tabCancel := chromedp.NewContext(allocCtx)
	defer tabCancel()
	tabCtx, deadlineCancel := context.WithTimeout(tabCtx, 110*time.Second)
	defer deadlineCancel()

	var (
		m3u8URLs []string
		allURLs  []string
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
		ch := bodyScanner.listen(cctx, trackURL, trackText)
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

	mainHandler := bodyScanner.listen(tabCtx, trackURL, trackText)
	chromedp.ListenTarget(tabCtx, func(ev interface{}) {
		mainHandler(ev)
		if e, ok := ev.(*target.EventAttachedToTarget); ok {
			t := string(e.TargetInfo.Type)
			if t == "iframe" || t == "page" || t == "service_worker" || t == "worker" {
				go attachChild(tabCtx, e.TargetInfo.TargetID)
			}
		}
	})

	actions := []chromedp.Action{
		target.SetAutoAttach(true, false).WithFlatten(true),
		network.Enable().WithMaxTotalBufferSize(100 * 1024 * 1024).WithMaxResourceBufferSize(maxResponseBodyScanBytes),
		chromedp.ActionFunc(func(ctx context.Context) error {
			_, err := page.AddScriptToEvaluateOnNewDocument(stealthScript).Do(ctx)
			return err
		}),
		chromedp.Navigate(req.URL),
	}
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
	mu.Unlock()

	if runErr != nil {
		return "", m3u8s, all, fmt.Errorf("chromedp: %w", runErr)
	}
	return htmlContent, m3u8s, all, nil
}

// ─── Response body scanner ────────────────────────────────────────────────────

type responseMeta struct {
	url               string
	resourceType      network.ResourceType
	mime              string
	encodedDataLength float64
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

func (s *responseBodyScanner) listen(ctx context.Context, trackURL func(string), trackText func(string, string)) func(interface{}) {
	return func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventRequestWillBeSent:
			if e.Request != nil {
				trackURL(e.Request.URL)
				s.remember(e.RequestID, responseMeta{url: e.Request.URL, resourceType: e.Type})
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
			s.remember(e.RequestID, responseMeta{url: e.Response.URL, resourceType: e.Type, mime: e.Response.MimeType})
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
	for {
		jobs := scanner.drain()
		if len(jobs) == 0 || time.Now().After(deadline) {
			return
		}
		for _, job := range jobs {
			if time.Now().After(deadline) {
				return
			}
			bctx, cancel := context.WithTimeout(job.ctx, time.Until(deadline))
			var body []byte
			err := chromedp.Run(bctx, chromedp.ActionFunc(func(ctx context.Context) error {
				var e error
				body, e = network.GetResponseBody(job.requestID).Do(ctx)
				return e
			}))
			cancel()
			if err == nil && len(body) > 0 {
				trackText(string(body), job.baseURL)
			}
		}
	}
}

func shouldInspectResponseBody(meta responseMeta) bool {
	if meta.url != "" && isMediaCandidateURL(meta.url) {
		return true
	}
	if meta.encodedDataLength > maxResponseBodyScanBytes {
		return false
	}
	mime := strings.ToLower(meta.mime)
	if strings.ContainsAny(mime, "json,javascript,mpegurl,dash+xml,text,html,xml") {
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

// ─── Header probing ───────────────────────────────────────────────────────────

// BUG FIX 3: use a real browser UA so CDNs don't 403 us.
const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"

func collectM3U8Headers(ctx context.Context, mediaURLs []string) []CapturedURLHeader {
	unique := dedupe(mediaURLs)
	filtered := make([]string, 0, maxHeaderProbes)
	for _, u := range unique {
		if isM3U8URL(u) {
			filtered = append(filtered, u)
			if len(filtered) >= maxHeaderProbes {
				break
			}
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	client := &http.Client{Timeout: headerProbeTimeout}
	out := make([]CapturedURLHeader, 0, len(filtered))
	for _, u := range filtered {
		entry := CapturedURLHeader{URL: u}
		status, method, headers, err := probeHeaders(ctx, client, u)
		if err != nil {
			entry.Error = err.Error()
		} else {
			entry.Status = status
			entry.Method = method
			entry.Headers = headers
		}
		out = append(out, entry)
	}
	return out
}

func probeHeaders(ctx context.Context, client *http.Client, rawURL string) (int, string, map[string]string, error) {
	do := func(method string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", browserUA)
		return client.Do(req)
	}

	if resp, err := do(http.MethodHead); err == nil && resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotImplemented {
		_ = resp.Body.Close()
		return resp.StatusCode, http.MethodHead, flattenHeaders(resp.Header), nil
	}

	resp, err := do(http.MethodGet)
	if err != nil {
		return 0, http.MethodGet, nil, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, http.MethodGet, flattenHeaders(resp.Header), nil
}

func flattenHeaders(in http.Header) map[string]string {
	out := make(map[string]string, len(in))
	for k, vs := range in {
		out[k] = strings.Join(vs, ", ")
	}
	return out
}

// ─── URL utilities ────────────────────────────────────────────────────────────

func normalizeCapturedURL(raw, baseURL string) string {
	raw = strings.TrimSpace(unescapeURLText(raw))
	if raw == "" {
		return ""
	}
	raw = strings.Trim(raw, " \t\r\n\"'<>`),;")
	raw = strings.TrimRight(raw, ".")
	if raw == "" || raw == "about:blank" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		if base, err := url.Parse(baseURL); err == nil && base.Scheme != "" {
			raw = base.Scheme + ":" + raw
		} else {
			raw = "https:" + raw
		}
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if parsed.Scheme == "" && baseURL != "" {
		if base, err := url.Parse(baseURL); err == nil {
			parsed = base.ResolveReference(parsed)
		}
	}
	if parsed.Scheme == "http" || parsed.Scheme == "https" || parsed.Scheme == "ws" || parsed.Scheme == "wss" {
		return parsed.String()
	}
	if parsed.Scheme == "" {
		return parsed.String()
	}
	return ""
}

func sameCapturedURL(a, b string) bool {
	pa, ea := url.Parse(a)
	pb, eb := url.Parse(b)
	if ea != nil || eb != nil {
		return a == b
	}
	pa.Fragment = ""
	pb.Fragment = ""
	return strings.EqualFold(pa.Scheme, pb.Scheme) &&
		strings.EqualFold(pa.Host, pb.Host) &&
		pa.EscapedPath() == pb.EscapedPath() &&
		pa.RawQuery == pb.RawQuery
}

func unescapeURLText(text string) string {
	return strings.NewReplacer(
		`\/`, "/", `\u0026`, "&", `\u002F`, "/",
		`\u003d`, "=", `\u003D`, "=", `\u003f`, "?",
		`\u003F`, "?", `\u003a`, ":", `\u003A`, ":",
		"&amp;", "&",
	).Replace(text)
}

func extractCandidateURLs(text, baseURL string) []string {
	if text == "" {
		return nil
	}
	text = unescapeURLText(text)
	matches := mediaURLPattern.FindAllString(text, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		if u := normalizeCapturedURL(m, baseURL); u != "" {
			out = append(out, u)
		}
	}
	return dedupe(out)
}

func isMediaCandidateURL(raw string) bool {
	u := strings.ToLower(unescapeURLText(raw))
	return strings.Contains(u, "m3u8") || strings.Contains(u, ".mpd") ||
		strings.Contains(u, "/playlist") || strings.Contains(u, "/master") ||
		strings.Contains(u, "manifest") || strings.Contains(u, ".mp4")
}

func isM3U8URL(raw string) bool {
	return strings.Contains(strings.ToLower(unescapeURLText(raw)), ".m3u8")
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// ─── Browser action builder ───────────────────────────────────────────────────

func buildBrowserAction(a BrowserAction) chromedp.Action {
	switch strings.ToLower(a.Type) {
	case "wait", "sleep":
		return chromedp.Sleep(time.Duration(a.WaitMs) * time.Millisecond)
	case "click":
		if a.Selector != "" {
			return chromedp.Click(a.Selector, chromedp.ByQuery)
		}
		return chromedp.MouseClickXY(a.X, a.Y)
	case "double_click":
		if a.Selector != "" {
			return chromedp.DoubleClick(a.Selector, chromedp.ByQuery)
		}
		return chromedp.MouseClickXY(a.X, a.Y, chromedp.ClickCount(2))
	case "evaluate", "eval":
		return chromedp.Evaluate(a.Script, nil)
	case "scroll":
		return chromedp.Evaluate(fmt.Sprintf("window.scrollBy(%f,%f)", a.DeltaX, a.DeltaY), nil)
	case "send_keys", "type":
		return chromedp.SendKeys(a.Selector, a.Text, chromedp.ByQuery)
	case "wait_ready":
		return chromedp.WaitReady(a.Selector, chromedp.ByQuery)
	default:
		return chromedp.ActionFunc(func(context.Context) error {
			return fmt.Errorf("unsupported action type %q", a.Type)
		})
	}
}

// ─── Stealth script ───────────────────────────────────────────────────────────

const stealthScript = `
(function () {
	Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
	window.chrome = { runtime: {} };
	Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
	Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
	const origQuery = window.navigator.permissions.query;
	window.navigator.permissions.query = p =>
		p.name === 'notifications'
			? Promise.resolve({ state: Notification.permission })
			: origQuery(p);
})();
`

// ─── Watchdog, browser finder, validation, helpers ────────────────────────────

func startParentWatchdog(shutdown func(string)) {
	ppid := os.Getppid()
	log.Printf("Watchdog: monitoring parent PID %d", ppid)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			if isParentDead(ppid) {
				shutdown("parent process is gone")
				return
			}
		}
	}()
}

func findBrowser() (path, name string) {
	type candidate struct {
		name  string
		paths []string
	}
	var browsers []candidate
	switch runtime.GOOS {
	case "windows":
		browsers = []candidate{
			{"Microsoft Edge", []string{
				os.ExpandEnv(`${ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe`),
				os.ExpandEnv(`${ProgramFiles}\Microsoft\Edge\Application\msedge.exe`),
			}},
			{"Brave", []string{
				os.ExpandEnv(`${ProgramFiles}\BraveSoftware\Brave-Browser\Application\brave.exe`),
				os.ExpandEnv(`${LocalAppData}\BraveSoftware\Brave-Browser\Application\brave.exe`),
			}},
			{"Google Chrome", []string{
				os.ExpandEnv(`${ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe`),
				os.ExpandEnv(`${ProgramFiles}\Google\Chrome\Application\chrome.exe`),
				os.ExpandEnv(`${LocalAppData}\Google\Chrome\Application\chrome.exe`),
			}},
		}
	case "darwin":
		browsers = []candidate{
			{"Microsoft Edge", []string{"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"}},
			{"Brave", []string{"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"}},
			{"Google Chrome", []string{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}},
		}
	default:
		browsers = []candidate{
			{"Microsoft Edge", []string{"/usr/bin/microsoft-edge", "/usr/bin/microsoft-edge-stable"}},
			{"Brave", []string{"/usr/bin/brave-browser", "/usr/bin/brave"}},
			{"Google Chrome", []string{"/usr/bin/google-chrome", "/usr/bin/google-chrome-stable", "/usr/bin/chromium", "/usr/bin/chromium-browser"}},
		}
	}
	for _, b := range browsers {
		for _, p := range b.paths {
			if _, err := os.Stat(p); err == nil {
				return p, b.name
			}
		}
	}
	return "", ""
}

func validateRequest(req TaskRequest) error {
	if req.URL == "" {
		return errors.New("url is required")
	}
	parsed, err := url.ParseRequestURI(req.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("url must be a valid http/https URL")
	}
	if req.WaitMs < 0 || req.WaitMs > 15_000 {
		return errors.New("wait_ms must be between 0 and 15000")
	}
	for i, a := range req.Actions {
		if err := validateBrowserAction(a); err != nil {
			return fmt.Errorf("actions[%d]: %w", i, err)
		}
	}
	return nil
}

func validateBrowserAction(a BrowserAction) error {
	switch strings.ToLower(a.Type) {
	case "wait", "sleep":
		if a.WaitMs < 0 || a.WaitMs > 30_000 {
			return errors.New("wait_ms must be 0–30000")
		}
	case "click", "double_click":
		if a.Selector == "" && a.X == 0 && a.Y == 0 {
			return errors.New("click requires selector or x/y")
		}
	case "evaluate", "eval":
		if strings.TrimSpace(a.Script) == "" {
			return errors.New("evaluate requires script")
		}
	case "scroll":
		if a.DeltaX == 0 && a.DeltaY == 0 {
			return errors.New("scroll requires delta_x or delta_y")
		}
	case "send_keys", "type":
		if a.Selector == "" {
			return errors.New("send_keys requires selector")
		}
	case "wait_ready":
		if a.Selector == "" {
			return errors.New("wait_ready requires selector")
		}
	default:
		return fmt.Errorf("unsupported action type %q", a.Type)
	}
	return nil
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

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func safeFilePart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "job"
	}
	if b.Len() > 16 {
		return b.String()[:16]
	}
	return b.String()
}