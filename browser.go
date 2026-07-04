package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
)

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

type Tab struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	ErrMsg    string    `json:"error,omitempty"`

	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	m3u8s    []string
	captures []m3u8Capture
	urls     []string
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

	scanner := newResponseBodyScanner()

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
	trackCapture := func(c m3u8Capture) {
		tab.mu.Lock()
		tab.captures = append(tab.captures, c)
		tab.mu.Unlock()
	}
	trackText := func(text, base string) {
		for _, u := range extractCandidateURLs(text, base) {
			trackURL(u)
		}
	}

	mainHandler := scanner.listen(tabCtx, trackURL, trackText, trackCapture)
	var attachChild func(context.Context, target.ID)
	attachChild = func(parent context.Context, tid target.ID) {
		cctx, _ := chromedp.NewContext(parent, chromedp.WithTargetID(tid))
		ch := scanner.listen(cctx, trackURL, trackText, trackCapture)
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

func (p *TabPool) Snapshot(id string) (string, []string, []string, []m3u8Capture, error) {
	t, ok := p.Get(id)
	if !ok {
		return "", nil, nil, nil, errors.New("tab not found")
	}
	var html string
	if err := chromedp.Run(t.ctx, chromedp.OuterHTML("html", &html)); err != nil {
		return "", nil, nil, nil, err
	}
	t.mu.Lock()
	m3u8s := dedupe(append([]string(nil), t.m3u8s...))
	urls := dedupe(append([]string(nil), t.urls...))
	caps := append([]m3u8Capture(nil), t.captures...)
	t.mu.Unlock()
	return html, m3u8s, urls, caps, nil
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
	t.captures = nil
	t.mu.Unlock()
	return true
}

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
	return &BrowserManager{
		allocCtx:    allocCtx,
		allocCancel: allocCancel,
		opts:        opts,
		browserPath: browserPath,
		browserName: browserName,
		startedAt:   time.Now(),
	}
}

func (bm *BrowserManager) AllocCtx() context.Context {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.allocCtx
}

func (bm *BrowserManager) Restart() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
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
		chromedp.Flag("enable-features", "MediaSourceAPI,MSE,BackForwardCache"),
		chromedp.Flag("autoplay-policy", "no-user-gesture-required"),
	)
	if cfg.Headless {
		opts = append(opts, chromedp.Flag("headless", true))
	} else {
		opts = append(opts, chromedp.Flag("headless", false))
	}
	return opts
}

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
