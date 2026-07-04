package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var nextExtensionJobID atomic.Uint64

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
	Headers      map[string]string `json:"headers,omitempty"`
	Actions      []BrowserAction   `json:"actions,omitempty"`
	CloseTab     bool              `json:"close_tab"`
	Stream       bool              `json:"stream"`
	Debug        bool              `json:"debug"`
	IsHLSScrape  bool              `json:"is_hls_scrape,omitempty"`
}

type extensionJobResult struct {
	JobID    string        `json:"job_id"`
	Content  string        `json:"content"`
	Error    string        `json:"error,omitempty"`
	M3u8URLs []string      `json:"m3u8_urls,omitempty"`
	AllURLs  []string      `json:"all_urls,omitempty"`
	Captures []m3u8Capture `json:"captures,omitempty"`
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

func (h *extensionJobHub) cancel(jobID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.results, jobID)
	for i, job := range h.queue {
		if job.JobID == jobID {
			h.queue = append(h.queue[:i], h.queue[i+1:]...)
			break
		}
	}
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

type extensionBrowserProcess struct {
	cmd        *exec.Cmd
	profileDir string
}

const contentScript = `(function() {
  // 1. Popup Blocker (Always active)
  try {
    if (window.open) {
      window.open = function() {
        console.log("Blocked window.open call");
        return null;
      };
    }
  } catch (e) {}

  document.addEventListener('click', function(e) {
    let target = e.target;
    while (target && target.tagName !== 'A') {
      target = target.parentNode;
    }
    if (target && target.getAttribute('target') === '_blank') {
      console.log("Blocked target=_blank link click");
      e.preventDefault();
    }
  }, true);

  // 2. Query background script if HLS scraper / solver should run
  chrome.runtime.sendMessage({ type: "GET_SCRAPER_STATE" }, (response) => {
    if (response && response.active) {
      startSolver();
    }
  });

  function startSolver() {
    console.log("HLS Scraper solver active in frame:", window.location.href);

    const checkTurnstile = () => {
      const stage = document.getElementById('challenge-stage') || 
                    document.querySelector('.ctp-checkbox-label') || 
                    document.querySelector('input[type="checkbox"]') ||
                    document.querySelector('.mark');
      if (stage) {
        const cb = stage.querySelector('input') || stage.querySelector('.mark') || stage;
        if (cb) {
          console.log("Turnstile checkbox detected. Click triggered.");
          cb.click();
          const rect = cb.getBoundingClientRect();
          const x = rect.left + rect.width / 2;
          const y = rect.top + rect.height / 2;
          const opts = { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y };
          cb.dispatchEvent(new MouseEvent('mousedown', opts));
          cb.dispatchEvent(new MouseEvent('mouseup', opts));
          cb.dispatchEvent(new MouseEvent('click', opts));
        }
      }
    };

    const checkVideoPlay = () => {
      const videos = document.querySelectorAll('video');
      videos.forEach(v => {
        if (v.paused) {
          console.log("Found paused HTML5 video, playing...");
          v.play().catch(e => {});
          const rect = v.getBoundingClientRect();
          const x = rect.left + rect.width / 2;
          const y = rect.top + rect.height / 2;
          const opts = { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y };
          v.dispatchEvent(new MouseEvent('mousedown', opts));
          v.dispatchEvent(new MouseEvent('mouseup', opts));
          v.dispatchEvent(new MouseEvent('click', opts));
        }
      });

      const playSelectors = [
        '.jw-display-icon-container',
        '.vjs-big-play-button',
        '.plyr__control--overlaid',
        'button[aria-label="Play"]',
        'button[class*="play"]',
        'div[class*="play"]',
        'span[class*="play"]',
        '[id*="play"]',
        '.video-player',
        'div[class*="player"]',
        'video-js'
      ];
      playSelectors.forEach(sel => {
        const elements = document.querySelectorAll(sel);
        elements.forEach(el => {
          const rect = el.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            console.log("Found play element:", sel, "- clicking.");
            el.click();
            const opts = { bubbles: true, cancelable: true, view: window };
            el.dispatchEvent(new MouseEvent('mousedown', opts));
            el.dispatchEvent(new MouseEvent('mouseup', opts));
            el.dispatchEvent(new MouseEvent('click', opts));
          }
        });
      });

      const iframes = document.querySelectorAll('iframe');
      iframes.forEach(iframe => {
        try {
          const innerDoc = iframe.contentDocument || iframe.contentWindow.document;
          if (innerDoc) {
            const innerVideos = innerDoc.querySelectorAll('video');
            innerVideos.forEach(v => {
              if (v.paused) {
                v.play().catch(()=>{});
                v.click();
              }
            });
          }
        } catch (e) {
          const rect = iframe.getBoundingClientRect();
          if (rect.width > 0 && rect.height > 0) {
            console.log("Found cross-origin iframe, clicking center.");
            const x = rect.left + rect.width / 2;
            const y = rect.top + rect.height / 2;
            const opts = { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y };
            iframe.dispatchEvent(new MouseEvent('mousedown', opts));
            iframe.dispatchEvent(new MouseEvent('mouseup', opts));
            iframe.dispatchEvent(new MouseEvent('click', opts));
          }
        }
      });
    };

    let checksCount = 0;
    const timer = setInterval(() => {
      checkTurnstile();
      checkVideoPlay();
      checksCount++;
      if (checksCount > 30) {
        clearInterval(timer);
      }
    }, 1500);

    checkTurnstile();
    checkVideoPlay();
  }
})();`

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
  "permissions": ["webRequest", "declarativeNetRequest", "tabs", "scripting"],
  "host_permissions": ["<all_urls>"],
  "background": { "service_worker": "background.js" },
  "content_scripts": [
    {
      "matches": ["<all_urls>"],
      "js": ["content.js"],
      "run_at": "document_start",
      "all_frames": true
    }
  ]
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

// FIX #4: capture request headers for media URLs
const pendingHeaders = new Map(); // requestId -> headers

chrome.webRequest.onBeforeSendHeaders.addListener((d) => {
  if (!d.url || d.url.startsWith(API_BASE)) return;
  const lower = d.url.toLowerCase();
  const isMedia = lower.includes('.m3u8') || lower.includes('playlist') ||
                  lower.includes('manifest') || lower.includes('.mpd') ||
                  lower.includes('.mp4');
  if (!isMedia) return;
  const hdrs = {};
  (d.requestHeaders || []).forEach(h => { hdrs[h.name] = h.value; });
  pendingHeaders.set(d.requestId, hdrs);
}, { urls: ["<all_urls>"] }, ["requestHeaders", "extraHeaders"]);

let currentJob = null;
let mainTabId = null;
const capturedUrls = [];
const capturedMedia = [];
const capturedHeaders = [];

function isMediaURL(url) {
  const lower = url.toLowerCase();
  return lower.includes('.m3u8') || lower.includes('playlist') ||
         lower.includes('manifest') || lower.includes('.mpd') ||
         lower.includes('.mp4');
}

chrome.webRequest.onSendHeaders.addListener((d) => {
  if (!d.url || d.url.startsWith(API_BASE)) return;
  const jobId = tabJobs.get(d.tabId);
  if (!jobId || !currentJob) return;

  const isMedia = isMediaURL(d.url);
  const reqHeaders = pendingHeaders.get(d.requestId) || {};

  if (isMedia) {
    capturedMedia.push(d.url);
    capturedHeaders.push({
      url: d.url,
      status: 200,
      request_headers: reqHeaders
    });
  }
  if (currentJob.debug) {
    capturedUrls.push(d.url);
  }

  if (currentJob.stream) {
    post(CAPTURE, { job_id: jobId, url: d.url, tab_id: d.tabId, frame_id: d.frameId,
                    request_id: d.requestId, type: d.type, initiator: d.initiator||"",
                    request_headers: reqHeaders }).catch(()=>{});
  }
}, { urls: ["<all_urls>"] });

chrome.webRequest.onCompleted.addListener((d) => {
  pendingHeaders.delete(d.requestId);
}, { urls: ["<all_urls>"] });
chrome.webRequest.onErrorOccurred.addListener((d) => {
  pendingHeaders.delete(d.requestId);
}, { urls: ["<all_urls>"] });

chrome.tabs.onCreated.addListener((tab) => {
  if (currentJob && mainTabId && tab.id !== mainTabId) {
    console.log("Popup blocked:", tab.id);
    chrome.tabs.remove(tab.id).catch(() => {});
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "GET_SCRAPER_STATE") {
    sendResponse({ active: currentJob && currentJob.is_hls_scrape });
  }
  return true;
});

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
  currentJob = job;
  mainTabId = null;
  capturedUrls.length = 0;
  capturedMedia.length = 0;
  capturedHeaders.length = 0;
  let tab;
  try {
    tab = await chrome.tabs.create({ url: "about:blank", active: true });
    mainTabId = tab.id;
    tabJobs.set(tab.id, job.job_id);

    // Apply declarative rules for custom headers
    if (job.headers && Object.keys(job.headers).length > 0) {
      const requestHeaders = [];
      for (const [k, v] of Object.entries(job.headers)) {
        requestHeaders.push({
          header: k.toLowerCase() === "user-agent" ? "user-agent" : k,
          operation: "set",
          value: v
        });
      }
      await chrome.declarativeNetRequest.updateSessionRules({
        removeRuleIds: [1],
        addRules: [
          {
            id: 1,
            priority: 1,
            action: {
              type: "modifyHeaders",
              requestHeaders: requestHeaders
            },
            condition: {
              tabIds: [tab.id],
              resourceTypes: ["main_frame", "sub_frame"]
            }
          }
        ]
      });
    }

    await chrome.tabs.update(tab.id, { url: job.url });
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
    await post(RESULT, { job_id: job.job_id, content: content, m3u8_urls: capturedMedia, all_urls: capturedUrls, captures: capturedHeaders });
  } catch(e) {
    await post(RESULT, { job_id: job.job_id, content: "", error: e&&e.message?e.message:String(e), m3u8_urls: capturedMedia, all_urls: capturedUrls, captures: capturedHeaders }).catch(()=>{});
  } finally {
    currentJob = null;
    mainTabId = null;
    if (tab&&tab.id!==undefined) {
      tabJobs.delete(tab.id);
      if (job.close_tab!==false) chrome.tabs.remove(tab.id).catch(()=>{});
    }
    await chrome.declarativeNetRequest.updateSessionRules({
      removeRuleIds: [1]
    }).catch(()=>{});
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
	if err := os.WriteFile(filepath.Join(dir, "content.js"), []byte(contentScript), 0644); err != nil {
		return "", err
	}
	return dir, nil
}

func launchExtensionBrowser(browserPath, extensionDir, jobID, userAgent string) (*extensionBrowserProcess, error) {
	profileDir, err := os.MkdirTemp("", "shadoware-profile-"+safeFilePart(jobID)+"-")
	if err != nil {
		return nil, err
	}
	args := []string{
		"--user-data-dir=" + profileDir,
		"--headless=new",
		"--no-sandbox",
		"--disable-gpu",
		"--disable-dev-shm-usage",
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-blink-features=AutomationControlled",
		"--disable-extensions-except=" + extensionDir,
		"--load-extension=" + extensionDir,
		"--window-size=1365,768",
		"--mute-audio",
		"--autoplay-policy=no-user-gesture-required",
		"--enable-features=MediaSourceAPI,MSE",
		"--disable-background-networking",
		"--disable-background-timer-throttling",
		"--disable-backgrounding-occluded-windows",
		"--disable-breakpad",
		"--disable-client-side-phishing-detection",
		"--disable-default-apps",
		"--disable-hang-monitor",
		"--disable-ipc-flooding-protection",
		"--disable-prompt-on-repost",
		"--disable-renderer-backgrounding",
		"--disable-sync",
		"--metrics-recording-only",
		"--safebrowsing-disable-auto-update",
		"--password-store=basic",
		"--use-mock-keychain",
		"about:blank",
	}
	if userAgent != "" {
		args = append(args, "--user-agent="+userAgent)
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

func scrapeExtension(ctx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string, emit func(StreamEvent)) (string, []string, []string, []m3u8Capture, error) {
	var (
		m3u8URLs []string
		allURLs  []string
		captures []m3u8Capture
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

	unregister := captureHub.register(jobID, req.URL, trackURL)
	defer unregister()

	resultCh := jobHub.enqueue(&extensionJob{
		JobID:        jobID,
		URL:          req.URL,
		WaitMs:       req.WaitMs,
		LocalStorage: req.LocalStorage,
		Headers:      req.Headers,
		Actions:      req.Actions,
		CloseTab:     true,
		Stream:       emit != nil,
		Debug:        req.Debug || req.IncludeHeaders,
		IsHLSScrape:  req.IsHLSScrape,
	})

	browser, err := launchExtensionBrowser(browserPath, extensionDir, jobID, req.UserAgent)
	if err != nil {
		jobHub.cancel(jobID)
		return "", nil, nil, nil, err
	}
	defer closeExtensionBrowser(browser)

	var result extensionJobResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		return "", dedupe(m3u8URLs), dedupe(allURLs), nil, ctx.Err()
	}

	trackText(result.Content, req.URL)

	mu.Lock()
	if len(result.M3u8URLs) > 0 {
		m3u8URLs = append(m3u8URLs, result.M3u8URLs...)
	}
	if len(result.AllURLs) > 0 {
		allURLs = append(allURLs, result.AllURLs...)
	}
	if len(result.Captures) > 0 {
		captures = append(captures, result.Captures...)
	}

	m3u8s := dedupe(append([]string(nil), m3u8URLs...))
	all := dedupe(append([]string(nil), allURLs...))
	caps := append([]m3u8Capture(nil), captures...)
	mu.Unlock()

	if result.Error != "" {
		return result.Content, m3u8s, all, caps, errors.New(result.Error)
	}
	return result.Content, m3u8s, all, caps, nil
}
