package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var nextExtensionJobID atomic.Uint64

type extensionCaptureEvent struct {
	JobID          string            `json:"job_id,omitempty"`
	URL            string            `json:"url"`
	TabID          int               `json:"tab_id"`
	FrameID        int               `json:"frame_id"`
	RequestID      string            `json:"request_id"`
	Type           string            `json:"type"`
	Initiator      string            `json:"initiator,omitempty"`
	RequestHeaders map[string]string `json:"request_headers,omitempty"`
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
	JobID             string            `json:"job_id"`
	URL               string            `json:"url"`
	WaitMs            int               `json:"wait_ms"`
	LocalStorage      map[string]string `json:"local_storage,omitempty"`
	Headers           map[string]string `json:"headers,omitempty"`
	Actions           []BrowserAction   `json:"actions,omitempty"`
	CloseTab          bool              `json:"close_tab"`
	Stream            bool              `json:"stream"`
	Debug             bool              `json:"debug"`
	IsHLSScrape       bool              `json:"is_hls_scrape,omitempty"`
	IsManifestScrape  bool              `json:"is_manifest_scrape,omitempty"`
}

type extensionJobResult struct {
	JobID        string         `json:"job_id"`
	Content      string         `json:"content"`
	Error        string         `json:"error,omitempty"`
	M3u8URLs     []string       `json:"m3u8_urls,omitempty"`
	AllURLs      []string       `json:"all_urls,omitempty"`
	Captures     []m3u8Capture  `json:"captures,omitempty"`
	AjaxCaptures []ajaxCapture  `json:"ajax_captures,omitempty"`
}

type extensionJobHub struct {
	mu      sync.Mutex
	queue   []*extensionJob
	head    int
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
	if h.head >= len(h.queue) {
		h.queue = h.queue[:0]
		h.head = 0
		return nil, false
	}
	job := h.queue[h.head]
	h.head++
	if h.head > 64 && h.head > len(h.queue)/2 {
		h.queue = append(h.queue[:0], h.queue[h.head:]...)
		h.head = 0
	}
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
	for i := h.head; i < len(h.queue); i++ {
		if h.queue[i].JobID == jobID {
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

  // 2. Query background script to determine which scrapers should run.
  chrome.runtime.sendMessage({ type: "GET_SCRAPER_STATE" }, (response) => {
    if (response && response.active) {
      startSolver();
    }
    if (response && response.isManifestScrape) {
      startManifestCapture();
    }
  });

  // ─── HLS / Turnstile solver ────────────────────────────────────────────────
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

  // ─── Manifest (AJAX image-list) capture ───────────────────────────────────
  // Listens for CustomEvents fired by content_main.js (which runs in the MAIN
  // world and patches window.fetch / XMLHttpRequest directly on the page's
  // globals). This isolated-world script forwards captured bodies to the
  // background service worker via chrome.runtime.sendMessage, which is not
  // available in the MAIN world. Activation is gated on isManifestScrape so
  // it is a no-op for HLS/execute tabs.
  function startManifestCapture() {
    console.log("Manifest capture listener active in frame:", window.location.href);
    window.addEventListener('__wmu_ajax_capture', function(e) {
      if (!e || !e.detail) return;
      chrome.runtime.sendMessage({
        type: "AJAX_RESPONSE_BODY",
        url: e.detail.url,
        content_type: e.detail.content_type,
        body: e.detail.body,
        truncated: e.detail.truncated
      }).catch(() => {});
    });
  }
})();`

// contentMainScript runs in the MAIN world (the page's own JavaScript context)
// so it can patch the real window.fetch and XMLHttpRequest that page code uses.
// It fires CustomEvents (not chrome.runtime.sendMessage, which is unavailable in
// the MAIN world) so the isolated-world content.js can receive and forward them.
// This script is always injected but only sends events; the isolated-world script
// gates forwarding based on whether the tab is a manifest-scrape job.
const contentMainScript = `(function() {
  const BODY_CAP = 2 * 1024 * 1024; // 2 MB cap — must match maxResponseBodyScanBytes on the Go side
  const EV = '__wmu_ajax_capture';

  function looksLikeData(ct) {
    if (!ct) return false;
    const lower = ct.toLowerCase();
    return lower.includes('json') || lower.includes('xml') ||
           lower.includes('text/plain') || lower.includes('text/html') ||
           lower.includes('application/octet-stream') ||
           lower.includes('application/x-www-form-urlencoded');
  }

  function dispatch(url, contentType, text) {
    let body = text;
    let truncated = false;
    if (text && text.length > BODY_CAP) {
      body = text.slice(0, BODY_CAP);
      truncated = true;
    }
    window.dispatchEvent(new CustomEvent(EV, {
      detail: { url: url, content_type: contentType || '', body: body, truncated: truncated }
    }));
  }

  // ── Patch window.fetch ────────────────────────────────────────────────────
  const _fetch = window.fetch.bind(window);
  window.fetch = function(input, init) {
    const url = (typeof input === 'string') ? input
              : (input && input.url) ? input.url : String(input);
    return _fetch(input, init).then(function(response) {
      const clone = response.clone();
      const ct = clone.headers.get('content-type') || '';
      if (looksLikeData(ct)) {
        clone.text().then(function(text) { dispatch(url, ct, text); }).catch(function() {});
      }
      return response;
    });
  };

  // ── Patch XMLHttpRequest ──────────────────────────────────────────────────
  const _XHROpen = XMLHttpRequest.prototype.open;
  const _XHRSend = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function(method, url) {
    this._wmuURL = typeof url === 'string' ? url : String(url);
    return _XHROpen.apply(this, arguments);
  };

  XMLHttpRequest.prototype.send = function() {
    var self = this;
    this.addEventListener('load', function() {
      try {
        var ct = self.getResponseHeader('content-type') || '';
        if (looksLikeData(ct)) {
          var text = typeof self.responseText === 'string' ? self.responseText : '';
          dispatch(self._wmuURL || '', ct, text);
        }
      } catch(e) {}
    });
    return _XHRSend.apply(this, arguments);
  };
})();`



func ensureCaptureExtension(serverAddr string) (string, error) {
	portPart := strings.TrimPrefix(serverAddr, ":")
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("webmediautil-ext-%s", portPart))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	apiBase := "http://127.0.0.1" + serverAddr
	manifest := `{
  "manifest_version": 3,
  "name": "WebMediaUtil Capture",
  "version": "1.0.0",
  "permissions": ["webRequest", "declarativeNetRequest", "tabs", "scripting"],
  "host_permissions": ["<all_urls>"],
  "background": { "service_worker": "background.js" },
  "content_scripts": [
    {
      "matches": ["<all_urls>"],
      "js": ["content_main.js"],
      "run_at": "document_start",
      "all_frames": true,
      "world": "MAIN"
    },
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

const jobs = new Map();
const jobByJobId = new Map();
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function post(url, body) {
  await fetch(url, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify(body) });
}

function isMediaURL(url) {
  const lower = url.toLowerCase();
  return lower.includes('.m3u8') || lower.includes('playlist') ||
         lower.includes('manifest') || lower.includes('.mpd') ||
         lower.includes('.mp4');
}

const pendingHeaders = new Map();

chrome.webRequest.onBeforeSendHeaders.addListener((d) => {
  if (!d.url || d.url.startsWith(API_BASE)) return;
  const context = jobs.get(d.tabId);
  if (!context) return;

  const isMedia = isMediaURL(d.url);
  if (!isMedia) return;

  const hdrs = {};
  (d.requestHeaders || []).forEach(h => { hdrs[h.name] = h.value; });
  pendingHeaders.set(d.requestId, hdrs);
}, { urls: ["<all_urls>"] }, ["requestHeaders", "extraHeaders"]);

chrome.webRequest.onSendHeaders.addListener((d) => {
  if (!d.url || d.url.startsWith(API_BASE)) return;
  const context = jobs.get(d.tabId);
  if (!context) return;

  const isMedia = isMediaURL(d.url);
  const reqHeaders = pendingHeaders.get(d.requestId) || {};

  if (isMedia) {
    context.capturedMedia.push(d.url);
    context.capturedHeaders.push({
      url: d.url,
      status: 200,
      request_headers: reqHeaders
    });
  }
  if (context.debug) {
    context.capturedUrls.push(d.url);
  }

  if (context.stream) {
    post(CAPTURE, { job_id: context.jobId, url: d.url, tab_id: d.tabId, frame_id: d.frameId,
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
  if (tab.openerTabId && jobs.has(tab.openerTabId)) {
    console.log("Popup blocked:", tab.id, "opener:", tab.openerTabId);
    chrome.tabs.remove(tab.id).catch(() => {});
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "GET_SCRAPER_STATE") {
    if (sender.tab && sender.tab.id !== undefined) {
      const context = jobs.get(sender.tab.id);
      if (context) {
        sendResponse({
          active: !!context.isHLSScrape,
          isManifestScrape: !!context.isManifestScrape
        });
        return true;
      }
    }
    sendResponse({ active: false, isManifestScrape: false });
    return true;
  }

  if (message.type === "AJAX_RESPONSE_BODY") {
    // Received from the content script's fetch/XHR monkey-patch.
    // Append to capturedAjax for this tab so the final result POST includes it.
    if (sender.tab && sender.tab.id !== undefined) {
      const context = jobs.get(sender.tab.id);
      if (context && context.isManifestScrape) {
        context.capturedAjax.push({
          url: message.url || "",
          content_type: message.content_type || "",
          body: message.body || "",
          truncated: !!message.truncated
        });
      }
    }
    sendResponse({});
    return true;
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
  const jobId = job.job_id;
  const timeoutMs = (job.wait_ms || 0) + 35000;
  let tab;
  let timer;
  try {
    tab = await chrome.tabs.create({ url: "about:blank", active: false });
    const tabId = tab.id;
    const context = {
      jobId: jobId,
      tabId: tabId,
      url: job.url,
      debug: job.debug,
      stream: job.stream,
      isHLSScrape: job.is_hls_scrape,
      isManifestScrape: job.is_manifest_scrape,
      capturedUrls: [],
      capturedMedia: [],
      capturedHeaders: [],
      capturedAjax: [],
      pendingHeaders: new Map()
    };
    jobs.set(tabId, context);
    jobByJobId.set(jobId, context);

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
        removeRuleIds: [tabId],
        addRules: [
          {
            id: tabId,
            priority: 1,
            action: {
              type: "modifyHeaders",
              requestHeaders: requestHeaders
            },
            condition: {
              tabIds: [tabId]
            }
          }
        ]
      });
    }

    const timeoutPromise = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("Job execution timed out in browser extension")), timeoutMs);
    });

    const workPromise = (async () => {
      await chrome.tabs.update(tabId, { url: job.url });
      await waitComplete(tabId);
      if (job.local_storage && Object.keys(job.local_storage).length) {
        await chrome.scripting.executeScript({ target:{tabId:tabId}, args:[job.local_storage],
          func:(items)=>{ for(const[k,v] of Object.entries(items)) localStorage.setItem(k,v); } });
        await chrome.tabs.reload(tabId);
        await waitComplete(tabId);
      }
      for (const action of job.actions||[]) await runAction(tabId, action);
      if (job.wait_ms) await sleep(job.wait_ms);
      const frames = await chrome.scripting.executeScript({ target:{tabId:tabId,allFrames:true},
        func:()=>document.documentElement.outerHTML });
      return frames.map(f=>f.result||"").join("\n");
    })();

    const content = await Promise.race([workPromise, timeoutPromise]);
    await post(RESULT, {
      job_id: jobId,
      content: content,
      m3u8_urls: context.capturedMedia,
      all_urls: context.capturedUrls,
      captures: context.capturedHeaders,
      ajax_captures: context.capturedAjax
    });
  } catch(e) {
    const context = jobByJobId.get(jobId) || { capturedMedia: [], capturedUrls: [], capturedHeaders: [], capturedAjax: [] };
    await post(RESULT, {
      job_id: jobId,
      content: "",
      error: e&&e.message?e.message:String(e),
      m3u8_urls: context.capturedMedia,
      all_urls: context.capturedUrls,
      captures: context.capturedHeaders,
      ajax_captures: context.capturedAjax
    }).catch(()=>{});
  } finally {
    if (timer) clearTimeout(timer);
    if (tab && tab.id !== undefined) {
      const tabId = tab.id;
      jobs.delete(tabId);
      jobByJobId.delete(jobId);
      chrome.tabs.remove(tabId).catch(()=>{});
      await chrome.declarativeNetRequest.updateSessionRules({
        removeRuleIds: [tabId]
      }).catch(()=>{});
    }
  }
}

async function poll() {
  for(;;) {
    try {
      const r = await fetch(COMMAND);
      if (r.status===200) {
        const job = await r.json();
        runJob(job).catch(err => console.error("Job error:", err));
      } else {
        await sleep(250);
      }
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
	if err := os.WriteFile(filepath.Join(dir, "content_main.js"), []byte(contentMainScript), 0644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "content.js"), []byte(contentScript), 0644); err != nil {
		return "", err
	}
	return dir, nil
}

func scrapeExtension(ctx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub, emit func(StreamEvent)) (string, []string, []string, []m3u8Capture, error) {
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
		JobID:            jobID,
		URL:              req.URL,
		WaitMs:           req.WaitMs,
		LocalStorage:     req.LocalStorage,
		Headers:          req.Headers,
		Actions:          req.Actions,
		CloseTab:         true,
		Stream:           emit != nil,
		Debug:            req.Debug || req.IncludeHeaders,
		IsHLSScrape:      req.IsHLSScrape,
		IsManifestScrape: req.IsManifestScrape,
	})

	var result extensionJobResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		jobHub.cancel(jobID)
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

// scrapeManifestExtension is like scrapeExtension but also returns the AJAX
// response bodies captured by the extension's fetch/XHR monkey-patch.
// It is used exclusively by the /scrape/ajaximg handler.
func scrapeManifestExtension(ctx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub) (string, []string, []ajaxCapture, error) {
	var (
		allURLs []string
		mu      sync.Mutex
	)

	trackURL := func(u string) {
		u = normalizeCapturedURL(u, req.URL)
		if u == "" {
			return
		}
		mu.Lock()
		allURLs = append(allURLs, u)
		mu.Unlock()
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
		JobID:            jobID,
		URL:              req.URL,
		WaitMs:           req.WaitMs,
		LocalStorage:     req.LocalStorage,
		Headers:          req.Headers,
		Actions:          req.Actions,
		CloseTab:         true,
		Stream:           false,
		Debug:            true, // always collect all_urls for fallback mode
		IsManifestScrape: true,
	})

	var result extensionJobResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		jobHub.cancel(jobID)
		return "", dedupe(allURLs), nil, ctx.Err()
	}

	trackText(result.Content, req.URL)

	mu.Lock()
	if len(result.AllURLs) > 0 {
		allURLs = append(allURLs, result.AllURLs...)
	}
	all := dedupe(append([]string(nil), allURLs...))
	ajax := append([]ajaxCapture(nil), result.AjaxCaptures...)
	mu.Unlock()

	if result.Error != "" {
		return result.Content, all, ajax, errors.New(result.Error)
	}
	return result.Content, all, ajax, nil
}
