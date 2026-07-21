package main

import (
	"time"
)

const (
	maxHeaderProbes          = 8
	headerProbeTimeout       = 12 * time.Second
	maxResponseBodyScanBytes = 2 * 1024 * 1024
)

type Config struct {
	Port            string
	Headless        bool
	MaxTabs         int
	Browser         string
	Timeout         time.Duration
	DisableWatchdog bool
}

type TaskRequest struct {
	URL              string            `json:"url"`
	WaitMs           int               `json:"wait_ms"`
	LocalStorage     map[string]string `json:"local_storage,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	Actions          []BrowserAction   `json:"actions,omitempty"`
	Debug            bool              `json:"debug,omitempty"`
	IncludeHeaders   bool              `json:"include_headers,omitempty"`
	Stream           bool              `json:"stream,omitempty"`
	UserAgent        string            `json:"user_agent,omitempty"`
	IsHLSScrape      bool              `json:"is_hls_scrape,omitempty"`
	IsManifestScrape bool              `json:"is_manifest_scrape,omitempty"` // internal: set by /scrape/ajaximg handler
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
	URL             string            `json:"url"`
	Method          string            `json:"method,omitempty"`
	Status          int               `json:"status,omitempty"`
	RequestHeaders  map[string]string `json:"request_headers,omitempty"`
	ResponseHeaders map[string]string `json:"response_headers,omitempty"`
	Error           string            `json:"error,omitempty"`
}

type StreamEvent struct {
	Type     string        `json:"type"`
	URL      string        `json:"url,omitempty"`
	IsMedia  bool          `json:"is_media,omitempty"`
	Response *TaskResponse `json:"response,omitempty"`
	Error    string        `json:"error,omitempty"`
}

type m3u8Capture struct {
	URL             string            `json:"url"`
	RequestHeaders  map[string]string `json:"request_headers,omitempty"`
	ResponseHeaders map[string]string `json:"response_headers,omitempty"`
	Status          int               `json:"status,omitempty"`
}

// ajaxCapture holds a single XHR/fetch response body captured by the extension
// content script during a manifest scrape. Only populated when is_manifest_scrape
// is true on the job; bodies are capped at maxResponseBodyScanBytes.
type ajaxCapture struct {
	URL         string `json:"url"`
	ContentType string `json:"content_type,omitempty"`
	Body        string `json:"body,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
}

type HLSScrapeRequest struct {
	URL          string            `json:"url"`
	WaitMs       int               `json:"wait_ms"`
	LocalStorage map[string]string `json:"local_storage,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// ManifestScrapeRequest is the request body for POST /scrape/ajaximg.
type ManifestScrapeRequest struct {
	URL           string            `json:"url"`                        // required
	WaitMS        int               `json:"wait_ms,omitempty"`          // max 15000ms, same semantics as other endpoints
	Headers       map[string]string `json:"headers,omitempty"`          // request headers to apply
	LocalStorage  map[string]string `json:"local_storage,omitempty"`    // local storage to inject
	Actions       []BrowserAction   `json:"actions,omitempty"`          // custom actions (click/scroll/wait/eval etc.)
	URLPatterns   []string          `json:"url_patterns,omitempty"`     // optional regex allowlist for which response URLs to consider
	ImageExtRegex string            `json:"image_ext_regex,omitempty"` // override default image-extension regex
	MinMatchCount int               `json:"min_match_count,omitempty"` // qualify-as-manifest threshold; default 3
	AccumulateAll *bool             `json:"accumulate_all,omitempty"`  // default true — accumulate all qualifying responses
	Debug         bool              `json:"debug,omitempty"`            // include candidate list in response
}

// ManifestScrapeResponse is the response body for POST /scrape/ajaximg.
type ManifestScrapeResponse struct {
	Images       []string           `json:"images"`                  // ordered, deduped image URLs
	ManifestURLs []string           `json:"manifest_urls"`           // response URLs identified as manifests
	Headers      map[string]string  `json:"headers,omitempty"`       // headers needed to fetch images (Referer/UA/Origin)
	FallbackUsed bool               `json:"fallback_used"`           // true if no manifest scored above threshold
	Candidates   []ManifestCandidate `json:"candidates,omitempty"`   // only set when debug:true
	Error        string             `json:"error,omitempty"`
}

// ManifestCandidate is one scored candidate response for debug output.
type ManifestCandidate struct {
	URL         string `json:"url"`
	ContentType string `json:"content_type"`
	MatchCount  int    `json:"match_count"`
	Format      string `json:"format"` // "json" | "xml" | "unknown"
}

type HLSQuality struct {
	Quality string            `json:"quality"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

type HLSScrapeResponse struct {
	PlayableURL string            `json:"playable_url"`
	Qualities   []HLSQuality      `json:"qualities,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type scrapeResult struct {
	content  string
	m3u8s    []string
	allURLs  []string
	captures []m3u8Capture
	err      error
}
