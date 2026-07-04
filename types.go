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
	Port     string
	Headless bool
	MaxTabs  int
	Browser  string
	Timeout  time.Duration
	Mode     string
}

type TaskRequest struct {
	URL            string            `json:"url"`
	Mode           string            `json:"mode,omitempty"`
	WaitMs         int               `json:"wait_ms"`
	LocalStorage   map[string]string `json:"local_storage,omitempty"`
	Actions        []BrowserAction   `json:"actions,omitempty"`
	Debug          bool              `json:"debug,omitempty"`
	IncludeHeaders bool              `json:"include_headers,omitempty"`
	Stream         bool              `json:"stream,omitempty"`
	UserAgent      string            `json:"user_agent,omitempty"`
	IsHLSScrape    bool              `json:"is_hls_scrape,omitempty"`
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
	RequestHeaders  map[string]string `json:"request_headers,omitempty"`  // headers sent by the browser
	ResponseHeaders map[string]string `json:"response_headers,omitempty"` // headers returned by the server
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

type scrapeResult struct {
	content  string
	m3u8s    []string
	allURLs  []string
	captures []m3u8Capture
	err      error
}
