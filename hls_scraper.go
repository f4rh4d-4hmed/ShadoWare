package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"
)

const androidChromeUA = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"

type HLSScrapeRequest struct {
	URL          string            `json:"url"`
	WaitMs       int               `json:"wait_ms"`
	LocalStorage map[string]string `json:"local_storage,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
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

func handleScrapeHLS(bm *BrowserManager, sem *semaphore.Weighted, cfg Config, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string) http.HandlerFunc {
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

		if !sem.TryAcquire(1) {
			writeHLSScrapeError(w, "Server busy, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer sem.Release(1)

		taskReq := TaskRequest{
			URL:            req.URL,
			Mode:           "extension",
			WaitMs:         req.WaitMs,
			LocalStorage:   req.LocalStorage,
			Headers:        req.Headers,
			IncludeHeaders: true,
			UserAgent:      androidChromeUA,
			IsHLSScrape:    true,
		}

		_, m3u8s, _, captures, err := scrapeExtension(ctx, taskReq, captureHub, jobHub, browserPath, extensionDir, nil)
		if err != nil {
			writeHLSScrapeError(w, "Scraping failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if len(m3u8s) == 0 {
			writeHLSScrapeError(w, "No HLS streams found", http.StatusNotFound)
			return
		}

		log.Printf("[HLSScraper] Found %d candidate HLS URLs: %v", len(m3u8s), m3u8s)

		// Find the best playable URL
		var playableURL string
		var finalQualities []HLSQuality
		var finalHeaders map[string]string

		for _, candidate := range m3u8s {
			// Find captured headers for this URL
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
			// Inject browser User-Agent if not present
			if _, ok := reqHeaders["User-Agent"]; !ok {
				reqHeaders["User-Agent"] = androidChromeUA
			}

			// Validate and parse playlist
			log.Printf("[HLSScraper] Validating candidate: %s with headers: %v", candidate, reqHeaders)
			qualities, playErr := validateAndParseHLS(ctx, candidate, reqHeaders)
			if playErr == nil {
				log.Printf("[HLSScraper] Candidate is playable! Qualities: %d", len(qualities))
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}

func writeHLSScrapeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(HLSScrapeResponse{Error: msg})
}

var (
	bandwidthRegex  = regexp.MustCompile(`BANDWIDTH=(\d+)`)
	resolutionRegex = regexp.MustCompile(`RESOLUTION=(\d+x\d+)`)
)

func validateAndParseHLS(ctx context.Context, manifestURL string, headers map[string]string) ([]HLSQuality, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		if strings.EqualFold(k, "accept-encoding") {
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyScanBytes))
	if err != nil {
		return nil, err
	}

	bodyText := string(bodyBytes)
	if !strings.HasPrefix(bodyText, "#EXTM3U") {
		return nil, errors.New("invalid m3u8 playlist: missing #EXTM3U header")
	}

	var qualities []HLSQuality
	lines := strings.Split(bodyText, "\n")
	var currentInfo string

	baseURL, err := url.Parse(manifestURL)
	if err != nil {
		baseURL = nil
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-STREAM-INF:") {
			currentInfo = line
		} else if !strings.HasPrefix(line, "#") {
			if currentInfo != "" {
				resolution := "unknown"
				if match := resolutionRegex.FindStringSubmatch(currentInfo); len(match) > 1 {
					resolution = match[1]
				} else if match := bandwidthRegex.FindStringSubmatch(currentInfo); len(match) > 1 {
					bandwidth, _ := strconv.Atoi(match[1])
					resolution = fmt.Sprintf("%d Kbps", bandwidth/1000)
				}

				streamURL := line
				if baseURL != nil {
					if parsedStream, err := url.Parse(streamURL); err == nil {
						streamURL = baseURL.ResolveReference(parsedStream).String()
					}
				}

				qualities = append(qualities, HLSQuality{
					Quality: resolution,
					URL:     streamURL,
					Headers: headers,
				})
				currentInfo = ""
			}
		}
	}

	return qualities, nil
}
