package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

var mediaURLPattern = regexp.MustCompile(`(?i)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*(?:\.m3u8|m3u8|\.mpd|/playlist|/master|manifest)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*`)

var (
	bandwidthRegex  = regexp.MustCompile(`BANDWIDTH=(\d+)`)
	resolutionRegex = regexp.MustCompile(`RESOLUTION=(\d+x\d+)`)
)

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
			entry.ResponseHeaders = headers
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
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func startParentWatchdog(shutdown func(string)) {
	ppid := os.Getppid()
	if ppid <= 4 {
		log.Printf("Watchdog disabled: parent PID %d is system/init process", ppid)
		return
	}
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
				quality := "unknown"
				if resMatch := resolutionRegex.FindStringSubmatch(currentInfo); len(resMatch) > 1 {
					quality = resMatch[1]
					if bwMatch := bandwidthRegex.FindStringSubmatch(currentInfo); len(bwMatch) > 1 {
						bandwidth, _ := strconv.Atoi(bwMatch[1])
						quality = fmt.Sprintf("%s @ %d Kbps", quality, bandwidth/1000)
					}
				} else if bwMatch := bandwidthRegex.FindStringSubmatch(currentInfo); len(bwMatch) > 1 {
					bandwidth, _ := strconv.Atoi(bwMatch[1])
					quality = fmt.Sprintf("%d Kbps", bandwidth/1000)
				}

				streamURL := line
				if baseURL != nil {
					if parsedStream, err := url.Parse(streamURL); err == nil {
						streamURL = baseURL.ResolveReference(parsedStream).String()
					}
				}

				qualities = append(qualities, HLSQuality{
					Quality: quality,
					URL:     streamURL,
					Headers: headers,
				})
				currentInfo = ""
			}
		}
	}

	return qualities, nil
}
