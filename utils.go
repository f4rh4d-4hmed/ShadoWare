package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"

var mediaURLPattern = regexp.MustCompile(`(?i)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*(?:\.m3u8|m3u8|\.mpd|/playlist|/master|manifest)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*`)

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
	out := in[:0]
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

const hlsScraperInjectedScript = `(function() {
  // 1. Popup Blocker
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

  // 2. Cloudflare Turnstile Bypass
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

  // 3. Video Play Autoplay Clicker
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
})();`

