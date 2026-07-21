# WebMediaUtil

`WebMediaUtil` is a high-performance, completely headless, standalone subprocess utility designed to extract `.m3u8` links (or HLS streams) from web pages. 

By leveraging a single persistent Chromium/Chrome browser running a custom capture extension, it routes multiple scraper requests concurrently inside separate tabs.

---

## Key Features
- **Persistent Headless Browser**: A single browser instance runs continuously in the background. If the process is terminated or crashes, the Go daemon automatically restarts it within one second.
- **Concurrent Tab Scrapers**: Requests are processed concurrently in separate tabs of the persistent browser. Tab popup protection, tab-specific header rewrites (e.g. User-Agents), and safety watchdog timers are managed on a per-tab basis.
- **Zero External Dependencies**: Implemented entirely using the Go Standard Library (no third-party CDP packages like `chromedp` or `playwright`).
- **Dynamic Port Collision Increment**: If the default port is in use, `WebMediaUtil` increments the port number and retries until a free port is successfully bound. It outputs the finalized port to stdout (`PORT_BOUND: :<port>`) so that the parent process can immediately read and connect.
- **Process Watchdog**: Monitors the parent process PID and automatically shuts down if the parent becomes unavailable. (Disabled if launched without a parent process, i.e., system PID <= 4).
- **Embedded Turnstile & Autoplay Solvers**: Emulates mouse events to solve Cloudflare Turnstile checkboxes and triggers HTML5 video players automatically.

---

## Build & Installation

Ensure you have [Go](https://go.dev/) (v1.26+) installed.

```bash
# Navigate to the project directory
cd WebMediaUtil

# Build the executable
go build -o webmediautil.exe
```

---

## CLI Configuration Flags

```bash
Usage of webmediautil:
  -browser string
        Override browser executable path (e.g., path to chrome.exe / msedge.exe)
  -disable-watchdog
        Disable parent process watchdog
  -headless
        Run browser headless (always true for this repo, defaults to true)
  -max-tabs int
        Max concurrent browser tabs (default 5)
  -port string
        HTTP listen address (default ":8080")
  -timeout duration
        Per-request timeout (default 2m0s)
  -version
        Print version and exit
```

On startup, if `-browser` is omitted, the daemon automatically searches for Edge, Brave, and Google Chrome in common system paths.

---

## API Documentation

### 1. Scrape HLS Stream (`POST /scrape/hls`)
Recommended for extracting and validating playable HLS streams. It automatically uses an Android Chrome User-Agent, triggers theTurnstile solver, clicks standard play buttons, intercepts the media requests, and validates manifest playability.

#### Request Body
- `url` (string, required): The URL of the video streaming web page.
- `wait_ms` (int, optional): Duration in milliseconds to wait after the page completes loading (max 15000ms).
- `local_storage` (map[string]string, optional): Local storage keys and values to inject before loading the page.
- `headers` (map[string]string, optional): Key-value pairs of request headers to apply.

```json
{
  "url": "https://example-streaming-site.com/video/123",
  "wait_ms": 3000,
  "local_storage": {
    "auth_token": "xyz789"
  },
  "headers": {
    "Referer": "https://referrer.com/"
  }
}
```

#### Response Body (Success)
- `playable_url` (string): The resolved and validated `.m3u8` playlist URL.
- `qualities` (array): A list of parsed resolutions and stream links found inside the master manifest.
- `headers` (map[string]string): The HTTP headers captured with the `.m3u8` request, necessary to play the stream (e.g., `User-Agent`, `Referer`, `Origin`).

```json
{
  "playable_url": "https://cdn.example.com/playlist.m3u8",
  "qualities": [
    {
      "quality": "1920x1080",
      "url": "https://cdn.example.com/1080p.m3u8",
      "headers": {
        "User-Agent": "Mozilla/5.0...",
        "Referer": "https://example-streaming-site.com/"
      }
    },
    {
      "quality": "1280x720",
      "url": "https://cdn.example.com/720p.m3u8",
      "headers": {
        "User-Agent": "Mozilla/5.0...",
        "Referer": "https://example-streaming-site.com/"
      }
    }
  ],
  "headers": {
    "User-Agent": "Mozilla/5.0...",
    "Referer": "https://example-streaming-site.com/"
  }
}
```

#### Response Body (Failure / Error)
```json
{
  "error": "Found HLS links but none were playable"
}
```

---

### 2. General Scrape Execution (`POST /execute`)
Runs general-purpose page scraping. Navigates, executes custom browser actions, collects all resource URLs, extracts page content, and captures HLS requests.

#### Request Body
- `url` (string, required): Target URL.
- `wait_ms` (int, optional): Duration to wait in milliseconds (max 15000ms).
- `headers` (map[string]string, optional): Request headers.
- `local_storage` (map[string]string, optional): Local storage items.
- `debug` (bool, optional): Include all intercepted network requests.
- `include_headers` (bool, optional): Probe and include captured request headers.
- `stream` (bool, optional): Return an NDJSON stream of events.
- `actions` (array, optional): Custom actions to run sequentially. Available types:
  - `wait` or `sleep` (wait_ms)
  - `wait_ready` (selector, wait_ms)
  - `click` or `double_click` (selector, or x/y)
  - `scroll` (delta_x, delta_y)
  - `type` or `send_keys` (selector, text)
  - `evaluate` or `eval` (script)

```json
{
  "url": "https://example.com",
  "wait_ms": 2000,
  "actions": [
    { "type": "click", "selector": "#play-btn" },
    { "type": "wait", "wait_ms": 1000 }
  ]
}
```

#### Response Body (Success)
- `content` (string): The outer HTML content of the main page and nested iframes.
- `m3u8_urls` (array): List of all intercepted media/m3u8 urls.
- `all_urls` (array, only if debug is true): All resource URLs requested by the page.
- `m3u8_headers` (array, only if debug/include_headers is true): Captured request/response headers for the matched HLS links.

```json
{
  "content": "<html>...</html>",
  "m3u8_urls": [
    "https://cdn.com/stream.m3u8"
  ]
}
```

---

### 3. Service Health (`GET /health`)
Returns the health status.

#### Response Body
```json
{
  "status": "ok"
}
```

---

### 4. Service Version (`GET /version`)
Returns the version of the application as plain text (e.g. `1.0.0` or `dev`).

#### Response Body
```text
2.0.1
```

---

### 5. Configuration & Browser State (`GET /config`)
Returns the configuration flags and status of the persistent background browser process.

#### Response Body
```json
{
  "port": ":8080",
  "headless": true,
  "max_tabs": 5,
  "timeout": "2m0s",
  "browser": "Google Chrome",
  "browser_path": "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "status": "running",
  "pid": 11340,
  "uptime": "5m23s"
}
```

---

### 6. Scrape Image Manifest (`POST /scrape/ajaximg`)

Recommended for extracting ordered page-image lists from manga, webtoon, and book-reader sites. It navigates to the reader page, intercepts all `fetch`/`XMLHttpRequest` response bodies (via a patched content script injected by the extension), scores each JSON/XML response by how many image URLs it contains, and returns the ordered, deduplicated image list from the highest-scoring response(s).

If no response scores above the minimum threshold (e.g. the site loads images individually rather than via a JSON manifest), it falls back to returning all image-extension URLs seen as network requests, and sets `fallback_used: true` so callers can distinguish the two modes.

#### Request Body
- `url` (string, required): The URL of the manga/comic/book reader page.
- `wait_ms` (int, optional): Duration in milliseconds to wait after page load (max 15000ms). Useful for pages that lazy-load the manifest on scroll.
- `headers` (map[string]string, optional): Key-value pairs of request headers to apply.
- `local_storage` (map[string]string, optional): Local storage keys/values to inject before loading the page.
- `actions` (array, optional): Custom browser actions to run before capture (same action types as `/execute` — `click`, `scroll`, `wait`, `eval`, etc.). Useful for triggering lazy-loaded manifests.
- `url_patterns` (array of strings, optional): Regex allowlist to restrict which response URLs are even considered as manifest candidates. Example: `["/api/chapter/", "/pages/"]`. Default: consider all JSON/XML responses.
- `image_ext_regex` (string, optional): Override the default image-URL regex. Useful for CDNs that serve extensionless URLs (e.g. `?format=webp`). Default matches `.jpg`, `.jpeg`, `.png`, `.webp`, `.avif`, `.gif`.
- `min_match_count` (int, optional): Minimum number of image URLs a response must contain to qualify as a manifest candidate. Default `3`.
- `accumulate_all` (bool, optional): Default `true` — accumulate image URLs from **all** qualifying responses (for paginated/batched manifests). Set to `false` to return only the single highest-scoring response.
- `debug` (bool, optional): Include a `candidates` array of all scored responses in the output.

```json
{
  "url": "https://manga-reader.example.com/chapter/42",
  "wait_ms": 2000,
  "headers": {
    "Referer": "https://manga-reader.example.com/"
  },
  "url_patterns": ["/api/chapter/"],
  "min_match_count": 3,
  "accumulate_all": true
}
```

#### Response Body (Success)
- `images` (array): Ordered, deduplicated list of image URLs for the chapter/volume.
- `manifest_urls` (array): The API response URL(s) identified as the manifest source.
- `headers` (map[string]string): Headers useful for fetching the images (`Referer`, `Origin`), derived from the reader page URL.
- `fallback_used` (bool): `true` if no response scored above the threshold and the endpoint fell back to raw network-request capture.
- `candidates` (array, only if `debug: true`): All scored candidate responses with their URL, content-type, match count, and detected format (`"json"` | `"xml"` | `"unknown"`).

```json
{
  "images": [
    "https://cdn.example.com/chapter/42/page-001.jpg",
    "https://cdn.example.com/chapter/42/page-002.jpg",
    "https://cdn.example.com/chapter/42/page-003.jpg"
  ],
  "manifest_urls": [
    "https://manga-reader.example.com/api/chapter/42/pages"
  ],
  "headers": {
    "Referer": "https://manga-reader.example.com/chapter/42",
    "Origin": "https://manga-reader.example.com"
  },
  "fallback_used": false
}
```

#### Response Body (Fallback — no manifest found)
```json
{
  "images": [
    "https://cdn.example.com/page-001.jpg",
    "https://cdn.example.com/page-002.jpg"
  ],
  "manifest_urls": [],
  "headers": {
    "Referer": "https://manga-reader.example.com/chapter/42",
    "Origin": "https://manga-reader.example.com"
  },
  "fallback_used": true
}
```

#### Response Body (Error)
```json
{
  "images": [],
  "manifest_urls": [],
  "error": "url must be a valid http/https URL"
}
```
