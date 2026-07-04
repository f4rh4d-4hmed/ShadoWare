# ShadoWare

ShadoWare is a local HTTP service that automates a Chromium-based browser to capture page HTML and discover media stream URLs (`.m3u8`, `.mpd`, `/playlist`, `/master`, `manifest`, `.mp4`).

It supports two automation modes:
- **`extension` mode (default):** Uses an auto-generated Chrome extension to run actions and capture network traffic (intercepts requests from iframes, service workers, and web workers).
- **`cdp` mode:** Controls the browser directly via Chrome DevTools Protocol.

---

## Getting Started

### Requirements
- **Go 1.22+**
- A Chromium-based browser (Chrome, Edge, Brave, or Chromium)

### Run
```bash
go run .
```

### CLI Options
| Flag | Default | Description |
| :--- | :--- | :--- |
| `-port` | `:8080` | Listen address (e.g. `:8080`, `0.0.0.0:9000`) |
| `-headless` | `false` | Run browser in headless mode |
| `-max-tabs` | `5` | Maximum concurrent one-shot `/execute` and `/scrape/hls` tabs |
| `-browser` | _(auto)_ | Path to custom browser executable |
| `-timeout` | `120s` | Per-request timeout limit |
| `-mode` | `extension`| Default scraping mode (`extension` or `cdp`) |
| `-version` | `version`| Check version of app thats installed |

---

## API Endpoints

### 1. General & Health
- **`GET /health`**: Returns `{"status": "ok"}`
- **`GET /config`**: Returns current server configuration
- **`GET /browser`**: Returns browser info, executable path, and uptime
- **`POST /browser/restart`**: Closes persistent tabs, terminates the browser, and launches a fresh instance

---

### 2. One-Shot Scraping

#### `POST /execute`
Runs browser actions, captures HTML, and collects media URLs.

**Request:**
```json
{
  "url": "https://example.com",
  "mode": "extension",
  "wait_ms": 2000,
  "local_storage": { "token": "session-id" },
  "actions": [
    { "type": "wait_ready", "selector": "#video-player" },
    { "type": "click", "selector": "button.play" }
  ],
  "debug": false,
  "include_headers": false,
  "stream": false
}
```

- **`stream`**: When `true`, returns an NDJSON (`application/x-ndjson`) stream of events as URLs are captured, ending with a `done` event containing the final payload.
- **`include_headers`**: Probes the media URLs (via HEAD/GET requests) and returns HTTP request/response headers.

**Response:**
```json
{
  "content": "<html>...</html>",
  "m3u8_urls": ["https://cdn.example.com/master.m3u8"],
  "all_urls": [],
  "m3u8_headers": [
    {
      "url": "https://cdn.example.com/master.m3u8",
      "status": 200,
      "response_headers": { "Content-Type": "application/x-mpegURL" }
    }
  ]
}
```

---

#### `POST /scrape/hls`
A specialized scraper tailored for HLS streams (`.m3u8`). It runs in extension mode with an Android Chrome User-Agent, automatically bypasses Cloudflare Turnstile, handles autoplay-blocking by simulating clicks on video players, and probes the streams to parse all available quality levels and request/response headers.

**Request:**
```json
{
  "url": "https://example.com/watch",
  "wait_ms": 3000,
  "local_storage": { "cookie": "xyz" }
}
```

**Response:**
```json
{
  "playable_url": "https://cdn.example.com/master.m3u8",
  "qualities": [
    {
      "quality": "1920x1080",
      "url": "https://cdn.example.com/1080p.m3u8",
      "headers": {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) ...",
        "Referer": "https://example.com/",
        "Origin": "https://example.com"
      }
    }
  ],
  "headers": {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) ...",
    "Referer": "https://example.com/",
    "Origin": "https://example.com"
  }
}
```

> [!IMPORTANT]
> - **Forwarding Captured Headers:** Many CDNs and stream hosts enforce hotlink protections. You **must** copy the returned `headers` (specifically `Referer`, `Origin`, and `User-Agent`) and include them in all subsequent network requests for the `.m3u8` manifests and `.ts` media segments when playing the video in external players (such as ExoPlayer, VLC, mpv, or web players).
> - **Header Extraction & Decompression:** The scraper captures headers using the Chrome extension's `onSendHeaders` hook to ensure accurate browser-sent headers. During stream validation, the scraper ignores the browser's `Accept-Encoding` header so the Go HTTP client can automatically request and decompress gzip payloads to parse plaintext playlist qualities.

---

### 3. Persistent Tab API
For long-lived browser sessions. Tabs run in CDP mode.

- **`GET /tabs`**: List open tabs
- **`POST /tabs`**: Open a new persistent tab (`{"url": "..."}`)
- **`GET /tabs/{id}`**: Get tab state (`loading`, `ready`, or `error`)
- **`DELETE /tabs/{id}`**: Close tab
- **`POST /tabs/{id}/navigate`**: Navigate to a new URL (`{"url": "..."}`)
- **`POST /tabs/{id}/actions`**: Execute browser actions (`{"actions": [...], "wait_ms": 1000}`)
- **`GET /tabs/{id}/snapshot`**: Get current HTML, captured media, and all URLs
- **`POST /tabs/{id}/evaluate`**: Evaluate custom JavaScript (`{"script": "..."}`)
- **`DELETE /tabs/{id}/urls`**: Reset the tab's captured URL history

---

## Browser Actions

Use these action objects in `actions` arrays to interact with pages:

| Type | Required Fields | Notes |
| :--- | :--- | :--- |
| `wait` / `sleep` | `wait_ms` | Delay execution (0–30000ms) |
| `wait_ready` | `selector` | Wait until element exists in the DOM |
| `click` | `selector` or `x`, `y` | Click an element or coordinates |
| `double_click` | `selector` or `x`, `y` | Double-click an element or coordinates |
| `scroll` | `delta_x`, `delta_y` | Scroll page horizontally or vertically |
| `send_keys` / `type` | `selector`, `text` | Focuses element, sets value, fires input events |
| `evaluate` / `eval` | `script` | Evaluates JavaScript in the page |

---

## Technical Features
- **Anti-Bot Stealth**: Injected scripts disguise automation by removing `navigator.webdriver` and patching browser properties.
- **Parent Watchdog**: The service monitors the parent process's PID and automatically shuts down if the parent exits.
- **CORS Enabled**: All endpoints allow cross-origin requests (`Access-Control-Allow-Origin: *`).
- **Response Body Scanning**: Inspects responses for JSON, JS, and HTML, extracting matching URLs hidden inside scripts or API responses.
