# ShadoWare

ShadoWare is a local HTTP service that automates a Chromium-based browser, captures page HTML, and discovers media-related URLs (`.m3u8`, `.mpd`, manifest, playlist, `.mp4`).

It supports two execution engines:

- **`extension` mode (default):** uses a temporary Chrome extension to run actions and capture network URLs — catches requests from iframes, service workers, and web workers that CDP alone misses.
- **`cdp` mode:** uses `chromedp` directly via the Chrome DevTools Protocol.

It also exposes a **persistent tab API** for external apps that need full, long-lived control over individual browser tabs.

---

## Requirements

- Go **1.22+** (path-parameter routing in `net/http` requires 1.22)
- A Chromium-based browser installed:
  - Microsoft Edge
  - Brave
  - Google Chrome

---

## Run

```bash
go run .
```

### CLI flags

All flags are optional. Zero-config works out of the box.

| Flag | Default | Description |
|---|---|---|
| `-port` | `:8080` | Listen address (`0.0.0.0:9000` also valid) |
| `-headless` | `false` | Run browser headless (set `true` for production) |
| `-max-tabs` | `5` | Max concurrent one-shot `/execute` tabs |
| `-browser` | _(auto)_ | Override browser executable path |
| `-timeout` | `120s` | Per-request deadline |
| `-mode` | `extension` | Default scrape mode: `extension` or `cdp` |

Example:

```bash
./shadoware -port :9000 -headless -max-tabs 10 -timeout 90s -mode cdp
```

```bash
./shadoware -browser "C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"
```

---

## Build and release workflow

GitHub Actions workflow: `.github/workflows/release.yml`

- Push a tag like `v1.0.0` to automatically build and publish a release.
- Or run **Build and Release** manually from Actions and provide a `tag`.
- Release assets include binaries for:
  - Windows (`windows-amd64`)
  - Linux (`linux-amd64`)
  - macOS (`macos-arm64`)

---

## API

### Health check

`GET /health`

```json
{"status": "ok"}
```

---

### Config

`GET /config`

Returns the current runtime configuration.

```json
{
  "port": ":8080",
  "headless": false,
  "max_tabs": 5,
  "timeout": "2m0s",
  "mode": "extension",
  "browser_name": "Microsoft Edge",
  "browser_path": "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe"
}
```

---

### Browser control

`GET /browser` — browser status and uptime.

```json
{
  "browser": "Microsoft Edge",
  "path": "C:\\...\\msedge.exe",
  "started_at": "2025-01-01T00:00:00Z",
  "uptime": "4m32s"
}
```

`POST /browser/restart` — gracefully closes all persistent tabs, kills the browser process, and starts a fresh instance. One-shot `/execute` requests in-flight are unaffected.

```json
{"status": "restarted"}
```

---

### One-shot execute

`POST /execute`

Launches a temporary browser session, runs the requested actions, returns captured HTML and media URLs, then closes the tab. The `mode` field overrides the server default for this request only.

**Request:**

```json
{
  "url": "https://example.com",
  "mode": "extension",
  "wait_ms": 3000,
  "local_storage": {
    "token": "abc123"
  },
  "actions": [
    {"type": "wait_ready", "selector": "body", "wait_ms": 5000},
    {"type": "click", "selector": "button.play"},
    {"type": "wait", "wait_ms": 2000}
  ],
  "debug": false,
  "include_headers": false,
  "stream": false
}
```

| Field | Required | Description |
|---|---|---|
| `url` | yes | Valid `http/https` URL |
| `mode` | no | `extension` or `cdp`; inherits server default if omitted |
| `wait_ms` | no | Extra wait after actions complete (`0–15000`) |
| `local_storage` | no | Key/value pairs written to `localStorage` before reload |
| `actions` | no | Ordered browser actions (see [Actions](#actions)) |
| `debug` | no | Include all captured URLs in `all_urls` |
| `include_headers` | no | Probe and return headers for `.m3u8` URLs |
| `stream` | no | Return NDJSON event stream instead of a single response |

**Response:**

```json
{
  "content": "<html>...</html>",
  "m3u8_urls": ["https://cdn.example.com/master.m3u8"],
  "all_urls": ["https://..."],
  "m3u8_headers": [
    {
      "url": "https://cdn.example.com/master.m3u8",
      "method": "HEAD",
      "status": 200,
      "headers": {"Content-Type": "application/vnd.apple.mpegurl"}
    }
  ],
  "error": ""
}
```

| Field | Description |
|---|---|
| `content` | Captured outer HTML |
| `m3u8_urls` | Deduplicated media-candidate URLs |
| `all_urls` | All captured URLs — only present when `debug=true` |
| `m3u8_headers` | Present when `include_headers=true` or `debug=true` |
| `error` | Non-empty if execution failed |

---

### Persistent tab API

Persistent tabs stay open between requests, giving the caller full control over navigation, actions, and URL capture. Tabs use CDP mode and are not subject to the `/execute` concurrency limit.

#### Create tab

`POST /tabs`

```json
{"url": "https://example.com"}
```

Returns immediately with the tab ID while the page loads asynchronously.

```json
{
  "id": "a1b2c3d4e5f6a7b8",
  "url": "https://example.com",
  "status": "loading",
  "created_at": "2025-01-01T00:00:00Z",
  "url_count": 0,
  "m3u8_count": 0
}
```

#### List tabs

`GET /tabs`

Returns all open tabs sorted by creation time.

#### Get tab

`GET /tabs/{id}`

```json
{
  "id": "a1b2c3d4e5f6a7b8",
  "url": "https://example.com",
  "status": "ready",
  "created_at": "2025-01-01T00:00:00Z",
  "url_count": 42,
  "m3u8_count": 1
}
```

`status` values: `loading` | `ready` | `error`

#### Close tab

`DELETE /tabs/{id}` → `204 No Content`

#### Navigate tab

`POST /tabs/{id}/navigate`

```json
{"url": "https://example.com/watch/episode-2"}
```

Returns updated tab info.

#### Run actions on tab

`POST /tabs/{id}/actions`

```json
{
  "actions": [
    {"type": "click", "selector": ".play-button"},
    {"type": "wait", "wait_ms": 3000}
  ],
  "wait_ms": 1000
}
```

Returns the page HTML after all actions complete:

```json
{"content": "<html>...</html>"}
```

#### Snapshot tab

`GET /tabs/{id}/snapshot`

Returns the current HTML and all URLs captured since the tab was opened.

```json
{
  "content": "<html>...</html>",
  "m3u8_urls": ["https://cdn.example.com/master.m3u8"],
  "all_urls": ["https://..."]
}
```

#### Evaluate JavaScript

`POST /tabs/{id}/evaluate`

```json
{"script": "document.title"}
```

```json
{"result": "One Piece - Episode 1"}
```

#### Clear captured URLs

`DELETE /tabs/{id}/urls` → `204 No Content`

Resets the URL capture history for a tab without closing it. Useful when navigating to a new episode and wanting a clean capture.

---

### Streaming mode

When `stream=true` on `/execute`, the response is `application/x-ndjson`. Each line is a JSON event:

```json
{"type":"url","url":"https://cdn.example.com/master.m3u8","is_media":true}
{"type":"url","url":"https://cdn.example.com/segment0.ts","is_media":false}
{"type":"done","response":{...final TaskResponse...}}
```

```bash
curl -N -X POST http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","stream":true}'
```

---

## Actions

| Type | Required fields | Notes |
|---|---|---|
| `wait` / `sleep` | `wait_ms` | `0–30000` ms |
| `wait_ready` | `selector` | Waits until element is in DOM |
| `click` | `selector` or `x`+`y` | Dispatches full mouse event sequence |
| `double_click` | `selector` or `x`+`y` | |
| `scroll` | `delta_x` or `delta_y` | |
| `send_keys` / `type` | `selector`, `text` | Sets value and fires `input`/`change` |
| `evaluate` / `eval` | `script` | Runs in page context |

---

## Notes

- **CORS** is enabled for all origins (`*`).
- **Parent watchdog:** if the parent process (e.g. your Flutter app) exits, ShadoWare shuts itself down automatically.
- **Anti-bot stealth:** a script injected before page JS removes `navigator.webdriver`, restores `window.chrome`, and patches `navigator.plugins` and `permissions.query`.
- **Response body scanning:** JSON, JS, and XHR response bodies are scanned for embedded media URLs, catching the common pattern where the `.m3u8` URL is inside an API response rather than a direct network request.
- Internal extension endpoints (`/extension-command`, `/extension-capture`, `/extension-result`) are used by the injected browser extension and are not intended for external callers.