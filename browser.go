package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type BrowserDaemon struct {
	mu           sync.RWMutex
	browserPath  string
	browserName  string
	extensionDir string
	headless     bool
	process      *exec.Cmd
	profileDir   string
	startedAt    time.Time
	ctx          context.Context
	cancel       context.CancelFunc
	running      bool
}

func NewBrowserDaemon(browserPath, browserName, extensionDir string, headless bool) *BrowserDaemon {
	return &BrowserDaemon{
		browserPath:  browserPath,
		browserName:  browserName,
		extensionDir: extensionDir,
		headless:     headless,
	}
}

func (bd *BrowserDaemon) Start() {
	bd.mu.Lock()
	if bd.running {
		bd.mu.Unlock()
		return
	}
	bd.running = true
	bd.startedAt = time.Now()
	bd.ctx, bd.cancel = context.WithCancel(context.Background())
	bd.mu.Unlock()

	go bd.monitorLoop()
}

func (bd *BrowserDaemon) monitorLoop() {
	for {
		bd.mu.RLock()
		if !bd.running {
			bd.mu.RUnlock()
			return
		}
		bd.mu.RUnlock()

		profileDir, err := os.MkdirTemp("", "webmediautil-profile-")
		if err != nil {
			log.Printf("BrowserDaemon: Failed to create temp profile dir: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		bd.mu.Lock()
		bd.profileDir = profileDir
		args := buildBrowserArgs(bd.extensionDir, profileDir, bd.headless)
		cmd := exec.Command(bd.browserPath, args...)
		bd.process = cmd
		bd.mu.Unlock()

		log.Printf("BrowserDaemon: Launching %s (%s)...", bd.browserName, bd.browserPath)
		if err := cmd.Start(); err != nil {
			log.Printf("BrowserDaemon: Failed to start browser process: %v", err)
			_ = os.RemoveAll(profileDir)
			bd.mu.Lock()
			bd.profileDir = ""
			bd.process = nil
			bd.mu.Unlock()
			time.Sleep(2 * time.Second)
			continue
		}

		err = cmd.Wait()

		bd.mu.Lock()
		log.Printf("BrowserDaemon: Browser process exited (pid=%d, err=%v)", cmd.Process.Pid, err)
		if bd.profileDir != "" {
			_ = os.RemoveAll(bd.profileDir)
			bd.profileDir = ""
		}
		bd.process = nil
		running := bd.running
		bd.mu.Unlock()

		if !running {
			return
		}

		log.Println("BrowserDaemon: Restarting browser process in 1 second...")
		select {
		case <-bd.ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (bd *BrowserDaemon) Stop() {
	bd.mu.Lock()
	if !bd.running {
		bd.mu.Unlock()
		return
	}
	bd.running = false
	if bd.cancel != nil {
		bd.cancel()
	}
	cmd := bd.process
	profileDir := bd.profileDir
	bd.process = nil
	bd.profileDir = ""
	bd.mu.Unlock()

	if cmd != nil && cmd.Process != nil {
		log.Printf("BrowserDaemon: Terminating browser process (pid=%d)...", cmd.Process.Pid)
		if runtime.GOOS == "windows" {
			_ = exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
		} else {
			_ = cmd.Process.Kill()
		}
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			log.Println("BrowserDaemon: Timeout waiting for browser shutdown.")
		}
	}

	if profileDir != "" {
		_ = os.RemoveAll(profileDir)
	}
}

func (bd *BrowserDaemon) Status() map[string]interface{} {
	bd.mu.RLock()
	defer bd.mu.RUnlock()

	status := "stopped"
	pid := 0
	if bd.running {
		status = "running"
		if bd.process != nil && bd.process.Process != nil {
			pid = bd.process.Process.Pid
		}
	}

	return map[string]interface{}{
		"browser":    bd.browserName,
		"path":       bd.browserPath,
		"status":     status,
		"pid":        pid,
		"started_at": bd.startedAt.Format(time.RFC3339),
		"uptime":     time.Since(bd.startedAt).String(),
	}
}

func buildBrowserArgs(extensionDir, profileDir string, headless bool) []string {
	args := []string{
		"--user-data-dir=" + profileDir,
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
		"--enable-features=MediaSourceAPI,MSE,BackForwardCache",
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
		"--js-flags=--max-old-space-size=256",
		"--disable-logging",
		"about:blank",
	}
	if runtime.GOOS == "linux" {
		args = append(args, "--no-sandbox")
	}
	if headless {
		args = append([]string{"--headless=new"}, args...)
	}
	return args
}

func findBrowser() (path, name string) {
	type candidate struct {
		name  string
		paths []string
	}
	var browsers []candidate
	switch runtime.GOOS {
	case "windows":
		browsers = []candidate{
			{"Microsoft Edge", []string{
				os.ExpandEnv(`${ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe`),
				os.ExpandEnv(`${ProgramFiles}\Microsoft\Edge\Application\msedge.exe`),
			}},
			{"Brave", []string{
				os.ExpandEnv(`${ProgramFiles}\BraveSoftware\Brave-Browser\Application\brave.exe`),
				os.ExpandEnv(`${LocalAppData}\BraveSoftware\Brave-Browser\Application\brave.exe`),
			}},
			{"Google Chrome", []string{
				os.ExpandEnv(`${ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe`),
				os.ExpandEnv(`${ProgramFiles}\Google\Chrome\Application\chrome.exe`),
				os.ExpandEnv(`${LocalAppData}\Google\Chrome\Application\chrome.exe`),
			}},
		}
	case "darwin":
		browsers = []candidate{
			{"Microsoft Edge", []string{"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"}},
			{"Brave", []string{"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"}},
			{"Google Chrome", []string{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}},
		}
	default:
		browsers = []candidate{
			{"Microsoft Edge", []string{"/usr/bin/microsoft-edge", "/usr/bin/microsoft-edge-stable"}},
			{"Brave", []string{"/usr/bin/brave-browser", "/usr/bin/brave"}},
			{"Google Chrome", []string{"/usr/bin/google-chrome", "/usr/bin/google-chrome-stable", "/usr/bin/chromium", "/usr/bin/chromium-browser"}},
		}
	}
	for _, b := range browsers {
		for _, p := range b.paths {
			if _, err := os.Stat(p); err == nil {
				return p, b.name
			}
		}
	}
	return "", ""
}
