package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var Version = "dev"

func parseConfig() Config {
	var cfg Config
	flag.StringVar(&cfg.Port, "port", ":8080", "HTTP listen address")
	flag.BoolVar(&cfg.Headless, "headless", true, "Run browser headless")
	flag.IntVar(&cfg.MaxTabs, "max-tabs", 5, "Max concurrent browser tabs (1–50)")
	flag.StringVar(&cfg.Browser, "browser", "", "Override browser executable path")
	flag.DurationVar(&cfg.Timeout, "timeout", 120*time.Second, "Per-request timeout")
	flag.BoolVar(&cfg.DisableWatchdog, "disable-watchdog", false, "Disable parent process watchdog")
	flag.Parse()

	if !strings.HasPrefix(cfg.Port, ":") && !strings.Contains(cfg.Port, ":") {
		cfg.Port = ":" + cfg.Port
	}
	if cfg.MaxTabs < 1 || cfg.MaxTabs > 50 {
		log.Fatalf("max-tabs must be between 1 and 50, got %d", cfg.MaxTabs)
	}
	return cfg
}

func listenWithRetry(requestedPort string) (net.Listener, string, error) {
	portStr := requestedPort
	if strings.HasPrefix(portStr, ":") {
		portStr = portStr[1:]
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 8080
	}

	for {
		addr := fmt.Sprintf(":%d", port)
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, fmt.Sprintf(":%d", port), nil
		}
		log.Printf("Port %d is already in use, trying %d...", port, port+1)
		port++
		if port > 65535 {
			return nil, "", errors.New("no available ports in range")
		}
	}
}

func main() {
	cfg := parseConfig()

	browserPath := cfg.Browser
	var browserName string
	if browserPath == "" {
		browserPath, browserName = findBrowser()
		if browserPath == "" {
			log.Fatal("No Chromium-based browser found. Install Chrome, Edge, or Brave, or pass -browser=<path>")
		}
	} else {
		browserName = "custom"
	}
	log.Printf("Using browser: %s (%s)", browserName, browserPath)

	ln, boundPort, err := listenWithRetry(cfg.Port)
	if err != nil {
		log.Fatalf("Failed to bind to any port: %v", err)
	}
	cfg.Port = boundPort

	fmt.Printf("PORT_BOUND: %s\n", boundPort)
	os.Stdout.Sync()

	captureExtensionDir, err := ensureCaptureExtension(cfg.Port)
	if err != nil {
		_ = ln.Close()
		log.Fatalf("Failed to prepare capture extension: %v", err)
	}
	log.Printf("Capture extension ready at %s", captureExtensionDir)

	bd := NewBrowserDaemon(browserPath, browserName, captureExtensionDir, cfg.Headless)
	bd.Start()

	captureHub := newExtensionCaptureHub()
	jobHub := newExtensionJobHub()
	sem := NewSemaphore(cfg.MaxTabs)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /execute", handleExecute(bd, sem, cfg, captureHub, jobHub))
	mux.HandleFunc("POST /scrape/hls", handleScrapeHLS(bd, sem, cfg, captureHub, jobHub))
	mux.HandleFunc("POST /scrape/ajaximg", handleScrapeAjaxImg(bd, sem, cfg, captureHub, jobHub))
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /config", handleConfigGet(cfg, bd))
	mux.HandleFunc("GET /version", handleVersion)

	mux.HandleFunc("GET /extension-command", handleExtensionCommand(jobHub))
	mux.HandleFunc("POST /extension-capture", handleExtensionCapture(captureHub))
	mux.HandleFunc("POST /extension-result", handleExtensionResult(jobHub))

	timeout := cfg.Timeout
	srv := &http.Server{
		Handler:      withCORS(withLogging(mux)),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: timeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdown := func(reason string) {
		log.Printf("Shutting down: %s", reason)
		bd.Stop()
		_ = os.RemoveAll(captureExtensionDir)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Forced shutdown: %v", err)
		}
	}

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		sig := <-quit
		shutdown(fmt.Sprintf("signal %s received", sig))
	}()

	if !cfg.DisableWatchdog {
		startParentWatchdog(shutdown)
	} else {
		log.Println("Parent watchdog disabled")
	}

	log.Printf("WebMediaUtil active on http://localhost%s  (parent PID: %d)", cfg.Port, os.Getppid())
	log.Printf("Headless: true | MaxTabs: %d | Timeout: %s", cfg.MaxTabs, cfg.Timeout)

	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
