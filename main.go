package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/semaphore"
)

func parseConfig() Config {
	var cfg Config
	flag.StringVar(&cfg.Port, "port", ":8080", "HTTP listen address")
	flag.BoolVar(&cfg.Headless, "headless", false, "Run browser headless")
	flag.IntVar(&cfg.MaxTabs, "max-tabs", 5, "Max concurrent browser tabs")
	flag.StringVar(&cfg.Browser, "browser", "", "Override browser executable path")
	flag.DurationVar(&cfg.Timeout, "timeout", 120*time.Second, "Per-request timeout")
	flag.StringVar(&cfg.Mode, "mode", "extension", "Default scrape mode: extension | cdp")
	flag.Parse()

	if !strings.HasPrefix(cfg.Port, ":") && !strings.Contains(cfg.Port, ":") {
		cfg.Port = ":" + cfg.Port
	}
	cfg.Mode = strings.ToLower(cfg.Mode)
	return cfg
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

	captureExtensionDir, err := ensureCaptureExtension(cfg.Port)
	if err != nil {
		log.Fatalf("Failed to prepare capture extension: %v", err)
	}
	log.Printf("Capture extension ready at %s", captureExtensionDir)

	opts := buildAllocatorOptions(browserPath, captureExtensionDir, cfg)
	bm := newBrowserManager(browserPath, browserName, opts)

	captureHub := newExtensionCaptureHub()
	jobHub := newExtensionJobHub()
	pool := newTabPool(bm)
	bm.pool = pool

	sem := semaphore.NewWeighted(int64(cfg.MaxTabs))

	mux := http.NewServeMux()

	mux.HandleFunc("POST /execute", handleExecute(bm, sem, cfg, captureHub, jobHub, browserPath, captureExtensionDir))
	mux.HandleFunc("POST /scrape/hls", handleScrapeHLS(bm, sem, cfg, captureHub, jobHub, browserPath, captureExtensionDir))

	mux.HandleFunc("GET /tabs", handleTabList(pool))
	mux.HandleFunc("POST /tabs", handleTabCreate(pool))
	mux.HandleFunc("GET /tabs/{id}", handleTabGet(pool))
	mux.HandleFunc("DELETE /tabs/{id}", handleTabClose(pool))
	mux.HandleFunc("POST /tabs/{id}/navigate", handleTabNavigate(pool))
	mux.HandleFunc("POST /tabs/{id}/actions", handleTabActions(pool))
	mux.HandleFunc("GET /tabs/{id}/snapshot", handleTabSnapshot(pool))
	mux.HandleFunc("POST /tabs/{id}/evaluate", handleTabEvaluate(pool))
	mux.HandleFunc("DELETE /tabs/{id}/urls", handleTabClearURLs(pool))

	mux.HandleFunc("GET /browser", handleBrowserStatus(bm))
	mux.HandleFunc("POST /browser/restart", handleBrowserRestart(bm))

	mux.HandleFunc("GET /config", handleConfigGet(cfg, browserPath, browserName))

	mux.HandleFunc("GET /extension-command", handleExtensionCommand(jobHub))
	mux.HandleFunc("POST /extension-capture", handleExtensionCapture(captureHub))
	mux.HandleFunc("POST /extension-result", handleExtensionResult(jobHub))

	mux.HandleFunc("GET /health", handleHealth)

	timeout := cfg.Timeout
	srv := &http.Server{
		Addr:         cfg.Port,
		Handler:      withCORS(withLogging(mux)),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: timeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdown := func(reason string) {
		log.Printf("Shutting down: %s", reason)
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

	startParentWatchdog(shutdown)

	fmt.Printf("Shadoware active on http://localhost%s  (parent PID: %d)\n", cfg.Port, os.Getppid())
	fmt.Printf("Mode: %s | Headless: %v | MaxTabs: %d | Timeout: %s\n",
		cfg.Mode, cfg.Headless, cfg.MaxTabs, cfg.Timeout)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}