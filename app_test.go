package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestEndpoints(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
			<!DOCTYPE html>
			<html>
			<head><title>Mock Video Page</title></head>
			<body>
				<h1>Mock Video</h1>
				<video src="https://cdn.example.com/playlist.m3u8" controls></video>
				<script>
					console.log("Mock page loaded");
				</script>
			</body>
			</html>
		`))
	}))
	defer mockServer.Close()

	browserPath, browserName := findBrowser()
	if browserPath == "" {
		t.Skip("Skipping test: no Chromium browser found on system")
	}
	t.Logf("Using browser: %s (%s)", browserName, browserPath)

	apiPort := ":9898"
	captureDir, err := ensureCaptureExtension(apiPort)
	if err != nil {
		t.Fatalf("Failed to prepare extension: %v", err)
	}
	defer os.RemoveAll(captureDir)

	bd := NewBrowserDaemon(browserPath, browserName, captureDir, true)
	bd.Start()
	defer bd.Stop()

	captureHub := newExtensionCaptureHub()
	jobHub := newExtensionJobHub()
	sem := NewSemaphore(2)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /execute", handleExecute(bd, sem, Config{Timeout: 30 * time.Second}, captureHub, jobHub))
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /version", handleVersion)
	mux.HandleFunc("GET /config", handleConfigGet(Config{Port: apiPort, Headless: true, MaxTabs: 2, Timeout: 30 * time.Second}, bd))
	mux.HandleFunc("GET /extension-command", handleExtensionCommand(jobHub))
	mux.HandleFunc("POST /extension-capture", handleExtensionCapture(captureHub))
	mux.HandleFunc("POST /extension-result", handleExtensionResult(jobHub))

	apiServer := httptest.NewUnstartedServer(mux)
	l, err := net.Listen("tcp", "127.0.0.1"+apiPort)
	if err != nil {
		t.Fatalf("Failed to bind API port: %v", err)
	}
	apiServer.Listener = l
	apiServer.Start()
	defer apiServer.Close()

	time.Sleep(1 * time.Second)

	resp, err := http.Get("http://127.0.0.1" + apiPort + "/health")
	if err != nil {
		t.Fatalf("Failed to query /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK from /health, got %d", resp.StatusCode)
	}
	var healthData map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&healthData); err != nil {
		t.Fatalf("Failed to decode /health JSON: %v", err)
	}
	if healthData["status"] != "ok" {
		t.Errorf("Expected status: ok, got: %s", healthData["status"])
	}

	resp, err = http.Get("http://127.0.0.1" + apiPort + "/version")
	if err != nil {
		t.Fatalf("Failed to query /version: %v", err)
	}
	defer resp.Body.Close()
	versionBytes, _ := io.ReadAll(resp.Body)
	versionStr := string(versionBytes)
	if versionStr != Version {
		t.Errorf("Expected version %s, got %s", Version, versionStr)
	}

	reqBody := TaskRequest{
		URL:    mockServer.URL,
		WaitMs: 1000,
	}
	reqJSON, _ := json.Marshal(reqBody)
	resp, err = http.Post("http://127.0.0.1"+apiPort+"/execute", "application/json", bytes.NewBuffer(reqJSON))
	if err != nil {
		t.Fatalf("Failed to query /execute: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK from /execute, got %d", resp.StatusCode)
	}

	var taskResp TaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&taskResp); err != nil {
		t.Fatalf("Failed to decode /execute response: %v", err)
	}

	if len(taskResp.M3u8URLs) == 0 {
		t.Errorf("Expected to extract mock m3u8 link, but found none")
	} else {
		t.Logf("Extracted m3u8 URLs: %v", taskResp.M3u8URLs)
		foundMockURL := false
		for _, u := range taskResp.M3u8URLs {
			if u == "https://cdn.example.com/playlist.m3u8" {
				foundMockURL = true
			}
		}
		if !foundMockURL {
			t.Errorf("Expected to extract 'https://cdn.example.com/playlist.m3u8', but extracted: %v", taskResp.M3u8URLs)
		}
	}
}
