package main

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// imageRe is the default compiled image-URL regex used across manifest tests.
var testImageRe = regexp.MustCompile(defaultImageExtPattern)

// makeJSONCapture builds an ajaxCapture whose body is a JSON array of imageURLs.
func makeJSONCapture(apiURL string, imageURLs []string) ajaxCapture {
	b, _ := json.Marshal(imageURLs)
	return ajaxCapture{
		URL:         apiURL,
		ContentType: "application/json",
		Body:        string(b),
	}
}

// parseJSON is a thin helper that panics on bad input, for brevity in test bodies.
func parseJSON(s string) interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		panic("parseJSON: " + err.Error())
	}
	return v
}

// equalSlices returns true when two string slices are element-wise equal.
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ─── scanJSONForImages ────────────────────────────────────────────────────────

func TestScanJSONForImages_ArrayManifest(t *testing.T) {
	got := scanJSONForImages(parseJSON(`{"pages":["https://cdn.com/p1.jpg","https://cdn.com/p2.png","https://cdn.com/p3.webp"]}`), testImageRe)
	want := []string{"https://cdn.com/p1.jpg", "https://cdn.com/p2.png", "https://cdn.com/p3.webp"}
	if !equalSlices(got, want) {
		t.Errorf("scanJSONForImages = %v, want %v", got, want)
	}
}

func TestScanJSONForImages_NestedObject(t *testing.T) {
	got := scanJSONForImages(parseJSON(`{"chapter":{"images":["https://cdn.com/a.jpeg","https://cdn.com/b.avif"]}}`), testImageRe)
	if len(got) != 2 {
		t.Errorf("nested object: expected 2 images, got %d: %v", len(got), got)
	}
}

func TestScanJSONForImages_URLsInString(t *testing.T) {
	// Some APIs embed space-separated URL lists inside a single string value.
	got := scanJSONForImages(parseJSON(`{"data":"https://cdn.com/img1.jpg https://cdn.com/img2.png"}`), testImageRe)
	if len(got) != 2 {
		t.Errorf("URLs in string: expected 2 images, got %d: %v", len(got), got)
	}
}

func TestScanJSONForImages_QueryStringPreserved(t *testing.T) {
	got := scanJSONForImages(parseJSON(`["https://cdn.com/p1.jpg?w=800&q=90","https://cdn.com/p2.png?v=2"]`), testImageRe)
	if len(got) != 2 {
		t.Errorf("query string: expected 2 images, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "?w=800") {
		t.Errorf("query string not preserved in first result: %s", got[0])
	}
}

// ─── scanXMLForImages ─────────────────────────────────────────────────────────

func TestScanXMLForImages_CharData(t *testing.T) {
	body := `<pages><page>https://cdn.com/p1.jpg</page><page>https://cdn.com/p2.png</page></pages>`
	got := scanXMLForImages([]byte(body), testImageRe)
	if len(got) != 2 {
		t.Errorf("XML CharData: expected 2 images, got %d: %v", len(got), got)
	}
}

func TestScanXMLForImages_Attributes(t *testing.T) {
	body := `<pages><page src="https://cdn.com/p1.jpeg"/><page src="https://cdn.com/p2.gif"/></pages>`
	got := scanXMLForImages([]byte(body), testImageRe)
	if len(got) != 2 {
		t.Errorf("XML attributes: expected 2 images, got %d: %v", len(got), got)
	}
}

// ─── scoreCapture ─────────────────────────────────────────────────────────────

func TestScoreCapture_CleanJSONArray(t *testing.T) {
	imageURLs := make([]string, 20)
	for i := range imageURLs {
		imageURLs[i] = "https://cdn.com/page" + strings.Repeat("x", i%5) + ".jpg"
	}
	cap := makeJSONCapture("https://api.example.com/chapter/1", imageURLs)
	r := scoreCapture(cap, testImageRe)
	if r.format != "json" {
		t.Errorf("expected format=json, got %s", r.format)
	}
	if r.matchCount != 20 {
		t.Errorf("expected 20 matches, got %d", r.matchCount)
	}
}

func TestScoreCapture_MislabeledJSON(t *testing.T) {
	// Content-Type claims text/html but body is valid JSON.
	imageURLs := []string{"https://cdn.com/p1.jpg", "https://cdn.com/p2.jpg", "https://cdn.com/p3.jpg"}
	b, _ := json.Marshal(imageURLs)
	cap := ajaxCapture{
		URL:         "https://api.example.com/pages",
		ContentType: "text/html; charset=utf-8",
		Body:        string(b),
	}
	r := scoreCapture(cap, testImageRe)
	if r.format != "json" {
		t.Errorf("mislabeled JSON: expected format=json, got %s", r.format)
	}
	if r.matchCount != 3 {
		t.Errorf("mislabeled JSON: expected 3 matches, got %d", r.matchCount)
	}
}

func TestScoreCapture_XMLVariant(t *testing.T) {
	body := `<?xml version="1.0"?><chapter><page>https://cdn.com/p1.png</page><page>https://cdn.com/p2.png</page><page>https://cdn.com/p3.webp</page></chapter>`
	cap := ajaxCapture{URL: "https://api.example.com/chapter.xml", ContentType: "application/xml", Body: body}
	r := scoreCapture(cap, testImageRe)
	if r.format != "xml" {
		t.Errorf("XML variant: expected format=xml, got %s", r.format)
	}
	if r.matchCount != 3 {
		t.Errorf("XML variant: expected 3 matches, got %d", r.matchCount)
	}
}

func TestScoreCapture_FalsePositiveAnalytics(t *testing.T) {
	body := `{"event":"pageview","og_image":"https://cdn.com/thumb.jpg","user":"anonymous"}`
	cap := ajaxCapture{URL: "https://analytics.example.com/track", ContentType: "application/json", Body: body}
	r := scoreCapture(cap, testImageRe)
	if r.matchCount != 1 {
		t.Errorf("analytics false positive: expected score=1, got %d", r.matchCount)
	}
}

func TestScoreCapture_EmptyBody(t *testing.T) {
	cap := ajaxCapture{URL: "https://example.com/empty", Body: ""}
	r := scoreCapture(cap, testImageRe)
	if r.matchCount != 0 || r.format != "unknown" {
		t.Errorf("empty body: expected score=0/unknown, got %d/%s", r.matchCount, r.format)
	}
}

// ─── scoreAndExtract ──────────────────────────────────────────────────────────

func TestScoreAndExtract_AccumulateAll(t *testing.T) {
	batch1 := make([]string, 10)
	batch2 := make([]string, 10)
	for i := 0; i < 10; i++ {
		batch1[i] = "https://cdn.com/ch1/page1_" + strings.Repeat("a", i+1) + ".jpg"
		batch2[i] = "https://cdn.com/ch1/page2_" + strings.Repeat("b", i+1) + ".jpg"
	}
	captures := []ajaxCapture{
		makeJSONCapture("https://api.example.com/batch/1", batch1),
		makeJSONCapture("https://api.example.com/batch/2", batch2),
	}
	images, manifestURLs, _, fallback := scoreAndExtract(captures, nil, testImageRe, 3, true, false, nil)
	if fallback {
		t.Error("accumulate all: expected no fallback")
	}
	if len(images) != 20 {
		t.Errorf("accumulate all: expected 20 images, got %d", len(images))
	}
	if len(manifestURLs) != 2 {
		t.Errorf("accumulate all: expected 2 manifest URLs, got %d", len(manifestURLs))
	}
}

func TestScoreAndExtract_AccumulateDedup(t *testing.T) {
	shared := []string{
		"https://cdn.com/p1.jpg", "https://cdn.com/p2.jpg", "https://cdn.com/p3.jpg",
	}
	unique := []string{
		"https://cdn.com/p4.jpg", "https://cdn.com/p5.jpg", "https://cdn.com/p6.jpg",
	}
	// batch1 has 6 URLs; batch2 repeats the 3 shared URLs.
	captures := []ajaxCapture{
		makeJSONCapture("https://api.example.com/batch/1", append(shared, unique...)),
		makeJSONCapture("https://api.example.com/batch/2", shared),
	}
	images, _, _, fallback := scoreAndExtract(captures, nil, testImageRe, 3, true, false, nil)
	if fallback {
		t.Error("dedup: expected no fallback")
	}
	if len(images) != 6 {
		t.Errorf("dedup: expected 6 unique images, got %d: %v", len(images), images)
	}
}

func TestScoreAndExtract_SingleWinner(t *testing.T) {
	small := []string{"https://cdn.com/p1.jpg", "https://cdn.com/p2.jpg", "https://cdn.com/p3.jpg"}
	big := []string{
		"https://cdn.com/a.jpg", "https://cdn.com/b.jpg", "https://cdn.com/c.jpg",
		"https://cdn.com/d.jpg", "https://cdn.com/e.jpg",
	}
	captures := []ajaxCapture{
		makeJSONCapture("https://api.example.com/small", small),
		makeJSONCapture("https://api.example.com/big", big),
	}
	images, manifestURLs, _, fallback := scoreAndExtract(captures, nil, testImageRe, 3, false, false, nil)
	if fallback {
		t.Error("single winner: expected no fallback")
	}
	if len(images) != 5 {
		t.Errorf("single winner: expected 5 images from highest-scoring capture, got %d: %v", len(images), images)
	}
	if len(manifestURLs) != 1 {
		t.Errorf("single winner: expected 1 manifest URL, got %d", len(manifestURLs))
	}
}

func TestScoreAndExtract_FallbackBelowThreshold(t *testing.T) {
	// Only 1 image URL — below default threshold of 3 → fallback.
	analyticsCapture := ajaxCapture{
		URL:         "https://analytics.example.com/track",
		ContentType: "application/json",
		Body:        `{"og_image":"https://cdn.com/thumb.jpg"}`,
	}
	allURLs := []string{"https://cdn.com/img1.jpg", "https://cdn.com/img2.png"}
	images, _, _, fallback := scoreAndExtract([]ajaxCapture{analyticsCapture}, nil, testImageRe, 3, true, false, allURLs)
	if !fallback {
		t.Error("expected fallback=true when no candidate meets threshold")
	}
	if len(images) != 2 {
		t.Errorf("fallback: expected 2 images from allURLs, got %d: %v", len(images), images)
	}
}

func TestScoreAndExtract_FallbackNoCaptures(t *testing.T) {
	allURLs := []string{"https://cdn.com/fallback.jpg"}
	images, _, _, fallback := scoreAndExtract(nil, nil, testImageRe, 3, true, false, allURLs)
	if !fallback {
		t.Error("expected fallback=true with empty captures")
	}
	if len(images) != 1 {
		t.Errorf("fallback no captures: expected 1 image, got %d", len(images))
	}
}

func TestScoreAndExtract_URLPatternFilter(t *testing.T) {
	// cap1's URL doesn't match the pattern; cap2 does.
	cap1 := makeJSONCapture("https://api.example.com/analytics", []string{
		"https://cdn.com/p1.jpg", "https://cdn.com/p2.jpg", "https://cdn.com/p3.jpg",
	})
	cap2 := makeJSONCapture("https://api.example.com/chapter/pages", []string{
		"https://cdn.com/a.jpg", "https://cdn.com/b.jpg", "https://cdn.com/c.jpg", "https://cdn.com/d.jpg",
	})
	patterns := []*regexp.Regexp{regexp.MustCompile(`/chapter/`)}
	images, _, _, fallback := scoreAndExtract([]ajaxCapture{cap1, cap2}, patterns, testImageRe, 3, true, false, nil)
	if fallback {
		t.Error("URL pattern filter: expected no fallback")
	}
	if len(images) != 4 {
		t.Errorf("URL pattern filter: expected 4 images from cap2 only, got %d: %v", len(images), images)
	}
}

func TestScoreAndExtract_CustomImageRegex(t *testing.T) {
	// CDN serves extensionless URLs with a format query param.
	customRe := regexp.MustCompile(`(?i)https?://[^\s"'<>]+\?[^\s"'<>]*format=webp[^\s"'<>]*`)
	b, _ := json.Marshal([]string{
		"https://cdn.com/p1?format=webp",
		"https://cdn.com/p2?format=webp",
		"https://cdn.com/p3?format=webp",
	})
	cap := ajaxCapture{URL: "https://api.example.com/pages", ContentType: "application/json", Body: string(b)}
	images, _, _, fallback := scoreAndExtract([]ajaxCapture{cap}, nil, customRe, 3, true, false, nil)
	if fallback {
		t.Error("custom regex: expected no fallback")
	}
	if len(images) != 3 {
		t.Errorf("custom regex: expected 3 images, got %d: %v", len(images), images)
	}
}

func TestScoreAndExtract_DebugCandidates(t *testing.T) {
	captures := []ajaxCapture{
		makeJSONCapture("https://api.example.com/pages", []string{
			"https://cdn.com/p1.jpg", "https://cdn.com/p2.jpg", "https://cdn.com/p3.jpg",
		}),
	}
	_, _, candidates, _ := scoreAndExtract(captures, nil, testImageRe, 3, true, true, nil)
	if len(candidates) != 1 {
		t.Fatalf("debug: expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].MatchCount != 3 {
		t.Errorf("debug: expected MatchCount=3, got %d", candidates[0].MatchCount)
	}
	if candidates[0].Format != "json" {
		t.Errorf("debug: expected Format=json, got %s", candidates[0].Format)
	}
}

// ─── buildImageRegex ──────────────────────────────────────────────────────────

func TestBuildImageRegex_Default(t *testing.T) {
	re, err := buildImageRegex("")
	if err != nil {
		t.Fatalf("buildImageRegex default: unexpected error: %v", err)
	}
	tests := []struct {
		url   string
		match bool
	}{
		{"https://cdn.com/img.jpg", true},
		{"https://cdn.com/img.jpeg", true},
		{"https://cdn.com/img.png", true},
		{"https://cdn.com/img.webp?q=90", true},
		{"https://cdn.com/img.avif", true},
		{"https://cdn.com/img.gif", true},
		{"https://cdn.com/api/data.json", false},
		{"https://cdn.com/video.mp4", false},
	}
	for _, tt := range tests {
		got := re.MatchString(tt.url)
		if got != tt.match {
			t.Errorf("regex.MatchString(%q) = %v, want %v", tt.url, got, tt.match)
		}
	}
}

func TestBuildImageRegex_InvalidOverride(t *testing.T) {
	_, err := buildImageRegex(`[invalid`)
	if err == nil {
		t.Error("expected error for invalid regex override")
	}
}

// ─── extractImageHeaders ──────────────────────────────────────────────────────

func TestExtractImageHeaders(t *testing.T) {
	h := extractImageHeaders("https://manga.example.com/chapter/42")
	if h["Referer"] != "https://manga.example.com/chapter/42" {
		t.Errorf("Referer: got %s", h["Referer"])
	}
	if h["Origin"] != "https://manga.example.com" {
		t.Errorf("Origin: got %s", h["Origin"])
	}
}

func TestExtractImageHeaders_InvalidURL(t *testing.T) {
	h := extractImageHeaders("not-a-url")
	if h != nil {
		t.Errorf("invalid URL: expected nil headers, got %v", h)
	}
}
