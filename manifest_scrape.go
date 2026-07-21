package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"log"
	"net/url"
	"regexp"
	"strings"
)

const defaultImageExtPattern = `(?i)https?://[^\s"'<>]+\.(?:jpe?g|png|webp|avif|gif)(?:\?[^\s"'<>]*)?`

const defaultMinMatchCount = 3

func buildImageRegex(override string) (*regexp.Regexp, error) {
	if override != "" {
		return regexp.Compile(override)
	}
	return regexp.MustCompile(defaultImageExtPattern), nil
}

func scanJSONForImages(v interface{}, re *regexp.Regexp) []string {
	var out []string
	switch val := v.(type) {
	case string:
		if matches := re.FindAllString(val, -1); len(matches) > 0 {
			out = append(out, matches...)
		}
	case []interface{}:
		for _, elem := range val {
			out = append(out, scanJSONForImages(elem, re)...)
		}
	case map[string]interface{}:
		for _, child := range val {
			out = append(out, scanJSONForImages(child, re)...)
		}
	}
	return out
}

func scanXMLForImages(body []byte, re *regexp.Regexp) []string {
	var out []string
	dec := xml.NewDecoder(strings.NewReader(string(body)))
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.CharData:
			if matches := re.FindAllString(string(t), -1); len(matches) > 0 {
				out = append(out, matches...)
			}
		case xml.StartElement:
			// Also scan attribute values
			for _, attr := range t.Attr {
				if matches := re.FindAllString(attr.Value, -1); len(matches) > 0 {
					out = append(out, matches...)
				}
			}
		}
	}
	return out
}

type candidateResult struct {
	capture    ajaxCapture
	images     []string
	format     string
	matchCount int
}

func scoreCapture(cap ajaxCapture, re *regexp.Regexp) candidateResult {
	res := candidateResult{
		capture: cap,
		format:  "unknown",
	}

	body := []byte(cap.Body)
	if len(body) == 0 {
		return res
	}

	var parsed interface{}
	if err := json.Unmarshal(body, &parsed); err == nil {
		images := scanJSONForImages(parsed, re)
		res.format = "json"
		res.images = images
		res.matchCount = len(images)
		return res
	}

	images := scanXMLForImages(body, re)
	if len(images) > 0 {
		res.format = "xml"
		res.images = images
		res.matchCount = len(images)
	}
	return res
}

func matchesURLPatterns(rawURL string, patterns []*regexp.Regexp) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if p.MatchString(rawURL) {
			return true
		}
	}
	return false
}

func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		compiled, err := regexp.Compile(p)
		if err != nil {
			return nil, err
		}
		out = append(out, compiled)
	}
	return out, nil
}

func scoreAndExtract(
	captures []ajaxCapture,
	urlPatterns []*regexp.Regexp,
	imageRe *regexp.Regexp,
	minMatch int,
	accumulate bool,
	wantCandidates bool,
	allURLs []string,
) (images []string, manifestURLs []string, candidates []ManifestCandidate, fallbackUsed bool) {

	scored := make([]candidateResult, 0, len(captures))
	for _, cap := range captures {
		if !matchesURLPatterns(cap.URL, urlPatterns) {
			continue
		}
		r := scoreCapture(cap, imageRe)
		scored = append(scored, r)
		if wantCandidates {
			candidates = append(candidates, ManifestCandidate{
				URL:         cap.URL,
				ContentType: cap.ContentType,
				MatchCount:  r.matchCount,
				Format:      r.format,
			})
		}
	}

	bestScore := 0
	for _, r := range scored {
		if r.matchCount > bestScore {
			bestScore = r.matchCount
		}
	}

	if bestScore < minMatch {
		fallbackUsed = true
		imageReFallback := regexp.MustCompile(defaultImageExtPattern)
		seen := make(map[string]struct{})
		for _, u := range allURLs {
			if imageReFallback.MatchString(u) {
				if _, ok := seen[u]; !ok {
					seen[u] = struct{}{}
					images = append(images, u)
				}
			}
		}
		return
	}

	seen := make(map[string]struct{})
	addImages := func(imgs []string, manifestURL string) {
		manifestURLs = append(manifestURLs, manifestURL)
		for _, img := range imgs {
			if _, ok := seen[img]; !ok {
				seen[img] = struct{}{}
				images = append(images, img)
			}
		}
	}

	if accumulate {
		for _, r := range scored {
			if r.matchCount >= minMatch {
				addImages(r.images, r.capture.URL)
			}
		}
	} else {
		for _, r := range scored {
			if r.matchCount == bestScore {
				addImages(r.images, r.capture.URL)
				break
			}
		}
	}

	return
}

func extractImageHeaders(pageURL string) map[string]string {
	headers := make(map[string]string)
	parsed, err := url.Parse(pageURL)
	if err != nil || parsed.Host == "" {
		return nil
	}
	origin := parsed.Scheme + "://" + parsed.Host
	headers["Referer"] = pageURL
	headers["Origin"] = origin
	return headers
}

func scrapeManifest(ctx context.Context, req ManifestScrapeRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub) (ManifestScrapeResponse, error) {
	minMatch := req.MinMatchCount
	if minMatch <= 0 {
		minMatch = defaultMinMatchCount
	}

	accumulate := true
	if req.AccumulateAll != nil {
		accumulate = *req.AccumulateAll
	}

	imageRe, err := buildImageRegex(req.ImageExtRegex)
	if err != nil {
		return ManifestScrapeResponse{Error: "invalid image_ext_regex: " + err.Error()}, nil
	}

	urlPatterns, err := compilePatterns(req.URLPatterns)
	if err != nil {
		return ManifestScrapeResponse{Error: "invalid url_patterns: " + err.Error()}, nil
	}

	taskReq := TaskRequest{
		URL:              req.URL,
		WaitMs:           req.WaitMS,
		Headers:          req.Headers,
		LocalStorage:     req.LocalStorage,
		Actions:          req.Actions,
		IsManifestScrape: true,
		Debug:            true,
	}

	log.Printf("[ManifestScraper] Starting scrape: %s", req.URL)

	_, allURLs, ajaxCaptures, scrapeErr := scrapeManifestExtension(ctx, taskReq, captureHub, jobHub)
	if scrapeErr != nil {
		log.Printf("[ManifestScraper] Scrape error: %v", scrapeErr)
	}

	log.Printf("[ManifestScraper] Collected %d AJAX captures, %d total URLs", len(ajaxCaptures), len(allURLs))

	images, manifestURLs, candidates, fallbackUsed := scoreAndExtract(
		ajaxCaptures,
		urlPatterns,
		imageRe,
		minMatch,
		accumulate,
		req.Debug,
		allURLs,
	)

	log.Printf("[ManifestScraper] Result: %d images, fallback=%v", len(images), fallbackUsed)

	resp := ManifestScrapeResponse{
		Images:       images,
		ManifestURLs: manifestURLs,
		FallbackUsed: fallbackUsed,
		Headers:      extractImageHeaders(req.URL),
	}
	if req.Debug {
		resp.Candidates = candidates
	}
	if scrapeErr != nil {
		resp.Error = scrapeErr.Error()
	}

	if resp.Images == nil {
		resp.Images = []string{}
	}
	if resp.ManifestURLs == nil {
		resp.ManifestURLs = []string{}
	}

	return resp, nil
}
