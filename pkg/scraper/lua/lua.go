package lua

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
)

var (
	luaChapterRegex = regexp.MustCompile(`/chapter[-\s/]?\d+`)
	totalPagesRegex = regexp.MustCompile(`(?i)"totalPages"\s*:\s*(\d+)`)
	luaJSONURLRegex = regexp.MustCompile(`\{"url"\s*:\s*"([^"]+)","width"\s*:\s*\d+,"height"\s*:\s*\d+\}`)
	luaMediaRegex   = regexp.MustCompile(`(?:https://)?media\.luacomic\.org/[^"'\s>]+`)
)

// Scraper implements the scraper.Scraper interface for LuaComic
type Scraper struct {
	apiBaseURL    string
	usePlaywright bool
}

// NewScraper creates a new LuaComic scraper
func NewScraper() *Scraper {
	return &Scraper{
		apiBaseURL:    "https://api.luacomic.org",
		usePlaywright: false,
	}
}

// Name returns the site name
func (s *Scraper) Name() string {
	return "luacomic"
}

// CanHandle checks if this scraper can handle the given URL
func (s *Scraper) CanHandle(url string) bool {
	return strings.Contains(url, "luacomic")
}

// ExtractChapters extracts chapters from the series page HTML
func (s *Scraper) ExtractChapters(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[LUA-EXTRACT] Starting extraction from: %s", baseURL)

	// Check if page has static chapter links (if it says "Loading..." it needs JS)
	if strings.Contains(html, "Loading...") || strings.Contains(html, "loading") {
		log.Printf("[LUA-EXTRACT] Page appears to need JS rendering, will use API")
	}

	// Extract series slug from URL and fetch series ID from API
	seriesSlug := extractSeriesSlug(baseURL)
	if seriesSlug != "" {
		log.Printf("[LUA-EXTRACT] Extracted series slug: %s", seriesSlug)
		seriesID, err := s.fetchSeriesID(seriesSlug)
		if err == nil && seriesID != "" {
			log.Printf("[LUA-EXTRACT] Fetched series ID from API: %s", seriesID)
			apiChapters, err := s.fetchChaptersFromAPI(seriesID, seriesSlug)
			if err == nil && len(apiChapters) > 0 {
				return apiChapters, nil
			}
			log.Printf("[LUA-EXTRACT] API fetch failed: %v", err)
		} else {
			log.Printf("[LUA-EXTRACT] Failed to fetch series ID: %v", err)
		}
	}

	// Extract from static HTML links as fallback
	return s.extractChaptersFromHTML(html, baseURL)
}

// extractSeriesSlug extracts the series slug from the URL
// e.g., https://luacomic.org/series/sigrid -> sigrid
func extractSeriesSlug(url string) string {
	// Find the last path segment
	if idx := strings.LastIndex(url, "/"); idx != -1 && idx < len(url)-1 {
		return url[idx+1:]
	}
	return ""
}

// fetchSeriesID fetches the series ID from the API using the slug
func (s *Scraper) fetchSeriesID(slug string) (string, error) {
	url := fmt.Sprintf("%s/series/%s", s.apiBaseURL, slug)
	log.Printf("[LUA-API] Fetching series info from: %s", url)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	// Set headers to match browser request
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Referer", "https://luacomic.org/")
	req.Header.Set("Origin", "https://luacomic.org")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var seriesData struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(body, &seriesData); err != nil {
		return "", err
	}

	if seriesData.ID == 0 {
		return "", fmt.Errorf("no series ID found in response")
	}

	return strconv.Itoa(seriesData.ID), nil
}

// extractChaptersFromHTML extracts chapters from HTML when API fails
func (s *Scraper) extractChaptersFromHTML(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[LUA-EXTRACT] Falling back to HTML link extraction")

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	var chapters []models.Chapter
	seen := make(map[string]bool)

	doc.Find("a[href]").Each(func(_ int, selection *goquery.Selection) {
		href, exists := selection.Attr("href")
		if !exists || href == "" {
			return
		}

		var fullURL string
		if strings.HasPrefix(href, "http") {
			fullURL = href
		} else {
			fullURL = scraper.NormalizeURLWithBase(href, baseURL)
		}

		if !luaChapterRegex.MatchString(fullURL) {
			return
		}

		if seen[fullURL] {
			return
		}
		seen[fullURL] = true

		title := strings.TrimSpace(selection.Text())
		if title == "" {
			title = selection.Find("span, div").First().Text()
			title = strings.TrimSpace(title)
		}

		if title == "" {
			num := scraper.ParseChapterNumber(fullURL)
			title = "Chapter " + strconv.FormatFloat(num, 'f', -1, 64)
		}

		number := scraper.ParseChapterNumber(fullURL)
		if number == 0 {
			number = scraper.ParseChapterNumber(title)
		}

		chapters = append(chapters, models.Chapter{
			ID:     scraper.GenerateID(),
			URL:    fullURL,
			Title:  title,
			Number: number,
		})
	})

	// Sort by chapter number descending (newest first)
	scraper.SortChaptersByNumber(chapters)
	return chapters, nil
}

// fetchChaptersFromAPI fetches chapters from the LuaComic API
func (s *Scraper) fetchChaptersFromAPI(seriesID string, seriesSlug string) ([]models.Chapter, error) {
	log.Printf("[LUA-API] Starting fetchChaptersFromAPI for seriesID: %s, seriesSlug: %s", seriesID, seriesSlug)

	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return nil
		},
	}

	headers := map[string]string{
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Accept":          "application/json, text/plain, */*",
		"Accept-Language": "en-US,en;q=0.9",
		"Referer":         "https://luacomic.org/",
		"Origin":          "https://luacomic.org",
	}

	id, err := strconv.Atoi(seriesID)
	if err != nil {
		return nil, err
	}

	// Paginate through all pages — a single page caps at 200 chapters, so
	// long series would otherwise be silently truncated.
	const perPage = 200
	var allChapters []models.Chapter
	for page := 1; page <= 100; page++ { // hard cap: 100 pages = 20k chapters
		chaptersURL := fmt.Sprintf("%s/chapter/query?page=%d&perPage=%d&order=desc&series_id=%d", s.apiBaseURL, page, perPage, id)
		if page == 1 {
			log.Printf("[LUA-API] Fetching chapters from: %s", chaptersURL)
		}

		req, err := http.NewRequest("GET", chaptersURL, nil)
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		chapters, rawCount, err := s.parseChaptersResponse(body, seriesSlug)
		if err != nil {
			return nil, err
		}
		allChapters = append(allChapters, chapters...)

		// Stop when a short RAW page signals the end of the list. The raw row
		// count is used (not the parsed chapter count) because rows with
		// unparseable numbers are skipped during parsing.
		if rawCount < perPage {
			break
		}
	}
	return allChapters, nil
}

// parseChaptersResponse parses the API response trying multiple JSON structures.
// It returns the parsed chapters plus the RAW number of rows in the response
// (including skipped rows) so pagination callers can detect the last page.
func (s *Scraper) parseChaptersResponse(body []byte, seriesSlug string) ([]models.Chapter, int, error) {
	var chapters []models.Chapter

	// Try with "chapters" key first
	var chaptersResp struct {
		Chapters []struct {
			ChapterName  string `json:"chapter_name"`
			ChapterTitle string `json:"chapter_title"`
			ChapterSlug  string `json:"chapter_slug"`
		} `json:"chapters"`
	}

	if err := json.Unmarshal(body, &chaptersResp); err == nil && len(chaptersResp.Chapters) > 0 {
		rawCount := len(chaptersResp.Chapters)
		for _, ch := range chaptersResp.Chapters {
			number := scraper.ParseChapterNumber(ch.ChapterName)
			if number == 0 {
				number = scraper.ParseChapterNumber(ch.ChapterSlug)
			}
			if number == 0 {
				// A DB row ID is not a chapter number; using it would corrupt
				// ordering and dedupe. Skip and warn.
				log.Printf("[LUA-API] Skipping chapter %q: unparseable number (name=%q, slug=%q)", ch.ChapterSlug, ch.ChapterName, ch.ChapterSlug)
				continue
			}
			chapters = append(chapters, models.Chapter{
				ID:     scraper.GenerateID(),
				URL:    fmt.Sprintf("https://luacomic.org/series/%s/%s", seriesSlug, ch.ChapterSlug),
				Title:  ch.ChapterName,
				Number: number,
			})
		}
		return chapters, rawCount, nil
	}

	// Try alternative JSON path with "data" key
	var altResp struct {
		Data []struct {
			ChapterName  string `json:"chapter_name"`
			ChapterTitle string `json:"chapter_title"`
			ChapterSlug  string `json:"chapter_slug"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &altResp); err == nil && len(altResp.Data) > 0 {
		rawCount := len(altResp.Data)
		for _, ch := range altResp.Data {
			number := scraper.ParseChapterNumber(ch.ChapterName)
			if number == 0 {
				number = scraper.ParseChapterNumber(ch.ChapterSlug)
			}
			if number == 0 {
				log.Printf("[LUA-API] Skipping chapter %q: unparseable number (name=%q, slug=%q)", ch.ChapterSlug, ch.ChapterName, ch.ChapterSlug)
				continue
			}
			chapters = append(chapters, models.Chapter{
				ID:     scraper.GenerateID(),
				URL:    fmt.Sprintf("https://luacomic.org/series/%s/%s", seriesSlug, ch.ChapterSlug),
				Title:  ch.ChapterName,
				Number: number,
			})
		}
		return chapters, rawCount, nil
	}

	return nil, 0, fmt.Errorf("no chapters found in API response")
}

// ExtractImages extracts image URLs from a chapter page
func (s *Scraper) ExtractImages(html string, chapterURL string) ([]models.Image, error) {
	var images []models.Image
	seen := make(map[string]bool)

	log.Printf("[LUA-EXTRACT] Extracting images from: %s", chapterURL)

	// Try to find JSON data containing image URLs
	jsonMatches := luaJSONURLRegex.FindAllStringSubmatch(html, -1)

	for _, match := range jsonMatches {
		if len(match) < 2 {
			continue
		}
		imgURL := match[1]

		// The regex already captures the decoded JSON string value; a second
		// QueryUnescape would corrupt URLs containing '+' or '%'.
		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			continue
		}

		if seen[imgURL] {
			continue
		}
		seen[imgURL] = true

		images = append(images, models.Image{
			URL:     imgURL,
			Page:    len(images) + 1,
			Quality: "original",
		})
	}

	// Also try media.luacomic.org pattern directly
	if len(images) == 0 {
		mediaMatches := luaMediaRegex.FindAllString(html, -1)

		for _, imgURL := range mediaMatches {
			imgURL = strings.TrimSuffix(imgURL, "\\")
			if idx := strings.Index(imgURL, "?"); idx != -1 {
				imgURL = imgURL[:idx]
			}
			if idx := strings.Index(imgURL, "#"); idx != -1 {
				imgURL = imgURL[:idx]
			}
			if !strings.HasPrefix(imgURL, "http") {
				imgURL = "https://" + imgURL
			}

			if imgURL == "" || imgURL == "https://" {
				continue
			}
			if seen[imgURL] {
				continue
			}
			seen[imgURL] = true

			images = append(images, models.Image{
				URL:     imgURL,
				Page:    len(images) + 1,
				Quality: "original",
			})
		}
	}

	// Sort images by page number to preserve extraction order.
	// JSON and regex extraction follow the page's source order, which matches
	// the correct reading order. Sorting by URL structure can produce incorrect
	// page sequences when URLs don't have naturally ordered filenames.
	scraper.SortImagesByPageNumber(images)
	for i := range images {
		images[i].Page = i + 1
	}

	log.Printf("[LUA-EXTRACT] Total images extracted: %d", len(images))
	return images, nil
}

// ExtractSeriesInfo extracts series-level metadata from a LuaComic series page.
func (s *Scraper) ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return models.SeriesInfo{Source: s.Name()}, err
	}

	title := strings.TrimSpace(doc.Find("h1").First().Text())
	if title == "" {
		title = scraper.ExtractMetaContent(html, "og:title")
	}

	coverURL := scraper.ExtractMetaContent(html, "og:image")
	if coverURL != "" && !isLuaCover(coverURL) {
		coverURL = ""
	}

	// Next.js app-router pages render status as a plain uppercase badge.
	// Scope to status-like elements first; the raw substring scan is a last
	// resort because chapter titles can contain status words.
	status := ""
	if badge := strings.TrimSpace(doc.Find(".status, [class*='status']").First().Text()); badge != "" {
		for _, word := range []string{"Ongoing", "Completed", "Hiatus", "Dropped"} {
			if strings.Contains(badge, word) {
				status = word
				break
			}
		}
	}
	if status == "" {
		for _, word := range []string{"Ongoing", "Completed", "Hiatus", "Dropped"} {
			if strings.Contains(html, word) {
				status = word
				break
			}
		}
	}

	var genres []string
	doc.Find(".genres-content a, a[href*=\"genres\"]").Each(func(_ int, sel *goquery.Selection) {
		genres = append(genres, strings.TrimSpace(sel.Text()))
	})

	// The page is rendered from Next.js flight data; the full synopsis is in
	// the meta description tag. Strip the site prefix so it starts with the
	// actual summary.
	description := ""
	for _, meta := range []string{"description", "og:description"} {
		value := scraper.ExtractMetaContent(html, meta)
		if value != "" {
			description = stripLuaDescriptionPrefix(value)
			break
		}
	}

	return models.SeriesInfo{
		Title:       title,
		Description: scraper.StripHTMLText(description),
		Author:      "",
		Status:      status,
		Genres:      scraper.NormalizeGenres(genres),
		CoverURL:    coverURL,
		Source:      s.Name(),
	}, nil
}

// stripLuaDescriptionPrefix removes the "Read <Title> on Lua Comic - "
// prefix that Lua adds to meta descriptions.
func stripLuaDescriptionPrefix(value string) string {
	value = strings.TrimSpace(value)
	if idx := strings.Index(value, " on Lua Comic - "); idx >= 0 {
		return strings.TrimSpace(value[idx+len(" on Lua Comic - "):])
	}
	return value
}

func isLuaCover(imgURL string) bool {
	return strings.Contains(imgURL, "media.luacomic.org") || strings.Contains(imgURL, "luacomic.org")
}

func (s *Scraper) ExtractExpectedPageCount(html string) int {
	if m := totalPagesRegex.FindStringSubmatch(html); len(m) > 1 {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			return n
		}
	}

	matches := luaJSONURLRegex.FindAllStringSubmatch(html, -1)
	seen := make(map[string]bool)
	count := 0
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		imgURL := m[1]
		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			continue
		}
		if seen[imgURL] {
			continue
		}
		seen[imgURL] = true
		count++
	}
	return count
}
