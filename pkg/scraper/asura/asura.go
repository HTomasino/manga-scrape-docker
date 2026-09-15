package asura

import (
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
	"github.com/user/comic-scraper/pkg/scraper/browser"
)

var (
	asuraChapterPattern = regexp.MustCompile(`/chapter[/-]\d+(?:\.\d+)?`)
	asuraPagesPattern   = regexp.MustCompile(`(?:"|&quot;)pages(?:"|&quot;)\s*:\s*\[\d+,\s*(\[\[.+?\]\])\]`)
	asuraURLPattern     = regexp.MustCompile(`"url"\s*:\s*\[\s*\d+\s*,\s*"([^"]+)"\s*\]`)
	asuraCDNPattern     = regexp.MustCompile(`https?://[^\s"'<>]+asura-images/chapters/[^\s"'<>]+\.(?:webp|jpg|jpeg|png|gif)`)
)

// BrowserConfig is an alias for browser.Config for convenient construction.
type BrowserConfig = browser.Config

// Scraper implements BrowserFetcher and CookieProvider for AsuraScans.
// It caches cookies and User-Agent from the Playwright session so they can be
// injected into the plain HTTP client for image downloads.
type Scraper struct {
	cookies      []*http.Cookie
	userAgent    string
	mu           sync.Mutex
	browserCfg   BrowserConfig
	browserQueue *browser.Queue
}

// NewScraper creates a new AsuraScans scraper without Playwright support.
func NewScraper() *Scraper {
	return &Scraper{}
}

// NewScraperWithPlaywright creates a scraper with Playwright support for
// bypassing Cloudflare and rendering lazy-loaded images.
func NewScraperWithPlaywright(cfg BrowserConfig, q *browser.Queue) *Scraper {
	return &Scraper{browserCfg: cfg, browserQueue: q}
}

// Name returns the site name
func (s *Scraper) Name() string {
	return "asurascans"
}

// CanHandle checks if this scraper can handle the given URL
func (s *Scraper) CanHandle(url string) bool {
	return strings.Contains(url, "asurascans")
}

// FetchHTML fetches page content using Playwright to bypass Cloudflare and
// render lazy-loaded images. AsuraScans heavily uses lazy loading, so we
// must scroll the page to trigger all image loads before extracting content.
func (s *Scraper) FetchHTML(url string) (string, error) {
	log.Printf("[ASURA-FETCH] Using Playwright to bypass Cloudflare for: %s", url)
	html, cookies, userAgent, err := FetchWithPlaywright(url, s.browserCfg, s.browserQueue)
	if err != nil {
		return "", err
	}

	// Cache cookies and User-Agent for subsequent image downloads
	s.mu.Lock()
	s.cookies = cookies
	s.userAgent = userAgent
	s.mu.Unlock()

	return html, nil
}

// GetCookies returns the Cloudflare cookies from the last Playwright session.
// These must be injected into the HTTP client for image downloads to succeed.
func (s *Scraper) GetCookies() []*http.Cookie {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cookies
}

// GetUserAgent returns the User-Agent that was used in the Playwright session.
// Cloudflare ties cf_clearance to the UA that solved the challenge.
func (s *Scraper) GetUserAgent() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.userAgent
}

// ExtractChapters extracts chapters from the series page HTML
func (s *Scraper) ExtractChapters(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[ASURA-EXTRACT] Starting extraction from: %s", baseURL)

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

		// Only include URLs that match the chapter pattern
		if !asuraChapterPattern.MatchString(fullURL) {
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

	log.Printf("[ASURA-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

// ExtractImages extracts image URLs from a chapter page
// The HTML should already be fully rendered (via FetchHTML/Playwright) which
// scrolls the page to trigger lazy loading, so all image src attributes are
// populated.
func (s *Scraper) ExtractImages(html string, chapterURL string) ([]models.Image, error) {
	var images []models.Image
	seen := make(map[string]bool)

	log.Printf("[ASURA-EXTRACT] Extracting images from: %s", chapterURL)

	jsonMatch := asuraPagesPattern.FindStringSubmatch(html)
	if len(jsonMatch) > 1 {
		jsonStr := strings.ReplaceAll(jsonMatch[0], "&quot;", "\"")
		jsonStr = strings.ReplaceAll(jsonStr, "&amp;", "&")

		urls := asuraURLPattern.FindAllStringSubmatch(jsonStr, -1)
		for _, u := range urls {
			if len(u) > 1 {
				imgURL := u[1]
				if strings.HasPrefix(imgURL, "http") &&
					(strings.Contains(imgURL, "asura") || strings.Contains(imgURL, "cdn")) {
					imgURL = scraper.NormalizeURL(imgURL)
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
		}
		if len(images) > 0 {
			scraper.SortImagesByPageNumber(images)
			images = scraper.FilterSuspectImages(images)
			for i := range images {
				images[i].Page = i + 1
			}
			return images, nil
		}
	}

	// Pattern 2: Direct URL extraction from Asura CDN domains in HTML/JSON
	// This catches various formats including simple JSON arrays and HTML attributes
	asuraURLs := asuraCDNPattern.FindAllString(html, -1)
	for _, imgURL := range asuraURLs {
		imgURL = strings.TrimRight(imgURL, "\\")
		imgURL = scraper.NormalizeURL(imgURL)
		// Skip cover, profile, and comment images
		if strings.Contains(imgURL, "/covers/") ||
			strings.Contains(imgURL, "/profiles/") ||
			strings.Contains(imgURL, "/comments/") {
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
	if len(images) > 0 {
		scraper.SortImagesByPageNumber(images)
		images = scraper.FilterSuspectImages(images)
		for i := range images {
			images[i].Page = i + 1
		}
		return images, nil
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	// Valid Asura CDN domains
	validCDNs := []string{
		"asurascans.com",
		"asura-images",
		"cdn.asurascans.com",
	}

	doc.Find("img").Each(func(_ int, selection *goquery.Selection) {
		attrs := []string{"src", "data-src", "data-lazy-src", "data-original", "data-srcset"}

		for _, attr := range attrs {
			imgURL, exists := selection.Attr(attr)
			if !exists || imgURL == "" {
				continue
			}

			// Skip placeholders and ads
			lower := strings.ToLower(imgURL)
			if strings.Contains(lower, "placeholder") ||
				strings.Contains(lower, "advertisement") ||
				strings.Contains(lower, "lazy") {
				continue
			}

			imgURL = scraper.NormalizeURL(imgURL)
			if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
				continue
			}

			// Check if URL is from valid Asura CDN
			validCDN := false
			for _, cdn := range validCDNs {
				if strings.Contains(imgURL, cdn) {
					validCDN = true
					break
				}
			}
			if !validCDN {
				continue
			}

			// Skip cover, profile, and comment images
			if strings.Contains(imgURL, "/covers/") ||
				strings.Contains(imgURL, "/profiles/") ||
				strings.Contains(imgURL, "/comments/") {
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
	})

	log.Printf("[ASURA-EXTRACT] Total images extracted: %d", len(images))

	if len(images) > 0 {
		scraper.SortImagesByPageNumber(images)
		images = scraper.FilterSuspectImages(images)
		for i := range images {
			images[i].Page = i + 1
		}
	}

	return images, nil
}

// ExtractSeriesInfo extracts series-level metadata from an Asura series page.
// Asura uses Astro-generated markup; metadata is available via schema.org
// JSON-LD as well as through modern DOM classes.
func (s *Scraper) ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return models.SeriesInfo{Source: s.Name()}, err
	}

	info := models.SeriesInfo{Source: s.Name()}

	// 1. Try schema.org ComicSeries JSON-LD first (most reliable on Astro pages).
	doc.Find("script[type=\"application/ld+json\"]").Each(func(_ int, sel *goquery.Selection) {
		if info.Title != "" && len(info.Genres) > 0 && info.Author != "" && info.Status != "" {
			return
		}
		jsonText := sel.Text()
		if !strings.Contains(jsonText, "ComicSeries") {
			return
		}
		info = mergeFromAsuraJSONLD(info, jsonText)
	})

	// 2. Fallback to meta tags / DOM.
	if info.Title == "" {
		info.Title = scraper.ExtractMetaContent(html, "og:title")
	}
	if info.Title == "" {
		info.Title = strings.TrimSpace(doc.Find("h1").First().Text())
	}

	if info.CoverURL == "" {
		info.CoverURL = scraper.ExtractMetaContent(html, "og:image")
	}
	if info.CoverURL != "" && !isAsuraCover(info.CoverURL) {
		info.CoverURL = ""
	}

	if info.Author == "" {
		info.Author = strings.TrimSpace(doc.Find("a[href*=\"/browse?author=\"]").First().Text())
	}
	if info.Author == "" {
		info.Author = extractAsuraLabelValue(doc, "Author")
	}

	if info.Status == "" {
		info.Status = extractAsuraLabelValue(doc, "Status")
	}
	// Astro status is rendered inside a .capitalize span, but genre chips can
	// share the class — only accept text that actually names a status.
	if info.Status == "" {
		doc.Find(".capitalize").Each(func(_ int, sel *goquery.Selection) {
			text := strings.TrimSpace(sel.Text())
			for _, word := range []string{"Ongoing", "Completed", "Hiatus", "Dropped", "Cancelled"} {
				if strings.Contains(text, word) {
					info.Status = text
					return
				}
			}
		})
	}

	if len(info.Genres) == 0 {
		doc.Find("a[href*=\"/browse?genres=\"], .genres-content a, .manga-info-row a[href*=\"genre\"]").Each(func(_ int, sel *goquery.Selection) {
			info.Genres = append(info.Genres, strings.TrimSpace(sel.Text()))
		})
	}

	if info.Description == "" {
		description := scraper.ExtractMetaContent(html, "description")
		if description == "" {
			description = scraper.ExtractMetaContent(html, "og:description")
		}
		info.Description = description
	}

	info.Genres = scraper.NormalizeGenres(info.Genres)
	info.Description = scraper.StripHTMLText(info.Description)
	return info, nil
}

// mergeFromAsuraJSONLD populates a SeriesInfo from a schema.org ComicSeries
// JSON-LD block. It performs only partial parsing to avoid adding heavy deps.
func mergeFromAsuraJSONLD(info models.SeriesInfo, jsonText string) models.SeriesInfo {
	// Genre array.
	if m := regexp.MustCompile(`"genre"\s*:\s*\[\s*([^\]]+)\s*\]`).FindStringSubmatch(jsonText); len(m) > 1 {
		for _, g := range regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(m[1], -1) {
			info.Genres = append(info.Genres, g[1])
		}
	}
	// Author object.
	if m := regexp.MustCompile(`"author"\s*:\s*\{\s*"@type"\s*:\s*"Person"\s*,\s*"name"\s*:\s*"([^"]+)"`).FindStringSubmatch(jsonText); len(m) > 1 {
		info.Author = m[1]
	}
	// Description. Allow escaped quotes inside the JSON string value.
	if m := regexp.MustCompile(`"description"\s*:\s*"((?:[^"\\]|\\.)*)"`).FindStringSubmatch(jsonText); len(m) > 1 {
		info.Description = m[1]
	}
	// Name (use only if cleaner than og:title).
	if m := regexp.MustCompile(`"name"\s*:\s*"([^"]+)"`).FindStringSubmatch(jsonText); len(m) > 1 {
		info.Title = m[1]
	}
	// Status from JSON-LD if present.
	if m := regexp.MustCompile(`"creativeWorkStatus"\s*:\s*"([^"]+)"`).FindStringSubmatch(jsonText); len(m) > 1 {
		info.Status = m[1]
	}
	// Cover from JSON-LD image.
	if m := regexp.MustCompile(`"image"\s*:\s*"([^"]+)"`).FindStringSubmatch(jsonText); len(m) > 1 {
		if isAsuraCover(m[1]) {
			info.CoverURL = m[1]
		}
	}
	return info
}

func isAsuraCover(imgURL string) bool {
	return strings.Contains(imgURL, "asura") || strings.Contains(imgURL, "cdn")
}

// extractAsuraLabelValue finds a small label span (e.g., "Author") and returns
// the next sibling link/text.
func extractAsuraLabelValue(doc *goquery.Document, label string) string {
	var value string
	labelLower := strings.ToLower(label)
	doc.Find("span").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		if strings.ToLower(strings.TrimSpace(s.Text())) == labelLower {
			sel := s.Parent()
			if sel.Length() == 0 {
				sel = s
			}
			nextLink := sel.Next().Find("a").First()
			if nextLink.Length() == 0 {
				nextLink = sel.Next()
			}
			if nextLink.Length() > 0 {
				value = strings.TrimSpace(nextLink.Text())
				return false
			}
		}
		return true
	})
	return value
}

func (s *Scraper) ExtractExpectedPageCount(html string) int {
	jsonMatch := asuraPagesPattern.FindStringSubmatch(html)
	if len(jsonMatch) < 2 {
		return 0
	}

	jsonStr := strings.ReplaceAll(jsonMatch[0], "&quot;", "\"")
	jsonStr = strings.ReplaceAll(jsonStr, "&amp;", "&")

	urls := asuraURLPattern.FindAllStringSubmatch(jsonStr, -1)
	count := 0
	for _, u := range urls {
		if len(u) > 1 {
			imgURL := u[1]
			if strings.HasPrefix(imgURL, "http") &&
				(strings.Contains(imgURL, "asura") || strings.Contains(imgURL, "cdn")) {
				count++
			}
		}
	}
	return count
}
