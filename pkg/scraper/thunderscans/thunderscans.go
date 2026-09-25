package thunderscans

import (
	"encoding/json"
	"html"
	"log"
	"net/http"
	"net/url"
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
	tsReaderPattern         = regexp.MustCompile(`(?s)ts_reader\.run\(\s*(\{.+?\})\s*\);`)
	thunderscansBannerRegex = regexp.MustCompile(`background-image:\s*url\('([^']+)'\)`)
)

var thunderscansCDNDomains = []string{
	"en-thunderscans.com",
	"thunderscans.com",
	"i0.wp.com", // WordPress CDN proxy used by some chapter images
}

type tsReaderConfig struct {
	Sources []tsReaderSource `json:"sources"`
}

type tsReaderSource struct {
	Source string   `json:"source"`
	Images []string `json:"images"`
}

// Scraper implements BrowserFetcher and CookieProvider for ThunderScans.
// Cloudflare fronts en-thunderscans.com with a JS challenge that the plain
// HTTP client cannot pass (HTTP 403 + Cf-Mitigated: challenge), so page
// fetches go through Playwright. Cookies and User-Agent from the solved
// session are cached for image downloads — Cloudflare ties cf_clearance to
// the User-Agent that solved the challenge.
type BrowserConfig = browser.Config

type Scraper struct {
	cookies      []*http.Cookie
	userAgent    string
	mu           sync.Mutex
	browserCfg   BrowserConfig
	browserQueue *browser.Queue
}

func NewScraper() *Scraper {
	return &Scraper{}
}

func NewScraperWithPlaywright(cfg BrowserConfig, q *browser.Queue) *Scraper {
	return &Scraper{browserCfg: cfg, browserQueue: q}
}

func (s *Scraper) Name() string {
	return models.SiteThunderscans
}

// FetchHTML routes page fetches through Playwright to pass the Cloudflare
// challenge, mirroring the DrakeComic implementation.
func (s *Scraper) FetchHTML(url string) (string, error) {
	log.Printf("[THUNDERSCANS-FETCH] Using Playwright to bypass Cloudflare for: %s", url)
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

func (s *Scraper) CanHandle(pageURL string) bool {
	return strings.Contains(pageURL, "en-thunderscans.com")
}

func (s *Scraper) ExtractChapters(htmlStr string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[THUNDERSCANS-EXTRACT] Starting extraction from: %s", baseURL)

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return nil, err
	}

	var chapters []models.Chapter
	seen := make(map[string]bool)

	doc.Find("#chapterlist ul > li").Each(func(_ int, li *goquery.Selection) {
		a := li.Find("a").First()
		href, hasHref := a.Attr("href")

		if _, hasModal := a.Attr("data-bs-toggle"); hasModal {
			dataNum, _ := li.Attr("data-num")
			log.Printf("[THUNDERSCANS-EXTRACT] Skipping locked chapter (data-num=%s)", dataNum)
			return
		}

		if !hasHref || href == "" {
			return
		}

		href = strings.TrimSpace(href)

		var fullURL string
		if strings.HasPrefix(href, "http") {
			fullURL = href
		} else {
			fullURL = scraper.NormalizeURLWithBase(href, baseURL)
		}

		fullURL = scraper.NormalizeURL(fullURL)

		if seen[fullURL] {
			return
		}
		seen[fullURL] = true

		dataNum, _ := li.Attr("data-num")
		var number float64
		if dataNum != "" {
			if n, err := strconv.ParseFloat(dataNum, 64); err == nil {
				number = n
			}
		}
		if number == 0 {
			number = scraper.ParseChapterNumber(fullURL)
		}

		title := strings.TrimSpace(a.Find(".chapternum").Text())
		if title == "" {
			title = strings.TrimSpace(a.Text())
		}
		if title == "" {
			title = "Chapter " + strconv.FormatFloat(number, 'f', -1, 64)
		}

		chapters = append(chapters, models.Chapter{
			ID:     scraper.GenerateID(),
			URL:    fullURL,
			Title:  title,
			Number: number,
		})
	})

	scraper.SortChaptersByNumber(chapters)

	log.Printf("[THUNDERSCANS-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

func (s *Scraper) ExtractSeriesInfo(htmlStr string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return models.SeriesInfo{Source: s.Name()}, err
	}

	title := strings.TrimSpace(doc.Find("h1").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find(".manga-title, .entry-title").First().Text())
	}

	// ThunderScans theme uses a background banner plus a wp-post-image inside the article.
	coverURL := ""
	doc.Find("img.wp-post-image").Each(func(_ int, sel *goquery.Selection) {
		if coverURL != "" {
			return
		}
		if src, ok := sel.Attr("src"); ok && src != "" {
			if isThunderscansCDN(src) {
				coverURL = src
			}
		}
	})
	if coverURL == "" {
		coverURL = scraper.ExtractMetaContent(htmlStr, "og:image")
	}
	if coverURL == "" {
		if m := thunderscansBannerRegex.FindStringSubmatch(htmlStr); len(m) > 1 {
			coverURL = m[1]
		}
	}
	if coverURL != "" && !isThunderscansCDN(coverURL) {
		coverURL = ""
	}

	status := ""
	doc.Find(".extra-info .status i, .status i").Each(func(_ int, sel *goquery.Selection) {
		if status != "" {
			return
		}
		status = strings.TrimSpace(sel.Text())
	})

	var genres []string
	doc.Find(".genres-container .mgen a, .genres-content a, a[href*=\"/genres/\"]").Each(func(_ int, sel *goquery.Selection) {
		genres = append(genres, strings.TrimSpace(sel.Text()))
	})

	description := ""
	doc.Find(".summary .entry-content, .description-summary, .summary__content").First().Each(func(_ int, sel *goquery.Selection) {
		description = strings.TrimSpace(sel.Text())
	})
	if description == "" {
		description = scraper.ExtractMetaContent(htmlStr, "og:description")
	}

	return models.SeriesInfo{
		Title:       title,
		Description: scraper.StripHTMLText(description),
		Author:      "", // ThunderScans does not list an author on the series page.
		Status:      status,
		Genres:      scraper.NormalizeGenres(genres),
		CoverURL:    scraper.NormalizeURLWithBase(coverURL, baseURL),
		Source:      s.Name(),
	}, nil
}

func (s *Scraper) ExtractImages(htmlStr string, chapterURL string) ([]models.Image, error) {
	log.Printf("[THUNDERSCANS-EXTRACT] Extracting images from: %s", chapterURL)

	images := extractImagesFromReader(htmlStr)
	if len(images) == 0 {
		images = extractImagesFromDOM(htmlStr)
	}

	if len(images) > 0 {
		scraper.SortImagesByPageNumber(images)
		images = scraper.FilterSuspectImages(images)
		for i := range images {
			images[i].Page = i + 1
		}
	}

	log.Printf("[THUNDERSCANS-EXTRACT] Total images extracted: %d", len(images))
	return images, nil
}

func extractImagesFromReader(htmlStr string) []models.Image {
	match := tsReaderPattern.FindStringSubmatch(htmlStr)
	if len(match) < 2 {
		return nil
	}

	jsonStr := html.UnescapeString(match[1])

	var config tsReaderConfig
	if err := json.Unmarshal([]byte(jsonStr), &config); err != nil {
		log.Printf("[THUNDERSCANS-EXTRACT] Failed to parse ts_reader JSON: %v", err)
		return nil
	}

	if len(config.Sources) == 0 || len(config.Sources[0].Images) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	var images []models.Image

	for iterIdx, imgURL := range config.Sources[0].Images {
		imgURL = strings.TrimSpace(imgURL)
		imgURL = scraper.NormalizeURL(imgURL)

		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			continue
		}

		if !isThunderscansCDN(imgURL) {
			log.Printf("[THUNDERSCANS-EXTRACT] Skipping non-CDN URL: %s", imgURL)
			continue
		}

		if seen[imgURL] {
			continue
		}
		seen[imgURL] = true

		var fallbackURL string
		if len(config.Sources) > 1 {
			for i := 1; i < len(config.Sources); i++ {
				if iterIdx < len(config.Sources[i].Images) {
					fb := scraper.NormalizeURL(strings.TrimSpace(config.Sources[i].Images[iterIdx]))
					if fb != "" && strings.HasPrefix(fb, "http") {
						fallbackURL = fb
						break
					}
				}
			}
		}

		images = append(images, models.Image{
			URL:         imgURL,
			FallbackURL: fallbackURL,
			Page:        len(images) + 1,
		})
	}

	return images
}

func extractImagesFromDOM(htmlStr string) []models.Image {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return nil
	}

	var images []models.Image
	seen := make(map[string]bool)

	doc.Find("img.wp-manga-chapter-img").Each(func(_ int, sel *goquery.Selection) {
		attrs := []string{"data-src", "src", "data-lazy-src", "data-original"}

		for _, attr := range attrs {
			imgURL, exists := sel.Attr(attr)
			if !exists || imgURL == "" {
				continue
			}

			imgURL = strings.TrimSpace(imgURL)
			imgURL = scraper.NormalizeURL(imgURL)

			if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
				continue
			}

			if !isThunderscansCDN(imgURL) {
				continue
			}

			if seen[imgURL] {
				continue
			}
			seen[imgURL] = true

			images = append(images, models.Image{
				URL:  imgURL,
				Page: len(images) + 1,
			})
		}
	})

	return images
}

func (s *Scraper) ExtractExpectedPageCount(htmlStr string) int {
	match := tsReaderPattern.FindStringSubmatch(htmlStr)
	if len(match) < 2 {
		return 0
	}

	jsonStr := html.UnescapeString(match[1])

	var config tsReaderConfig
	if err := json.Unmarshal([]byte(jsonStr), &config); err != nil {
		return 0
	}

	if len(config.Sources) > 0 {
		return len(config.Sources[0].Images)
	}

	return 0
}

func isThunderscansCDN(imgURL string) bool {
	parsed, err := url.Parse(imgURL)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	for _, cdn := range thunderscansCDNDomains {
		if host == cdn || strings.HasSuffix(host, "."+cdn) {
			return true
		}
	}
	return false
}
