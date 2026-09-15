package drake

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
	"github.com/user/comic-scraper/pkg/scraper/browser"
)

var (
	// Chapter URLs on DrakeComic follow two patterns:
	//   Current:  drakecomic.org/{series-slug}-chapter-{num}/
	//   Legacy:   drakecomic.org/{id}-{seq}/
	drakeChapterPattern = regexp.MustCompile(`drakecomic\.org/[^/]+-chapter-\d+|drakecomic\.org/\d+-\d+`)
	// Dot-all and non-greedy so the capture stops at the first closing paren
	// even on minified pages where JS follows the call (mirrors thunderscans).
	tsReaderPattern = regexp.MustCompile(`(?s)ts_reader\.run\(\s*(\{.+?\})\s*\)`)
	i0wpPrefix      = "i0.wp.com/"
)

// Scraper implements BrowserFetcher and CookieProvider for DrakeComic.
// It caches Cloudflare cookies and User-Agent from the Playwright session so they
// can be injected into the plain HTTP client for image downloads. Cloudflare ties
// the cf_clearance cookie to the User-Agent that solved the challenge.
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
	return "drake"
}

func (s *Scraper) CanHandle(url string) bool {
	return strings.Contains(url, "drakecomic")
}

func (s *Scraper) FetchHTML(url string) (string, error) {
	log.Printf("[DRAKE-FETCH] Using Playwright to bypass Cloudflare for: %s", url)
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

func (s *Scraper) ExtractChapters(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[DRAKE-EXTRACT] Starting extraction from: %s", baseURL)

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	var chapters []models.Chapter
	seen := make(map[string]bool)

	// DrakeComic uses #chapterlist for the chapter list on series pages.
	// Each <li> contains an <a> with the chapter URL.
	doc.Find("#chapterlist li a[href], li.wp-manga-chapter a[href]").Each(func(_ int, selection *goquery.Selection) {
		href, exists := selection.Attr("href")
		if !exists || href == "" {
			return
		}

		href = strings.TrimSpace(href)

		var fullURL string
		if strings.HasPrefix(href, "http") {
			fullURL = href
		} else {
			fullURL = scraper.NormalizeURLWithBase(href, baseURL)
		}

		if !drakeChapterPattern.MatchString(fullURL) {
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

		number := scraper.ParseChapterNumber(title)
		if number == 0 {
			number = scraper.ParseChapterNumber(fullURL)
		}

		chapters = append(chapters, models.Chapter{
			ID:     scraper.GenerateID(),
			URL:    fullURL,
			Title:  title,
			Number: number,
		})
	})

	scraper.SortChaptersByNumber(chapters)

	log.Printf("[DRAKE-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

func (s *Scraper) ExtractImages(html string, chapterURL string) ([]models.Image, error) {
	log.Printf("[DRAKE-EXTRACT] Extracting images from: %s", chapterURL)

	images := extractFromTSReader(html)
	if len(images) > 0 {
		log.Printf("[DRAKE-EXTRACT] Extracted %d images from ts_reader.run()", len(images))
		return dedupeAndSort(images), nil
	}

	images = extractFromNoscript(html)
	if len(images) > 0 {
		log.Printf("[DRAKE-EXTRACT] Extracted %d images from noscript fallback", len(images))
		return dedupeAndSort(images), nil
	}

	images = extractFromImgTags(html)
	if len(images) > 0 {
		log.Printf("[DRAKE-EXTRACT] Extracted %d images from img tags", len(images))
		return dedupeAndSort(images), nil
	}

	log.Printf("[DRAKE-EXTRACT] No images found")
	return nil, nil
}

func (s *Scraper) ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return models.SeriesInfo{Source: s.Name()}, err
	}

	title := scraper.ExtractMetaContent(html, "og:title")
	if title == "" {
		title = strings.TrimSpace(doc.Find("h1").First().Text())
	}

	coverURL := scraper.ExtractMetaContent(html, "og:image")
	if coverURL == "" {
		doc.Find("article .wp-manga-portfolio img, .summary_image img").Each(func(_ int, sel *goquery.Selection) {
			if coverURL != "" {
				return
			}
			coverURL = s.firstImageAttr(sel)
		})
	}
	if coverURL != "" && !isValidDrakeCover(coverURL) {
		coverURL = ""
	}

	author := ""
	doc.Find(".imptdt").Each(func(_ int, sel *goquery.Selection) {
		if author != "" {
			return
		}
		if strings.Contains(strings.ToLower(sel.Text()), "author") {
			author = strings.TrimSpace(sel.Next().Text())
		}
	})

	status := ""
	doc.Find(".imptdt").Each(func(_ int, sel *goquery.Selection) {
		if status != "" {
			return
		}
		if strings.Contains(strings.ToLower(sel.Text()), "status") {
			status = strings.TrimSpace(sel.Next().Text())
		}
	})

	var genres []string
	doc.Find(".genres-content a").Each(func(_ int, sel *goquery.Selection) {
		genres = append(genres, strings.TrimSpace(sel.Text()))
	})

	description := ""
	for _, sel := range []string{".description-summary", ".summary__content"} {
		text := strings.TrimSpace(doc.Find(sel).First().Text())
		if text != "" {
			description = scraper.StripHTMLText(text)
			break
		}
	}
	if description == "" {
		doc.Find("h1, h2, h3, h4, h5, h6").EachWithBreak(func(_ int, sel *goquery.Selection) bool {
			if strings.Contains(strings.ToLower(sel.Text()), "synopsis") {
				next := sel.Next()
				if next.Length() > 0 {
					description = scraper.StripHTMLText(next.Text())
					return false
				}
			}
			return true
		})
	}

	return models.SeriesInfo{
		Title:       title,
		Description: description,
		Author:      author,
		Status:      status,
		Genres:      scraper.NormalizeGenres(genres),
		CoverURL:    scraper.NormalizeURLWithBase(coverURL, baseURL),
		Source:      s.Name(),
	}, nil
}

func isValidDrakeCover(imgURL string) bool {
	u := strings.ToLower(imgURL)
	return strings.Contains(u, "drakecomic") ||
		strings.Contains(u, "wp-content/uploads") ||
		strings.Contains(u, "i0.wp.com")
}

func (s *Scraper) firstImageAttr(sel *goquery.Selection) string {
	for _, attr := range []string{"src", "data-src", "data-lazy-src", "data-original"} {
		if v, _ := sel.Attr(attr); v != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func (s *Scraper) ExtractExpectedPageCount(html string) int {
	matches := tsReaderPattern.FindStringSubmatch(html)
	if len(matches) < 2 {
		return 0
	}

	var config struct {
		Sources []struct {
			Source string   `json:"source"`
			Images []string `json:"images"`
		} `json:"sources"`
	}

	jsonStr := matches[1]
	if err := json.Unmarshal([]byte(jsonStr), &config); err != nil {
		return 0
	}

	if len(config.Sources) == 0 {
		return 0
	}

	count := 0
	for _, imgURL := range config.Sources[0].Images {
		imgURL = strings.TrimSpace(imgURL)
		imgURL = normalizeImageURL(imgURL)
		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			continue
		}
		if isFilteredImage(imgURL) {
			continue
		}
		count++
	}
	return count
}

func extractFromTSReader(html string) []models.Image {
	matches := tsReaderPattern.FindStringSubmatch(html)
	if len(matches) < 2 {
		return nil
	}

	var config struct {
		Sources []struct {
			Source string   `json:"source"`
			Images []string `json:"images"`
		} `json:"sources"`
	}

	jsonStr := matches[1]
	if err := json.Unmarshal([]byte(jsonStr), &config); err != nil {
		log.Printf("[DRAKE-EXTRACT] Failed to parse ts_reader JSON: %v", err)
		return nil
	}

	if len(config.Sources) == 0 || len(config.Sources[0].Images) == 0 {
		return nil
	}

	var images []models.Image
	for _, imgURL := range config.Sources[0].Images {
		imgURL = strings.TrimSpace(imgURL)
		imgURL = normalizeImageURL(imgURL)
		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			continue
		}
		if isFilteredImage(imgURL) {
			continue
		}
		images = append(images, models.Image{
			URL:  imgURL,
			Page: len(images) + 1,
		})
	}

	return images
}

func extractFromNoscript(html string) []models.Image {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil
	}

	var images []models.Image
	doc.Find("#readerarea noscript img, .reading-content noscript img").Each(func(_ int, s *goquery.Selection) {
		src, exists := s.Attr("src")
		if !exists || src == "" {
			return
		}
		src = strings.TrimSpace(src)
		src = normalizeImageURL(src)
		if src == "" || !strings.HasPrefix(src, "http") {
			return
		}
		if isFilteredImage(src) {
			return
		}
		images = append(images, models.Image{
			URL:  src,
			Page: len(images) + 1,
		})
	})

	return images
}

func extractFromImgTags(html string) []models.Image {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil
	}

	var images []models.Image
	selector := "#readerarea img, .reading-content img, .ts-main-image"
	doc.Find(selector).Each(func(_ int, s *goquery.Selection) {
		attrs := []string{"data-src", "src", "data-lazy-src"}
		for _, attr := range attrs {
			src, exists := s.Attr(attr)
			if !exists || src == "" {
				continue
			}
			src = strings.TrimSpace(src)
			src = normalizeImageURL(src)
			if src == "" || !strings.HasPrefix(src, "http") {
				continue
			}
			if !strings.Contains(src, "wp-content/uploads") && !strings.Contains(src, "i0.wp.com") && !strings.Contains(src, "drakecomic.org") {
				continue
			}
			if isFilteredImage(src) {
				continue
			}
			images = append(images, models.Image{
				URL:  src,
				Page: len(images) + 1,
			})
			break
		}
	})

	return images
}

func normalizeImageURL(rawURL string) string {
	u := strings.TrimSpace(rawURL)

	if strings.Contains(u, "i0.wp.com/") {
		idx := strings.Index(u, i0wpPrefix)
		if idx != -1 {
			rest := u[idx+len(i0wpPrefix):]
			if !strings.HasPrefix(rest, "http") {
				rest = "https://" + rest
			}
			u = rest
		}
	}

	if strings.Contains(u, "?") {
		qIdx := strings.Index(u, "?")
		u = u[:qIdx]
	}

	return u
}

func isFilteredImage(imgURL string) bool {
	lower := strings.ToLower(imgURL)
	return strings.Contains(lower, "placeholder") ||
		strings.Contains(lower, "advertisement") ||
		strings.Contains(lower, "/covers/") ||
		strings.Contains(lower, "/thumbnails/")
}

func dedupeAndSort(images []models.Image) []models.Image {
	seen := make(map[string]bool)
	var deduped []models.Image

	for _, img := range images {
		key := normalizeDedupeKey(img.URL)
		if seen[key] {
			continue
		}
		seen[key] = true
		deduped = append(deduped, img)
	}

	if len(deduped) > 0 {
		scraper.SortImagesByPageNumber(deduped)
		deduped = scraper.FilterSuspectImages(deduped)
		for i := range deduped {
			deduped[i].Page = i + 1
		}
	}

	log.Printf("[DRAKE-EXTRACT] Total images after dedup: %d", len(deduped))
	return deduped
}

func normalizeDedupeKey(imgURL string) string {
	key := imgURL
	if strings.Contains(key, "i0.wp.com/") {
		idx := strings.Index(key, i0wpPrefix)
		if idx != -1 {
			key = key[idx+len(i0wpPrefix):]
		}
	}
	if strings.Contains(key, "?") {
		key = key[:strings.Index(key, "?")]
	}
	return key
}
