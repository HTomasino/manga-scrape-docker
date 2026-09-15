package manhuaus

import (
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
	manhuausChapterPattern = regexp.MustCompile(`/chapter-\d+`)
	pageCounterRegex       = regexp.MustCompile(`(?i)(?:page\s*\d+\s*of\s*(\d+)|(\d+)\s*pages?\b)`)
	entryIndexRegex        = regexp.MustCompile(`entry-index='(\d+)'`)
)

var manhuausCDNDomains = []string{
	"img.manhuaus.com",
	"img1.manhuaus.com",
	"manhuaus.com",
}

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
	return "manhuaus"
}

func (s *Scraper) CanHandle(url string) bool {
	return strings.Contains(url, "manhuaus.com")
}

func (s *Scraper) FetchHTML(url string) (string, error) {
	log.Printf("[MANHUAUS-FETCH] Using Playwright to bypass Cloudflare for: %s", url)
	html, cookies, userAgent, err := FetchWithPlaywright(url, s.browserCfg, s.browserQueue)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	s.cookies = cookies
	s.userAgent = userAgent
	s.mu.Unlock()

	return html, nil
}

func (s *Scraper) GetCookies() []*http.Cookie {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cookies
}

func (s *Scraper) GetUserAgent() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.userAgent
}

func (s *Scraper) ExtractChapters(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[MANHUAUS-EXTRACT] Starting extraction from: %s", baseURL)

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	var chapters []models.Chapter
	seen := make(map[string]bool)

	doc.Find("li.wp-manga-chapter a[href]").Each(func(_ int, selection *goquery.Selection) {
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

		if !manhuausChapterPattern.MatchString(fullURL) {
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

	scraper.SortChaptersByNumber(chapters)

	log.Printf("[MANHUAUS-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

func (s *Scraper) ExtractImages(html string, chapterURL string) ([]models.Image, error) {
	log.Printf("[MANHUAUS-EXTRACT] Extracting images from: %s", chapterURL)

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	var images []models.Image
	seen := make(map[string]bool)

	doc.Find("img.wp-manga-chapter-img").Each(func(_ int, selection *goquery.Selection) {
		attrs := []string{"data-src", "src", "data-lazy-src", "data-original"}

		for _, attr := range attrs {
			imgURL, exists := selection.Attr(attr)
			if !exists || imgURL == "" {
				continue
			}

			imgURL = strings.TrimSpace(imgURL)
			imgURL = scraper.NormalizeURL(imgURL)

			if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
				continue
			}

			if !isManhuausCDN(imgURL) {
				log.Printf("[MANHUAUS-EXTRACT] Skipping non-CDN URL: %s", imgURL)
				continue
			}

			lower := strings.ToLower(imgURL)
			if strings.Contains(lower, "placeholder") ||
				strings.Contains(lower, "advertisement") ||
				strings.Contains(lower, "/covers/") ||
				strings.Contains(lower, "/thumbnails/") {
				continue
			}

			if seen[imgURL] {
				continue
			}
			seen[imgURL] = true

			fallbackURL := generateManhuausFallback(imgURL)

			images = append(images, models.Image{
				URL:         imgURL,
				FallbackURL: fallbackURL,
				Page:        len(images) + 1,
			})
		}
	})

	if len(images) > 0 {
		scraper.SortImagesByPageNumber(images)
		images = scraper.FilterSuspectImages(images)
		for i := range images {
			images[i].Page = i + 1
		}
	}

	log.Printf("[MANHUAUS-EXTRACT] Total images extracted: %d", len(images))
	return images, nil
}

func isManhuausCDN(imgURL string) bool {
	parsed, err := url.Parse(imgURL)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	for _, cdn := range manhuausCDNDomains {
		if host == cdn || strings.HasSuffix(host, "."+cdn) {
			return true
		}
	}
	return false
}

func generateManhuausFallback(imgURL string) string {
	if strings.Contains(imgURL, "img.manhuaus.com") {
		return strings.Replace(imgURL, "img.manhuaus.com", "img1.manhuaus.com", 1)
	}
	return ""
}

func (s *Scraper) ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return models.SeriesInfo{}, err
	}

	title := scraper.ExtractMetaContent(html, "og:title")
	if title == "" {
		title = strings.TrimSpace(doc.Find("h1").First().Text())
	}

	coverURL := scraper.ExtractMetaContent(html, "og:image")
	if coverURL == "" {
		coverURL = s.firstCoverAttr(doc.Find(".summary_image img").First())
	}
	if coverURL != "" && !isManhuausCDN(coverURL) {
		coverURL = ""
	}

	author := ""
	doc.Find(".post-content_item").Each(func(_ int, sel *goquery.Selection) {
		if author != "" {
			return
		}
		label := strings.ToLower(sel.Find(".summary-heading").First().Text())
		if strings.Contains(label, "author") {
			author = normalizeFieldValue(sel.Find(".summary-content").First().Text())
		}
	})

	status := ""
	doc.Find(".post-content_item").Each(func(_ int, sel *goquery.Selection) {
		if status != "" {
			return
		}
		label := strings.ToLower(sel.Find(".summary-heading").First().Text())
		if strings.Contains(label, "status") {
			status = normalizeFieldValue(sel.Find(".summary-content").First().Text())
		}
	})

	var genres []string
	doc.Find(".genres-content a, .manga-info-row a[href*=genre]").Each(func(_ int, sel *goquery.Selection) {
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

func (s *Scraper) firstCoverAttr(sel *goquery.Selection) string {
	for _, attr := range []string{"src", "data-src", "data-lazy-src", "data-original"} {
		if v, _ := sel.Attr(attr); v != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func (s *Scraper) ExtractExpectedPageCount(html string) int {
	if m := pageCounterRegex.FindStringSubmatch(html); len(m) > 0 {
		for i := 1; i < len(m); i++ {
			if n, err := strconv.Atoi(m[i]); err == nil && n > 0 {
				return n
			}
		}
	}

	allMatches := entryIndexRegex.FindAllStringSubmatch(html, -1)
	maxIndex := 0
	for _, m := range allMatches {
		if len(m) > 1 {
			if n, err := strconv.Atoi(m[1]); err == nil && n > maxIndex {
				maxIndex = n
			}
		}
	}
	if maxIndex > 0 {
		return maxIndex
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return 0
	}
	if dataPages, exists := doc.Find("[data-pages]").First().Attr("data-pages"); exists {
		if n, err := strconv.Atoi(dataPages); err == nil && n > 0 {
			return n
		}
	}

	return 0
}

func normalizeFieldValue(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}
