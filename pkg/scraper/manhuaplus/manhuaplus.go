package manhuaplus

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
)

var (
	chapterPattern = regexp.MustCompile(`/chapter-\d+`)
	chapterIDRegex = regexp.MustCompile(`CHAPTER_ID\s*=\s*(\d+)`)
)

func isValidCDN(imgURL string) bool {
	parsed, err := url.Parse(imgURL)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	for _, domain := range validCDNDomains {
		if strings.HasPrefix(domain, ".") {
			if strings.HasSuffix(host, domain) || host == domain[1:] {
				return true
			}
		} else {
			if host == domain || strings.HasSuffix(host, "."+domain) {
				return true
			}
		}
	}
	return false
}

var validCDNDomains = []string{
	"cdn.manhuaplus.cc",
	"manhuaplus.cc",
	"manhuaplus.co",
	"manhuaplus.top",
	".wibu.asia",
	".wibu.live",
}

var cdnFallbackDomains = []string{
	"ntcdnqq.wibu.asia",
	"ntcdn242.wibu.asia",
	"ntcdn154.wibu.asia",
	"ntcdn01.wibu.live",
}

type Scraper struct {
	httpClient scraper.HTTPFetcher
	rawClient  *http.Client
	userAgent  string
}

func NewScraper(httpClient scraper.HTTPFetcher) *Scraper {
	return &Scraper{
		httpClient: httpClient,
		rawClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}
}

func (s *Scraper) Name() string {
	return models.SiteManhuaPlus
}

func (s *Scraper) CanHandle(url string) bool {
	return strings.Contains(url, "manhuaplus.top") || strings.Contains(url, "manhuaplus.co") || strings.Contains(url, "manhuaplus.cc")
}

func (s *Scraper) ExtractChapters(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[MANHUAPLUS-EXTRACT] Starting extraction from: %s", baseURL)

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	var chapters []models.Chapter
	seen := make(map[string]bool)

	doc.Find("#nt_listchapter nav ul li a[href], #nt_listchapter nav ul li.row a[href]").Each(func(_ int, selection *goquery.Selection) {
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

		if !chapterPattern.MatchString(fullURL) {
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

	log.Printf("[MANHUAPLUS-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

func (s *Scraper) ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return models.SeriesInfo{}, err
	}

	title := strings.TrimSpace(doc.Find("h1").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find(".manga-title").First().Text())
	}

	coverURL := s.firstCoverAttr(doc.Find(".summary_image img").First())
	if coverURL == "" {
		coverURL = scraper.ExtractMetaContent(html, "og:image")
	}
	if coverURL != "" && !isValidCDN(coverURL) {
		coverURL = ""
	}

	author := ""
	doc.Find(".post-content_item").Each(func(_ int, sel *goquery.Selection) {
		if author != "" {
			return
		}
		label := strings.ToLower(sel.Find(".summary-heading").First().Text())
		if strings.Contains(label, "author") {
			author = strings.TrimSpace(sel.Find(".summary-content").First().Text())
		}
	})

	status := ""
	doc.Find(".post-content_item").Each(func(_ int, sel *goquery.Selection) {
		if status != "" {
			return
		}
		label := strings.ToLower(sel.Find(".summary-heading").First().Text())
		if strings.Contains(label, "status") {
			status = strings.TrimSpace(sel.Find(".summary-content").First().Text())
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

func (s *Scraper) ExtractImages(html string, chapterURL string) ([]models.Image, error) {
	log.Printf("[MANHUAPLUS-EXTRACT] Extracting images from: %s", chapterURL)

	images, err := s.extractImagesFromHTML(html)
	if err != nil {
		return nil, err
	}

	if len(images) == 0 {
		log.Printf("[MANHUAPLUS-EXTRACT] No images found in DOM, trying AJAX fallback")
		images, err = s.ajaxImageFallback(html, chapterURL)
		if err != nil {
			log.Printf("[MANHUAPLUS-EXTRACT] AJAX fallback failed: %v", err)
		}
	}

	if len(images) > 0 {
		scraper.SortImagesByPageNumber(images)
		images = scraper.FilterSuspectImages(images)
		for i := range images {
			images[i].Page = i + 1
		}
	}

	log.Printf("[MANHUAPLUS-EXTRACT] Total images extracted: %d", len(images))
	return images, nil
}

func (s *Scraper) extractImagesFromHTML(html string) ([]models.Image, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	var images []models.Image
	seen := make(map[string]bool)

	doc.Find("#image_container .page-chapter img").Each(func(_ int, selection *goquery.Selection) {
		imgURL := s.extractImageURL(selection)
		if imgURL == "" {
			return
		}

		imgURL = scraper.NormalizeURL(imgURL)
		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			return
		}

		if !isValidCDN(imgURL) {
			return
		}

		lower := strings.ToLower(imgURL)
		if strings.Contains(lower, "placeholder") ||
			strings.Contains(lower, "advertisement") ||
			strings.Contains(lower, "/covers/") ||
			strings.Contains(lower, "/thumbnails/") {
			return
		}

		if seen[imgURL] {
			return
		}
		seen[imgURL] = true

		fallbackURL := generateFallbackURL(imgURL)

		images = append(images, models.Image{
			URL:         imgURL,
			FallbackURL: fallbackURL,
			Page:        len(images) + 1,
		})
	})

	return images, nil
}

func generateFallbackURL(imgURL string) string {
	if strings.Contains(imgURL, "cdn.manhuaplus.cc") {
		return strings.Replace(imgURL, "cdn.manhuaplus.cc", cdnFallbackDomains[0], 1)
	}
	return ""
}

func (s *Scraper) extractImageURL(selection *goquery.Selection) string {
	priorityAttrs := []string{"data-original", "data-src", "src"}

	for _, attr := range priorityAttrs {
		val, exists := selection.Attr(attr)
		if exists && val != "" {
			val = strings.TrimSpace(val)
			if strings.HasPrefix(val, "http") {
				return val
			}
		}
	}

	return ""
}

func (s *Scraper) ajaxImageFallback(html string, chapterURL string) ([]models.Image, error) {
	chapterID := s.extractChapterID(html)
	if chapterID == "" {
		return nil, fmt.Errorf("could not extract CHAPTER_ID from page")
	}

	parsedURL, err := url.Parse(chapterURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse chapter URL: %w", err)
	}

	ajaxURL := fmt.Sprintf("%s://%s/ajax/image/list/chap/%s?cache=0", parsedURL.Scheme, parsedURL.Host, chapterID)

	log.Printf("[MANHUAPLUS-EXTRACT] AJAX fallback request: %s", ajaxURL)

	respHTML, err := s.postAJAX(ajaxURL, chapterURL)
	if err != nil {
		return nil, fmt.Errorf("AJAX POST failed: %w", err)
	}

	return s.extractImagesFromHTML(respHTML)
}

func (s *Scraper) extractChapterID(html string) string {
	matches := chapterIDRegex.FindStringSubmatch(html)
	if len(matches) > 1 {
		return matches[1]
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return ""
	}

	selected := doc.Find("select#ctl00_mainContent_ddlSelectChapter option[selected]")
	if dataID, exists := selected.Attr("data-id"); exists && dataID != "" {
		return dataID
	}

	return ""
}

func (s *Scraper) postAJAX(ajaxURL string, referer string) (string, error) {
	req, err := http.NewRequest("POST", ajaxURL, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Referer", referer)

	time.Sleep(500 * time.Millisecond)

	resp, err := s.rawClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result struct {
		Status bool   `json:"status"`
		HTML   string `json:"html"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("failed to parse AJAX response: %w", err)
	}

	if !result.Status || result.HTML == "" {
		return "", fmt.Errorf("AJAX response indicated failure or empty HTML")
	}

	return result.HTML, nil
}
