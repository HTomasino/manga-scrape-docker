package ravenscans

import (
	"encoding/json"
	"html"
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
)

var (
	tsReaderPattern       = regexp.MustCompile(`(?s)ts_reader\.run\(\s*(\{.+?\})\s*\);`)
	ravenscansBannerRegex = regexp.MustCompile(`background-image:\s*url\('([^']+)'\)`)
)

type tsReaderConfig struct {
	Sources []tsReaderSource `json:"sources"`
}

type tsReaderSource struct {
	Source string   `json:"source"`
	Images []string `json:"images"`
}

type Scraper struct{}

func NewScraper() *Scraper {
	return &Scraper{}
}

func (s *Scraper) Name() string {
	return models.SiteRavenscans
}

func (s *Scraper) CanHandle(pageURL string) bool {
	return strings.Contains(pageURL, "ravenscans.org")
}

func (s *Scraper) ExtractChapters(htmlStr string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[RAVENSCANS-EXTRACT] Starting extraction from: %s", baseURL)

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
			log.Printf("[RAVENSCANS-EXTRACT] Skipping locked chapter (data-num=%s)", dataNum)
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

	log.Printf("[RAVENSCANS-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

func (s *Scraper) ExtractSeriesInfo(htmlStr string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return models.SeriesInfo{Source: s.Name()}, err
	}

	title := strings.TrimSpace(doc.Find("h1.entry-title").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find(".manga-title, .entry-title").First().Text())
	}

	coverURL := ""
	doc.Find("img.wp-post-image").Each(func(_ int, sel *goquery.Selection) {
		if coverURL != "" {
			return
		}
		if src, ok := sel.Attr("src"); ok && src != "" {
			if isRavenscansCDN(src) {
				coverURL = src
			}
		}
	})
	if coverURL == "" {
		coverURL = scraper.ExtractMetaContent(htmlStr, "og:image")
	}
	if coverURL == "" {
		if m := ravenscansBannerRegex.FindStringSubmatch(htmlStr); len(m) > 1 {
			coverURL = m[1]
		}
	}
	if coverURL != "" && !isRavenscansCDN(coverURL) {
		coverURL = ""
	}

	status := ""
	doc.Find(".tsinfo .imptdt i, .status i").Each(func(_ int, sel *goquery.Selection) {
		if status != "" {
			return
		}
		status = strings.TrimSpace(sel.Text())
	})

	var genres []string
	doc.Find(".mgen a").Each(func(_ int, sel *goquery.Selection) {
		genres = append(genres, strings.TrimSpace(sel.Text()))
	})

	description := ""
	doc.Find(".entry-content").First().Each(func(_ int, sel *goquery.Selection) {
		description = strings.TrimSpace(sel.Text())
	})
	if description == "" {
		description = scraper.ExtractMetaContent(htmlStr, "og:description")
	}

	return models.SeriesInfo{
		Title:       title,
		Description: scraper.StripHTMLText(description),
		Author:      "", // RavenScans does not list an author on the series page.
		Status:      status,
		Genres:      scraper.NormalizeGenres(genres),
		CoverURL:    scraper.NormalizeURLWithBase(coverURL, baseURL),
		Source:      s.Name(),
	}, nil
}

func (s *Scraper) ExtractImages(htmlStr string, chapterURL string) ([]models.Image, error) {
	log.Printf("[RAVENSCANS-EXTRACT] Extracting images from: %s", chapterURL)

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

	log.Printf("[RAVENSCANS-EXTRACT] Total images extracted: %d", len(images))
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
		log.Printf("[RAVENSCANS-EXTRACT] Failed to parse ts_reader JSON: %v", err)
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

		if !isRavenscansCDN(imgURL) {
			log.Printf("[RAVENSCANS-EXTRACT] Skipping non-CDN URL: %s", imgURL)
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

	doc.Find("#readerarea img, img.wp-manga-chapter-img").Each(func(_ int, sel *goquery.Selection) {
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

			if !isRavenscansCDN(imgURL) {
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

func isRavenscansCDN(imgURL string) bool {
	parsed, err := url.Parse(imgURL)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	return host == "ravenscans.org" || strings.HasSuffix(host, ".ravenscans.org")
}