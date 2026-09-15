package demonic

import (
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
	// Capture the chapter value from legacy chaptered.php URLs so they parse
	// to a real number instead of 0.
	demonicChapterPattern = regexp.MustCompile(`chaptered\.php\?manga=\d+&chapter=(\d+(?:\.\d+)?)|/title/[^/]+/chapter/(\d+(?:\.\d+)?)`)
)

var demonicCDNDomains = []string{
	"demonic",
	"demoniclibs",
	"librarydm",
	"mangafirst",
	"mangareadon",
	"readermc",
	"mangathird",
	"mangasecond",
	"mangafourth",
	"mangafifth",
}

type Scraper struct {
	usePlaywright bool
}

func NewScraper() *Scraper {
	return &Scraper{usePlaywright: false}
}

func NewScraperWithPlaywright() *Scraper {
	return &Scraper{usePlaywright: true}
}

func (s *Scraper) Name() string {
	return "demonicscans"
}

func (s *Scraper) CanHandle(url string) bool {
	return strings.Contains(url, "demonic") || strings.Contains(url, "librarydm")
}

func (s *Scraper) ExtractChapters(html string, baseURL string) ([]models.Chapter, error) {
	log.Printf("[DEMONIC-EXTRACT] Starting extraction from: %s", baseURL)

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

		if !demonicChapterPattern.MatchString(fullURL) {
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
			// Legacy chaptered.php URLs carry no trailing number; read the
			// captured query value instead.
			if m := demonicChapterPattern.FindStringSubmatch(fullURL); len(m) > 1 {
				for _, capGroup := range m[1:] {
					if capGroup != "" {
						if n, err := strconv.ParseFloat(capGroup, 64); err == nil {
							number = n
							break
						}
					}
				}
			}
		}
		if number == 0 {
			number = scraper.ParseChapterNumber(title)
		}
		if number == 0 {
			log.Printf("[DEMONIC-EXTRACT] Skipping chapter %q: unparseable number from %s", title, fullURL)
			return
		}

		chapters = append(chapters, models.Chapter{
			ID:     scraper.GenerateID(),
			URL:    fullURL,
			Title:  title,
			Number: number,
		})
	})

	scraper.SortChaptersByNumber(chapters)

	log.Printf("[DEMONIC-EXTRACT] Found %d chapters", len(chapters))
	return chapters, nil
}

func (s *Scraper) ExtractImages(html string, chapterURL string) ([]models.Image, error) {
	var images []models.Image

	log.Printf("[DEMONIC-EXTRACT] Extracting images from: %s", chapterURL)

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)

	doc.Find("img.imgholder").Each(func(_ int, selection *goquery.Selection) {
		imgURL, exists := selection.Attr("src")
		if !exists || imgURL == "" {
			return
		}

		imgURL = scraper.NormalizeURL(imgURL)
		if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
			return
		}

		if !isDemonicCDN(imgURL) {
			log.Printf("[DEMONIC-EXTRACT] Skipping non-CDN URL: %s", imgURL)
			return
		}

		lower := strings.ToLower(imgURL)
		if strings.Contains(lower, "/covers/") || strings.Contains(lower, "/thumbnails/") {
			return
		}

		if seen[imgURL] {
			return
		}
		seen[imgURL] = true

		fallbackURL := generateDemonicFallback(imgURL)

		images = append(images, models.Image{
			URL:         imgURL,
			FallbackURL: fallbackURL,
			Page:        len(images) + 1,
			Quality:     "original",
		})
	})

	if len(images) == 0 {
		doc.Find("img").Each(func(_ int, selection *goquery.Selection) {
			attrs := []string{"src", "data-src", "data-lazy-src"}

			for _, attr := range attrs {
				imgURL, exists := selection.Attr(attr)
				if !exists || imgURL == "" {
					continue
				}

				lower := strings.ToLower(imgURL)
				if strings.Contains(lower, "placeholder") ||
					strings.Contains(lower, "advertisement") ||
					strings.Contains(lower, "lazy") ||
					strings.Contains(lower, "/covers/") ||
					strings.Contains(lower, "/thumbnails/") {
					continue
				}

				imgURL = scraper.NormalizeURL(imgURL)
				if imgURL == "" || !strings.HasPrefix(imgURL, "http") {
					continue
				}

				if !isDemonicCDN(imgURL) {
					continue
				}

				if seen[imgURL] {
					continue
				}
				seen[imgURL] = true

				fallbackURL := generateDemonicFallback(imgURL)

				images = append(images, models.Image{
					URL:         imgURL,
					FallbackURL: fallbackURL,
					Page:        len(images) + 1,
					Quality:     "original",
				})
			}
		})
	}

	scraper.SortImagesByPageNumber(images)
	images = scraper.FilterSuspectImages(images)
	for i := range images {
		images[i].Page = i + 1
	}

	log.Printf("[DEMONIC-EXTRACT] Total images extracted: %d", len(images))
	return images, nil
}

func isDemonicCDN(imgURL string) bool {
	parsed, err := url.Parse(imgURL)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	for _, cdn := range demonicCDNDomains {
		if strings.Contains(host, cdn) {
			return true
		}
	}
	return false
}

func generateDemonicFallback(imgURL string) string {
	if strings.Contains(imgURL, "demoniclibs") {
		return strings.Replace(imgURL, "demoniclibs", "librarydm", 1)
	}
	if strings.Contains(imgURL, "mangafirst") {
		return strings.Replace(imgURL, "mangafirst", "mangareadon", 1)
	}
	return ""
}

// ExtractSeriesInfo extracts series-level metadata from a Demonic series page.
func (s *Scraper) ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return models.SeriesInfo{Source: s.Name()}, err
	}

	title := strings.TrimSpace(doc.Find("h1").First().Text())
	coverURL := scraper.ExtractMetaContent(html, "og:image")
	if coverURL != "" && !isDemonicCDN(coverURL) {
		coverURL = ""
	}

	author := extractDemonicField(doc, "Author")
	if isPlaceholderAuthor(author) {
		author = ""
	}
	status := extractDemonicField(doc, "Status")

	var genres []string
	doc.Find(".genres-list li, .genres a, .series-info a[href*=\"genre\"]").Each(func(_ int, sel *goquery.Selection) {
		genres = append(genres, strings.TrimSpace(sel.Text()))
	})

	description := ""
	doc.Find("#manga-info-rightColumn .white-font").First().Each(func(_ int, sel *goquery.Selection) {
		description = strings.TrimSpace(sel.Text())
	})
	if description == "" {
		doc.Find(".description-summary").First().Each(func(_ int, sel *goquery.Selection) {
			description = strings.TrimSpace(sel.Text())
		})
	}
	if description == "" {
		for _, heading := range []string{"h2", "h3", "h4"} {
			doc.Find(heading).EachWithBreak(func(_ int, sel *goquery.Selection) bool {
				if strings.Contains(strings.ToLower(sel.Text()), "the summary is") {
					description = strings.TrimSpace(sel.Next().Text())
					return false
				}
				return true
			})
			if description != "" {
				break
			}
		}
	}

	return models.SeriesInfo{
		Title:       title,
		Description: scraper.StripHTMLText(description),
		Author:      author,
		Status:      status,
		Genres:      scraper.NormalizeGenres(genres),
		CoverURL:    coverURL,
		Source:      s.Name(),
	}, nil
}

func extractDemonicField(doc *goquery.Document, label string) string {
	labelLower := strings.ToLower(label)

	// Current Demonic layout: #manga-info-stats uses .flex.flex-row > li pairs.
	var value string
	doc.Find("#manga-info-stats .flex.flex-row").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		cells := s.Find("li")
		if cells.Length() < 2 {
			return true
		}
		key := strings.ToLower(strings.TrimSpace(cells.First().Text()))
		if strings.Contains(key, labelLower) {
			value = strings.TrimSpace(cells.Slice(1, 2).Text())
			return false
		}
		return true
	})
	if value != "" {
		return value
	}

	// Fallback for older .info-row markup.
	doc.Find(".info-row").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		if strings.Contains(strings.ToLower(s.Text()), labelLower) {
			value = strings.TrimSpace(s.Find(".value").First().Text())
			if value == "" {
				value = strings.TrimSpace(s.Text())
			}
			return false
		}
		return true
	})
	return value
}

func isPlaceholderAuthor(value string) bool {
	placeholder := []string{"updating", "unknown", "n/a", "-"}
	v := strings.ToLower(strings.TrimSpace(value))
	for _, p := range placeholder {
		if v == p {
			return true
		}
	}
	return false
}
