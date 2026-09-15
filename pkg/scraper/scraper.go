package scraper

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/user/comic-scraper/pkg/models"
	"golang.org/x/net/html"
)

// Pre-compiled regex patterns
var (
	multiSlash    = regexp.MustCompile(`/{2,}`)
	chapterRegex  = regexp.MustCompile(`(?i)chapter[/-=\s]?(\d+(?:\.\d+)?)`)
	imageExtRegex = regexp.MustCompile(`\.(jpg|jpeg|png|gif|webp)(?:\?|$)`)

	TrailingNumRegex = regexp.MustCompile(`/(\d+)\.\w+(?:\?|$)`)
	PathNumRegex     = regexp.MustCompile(`/(\d+)/`)

	suspectFirstPatterns = regexp.MustCompile(`(?i)(/covers/|cover|/thumbnails/|thumbnail|banner|logo|avatar|favicon|profile)`)
	suspectLastPatterns  = regexp.MustCompile(`(?i)(nav|footer|credit|end-card|next|prev|advertisement)`)
)

// FilterSuspectImages removes first/last images that match known non-content
// patterns (covers, banners, navigation, etc.). Middle images are never filtered
// by suspect patterns — only by CDN validity checks.
// Callers should invoke this AFTER sorting and BEFORE final page number reassignment.
func FilterSuspectImages(images []models.Image) []models.Image {
	if len(images) <= 2 {
		return images
	}

	if suspectFirstPatterns.MatchString(images[0].URL) {
		images = images[1:]
	}

	if len(images) <= 1 {
		return images
	}

	if suspectLastPatterns.MatchString(images[len(images)-1].URL) {
		images = images[:len(images)-1]
	}

	return images
}

// Scraper is the interface for site-specific extractors
type Scraper interface {
	Name() string
	CanHandle(url string) bool
	ExtractChapters(html string, baseURL string) ([]models.Chapter, error)
	ExtractImages(html string, chapterURL string) ([]models.Image, error)
}

// BrowserFetcher is an optional interface that scrapers can implement when
// the target site requires a real browser (e.g., Cloudflare Turnstile).
// Callers should check for this interface before falling back to HTTP fetching.
type BrowserFetcher interface {
	Scraper
	FetchHTML(url string) (string, error)
}

// CookieProvider is an optional interface that scrapers can implement to
// provide cookies from a browser session (e.g., Cloudflare cookies).
// Callers should inject these cookies into HTTP clients for downloading
// resources from the same domain that requires Cloudflare bypass.
type CookieProvider interface {
	Scraper
	GetCookies() []*http.Cookie
}

// UAProvider is an optional interface that scrapers can implement to
// provide the User-Agent used during the browser session. Cloudflare ties
// cf_clearance cookies to the User-Agent that solved the challenge, so
// the HTTP client must use the exact same User-Agent for image downloads.
type UAProvider interface {
	Scraper
	GetUserAgent() string
}

// PageCountProvider is an optional interface that scrapers can implement
// to return the expected number of pages from site metadata (e.g., JSON
// image arrays, page counter elements). Returns 0 if unknown.
type PageCountProvider interface {
	Scraper
	ExtractExpectedPageCount(html string) int
}

// SeriesInfoProvider is an optional interface that scrapers can implement
// to extract series-level metadata (title, description, author, status,
// genres, cover URL) from a series page.
type SeriesInfoProvider interface {
	Scraper
	ExtractSeriesInfo(html string, baseURL string) (models.SeriesInfo, error)
}

// SequenceReport describes the result of inspecting an image list
type SequenceReport struct {
	FirstPage    int
	LastPage     int
	TotalImages  int
	ExpectedMin  int
	Gaps         []int
	SuspectFirst bool
	SuspectLast  bool
	Warnings     []string
}

// InspectImageSequence validates the completeness and ordering of an
// extracted image list. Images must already be sorted by Page number
// (1-based, sequential) as produced by SortImagesByPageNumber + reassignment.
//
// Note: because all scrapers reassign Page = i+1 after sorting, the Page
// fields are always [1..N] with no gaps. The Gaps field is therefore only
// populated when images are passed without the standard reassignment
// pipeline. Use DetectURLGaps for URL-based gap detection, and the
// expectedCount comparison for metadata-based completeness checks.
func InspectImageSequence(images []models.Image, expectedCount int) SequenceReport {
	report := SequenceReport{
		TotalImages: len(images),
		ExpectedMin: expectedCount,
	}

	if len(images) == 0 {
		report.Warnings = append(report.Warnings, "no images extracted")
		return report
	}

	report.FirstPage = images[0].Page
	report.LastPage = images[len(images)-1].Page

	if images[0].Page != 1 {
		report.SuspectFirst = true
		report.Warnings = append(report.Warnings, "first image is not page 1")
	}

	if images[len(images)-1].Page != len(images) {
		report.Warnings = append(report.Warnings, "last page number does not match total image count")
	}

	pageSet := make(map[int]bool, len(images))
	maxPage := 0
	for _, img := range images {
		pageSet[img.Page] = true
		if img.Page > maxPage {
			maxPage = img.Page
		}
	}

	if maxPage > len(images) {
		for p := 1; p <= maxPage; p++ {
			if !pageSet[p] {
				report.Gaps = append(report.Gaps, p)
			}
		}
	}

	if len(report.Gaps) > 0 {
		report.Warnings = append(report.Warnings, fmt.Sprintf("missing page numbers: %v", report.Gaps))
	}

	if suspectFirstPatterns.MatchString(images[0].URL) {
		report.SuspectFirst = true
		report.Warnings = append(report.Warnings, "first image URL matches cover/banner pattern")
	}

	if suspectLastPatterns.MatchString(images[len(images)-1].URL) {
		report.SuspectLast = true
		report.Warnings = append(report.Warnings, "last image URL matches navigation/footer pattern")
	}

	if expectedCount > 0 && len(images) < expectedCount {
		missing := expectedCount - len(images)
		report.Warnings = append(report.Warnings, fmt.Sprintf("expected %d images but only extracted %d (missing %d)", expectedCount, len(images), missing))
	}

	return report
}

// DetectURLGaps extracts numeric indices from image URLs using the same
// regex patterns as SortImagesByURL and reports any gaps in the numeric
// range [first, last]. Returns the list of missing numeric indices.
func DetectURLGaps(images []models.Image) []int {
	if len(images) < 2 {
		return nil
	}

	type indexed struct {
		num   int
		found bool
	}

	var entries []indexed
	for _, img := range images {
		var num int
		var found bool

		if m := TrailingNumRegex.FindStringSubmatch(img.URL); len(m) > 1 {
			num, _ = strconv.Atoi(m[1])
			found = true
		} else if all := PathNumRegex.FindAllStringSubmatch(img.URL, -1); len(all) > 0 {
			last := all[len(all)-1]
			num, _ = strconv.Atoi(last[1])
			found = true
		}

		entries = append(entries, indexed{num: num, found: found})
	}

	firstNum := -1
	lastNum := -1
	for _, e := range entries {
		if e.found {
			if firstNum == -1 || e.num < firstNum {
				firstNum = e.num
			}
			if lastNum == -1 || e.num > lastNum {
				lastNum = e.num
			}
		}
	}

	if firstNum == -1 || lastNum == -1 || firstNum == lastNum {
		return nil
	}

	numSet := make(map[int]bool)
	for _, e := range entries {
		if e.found {
			numSet[e.num] = true
		}
	}

	var gaps []int
	for n := firstNum + 1; n < lastNum; n++ {
		if !numSet[n] {
			gaps = append(gaps, n)
		}
	}

	return gaps
}

// SetCookiesOnHttpClient injects cookies from a CookieProvider and the
// User-Agent from a UAProvider into the HTTP client. This is essential for
// sites behind Cloudflare where both page HTML and image resources require
// Cloudflare clearance cookies AND the matching User-Agent.
func SetCookiesOnHttpClient(scr Scraper, httpClient HTTPFetcher, baseURL string) {
	// Inject cookies
	if cp, ok := scr.(CookieProvider); ok {
		type cookieSetter interface {
			SetDomainCookies(domain string, cookies []*http.Cookie)
		}
		if setter, ok := httpClient.(cookieSetter); ok {
			setter.SetDomainCookies(baseURL, cp.GetCookies())
		}
	}

	// Inject User-Agent
	if uap, ok := scr.(UAProvider); ok {
		ua := uap.GetUserAgent()
		if ua != "" {
			type uaSetter interface {
				SetUserAgent(ua string)
			}
			if setter, ok := httpClient.(uaSetter); ok {
				log.Printf("[CLOUDFLARE] Overriding HTTP client User-Agent to match browser: %s", ua)
				setter.SetUserAgent(ua)
			}
		}
	}
}

// Registry holds all registered scrapers
type Registry struct {
	scrapers []Scraper
}

// NewRegistry creates a new scraper registry
func NewRegistry() *Registry {
	return &Registry{
		scrapers: make([]Scraper, 0),
	}
}

// Register adds a scraper to the registry
func (r *Registry) Register(s Scraper) {
	r.scrapers = append(r.scrapers, s)
}

// GetScraper returns the first scraper that can handle the URL
func (r *Registry) GetScraper(url string) Scraper {
	for _, s := range r.scrapers {
		if s.CanHandle(url) {
			return s
		}
	}
	return nil
}

// ValidateSeriesURL parses a user-supplied series URL and rejects anything
// that is not a normal public http(s) URL. It blocks:
//   - empty or unparseable URLs
//   - non-http(s) schemes (file://, data:, gopher:, ...)
//   - userinfo (user:pass@host) which has no legitimate use for a series page
//   - loopback, link-local, private, and unicast-link-local IP literals, plus
//     ".local" mDNS hostnames, so a malicious add request can't drive the
//     server's HTTP client or Playwright browser at internal services.
//
// It does NOT restrict the host to a known allowlist — that is the job of
// GetScraper + each scraper's CanHandle. This function only closes the
// "any URL the scraper matches by substring gets fetched" gap.
func ValidateSeriesURL(raw string) error {
	if raw == "" {
		return errors.New("URL is required")
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL must use http or https (got %q)", u.Scheme)
	}
	if u.User != nil {
		return errors.New("URL must not contain userinfo")
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("URL must include a host")
	}
	if isBlockedHost(host) {
		return fmt.Errorf("URL host %q is not allowed", host)
	}
	return nil
}

// isBlockedHost reports whether host is a loopback, link-local, private, or
// otherwise internal address that a series URL should never target.
func isBlockedHost(host string) bool {
	host = strings.ToLower(host)
	// mDNS / Link-Local Multicast Name Resolution.
	if strings.HasSuffix(host, ".local") {
		return true
	}
	// Bracketed IPv6 (url.Hostname strips the brackets, but be defensive).
	host = strings.Trim(host, "[]")
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
			ip.IsInterfaceLocalMulticast() || ip.IsPrivate() || ip.IsUnspecified()
	}
	// Common internal hostnames that should never be a series source.
	switch host {
	case "localhost", "ip6-localhost", "ip6-loopback":
		return true
	}
	// ".internal" and ".localhost" TLDs (RFC 6761 / 8374 reserved).
	if strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".localhost") {
		return true
	}
	return false
}

// FetchHTML fetches HTML for the given URL. If the scraper implements
// BrowserFetcher (e.g., sites behind Cloudflare), it uses the browser;
// otherwise it falls back to the provided HTTP client.
func FetchHTML(scr Scraper, httpClient HTTPFetcher, url string) (string, error) {
	if bf, ok := scr.(BrowserFetcher); ok {
		return bf.FetchHTML(url)
	}
	return httpClient.FetchHTML(url)
}

// HTTPFetcher is the interface satisfied by the pkg/http Client.
type HTTPFetcher interface {
	FetchHTML(url string) (string, error)
}

// NormalizeURL normalizes a raw URL
func NormalizeURL(rawURL string) string {
	url := strings.TrimSpace(rawURL)

	// Handle protocol separately to avoid breaking it
	if strings.HasPrefix(url, "http://") {
		url = "http://" + multiSlash.ReplaceAllString(url[7:], "/")
	} else if strings.HasPrefix(url, "https://") {
		url = "https://" + multiSlash.ReplaceAllString(url[8:], "/")
	} else {
		url = multiSlash.ReplaceAllString(url, "/")
		if strings.HasPrefix(url, "//") {
			url = "https:" + url
		} else if strings.HasPrefix(url, "/") {
			return url
		} else {
			url = "https://" + url
		}
	}
	return url
}

// NormalizeURLWithBase joins a relative URL with a base URL using proper URL resolution
func NormalizeURLWithBase(href, baseURL string) string {
	href = strings.TrimSpace(href)
	if href == "" {
		// An empty reference would resolve to the base URL itself, which
		// callers would then treat as a usable resource URL (e.g. a cover).
		return ""
	}

	// Already absolute
	if strings.HasPrefix(href, "http") {
		return NormalizeURL(href)
	}

	// Protocol-relative URL
	if strings.HasPrefix(href, "//") {
		return "https:" + href
	}

	// Root-relative URL (starts with /)
	if strings.HasPrefix(href, "/") {
		parsed, err := url.Parse(baseURL)
		if err != nil {
			return baseURL + href
		}
		return parsed.Scheme + "://" + parsed.Host + href
	}

	// Relative URL (doesn't start with /) - use net/url for proper resolution
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return baseURL + "/" + href
	}
	ref, err := parsed.Parse(href)
	if err != nil {
		return baseURL + "/" + href
	}
	return ref.String()
}

// ParseChapterNumber extracts chapter number from text
func ParseChapterNumber(text string) float64 {
	text = strings.ToLower(text)
	matches := chapterRegex.FindStringSubmatch(text)
	if len(matches) < 2 {
		return 0
	}
	num, _ := strconv.ParseFloat(matches[1], 64)
	return num
}

// SortChaptersByNumber sorts chapters by number descending (newest first)
func SortChaptersByNumber(chapters []models.Chapter) {
	sort.Slice(chapters, func(i, j int) bool {
		return chapters[i].Number > chapters[j].Number
	})
}

// SortChaptersAscending sorts chapters by number ascending (oldest/lowest first).
// Use this before downloading to ensure chapters are saved in reading order.
func SortChaptersAscending(chapters []models.Chapter) {
	sort.Slice(chapters, func(i, j int) bool {
		return chapters[i].Number < chapters[j].Number
	})
}

// SortChaptersPtrAscending sorts chapter pointers by number ascending (oldest/lowest first).
// Use this before downloading to ensure chapters are saved in reading order.
func SortChaptersPtrAscending(chapters []*models.Chapter) {
	sort.Slice(chapters, func(i, j int) bool {
		return chapters[i].Number < chapters[j].Number
	})
}

// ExtractText extracts text from an HTML element
func ExtractText(html string, selector string) string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(doc.Find(selector).First().Text())
}

// ExtractTextFromHTML extracts text from the first element matching selector.
func ExtractTextFromHTML(html string, selector string) string {
	return ExtractText(html, selector)
}

// ExtractMetaContent returns the content attribute of a meta tag with the
// given property or name attribute.
func ExtractMetaContent(html, property string) string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return ""
	}
	var value string
	doc.Find("meta").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		if prop, _ := s.Attr("property"); strings.EqualFold(prop, property) {
			value, _ = s.Attr("content")
			return false
		}
		if name, _ := s.Attr("name"); strings.EqualFold(name, property) {
			value, _ = s.Attr("content")
			return false
		}
		return true
	})
	return strings.TrimSpace(value)
}

// NormalizeGenres trims, lower-cases, deduplicates, and sorts genre strings.
func NormalizeGenres(genres []string) []string {
	seen := make(map[string]bool)
	unique := make([]string, 0, len(genres))
	for _, g := range genres {
		g = strings.TrimSpace(strings.ToLower(g))
		if g == "" || seen[g] {
			continue
		}
		seen[g] = true
		unique = append(unique, g)
	}
	sort.Strings(unique)
	return unique
}

// StripHTMLText removes HTML tags from a fragment and collapses whitespace.
func StripHTMLText(htmlFragment string) string {
	doc, err := html.Parse(strings.NewReader(htmlFragment))
	if err != nil {
		return strings.TrimSpace(htmlFragment)
	}
	var walk func(*html.Node, bool) string
	walk = func(n *html.Node, inScript bool) string {
		if n.Type == html.TextNode {
			if inScript {
				return ""
			}
			return n.Data
		}
		script := n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style")
		var sb strings.Builder
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			sb.WriteString(walk(c, script || inScript))
		}
		if n.Type == html.ElementNode && (n.Data == "br" || n.Data == "p" || n.Data == "div" || n.Data == "li") {
			sb.WriteString(" ")
		}
		return sb.String()
	}
	text := walk(doc, false)
	re := regexp.MustCompile(`\s+`)
	return strings.TrimSpace(re.ReplaceAllString(text, " "))
}

// GetImageExtension extracts the file extension from an image URL
// Uses the pre-compiled imageExtRegex for efficiency
func GetImageExtension(url string) string {
	matches := imageExtRegex.FindStringSubmatch(url)
	if len(matches) > 1 {
		return "." + matches[1]
	}
	return ".jpg" // Default
}

// GenerateID creates a unique ID using the current timestamp
func GenerateID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

// SortImagesByPageNumber sorts images by their Page field in ascending order.
// This ensures images are always in top-to-bottom reading order regardless
// of extraction method. If Page is 0 for all images, it falls back to
// sorting by URL (which typically contains a page number).
func SortImagesByPageNumber(images []models.Image) {
	sort.Slice(images, func(i, j int) bool {
		if images[i].Page != images[j].Page {
			return images[i].Page < images[j].Page
		}
		// Fallback: sort by URL to ensure deterministic order
		return images[i].URL < images[j].URL
	})
}

// SortImagesByURL sorts images by extracting a numeric page index from their URLs.
// This is used when a scraper extracts images via regex (which may not guarantee order)
// to ensure they appear in the correct reading sequence.
//
// It tries multiple strategies to extract a sort key from each URL, in order:
//  1. A trailing number before the file extension (e.g., /001.webp, /12.jpg)
//  2. Any numeric segment in the URL path (e.g., /5/ in /chapters/5/something.webp)
//  3. Falls back to lexicographic URL comparison for deterministic ordering
func SortImagesByURL(images []models.Image) {
	sort.Slice(images, func(i, j int) bool {
		iURL := images[i].URL
		jURL := images[j].URL

		iMatch := TrailingNumRegex.FindStringSubmatch(iURL)
		jMatch := TrailingNumRegex.FindStringSubmatch(jURL)
		if len(iMatch) > 1 && len(jMatch) > 1 {
			iNum, _ := strconv.Atoi(iMatch[1])
			jNum, _ := strconv.Atoi(jMatch[1])
			if iNum != jNum {
				return iNum < jNum
			}
		} else if len(iMatch) > 1 {
			// Only i has a trailing number — it comes after ambiguous ones
			return false
		} else if len(jMatch) > 1 {
			// Only j has a trailing number — it comes after ambiguous ones
			return true
		}

		iPathNums := PathNumRegex.FindAllStringSubmatch(iURL, -1)
		jPathNums := PathNumRegex.FindAllStringSubmatch(jURL, -1)
		if len(iPathNums) > 0 && len(jPathNums) > 0 {
			// Compare the last numeric path segment from each URL
			iLast := iPathNums[len(iPathNums)-1]
			jLast := jPathNums[len(jPathNums)-1]
			iNum, _ := strconv.Atoi(iLast[1])
			jNum, _ := strconv.Atoi(jLast[1])
			if iNum != jNum {
				return iNum < jNum
			}
		}

		// Strategy 3: fallback to lexicographic comparison for deterministic order
		return iURL < jURL
	})
}
