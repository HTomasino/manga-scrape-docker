package hmanga

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/playwright-community/playwright-go"

	"github.com/user/comic-scraper/pkg/config"
	"github.com/user/comic-scraper/pkg/fileutil"
	"github.com/user/comic-scraper/pkg/scraper/browser"
)

const maxPagination = 50

// artistNameRe extracts the artist name from the ?q=artist:NAME query param.
// Exported via ExtractArtistName for reuse by callers.
var artistNameRe = regexp.MustCompile(`[?&]q=artist:([^&]+)`)

// ExtractArtistName parses the artist name from a HentaiNexus artist URL's
// ?q=artist:NAME parameter, URL-decodes it (turning %22 into " and + into a
// space), strips surrounding quotes, and trims whitespace. Returns "" if not
// found. Example: "?q=artist:%22Kurosu+Gatari%22" -> "Kurosu Gatari".
func ExtractArtistName(artistURL string) string {
	m := artistNameRe.FindStringSubmatch(artistURL)
	if len(m) < 2 {
		return ""
	}
	return cleanArtistName(m[1])
}

// CleanArtistName URL-decodes a raw artist query value and strips surrounding
// quotes and stray punctuation that HentaiNexus wraps artist names in (e.g.
// %22...%22 -> "..." -> ...). + is treated as a space (query-string encoding).
// Exported so the server can clean up legacy folder names on startup.
func CleanArtistName(raw string) string {
	return cleanArtistName(raw)
}

// cleanArtistName URL-decodes a raw artist query value and strips surrounding
// quotes and stray punctuation that HentaiNexus wraps artist names in (e.g.
// %22...%22 -> "..." -> ...). + is treated as a space (query-string encoding).
func cleanArtistName(raw string) string {
	// Query-string decode: + -> space, then percent-decode.
	raw = strings.ReplaceAll(raw, "+", " ")
	if decoded, err := url.QueryUnescape(raw); err == nil {
		raw = decoded
	}
	// Strip surrounding quotes (both " and the curly variants) and trim.
	raw = strings.Trim(raw, "\"\u201c\u201d' ")
	return strings.TrimSpace(raw)
}

// IsHentaiNexusURL reports whether raw is a URL on an allowed HentaiNexus host.
func IsHentaiNexusURL(raw string) bool {
	return isHentaiNexusURL(raw)
}

// hentaiNexusHostAllowlist is the set of hosts HentaiNexus serves content
// from. Used only by isHentaiNexusURL / IsHentaiNexusURL.
var hentaiNexusHostAllowlist = []string{"hentainexus.com", "www.hentainexus.com", "images.hentainexus.com"}

// isHentaiNexusURL reports whether raw is a URL on an allowed HentaiNexus host.
func isHentaiNexusURL(raw string) bool {
	if raw == "" {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return isHentaiNexusHost(u.Hostname())
}

// isHentaiNexusHost reports whether host matches an allowed domain exactly or
// as a .hentainexus.com subdomain suffix.
func isHentaiNexusHost(host string) bool {
	host = strings.ToLower(host)
	for _, d := range hentaiNexusHostAllowlist {
		if host == d {
			return true
		}
	}
	return strings.HasSuffix(host, ".hentainexus.com")
}

// HentaiNexusManager handles HentaiNexus scraping and downloads. All
// Playwright operations are routed through the shared *browser.Queue that is
// also used by the manga scrapers, so the two subsystems share one Chrome
// process tree, one persistent profile, and one Cloudflare session. The
// manager itself no longer owns a Playwright driver or a browser context --
// it borrows both from the queue on every operation.
//
// Concurrency is capped by per-operation semaphores (m.sem for downloads,
// m.scrapeSem for book-list scrapes). On shutdown, Stop() closes stopCh to
// reject new ops at the boundary and waits for in-flight ops to drain; it
// does NOT cancel in-flight downloads (Ctrl+C must not lose data).
type HentaiNexusManager struct {
	// cfgPtr is the current configuration. It is replaced wholesale by
	// UpdateConfig via atomic store so concurrent readers (worker
	// goroutines in Login/DownloadBook, plus the Stop goroutine) always
	// see a consistent *config.Config value. The Go memory model would
	// otherwise permit torn reads of the struct fields from racing
	// writes.
	cfgPtr atomic.Pointer[config.Config]

	// browserQueue is the shared Playwright serializer used for every
	// HentaiNexus operation. Must be non-nil; passing nil to NewManager
	// panics.
	browserQueue *browser.Queue

	// sem caps concurrent downloads at 2.
	sem chan struct{}
	// scrapeSem caps concurrent book-list scrapes (ExtractBooks) at 2 so the
	// scheduler's per-artist fanout can't open an unbounded number of pages.
	scrapeSem chan struct{}

	// loginKnown tracks whether the persistent profile is already logged in.
	loginMu      sync.Mutex
	loginKnown   bool
	loginChecked bool

	// userAgent is the browser's User-Agent string, cached after the
	// first call to getUserAgent. Used for direct HTTP downloads so the
	// request matches the browser and isn't rejected as a bot.
	userAgentMu sync.Mutex
	userAgent   string

	// active tracks in-flight operations as a single counter protected by
	// activeMu. Stop() blocks on activeCond until active == 0 (or the
	// configured shutdown timeout elapses, see m.cfg().ShutdownTimeoutSeconds).
	activeMu   sync.Mutex
	active     int64
	activeCond *sync.Cond

	stopOnce sync.Once
	stopCh   chan struct{}
	// waitStopCh is closed by waitInflightWithTimeout on its timeout branch
	// to break the waiter goroutine out of its activeCond.Wait() park. It
	// is separate from stopCh so Stop()'s close(m.stopCh) is the single
	// owner of that channel (closing it twice would panic).
	waitStopCh chan struct{}

	// progressCallback emits live download lifecycle ticks for the UI.
	progressCallback func(HentaiNexusManagerDownloadProgress)
	progressMu       sync.RWMutex
}

// HentaiNexusManagerDownloadProgress describes one progress tick for a single
// H-Manga download. URL/SuggestedFilename are only present on the first tick
// (when the download starts); later ticks carry only Bytes/Total.
type HentaiNexusManagerDownloadProgress struct {
	DownloadID string // Server-side record ID for O(1) lookup.
	ArtistID   string
	FolderName string
	BookID     string
	Title      string

	Phase string // "pending" | "starting" | "downloading" | "verifying" | "done" | "failed" | "cancelled" | "superseded"

	SuggestedFilename string
	DownloadURL       string

	BytesDownloaded int64
	TotalBytes      int64

	Err error
}

// NewManager creates a HentaiNexusManager. The supplied browserQueue is used
// for every Playwright operation; the manager does not own its own driver or
// persistent context. Pass the same *browser.Queue used by the manga
// scrapers so the two subsystems share one Chrome process tree and one
// Cloudflare session.
func NewManager(cfg *config.Config, q *browser.Queue) *HentaiNexusManager {
	if q == nil {
		panic("hmanga.NewManager: browserQueue must not be nil")
	}
	m := &HentaiNexusManager{
		browserQueue: q,
		sem:          make(chan struct{}, 2),
		scrapeSem:    make(chan struct{}, 2),
		stopCh:       make(chan struct{}),
		waitStopCh:   make(chan struct{}),
	}
	m.cfgPtr.Store(cfg)
	m.activeCond = sync.NewCond(&m.activeMu)
	return m
}

// UpdateConfig updates the in-memory config used by the manager. It does
// NOT affect the shared browser.Queue or its persistent profile -- those
// outlive any config change. The replacement is atomic so concurrent
// readers always see a fully-published *config.Config value rather than
// a partially-written one in flight.
func (m *HentaiNexusManager) UpdateConfig(cfg *config.Config) {
	m.cfgPtr.Store(cfg)
}

// cfg returns a snapshot of the current *config.Config. Callers must NOT
// mutate the returned pointer. The pointer is safe to read without
// holding any lock; the struct it points to is treated as read-only by
// all callers (only UpdateConfig writes, and it replaces the pointer
// wholesale).
func (m *HentaiNexusManager) cfg() *config.Config {
	return m.cfgPtr.Load()
}

// SetProgressCallback registers the callback that receives H-Manga download
// lifecycle ticks. It is safe to call repeatedly (last registration wins).
func (m *HentaiNexusManager) SetProgressCallback(fn func(HentaiNexusManagerDownloadProgress)) {
	m.progressMu.Lock()
	defer m.progressMu.Unlock()
	m.progressCallback = fn
}

func (m *HentaiNexusManager) emitProgress(p HentaiNexusManagerDownloadProgress) {
	m.progressMu.RLock()
	fn := m.progressCallback
	m.progressMu.RUnlock()
	if fn != nil {
		func() { defer func() { recover() }(); fn(p) }()
	}
}

// Stop signals "no new ops" and waits for in-flight operations to finish
// (bounded by m.cfg().ShutdownTimeoutSeconds). The shared browser.Queue is
// NOT stopped here; the caller (typically *Server.Shutdown) stops the
// queue separately so all subscribers wind down in the right order.
// In-flight downloads are NOT cancelled -- they run to natural completion
// (or until the timeout fires). Safe to call multiple times.
func (m *HentaiNexusManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)

		m.activeMu.Lock()
		inFlight := m.active
		m.activeMu.Unlock()
		if inFlight > 0 {
			timeout := time.Duration(m.cfg().ShutdownTimeoutSeconds) * time.Second
			if timeout <= 0 {
				timeout = time.Duration(config.DefaultShutdownTimeoutSeconds) * time.Second
			}
			log.Printf("[HMANGA] Stop: %d in-flight op(s); waiting up to %v for them to finish", inFlight, timeout)
		}
		m.waitInflightWithTimeout()
	})
}

// waitInflightWithTimeout waits for in-flight operations to finish. The
// upper bound is m.cfg().ShutdownTimeoutSeconds (default 10 minutes); past that
// the function returns so a stuck op can't hang the process forever.
//
// The waiter goroutine parks on activeCond.Wait() between checks. Two paths
// wake it: opDone's Broadcast when active reaches 0, and a close of
// waitStopCh from the timeout branch below. waitStopCh is separate from
// stopCh because Stop() is the sole owner of stopCh and closing it twice
// would panic.
//
// ORDERING INVARIANT -- DO NOT REORDER the timeout branch:
// The timeout branch holds activeMu while it does (1) Broadcast, then
// (2) close(waitStopCh), then releases. Reordering these breaks the wakeup:
//
//   - If Broadcast happens AFTER close(waitStopCh): the goroutine's
//     non-blocking select on waitStopCh returns "closed", exits cleanly.
//     OK this works.
//   - If Broadcast happens BEFORE close(waitStopCh) and we release activeMu
//     between them: the goroutine wakes from Wait, reacquires activeMu,
//     the non-blocking select sees waitStopCh not yet closed, falls
//     through to default, loops back into Wait, then close(waitStopCh)
//     fires -- but sync.Cond.Wait only wakes on Signal/Broadcast, so the
//     goroutine is parked forever and we leak it.
//   - To avoid this, Broadcast MUST run before close(waitStopCh) while
//     still holding activeMu. The waiter then reacquires activeMu AFTER
//     we release it (Broadcast wakes it, but it blocks on the mutex), and
//     by the time it reacquires the mutex, waitStopCh is already closed,
//     so the non-blocking select sees "closed" and exits cleanly.
//
// In short: keep Broadcast before close(waitStopCh), keep the mutex held
// across both, and the wakeup is deterministic. Reverse either and the
// waiter goroutine leaks for the process lifetime on a truly stuck op.
func (m *HentaiNexusManager) waitInflightWithTimeout() {
	timeout := time.Duration(m.cfg().ShutdownTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(config.DefaultShutdownTimeoutSeconds) * time.Second
	}
	deadline := time.Now().Add(timeout)

	done := make(chan struct{})
	go func() {
		for {
			m.activeMu.Lock()
			active := m.active
			m.activeMu.Unlock()
			if active == 0 {
				close(done)
				return
			}
			m.activeMu.Lock()
			m.activeCond.Wait()
			m.activeMu.Unlock()
			select {
			case <-m.waitStopCh:
				close(done)
				return
			default:
			}
		}
	}()

	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

	for {
		select {
		case <-done:
			return
		case <-progress.C:
			m.activeMu.Lock()
			active := m.active
			m.activeMu.Unlock()
			remaining := time.Until(deadline)
			if active > 0 {
				log.Printf("[HMANGA] Stop: waiting for %d in-flight op(s) to finish (%v remaining)", active, remaining.Round(time.Second))
			}
		case <-time.After(time.Until(deadline)):
			if time.Now().Before(deadline) {
				continue
			}
			m.activeMu.Lock()
			stillActive := m.active
			// ORDERING INVARIANT -- see the function-level comment.
			m.activeCond.Broadcast()
			select {
			case <-m.waitStopCh:
			default:
				close(m.waitStopCh)
			}
			m.activeMu.Unlock()
			log.Printf("[HMANGA] Stop: %d in-flight op(s) did not finish within %v, proceeding", stillActive, timeout)
			return
		}
	}
}

// opStart marks the start of an operation. Every opStart must be paired
// with a deferred opDone.
func (m *HentaiNexusManager) opStart() {
	m.activeMu.Lock()
	m.active++
	m.activeMu.Unlock()
}

// opDone decrements the active counter and wakes any Stop() waiters. It
// should be deferred by every operation that called opStart.
func (m *HentaiNexusManager) opDone() {
	m.activeMu.Lock()
	if m.active > 0 {
		m.active--
	}
	wake := m.active == 0
	m.activeMu.Unlock()
	if wake {
		m.activeCond.Broadcast()
	}
}

// stopped reports whether Stop() has been called (i.e. no new ops should
// be started).
func (m *HentaiNexusManager) stopped() bool {
	select {
	case <-m.stopCh:
		return true
	default:
		return false
	}
}

// runOnQueue runs fn under the shared browser.Queue. It does NOT
// touch the manager's active counter -- callers that need to participate
// in Stop()'s graceful-shutdown wait MUST call m.opStart()/defer
// m.opDone() at the function's top level (covering the full body
// including any post-queue work like the plain-HTTP download stream).
// Keeping the accounting at the public-method level ensures Stop() sees
// an in-flight op for the entire duration of the work, not just the
// brief portions that touch the queue.
func (m *HentaiNexusManager) runOnQueue(fn func(ctx playwright.BrowserContext) error) error {
	return m.browserQueue.Run(fn)
}

// isLoggedInOnPage checks whether the profile is currently logged in by
// visiting "/" and looking for the absence of a "Login" nav link. It must be
// called within a page the caller manages.
func isLoggedInOnPage(page playwright.Page) bool {
	// A logged-in session shows the username dropdown and no "Login" link.
	loginLink := page.GetByRole(*playwright.AriaRoleLink, playwright.PageGetByRoleOptions{Name: "Login"})
	count, err := loginLink.Count()
	if err != nil {
		return false
	}
	return count == 0
}

// Login ensures the persistent profile is logged in. If already logged in,
// it returns without doing anything. Credentials come from the manager's
// config. Returns an error if credentials are missing or login fails.
//
// NESTED-COUNT NOTE: Login brackets its own work with opStart/opDone. When
// called from within a DownloadBook that has already opStart'd, m.active
// briefly reports a +1 overcount for the duration of the nested Login. This
// is harmless (both increments are paired with decrements; the counter
// always returns to the correct value) and intentionally cheaper than
// adding a second entry point that callers can forget to defer.
func (m *HentaiNexusManager) Login() error {
	if m.stopped() {
		return errors.New("manager stopped")
	}
	m.opStart()
	defer m.opDone()

	m.loginMu.Lock()
	if m.loginChecked && m.loginKnown {
		m.loginMu.Unlock()
		return nil
	}
	m.loginMu.Unlock()

	c := m.cfg()
	if c.HentaiNexusUsername == "" || c.HentaiNexusPassword == "" {
		return errors.New("HentaiNexus credentials not configured")
	}

	return m.runOnQueue(func(ctx playwright.BrowserContext) error {
		page, err := ctx.NewPage()
		if err != nil {
			return fmt.Errorf("new page: %w", err)
		}
		defer func() { defer func() { recover() }(); page.Close() }()

		// First check if already logged in.
		if _, err := page.Goto("https://hentainexus.com/", playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		}); err != nil {
			return fmt.Errorf("navigate to home: %w", err)
		}
		if isLoggedInOnPage(page) {
			m.loginMu.Lock()
			m.loginChecked = true
			m.loginKnown = true
			m.loginMu.Unlock()
			log.Printf("[HMANGA] Already logged in")
			return nil
		}

		if _, err := page.Goto("https://hentainexus.com/login", playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		}); err != nil {
			return fmt.Errorf("navigate to login: %w", err)
		}

		userInput := page.GetByRole(*playwright.AriaRoleTextbox, playwright.PageGetByRoleOptions{Name: "Username or email"})
		passInput := page.GetByRole(*playwright.AriaRoleTextbox, playwright.PageGetByRoleOptions{Name: "Password"})
		if err := userInput.Fill(c.HentaiNexusUsername); err != nil {
			return fmt.Errorf("fill username: %w", err)
		}
		if err := passInput.Fill(c.HentaiNexusPassword); err != nil {
			return fmt.Errorf("fill password: %w", err)
		}

		loginBtn := page.GetByRole(*playwright.AriaRoleButton, playwright.PageGetByRoleOptions{Name: "Login"})
		if err := loginBtn.Click(); err != nil {
			return fmt.Errorf("click login: %w", err)
		}

		// Wait for redirect away from /login.
		if err := page.WaitForURL("https://hentainexus.com/", playwright.PageWaitForURLOptions{
			Timeout: playwright.Float(15000),
		}); err != nil {
			return fmt.Errorf("login failed (still on /login): %w", err)
		}

		if !isLoggedInOnPage(page) {
			return errors.New("login failed: Login link still present after submit")
		}

		m.loginMu.Lock()
		m.loginChecked = true
		m.loginKnown = true
		m.loginMu.Unlock()
		log.Printf("[HMANGA] Login successful")
		return nil
	})
}

// ExtractBooks scrapes the book listing for an artist URL, following
// pagination, and returns deduplicated books sorted by id descending (newest
// first, matching site order).
func (m *HentaiNexusManager) ExtractBooks(artistURL string) ([]Book, error) {
	if m.stopped() {
		return nil, errors.New("manager stopped")
	}
	m.opStart()
	defer m.opDone()

	artistName := ExtractArtistName(artistURL)

	select {
	case m.scrapeSem <- struct{}{}:
	case <-m.stopCh:
		return nil, errors.New("manager stopped")
	}
	defer func() { <-m.scrapeSem }()

	var books []Book
	err := m.runOnQueue(func(ctx playwright.BrowserContext) error {
		page, err := ctx.NewPage()
		if err != nil {
			return fmt.Errorf("new page: %w", err)
		}
		defer func() { defer func() { recover() }(); page.Close() }()

		seen := make(map[string]Book)
		current := artistURL
		for pageNum := 1; pageNum <= maxPagination; pageNum++ {
			if _, err := page.Goto(current, playwright.PageGotoOptions{
				WaitUntil: playwright.WaitUntilStateDomcontentloaded,
			}); err != nil {
				return fmt.Errorf("navigate to artist page: %w", err)
			}

			// Collect book links via in-page JS: all a[href*="/view/"] inside .column cards.
			result, err := page.Evaluate(`() => {
				const out = [];
				const seen = new Set();
			 document.querySelectorAll('a[href*="/view/"]').forEach(a => {
				 const href = a.getAttribute('href');
				 const m = href.match(/\/view\/(\d+)/);
				 if (!m) return;
				 const id = m[1];
				 if (seen.has(id)) return;
				 seen.add(id);
				 const card = a.closest('.column') || a;
				 let title = '';
				 const header = card.querySelector && card.querySelector('.card-header');
				 if (header) title = header.getAttribute('title') || '';
				 if (!title) {
					 const t = card.querySelector && card.querySelector('.card-header-title');
					 if (t) title = t.textContent.trim();
				 }
				 if (!title) title = a.textContent.trim();
				 out.push({ id, title, url: href });
				});
				return out;
			}`)
			if err != nil {
				return fmt.Errorf("extract books: %w", err)
			}

			items, _ := result.([]interface{})
			for _, it := range items {
				obj, ok := it.(map[string]interface{})
				if !ok {
					continue
				}
				id, _ := obj["id"].(string)
				title, _ := obj["title"].(string)
				href, _ := obj["url"].(string)
				if id == "" {
					continue
				}
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = Book{ID: id, Title: strings.TrimSpace(title), URL: normalizeViewURL(href)}
			}

			nextURL, err := nextPaginationURL(page, artistURL, artistName, pageNum)
			if err != nil {
				// A locator error here means pagination state is unknown —
				// surface it instead of silently reporting a truncated list.
				return fmt.Errorf("check pagination: %w", err)
			}
			if nextURL == "" {
				break
			}
			current = nextURL
		}

		bs := make([]Book, 0, len(seen))
		for _, b := range seen {
			bs = append(bs, b)
		}
		sort.Slice(bs, func(i, j int) bool { return bs[i].ID > bs[j].ID })
		books = bs
		return nil
	})
	if err != nil {
		return nil, err
	}
	return books, nil
}

// nextPaginationURL returns the next page URL for paginated artist results, or
// "" if there is no next page. HentaiNexus uses /page/N?q=artist:NAME links in
// the .pagination nav; the links are URL-encoded, so we match by parsed page
// number rather than by raw string comparison. artistName is used only for logs.
func nextPaginationURL(page playwright.Page, artistURL, artistName string, currentPage int) (string, error) {
	links, err := page.Locator(".pagination a[href*='/page/']").All()
	if err != nil {
		return "", err
	}
	for _, l := range links {
		href, err := l.GetAttribute("href")
		if err != nil || href == "" {
			continue
		}
		u, err := url.Parse(href)
		if err != nil {
			continue
		}
		pageNum, ok := parsePageNumber(u.Path)
		if !ok || pageNum <= currentPage {
			continue
		}
		if !u.IsAbs() {
			u.Scheme = "https"
			u.Host = "hentainexus.com"
		}
		log.Printf("[HMANGA] Artist %s: found page %d, continuing to page %d", artistName, currentPage, pageNum)
		return u.String(), nil
	}
	return "", nil
}

// parsePageNumber extracts the integer N from a HentaiNexus pagination path
// /page/N. It returns (0, false) for non-pagination paths or for paths
// whose N is not a positive integer.
func parsePageNumber(path string) (int, bool) {
	const prefix = "/page/"
	if !strings.HasPrefix(path, prefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(path, prefix))
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// normalizeViewURL converts a possibly-relative /view/{id} href into an
// absolute path-relative form (kept as "/view/{id}").
func normalizeViewURL(href string) string {
	if i := strings.Index(href, "/view/"); i >= 0 {
		return href[i:]
	}
	return href
}

// DownloadBook downloads a single book ZIP directly via HTTP using cookies
// from the Playwright browser session. Cookies and User-Agent are extracted
// inside q.Run so the shared queue keeps the context alive only as long as
// needed; the bulk of the download is a plain HTTP stream that does NOT
// hold up any other browser operation.
//
// The browser context is used for authentication only -- Login() ensures
// the session cookie is set, and we extract it via ctx.Cookies() before the
// HTTP request. The HTTP request carries the same User-Agent as the browser
// so HentaiNexus doesn't reject it as a bot.
func (m *HentaiNexusManager) DownloadBook(artistID, bookID, destDir, downloadID string) (filename string, size int64, err error) {
	if m.stopped() {
		return "", 0, errors.New("manager stopped")
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", 0, fmt.Errorf("create dest dir: %w", err)
	}

	// Acquire one of 2 concurrency slots.
	select {
	case m.sem <- struct{}{}:
	case <-m.stopCh:
		return "", 0, errors.New("manager stopped")
	}
	defer func() { <-m.sem }()

	// opStart/opDone brackets the ENTIRE DownloadBook body so Stop()
	// waits for the plain-HTTP download stream to complete, not just
	// the brief cookie-extraction phase. Without this, Stop()'s wait
	// could drop to active=0 between the queue call and the HTTP
	// stream and return while the file is still being written to
	// disk, losing the in-progress download.
	m.opStart()
	defer m.opDone()

	if lerr := m.Login(); lerr != nil {
		return "", 0, fmt.Errorf("login: %w", lerr)
	}

	zipURL := fmt.Sprintf("https://hentainexus.com/zip/%s", bookID)
	progress := HentaiNexusManagerDownloadProgress{DownloadID: downloadID, ArtistID: artistID, BookID: bookID}
	progress.Phase = "starting"
	m.emitProgress(progress)

	// Extract cookies and (lazily) the User-Agent under the queue. Once we
	// have them the rest of the download is a plain HTTP stream that
	// doesn't need the browser queue at all.
	var cookies []playwright.Cookie
	var ua string
	if err := m.runOnQueue(func(ctx playwright.BrowserContext) error {
		pcs, cerr := ctx.Cookies(zipURL)
		if cerr != nil {
			return fmt.Errorf("extract cookies: %w", cerr)
		}
		cookies = pcs
		// Cache the User-Agent so future HTTP downloads outside the queue
		// match the browser. A fresh probe page is used so the value
		// reflects the current browser state (the queue may have just
		// recreated the context).
		probe, perr := ctx.NewPage()
		if perr == nil {
			if v, eerr := probe.Evaluate("() => navigator.userAgent"); eerr == nil {
				if s, ok := v.(string); ok && s != "" {
					ua = s
					m.userAgentMu.Lock()
					m.userAgent = s
					m.userAgentMu.Unlock()
				}
			}
			func() { defer func() { recover() }(); probe.Close() }()
		}
		return nil
	}); err != nil {
		progress.Phase = "failed"
		progress.Err = err
		m.emitProgress(progress)
		return "", 0, err
	}
	if ua == "" {
		ua = m.getUserAgent()
	}

	// Use a non-cancellable context for the HTTP request. Stop() does NOT
	// interrupt in-flight downloads.
	downloadCtx := context.Background()

	// Try up to 2 attempts: the initial attempt, and a retry on 401/403
	// with re-login if the session expired.
	var lastErr error
	var lastStatusCode int
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			log.Printf("[HMANGA] HTTP %d on book %s, invalidating login cache and retrying", lastStatusCode, bookID)
			m.loginMu.Lock()
			m.loginKnown = false
			m.loginChecked = false
			m.loginMu.Unlock()
			if lerr := m.Login(); lerr != nil {
				progress.Phase = "failed"
				progress.Err = fmt.Errorf("login retry: %w", lerr)
				m.emitProgress(progress)
				return "", 0, progress.Err
			}
			// Re-extract cookies for the fresh login.
			if err := m.runOnQueue(func(ctx playwright.BrowserContext) error {
				pcs, cerr := ctx.Cookies(zipURL)
				if cerr != nil {
					return cerr
				}
				cookies = pcs
				return nil
			}); err != nil {
				lastErr = fmt.Errorf("extract cookies (retry): %w", err)
				continue
			}
		}

		req, err := http.NewRequestWithContext(downloadCtx, "GET", zipURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("create request: %w", err)
			continue
		}
		for _, c := range cookies {
			req.AddCookie(&http.Cookie{Name: c.Name, Value: c.Value, Domain: c.Domain, Path: c.Path})
		}
		if ua != "" {
			req.Header.Set("User-Agent", ua)
		}
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		req.Header.Set("Referer", "https://hentainexus.com/view/"+bookID)

		httpClient := &http.Client{Timeout: 10 * time.Minute}
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http get: %w", err)
			continue
		}

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			resp.Body.Close()
			lastStatusCode = resp.StatusCode
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, resp.Status)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			progress.Phase = "failed"
			progress.Err = fmt.Errorf("http %d: %s", resp.StatusCode, resp.Status)
			m.emitProgress(progress)
			return "", 0, progress.Err
		}

		suggested := ""
		if cd := resp.Header.Get("Content-Disposition"); cd != "" {
			suggested = parseContentDispositionFilename(cd)
		}
		if suggested == "" {
			suggested = bookID + ".zip"
		}
		safeName := filepath.Base(suggested)
		safeName = fileutil.SanitizeFileName(safeName)
		if safeName == "" || safeName == "." || safeName == ".." || strings.ContainsAny(safeName, `/\`) {
			safeName = bookID + ".zip"
		}
		safeName = m.applyZipNameRegex(safeName)
		destPath := filepath.Join(destDir, safeName)

		totalBytes := resp.ContentLength
		progress.Phase = "downloading"
		progress.SuggestedFilename = suggested
		progress.DownloadURL = zipURL
		progress.TotalBytes = totalBytes
		m.emitProgress(progress)

		out, err := os.Create(destPath)
		if err != nil {
			resp.Body.Close()
			lastErr = fmt.Errorf("create file: %w", err)
			continue
		}

		written := int64(0)
		// 256 KB buffer: large enough that per-read syscall overhead
		// doesn't bottleneck a typical 50-200 MBit home connection, small
		// enough to avoid GC pressure if hundreds of downloads are queued.
		buf := make([]byte, 256*1024)
		lastTick := time.Now()
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := out.Write(buf[:n]); werr != nil {
					out.Close()
					os.Remove(destPath)
					resp.Body.Close()
					progress.Phase = "failed"
					progress.Err = fmt.Errorf("write file: %w", werr)
					m.emitProgress(progress)
					return "", 0, progress.Err
				}
				written += int64(n)
				if time.Since(lastTick) >= 500*time.Millisecond {
					progress.BytesDownloaded = written
					m.emitProgress(progress)
					lastTick = time.Now()
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				// Body read failed (network error, connection reset, etc.).
				// Cancellation is NOT a possible cause here -- downloadCtx is
				// context.Background() because Stop() must not interrupt
				// in-flight downloads. If this fires with a context error,
				// suspect http.Client.Timeout (10m above) or the underlying
				// transport.
				out.Close()
				os.Remove(destPath)
				resp.Body.Close()
				progress.Phase = "failed"
				progress.Err = fmt.Errorf("read body: %w", readErr)
				m.emitProgress(progress)
				return "", 0, progress.Err
			}
		}
		if err := out.Close(); err != nil {
			os.Remove(destPath)
			resp.Body.Close()
			progress.Phase = "failed"
			progress.Err = fmt.Errorf("close file: %w", err)
			m.emitProgress(progress)
			return "", 0, progress.Err
		}
		resp.Body.Close()

		info, statErr := os.Stat(destPath)
		if statErr != nil || info.Size() == 0 {
			progress.Phase = "failed"
			progress.Err = fmt.Errorf("file missing or empty after download: %s", destPath)
			m.emitProgress(progress)
			return "", 0, progress.Err
		}

		finalSize := info.Size()
		if totalBytes > 0 && totalBytes != finalSize {
			// The body ended cleanly but short of Content-Length: the ZIP is
			// truncated and would be corrupt. Treat as a hard failure so the
			// book is not recorded as downloaded.
			os.Remove(destPath)
			progress.Phase = "failed"
			progress.Err = fmt.Errorf("download truncated: got %d bytes, expected %d for book %s", finalSize, totalBytes, bookID)
			m.emitProgress(progress)
			return "", 0, progress.Err
		}

		progress.SuggestedFilename = ""
		progress.DownloadURL = ""
		progress.BytesDownloaded = finalSize
		progress.TotalBytes = finalSize
		progress.Phase = "verifying"
		m.emitProgress(progress)
		progress.Phase = "done"
		m.emitProgress(progress)
		log.Printf("[HMANGA] Downloaded book %s -> %s (%d bytes)", bookID, destPath, finalSize)
		return safeName, finalSize, nil
	}

	progress.Phase = "failed"
	progress.Err = lastErr
	m.emitProgress(progress)
	return "", 0, lastErr
}

// getUserAgent returns the cached browser User-Agent string, or "" if it
// has not been observed yet. The cache is populated by DownloadBook on
// every successful cookie extraction, so by the time anyone calls this
// the value is usually set.
func (m *HentaiNexusManager) getUserAgent() string {
	m.userAgentMu.Lock()
	defer m.userAgentMu.Unlock()
	return m.userAgent
}

// applyZipNameRegex applies the configured HMangaZipNameRegex to a downloaded
// book's ZIP file name. Every regex match is removed, so "[Tag] Name_123.zip"
// with the default pattern `\[[^\]]*\]|_` becomes "Name123.zip". An empty
// pattern (explicitly disabled in config) is a no-op. If the regex would
// strip the name entirely or remove the ".zip" extension, the original name
// is kept so the book never ends up with an unusable name.
func (m *HentaiNexusManager) applyZipNameRegex(name string) string {
	pattern := m.cfg().HMangaZipNameRegex
	if pattern == "" {
		return name
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Printf("[HMANGA] Invalid hmangaZipNameRegex %q (%v); keeping original filename %s", pattern, err, name)
		return name
	}
	cleaned := re.ReplaceAllString(name, "")
	cleaned = strings.TrimSpace(cleaned)
	// Guard: the result must still be a usable .zip name (non-empty base
	// name plus a .zip extension). Otherwise keep the original so
	// .books.json/verification stay consistent.
	base := strings.TrimSuffix(cleaned, filepath.Ext(cleaned))
	if cleaned == "" || base == "" || !strings.EqualFold(filepath.Ext(cleaned), ".zip") {
		log.Printf("[HMANGA] Regex %q would strip the filename from %q; keeping original", pattern, name)
		return name
	}
	return cleaned
}

// parseContentDispositionFilename extracts the filename from a
// Content-Disposition header value. Handles both RFC 5987 (filename*=UTF-8”...")
// and the legacy form (filename="..." or filename=...).
//
// Returns the extracted filename (sanitized of path separators) or "" if no
// usable filename is present.
func parseContentDispositionFilename(cd string) string {
	cd = strings.TrimSpace(cd)
	// Look for filename* first (RFC 5987 takes precedence over legacy).
	if idx := strings.Index(cd, "filename*="); idx >= 0 {
		// rest looks like: UTF-8''percent-encoded-name
		rest := cd[idx+len("filename*"):]
		if eq := strings.Index(rest, "'"); eq >= 0 {
			rest = rest[eq+1:]
			if eq2 := strings.Index(rest, "'"); eq2 >= 0 {
				rest = rest[eq2+1:]
			}
		}
		if semi := strings.Index(rest, ";"); semi >= 0 {
			rest = rest[:semi]
		}
		rest = strings.TrimSpace(rest)
		// Guard against malformed headers where the value portion is
		// empty (e.g. "filename*=UTF-8''"). Without this check
		// filepath.Base("") returns "", which downstream callers
		// would silently treat as a usable name.
		if rest == "" || rest == "." || rest == ".." {
			return ""
		}
		// RFC 5987 values are percent-encoded; PathUnescape leaves '+' intact
		// (QueryUnescape would turn a literal '+' into a space).
		if decoded, err := url.PathUnescape(rest); err == nil {
			return filepath.Base(decoded)
		}
		return filepath.Base(rest)
	}
	// Fall back to the legacy filename= form. Handle both quoted
	// ("filename with; semicolon.zip") and unquoted (filename.zip)
	// values correctly, plus the edge case where the filename ends
	// the header (no trailing semicolon).
	if idx := strings.Index(cd, "filename="); idx >= 0 {
		rest := cd[idx+len("filename="):]
		rest = strings.TrimSpace(rest)
		// If the value is quoted, find the closing quote and only then
		// treat any trailing content as a separate parameter. This
		// avoids chopping a filename like "Book; Chapter 1.zip" at
		// the embedded semicolon.
		if strings.HasPrefix(rest, `"`) {
			if end := strings.Index(rest[1:], `"`); end >= 0 {
				rest = rest[1 : 1+end]
			} else {
				// Unterminated quote -- fall back to stripping the
				// opening quote only.
				rest = strings.TrimPrefix(rest, `"`)
			}
		} else if semi := strings.Index(rest, ";"); semi >= 0 {
			// Unquoted: cut at the first semicolon separator.
			rest = rest[:semi]
			rest = strings.TrimSpace(rest)
		}
		if rest == "" || rest == "." || rest == ".." {
			return ""
		}
		return filepath.Base(rest)
	}
	return ""
}
