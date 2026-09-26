package browser

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/playwright-community/playwright-go"
)

// ConvertPlaywrightCookies converts Playwright cookies into net/http cookies
// for injection into the HTTP client's cookie jar. Shared by every scraper
// that reuses browser session cookies for plain HTTP image downloads.
//
// Playwright reports session cookies with Expires = -1; converting that via
// time.Unix produces a 1969 expiry and the cookie jar drops them, so session
// cookies keep the zero time (which cookiejar treats as a session cookie).
func ConvertPlaywrightCookies(pcs []playwright.Cookie) []*http.Cookie {
	cookies := make([]*http.Cookie, 0, len(pcs))
	for _, pc := range pcs {
		sameSite := http.SameSiteDefaultMode
		if pc.SameSite != nil {
			switch string(*pc.SameSite) {
			case "Strict":
				sameSite = http.SameSiteStrictMode
			case "Lax":
				sameSite = http.SameSiteLaxMode
			case "None":
				sameSite = http.SameSiteNoneMode
			}
		}
		expires := time.Time{}
		if pc.Expires >= 0 {
			expires = time.Unix(int64(pc.Expires), 0)
		}
		cookies = append(cookies, &http.Cookie{
			Name:     pc.Name,
			Value:    pc.Value,
			Path:     pc.Path,
			Domain:   pc.Domain,
			Expires:  expires,
			HttpOnly: pc.HttpOnly,
			Secure:   pc.Secure,
			SameSite: sameSite,
		})
	}
	return cookies
}

const (
	UserDataDirName      = "comic-scraper-chrome-profile"
	idleTimeout          = 30 * time.Second
	contextRetryCooldown = 5 * time.Second
	ScrollPauseDuration  = 1 * time.Second
)

// ScrollPageForLazyImages scrolls a Playwright page progressively to trigger
// all lazy-loaded images. step controls the pixel increment between scrolls
// and maxAttempts caps the number of scroll iterations to prevent infinite
// loops on dynamically growing pages.
func ScrollPageForLazyImages(page playwright.Page, step float64, maxAttempts int) error {
	log.Printf("[BROWSER-SCROLL] Starting lazy-image scroll (step=%.0f, maxAttempts=%d)", step, maxAttempts)

	_, err := page.Evaluate("() => { window.scrollTo(0, 0); }")
	if err != nil {
		return fmt.Errorf("initial scroll failed (page may be unresponsive): %w", err)
	}

	scrollHeight, err := page.Evaluate("() => document.body.scrollHeight")
	if err != nil {
		log.Printf("[BROWSER-SCROLL] Warning: could not get scroll height: %v", err)
		scrollHeight = float64(5000)
	}

	totalHeight := 0.0
	if sh, ok := scrollHeight.(float64); ok {
		totalHeight = sh
	}

	attempts := 0
	for pos := 0.0; pos < totalHeight && attempts < maxAttempts; pos += step {
		_, err = page.Evaluate(fmt.Sprintf("() => { window.scrollTo(0, %d); }", int(pos)))
		if err != nil {
			log.Printf("[BROWSER-SCROLL] Warning: scroll failed at pos %.0f: %v", pos, err)
		}
		time.Sleep(ScrollPauseDuration)

		newHeight, err := page.Evaluate("() => document.body.scrollHeight")
		if err == nil {
			if nh, ok := newHeight.(float64); ok && nh > totalHeight {
				totalHeight = nh
			}
		}

		attempts++
	}

	_, _ = page.Evaluate("() => { window.scrollTo(0, 0); }")
	time.Sleep(1 * time.Second)

	log.Printf("[BROWSER-SCROLL] Completed after %d scroll steps (totalHeight=%.0f)", attempts, totalHeight)
	return nil
}

type Config struct {
	Background bool
}

// request represents a single Playwright operation submitted to the Queue.
type request struct {
	fn     func(ctx playwright.BrowserContext) error
	result chan error
}

// pwInitErr stores a Playwright initialisation error so that callers receive
// the real error message rather than a generic "not available" string.
type pwInitErr struct {
	original error
}

func (e *pwInitErr) Error() string {
	return fmt.Sprintf("Playwright instance not available: %v", e.original)
}

// Queue serializes all Playwright browser operations so that only one browser
// operation runs at a time.  It manages a browser context that is lazily
// launched on the first request and automatically closed after a period of
// inactivity (idleTimeout).  This keeps the browser window open only while
// there is work to do and closes it when the queue is idle, saving resources
// between download batches.
//
// Each operation creates a new tab (page) in the context and closes just that
// tab when done.  The context itself persists across consecutive requests but
// is torn down after the idle timeout, and re-created on the next request.
//
// The persistent user-data directory preserves Cloudflare cookies and session
// state across context lifetimes, so Cloudflare challenges are avoided even
// after the browser is closed and re-opened.
type Queue struct {
	cfg   Config
	reqch chan request
	done  chan struct{}

	// pw holds the shared Playwright driver instance. It is started lazily
	// on the first request and stays alive for the Queue's lifetime because
	// starting the driver process is expensive (~2-3 seconds).
	pw   *playwright.Playwright
	pwMu sync.Mutex

	// ctx holds the browser context. It is created on demand when a request
	// arrives and closed after idleTimeout of no activity.
	ctxMu sync.Mutex
	ctx   playwright.BrowserContext
	ctxOk bool // true when ctx is known to be alive and usable

	// page is a long-lived tab inside ctx. Operations that opt in reuse it
	// via RunWithPage and navigate in place (page.Goto) instead of opening
	// a new tab per item, which avoids re-triggering Cloudflare checks on
	// every new page. Created lazily with the context, closed with it.
	page playwright.Page

	// cookieFile is the path of an optional imported-cookie file (JSON
	// array or Netscape cookies.txt). When set, its cookies are injected
	// into every newly created browser context — the escape hatch for
	// sites whose Cloudflare challenge cannot pass in the container.
	cookieFilePath string

	// lastContextFail tracks when the last context creation attempt failed,
	// used to enforce a cooldown between retries.
	lastContextFail time.Time

	// inflight tracks in-flight requests so Stop() can wait for them
	// to complete before closing the context.
	inflight      sync.WaitGroup
	inflightMu    sync.Mutex
	inflightCount int

	// stopOnce ensures the done channel is closed exactly once.
	stopOnce sync.Once
}

// NewQueue creates a Playwright request queue that processes browser
// operations sequentially.  The browser context is started lazily on the
// first call to Run and automatically closed after idleTimeout of inactivity.
func NewQueue(cfg Config) *Queue {
	q := &Queue{
		cfg:   cfg,
		reqch: make(chan request),
		done:  make(chan struct{}),
	}
	go q.processLoop()
	return q
}

// processLoop is the single goroutine that executes queued requests
// sequentially.  After each request completes, it waits up to idleTimeout
// for the next request; if none arrives, it closes the browser context.
func (q *Queue) processLoop() {
	var idleTimer <-chan time.Time

	for {
		select {
		case <-q.done:
			return

		case req := <-q.reqch:
			// Cancel any pending idle close — a new request has arrived.
			idleTimer = nil

			ctx, err := q.ensureContext()
			if err != nil {
				log.Printf("[BROWSER-QUEUE] Browser context not available: %v", err)
				req.result <- err
				// Start idle timer even on failure so the Playwright driver
				// can be eventually cleaned up if errors persist.
				idleTimer = time.After(idleTimeout)
				continue
			}

			q.inflightMu.Lock()
			q.inflightCount++
			q.inflightMu.Unlock()
			q.inflight.Add(1)

			// Run the user's operation under a recover guard so a panic in
			// a scraper's Playwright callback doesn't kill the dispatcher
			// goroutine. The recovery is converted into an error so the
			// caller still sees a meaningful result via req.result.
			reqErr := safeRunRequest(req.fn, ctx)

			q.inflightMu.Lock()
			q.inflightCount--
			q.inflightMu.Unlock()
			q.inflight.Done()

			req.result <- reqErr

			// If the operation failed, only mark the context dead if the
			// error indicates the context itself is gone (not a transient
			// per-page error like ERR_NAME_NOT_RESOLVED). The previous
			// Pages()==nil heuristic was too aggressive: it tore down the
			// persistent Cloudflare session after every DNS blip, which
			// forced a fresh challenge on the next request and frequently
			// cascaded into further DNS errors. The new check only fires
			// on real "context disposed" / "target closed" errors, which
			// is the documented signal that the context is no longer
			// usable.
			if reqErr != nil && isContextClosedErr(reqErr) {
				q.ctxMu.Lock()
				if q.ctx != nil {
					log.Printf("[BROWSER-QUEUE] Context-closed error reported (%v); will re-create on next request", reqErr)
					q.ctxOk = false
					q.page = nil
				}
				q.ctxMu.Unlock()
			}

			// Start idle timer — if no new request arrives within
			// idleTimeout, the context will be closed to free resources.
			idleTimer = time.After(idleTimeout)

		case <-idleTimer:
			// No new requests arrived within the idle window — close the
			// browser context to free resources. It will be re-created on
			// the next request.
			log.Printf("[BROWSER-QUEUE] Idle timeout (%v) reached, closing browser context", idleTimeout)
			q.closeContext()
			idleTimer = nil
		}
	}
}

// closeContext closes the persistent browser context (but not the Playwright
// driver).  It is called from processLoop when the idle timer fires.
func (q *Queue) closeContext() {
	q.ctxMu.Lock()
	defer q.ctxMu.Unlock()
	if q.ctx != nil {
		q.page = nil // page dies with the context
		func() {
			defer func() {
				recover() // silently ignore panics from closing a dead context
			}()
			q.ctx.Close()
		}()
		q.ctx = nil
		q.ctxOk = false
		log.Printf("[BROWSER-QUEUE] Browser context closed (idle timeout)")
	}
}

// ensureContext lazily initialises the Playwright driver (if needed) and
// creates a browser context.  If the context is already alive it is returned
// directly.  If a previous creation attempt failed recently, a cooldown
// period is enforced to avoid rapid retry loops.
func (q *Queue) ensureContext() (playwright.BrowserContext, error) {
	// Verify the display server is up before creating a context: a crashed
	// Xvfb (container) previously left the browser unusable until restart.
	// Cheap unix-socket connect on Linux; no-op elsewhere.
	if err := ensureXvfb(); err != nil {
		return nil, fmt.Errorf("display server unavailable: %w", err)
	}

	q.pwMu.Lock()
	if q.pw == nil {
		pw, err := playwright.Run()
		if err != nil {
			q.pwMu.Unlock()
			return nil, &pwInitErr{original: err}
		}
		q.pw = pw
		log.Printf("[BROWSER-QUEUE] Playwright driver started")
	}
	// Capture the pointer while still holding pwMu: Stop() can set q.pw = nil
	// concurrently, and using the field after unlock risks a nil deref.
	pw := q.pw
	q.pwMu.Unlock()

	q.ctxMu.Lock()
	defer q.ctxMu.Unlock()
	if q.ctxOk && q.ctx != nil {
		return q.ctx, nil
	}

	// Enforce a cooldown between context creation retries to avoid
	// hammering the system if the browser keeps failing to start.
	if !q.lastContextFail.IsZero() && time.Since(q.lastContextFail) < contextRetryCooldown {
		wait := contextRetryCooldown - time.Since(q.lastContextFail)
		return nil, fmt.Errorf("browser context creation on cooldown, retry in %v", wait)
	}

	// Context is not usable — re-create it.
	if q.ctx != nil {
		func() {
			defer func() {
				recover() // silently ignore panics from closing a dead context
			}()
			q.ctx.Close()
		}()
		q.ctx = nil
		q.ctxOk = false
		q.page = nil
	}

	ctx, err := LaunchPersistentContext(pw, q.cfg, "BROWSER-QUEUE")
	if err != nil {
		q.lastContextFail = time.Now()
		log.Printf("[BROWSER-QUEUE] Failed to launch browser context: %v", err)
		return nil, fmt.Errorf("failed to launch browser context: %w", err)
	}
	q.ctx = ctx
	q.ctxOk = true
	q.lastContextFail = time.Time{}
	log.Printf("[BROWSER-QUEUE] Browser context opened")
	q.applyCookieFile(ctx)
	return ctx, nil
}

// ensurePage returns the persistent tab, creating it inside the context when
// missing (first RunWithPage after context creation, or after the tab was
// closed externally). Caller must hold ctxMu.
func (q *Queue) ensurePage() (playwright.Page, error) {
	if q.page != nil {
		return q.page, nil
	}
	page, err := q.ctx.NewPage()
	if err != nil {
		return nil, fmt.Errorf("failed to open persistent page: %w", err)
	}
	q.page = page
	log.Printf("[BROWSER-QUEUE] Persistent page opened")
	return page, nil
}

// RunWithPage enqueues a Playwright operation that reuses a single
// long-lived tab instead of opening a new one per request. The callback
// receives the persistent page and should navigate in place (page.Goto);
// it must NOT close the page or the context. Operations still run one at a
// time, so the shared page is never used concurrently.
func (q *Queue) RunWithPage(fn func(page playwright.Page) error) error {
	wrapped := func(ctx playwright.BrowserContext) error {
		q.ctxMu.Lock()
		page, perr := q.ensurePage()
		q.ctxMu.Unlock()
		if perr != nil {
			return fmt.Errorf("persistent page unavailable: %w", perr)
		}
		return fn(page)
	}
	return q.Run(wrapped)
}

// SetCookieFile registers an imported-cookie file to inject into every
// newly created browser context. Pass an empty path to disable. Takes
// effect on the next context (re)creation; the current context keeps its
// existing cookies.
func (q *Queue) SetCookieFile(path string) {
	q.ctxMu.Lock()
	defer q.ctxMu.Unlock()
	q.cookieFilePath = path
}

// applyCookieFile injects the imported cookies (when configured) into the
// given context. Called from ensureContext on every fresh context so a
// user-supplied cf_clearance etc. survives context recreation. Failures
// are logged but non-fatal: a missing file or an expired cookie set
// degrades to the normal challenge flow.
func (q *Queue) applyCookieFile(ctx playwright.BrowserContext) {
	path := q.cookieFilePath
	if path == "" {
		return
	}
	cookies, err := LoadCookieFile(path)
	if err != nil {
		log.Printf("[COOKIES] Failed to load %s: %v", path, err)
		return
	}
	if len(cookies) == 0 {
		return
	}
	if aerr := ctx.AddCookies(cookies); aerr != nil {
		log.Printf("[COOKIES] Failed to inject %d cookies from %s: %v", len(cookies), path, aerr)
		return
	}
	log.Printf("[COOKIES] Injected %d cookies from %s", len(cookies), path)
}

// Run enqueues a Playwright operation.  It blocks until the operation
// completes, guaranteeing that only one operation runs at a time.  The
// function fn receives the shared BrowserContext — callers should create a
// new page (tab) within it and close the page when done, but must NOT close
// the context itself.
func (q *Queue) Run(fn func(ctx playwright.BrowserContext) error) error {
	req := request{
		fn:     fn,
		result: make(chan error, 1),
	}

	// Use select on both reqch and done so we don't block forever if
	// Stop() is called concurrently after our initial stopped-check.
	select {
	case <-q.done:
		return fmt.Errorf("browser queue has been stopped")
	case q.reqch <- req:
	}

	log.Printf("[BROWSER-QUEUE] Request enqueued, waiting for execution")
	return <-req.result
}

// Stop shuts down the browser context, Playwright driver, and the
// dispatcher goroutine.  It waits for any in-flight request to finish
// before closing the context, so an active page operation completes
// gracefully.  It is safe to call Stop multiple times.
func (q *Queue) Stop() {
	q.stopOnce.Do(func() {
		// Signal the processLoop to stop accepting new requests.
		close(q.done)

		// Wait for any in-flight request to complete before closing
		// the browser context.  This prevents closing the context
		// while a page operation (navigation, content extraction) is
		// still running, which would cause panics.
		q.inflight.Wait()

		// Close the persistent browser context.
		q.ctxMu.Lock()
		if q.ctx != nil {
			q.page = nil
			func() {
				defer func() {
					recover() // silently ignore panics from closing a dead context
				}()
				q.ctx.Close()
			}()
			q.ctx = nil
			q.ctxOk = false
			log.Printf("[BROWSER-QUEUE] Browser context closed")
		}
		q.ctxMu.Unlock()

		// Remove the recorded Chrome PID so a future launch does not see a
		// stale PID and attempt to kill a process that no longer exists.
		// Chrome itself has been closed above; only our bookkeeping file
		// remains to be cleared.
		ClearChromePID(defaultUserDataDir())

		// Stop the Playwright driver.
		q.pwMu.Lock()
		if q.pw != nil {
			q.pw.Stop()
			q.pw = nil
			log.Printf("[BROWSER-QUEUE] Playwright driver stopped")
		}
		q.pwMu.Unlock()
	})
}

// LaunchPersistentContext launches a system Chrome instance with a persistent
// user-data directory.  This is critical for passing Cloudflare Turnstile
// because persistent storage makes the browser look like a real, established
// session.
func LaunchPersistentContext(pw *playwright.Playwright, cfg Config, logPrefix string) (playwright.BrowserContext, error) {
	return LaunchPersistentContextAt(pw, cfg, defaultUserDataDir(), logPrefix)
}

// defaultUserDataDir returns the default Chrome profile location used by the
// manga/H-Manga scraper. Centralised so cleanup logic on shutdown can target
// the same path as the launch path. Precedence: PROFILE_DIR env >
// DATA_DIR/profile > Windows LOCALAPPDATA/APPDATA > Linux
// ~/.cache/comic-scraper-chrome-profile.
func defaultUserDataDir() string {
	if dir := os.Getenv("PROFILE_DIR"); dir != "" {
		return dir
	}
	if data := os.Getenv("DATA_DIR"); data != "" {
		return filepath.Join(data, "profile")
	}
	if runtime.GOOS == "windows" {
		appData := os.Getenv("LOCALAPPDATA")
		if appData == "" {
			appData = os.Getenv("APPDATA")
		}
		return filepath.Join(appData, UserDataDirName)
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return UserDataDirName
	}
	return filepath.Join(home, ".cache", UserDataDirName)
}

// chromeProbeResult caches the system-Chrome availability decision for the
// process lifetime. Playwright's channel probe fails noisily on every launch
// in containers (no /opt/google/chrome/chrome) and its "Run npx playwright
// install chrome" hint reads like a redownload loop; probing the real
// executable paths ourselves avoids both.
var chromeProbeState atomic.Pointer[bool]

// systemChromePaths lists the well-known system Chrome install locations,
// checked before falling back to Playwright's channel probing.
var systemChromePaths = []string{
	"/usr/bin/google-chrome",
	"/usr/bin/google-chrome-stable",
	"/usr/bin/chromium",
	"/usr/bin/chromium-browser",
	"/opt/google/chrome/chrome",
}

// systemChromeAvailable reports whether a system Chrome binary exists. The
// check runs at most once per process (paths are static for a running
// container); the result is cached so repeated launches stay quiet and fast.
func systemChromeAvailable() bool {
	if cached := chromeProbeState.Load(); cached != nil {
		return *cached
	}
	found := false
	for _, p := range systemChromePaths {
		if _, err := os.Stat(p); err == nil {
			found = true
			break
		}
	}
	// Windows: Playwright's "chrome" channel resolves via the registry; the
	// standard install path is a good enough signal.
	if runtime.GOOS == "windows" && !found {
		programFiles := os.Getenv("ProgramFiles")
		if programFiles != "" {
			if _, err := os.Stat(filepath.Join(programFiles, "Google", "Chrome", "Application", "chrome.exe")); err == nil {
				found = true
			}
		}
	}
	// Multiple browser sessions can launch concurrently (manga + H-Manga);
	// both may probe and store — same value either way, and Store is atomic.
	chromeProbeState.Store(&found)
	if !found {
		log.Println("[BROWSER-QUEUE] System Chrome not installed; using Playwright Chromium (this is the expected container path)")
	}
	return found
}

// markSystemChromeUnavailable flips the cached probe result when a launch
// that passed the file probe still fails (e.g. a broken install), so
// subsequent launches skip the retry instead of re-failing every time.
func markSystemChromeUnavailable() {
	f := false
	chromeProbeState.Store(&f)
}

// LaunchPersistentContextAt launches a persistent Chrome context at an explicit
// user-data directory. Use this when you need a profile separate from the manga
// profile (e.g. the H-Manga subsystem). It uses the identical launch flags as
// LaunchPersistentContext so the browser behaves the same as the known-working
// manga path.
func LaunchPersistentContextAt(pw *playwright.Playwright, cfg Config, userDataDir, logPrefix string) (playwright.BrowserContext, error) {
	// Before launching, kill any Chrome process left behind by a previous
	// (crashed or killed) scraper session that was using this same profile.
	// Without this, the new Chrome cannot acquire the profile's lockfile and
	// reports ERR_NAME_NOT_RESOLVED on first navigation.
	cleanOrphanChrome(userDataDir)

	var context playwright.BrowserContext
	var err error

	chromeArgs := []string{
		"--disable-blink-features=AutomationControlled",
		"--disable-features=AutomationControlled",
		"--disable-infobars",
		"--no-first-run",
		"--no-default-browser-check",
	}

	// Chrome's renderer sandbox needs user namespaces, which Docker's default
	// seccomp profile denies (errno = Operation not permitted). The challenge
	// JS on stricter Cloudflare sites silently fails inside that broken
	// sandbox, so disable it on Linux -- the standard practice for
	// containerized Chrome.
	if runtime.GOOS == "linux" {
		chromeArgs = append(chromeArgs, "--no-sandbox", "--disable-dev-shm-usage")
	}

	if cfg.Background {
		chromeArgs = append(chromeArgs,
			"--window-position=-32000,-32000",
			"--disable-backgrounding-occluded-windows",
		)
		log.Printf("[%s] Background mode enabled — browser window positioned off-screen", logPrefix)
	}

	// Keep downloaded artifacts in a stable directory rather than a Playwright
	// temp directory. Without this, accepted downloads are written to a temp
	// folder that is deleted when the browser context closes, which can race
	// with download.SaveAs() and cause H-Manga ZIPs to vanish before they are
	// copied to the configured hmangaDownloadPath.
	downloadsDir := filepath.Join(userDataDir, "downloads")
	opts := playwright.BrowserTypeLaunchPersistentContextOptions{
		Headless:        playwright.Bool(false),
		Args:            chromeArgs,
		AcceptDownloads: playwright.Bool(true),
		DownloadsPath:   playwright.String(downloadsDir),
	}

	// Probe for system Chrome once per process. Playwright's channel probe
	// ("chrome is not found at /opt/google/chrome/chrome") fires on EVERY
	// launch attempt and, when the subsequent launch also fails, Playwright
	// prints its "Run npx playwright install chrome" banner — which reads
	// like it is redownloading the browser on every scrape. Caching the
	// probe result keeps the log to a single line per process and skips the
	// doomed attempt entirely.
	if systemChromeAvailable() {
		log.Printf("[%s] Launching system Chrome (channel: chrome) with persistent profile: %s", logPrefix, userDataDir)
		opts.Channel = playwright.String("chrome")
		context, err = pw.Chromium.LaunchPersistentContext(userDataDir, opts)
		if err != nil {
			log.Printf("[%s] System Chrome probe passed but launch failed, falling back to Playwright Chromium: %v", logPrefix, err)
			markSystemChromeUnavailable()
		} else {
			log.Printf("[%s] Running with system Chrome", logPrefix)
		}
	}

	if context == nil {
		opts.Channel = nil
		context, err = pw.Chromium.LaunchPersistentContext(userDataDir, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to launch browser: %w", err)
		}
		log.Printf("[%s] Running with Playwright Chromium", logPrefix)
	}

	// Stealth overrides for every page loaded in this context. Cloudflare's
	// managed challenge reads navigator.webdriver and bails into a retry
	// loop when it is true (console shows "No available adapters" + NaN
	// errors, then the page never resolves). Playwright sets webdriver=true
	// by default; override it plus a few hardware signals via an init
	// script that runs before any page script.
	initScript := `
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {
  get: () => [1, 2, 3, 4, 5],
});
Object.defineProperty(navigator, 'languages', {
  get: () => ['en-US', 'en'],
});
window.chrome = window.chrome || { runtime: {} };
`
	if err := context.AddInitScript(playwright.Script{Content: &initScript}); err != nil {
		log.Printf("[%s] Warning: failed to add stealth init script: %v", logPrefix, err)
	}

	// Record the Chrome PID so a future launch can clean up orphans if this
	// scraper process is killed before normal shutdown runs.
	if pid := readChromePIDFromLockfile(userDataDir); pid > 0 {
		if err := recordChromePID(userDataDir, pid); err != nil {
			log.Printf("[%s] Warning: failed to record Chrome PID: %v", logPrefix, err)
		} else {
			log.Printf("[%s] Recorded Chrome PID %d for orphan cleanup", logPrefix, pid)
		}
	}

	return context, nil
}

// safeRunRequest invokes a queued Playwright operation under a recover
// guard. A panic inside the user's fn (e.g. a nil-deref in a scraper's
// callback) is caught, logged WITH ITS STACK TRACE (the recovered value
// alone is rarely enough to locate the bug), and converted into a regular
// error so the request completes normally and the dispatcher goroutine
// survives. Without this guard, a panicking scraper would tear down
// processLoop and freeze every subsequent q.Run call.
func safeRunRequest(fn func(playwright.BrowserContext) error, ctx playwright.BrowserContext) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[BROWSER-QUEUE] Recovered panic in queued Playwright op: %v\n%s", r, debug.Stack())
			err = fmt.Errorf("panicked in Playwright callback: %v", r)
		}
	}()
	return fn(ctx)
}

// isContextClosedErr reports whether err indicates the Playwright browser
// context itself is no longer usable (as opposed to a transient per-page
// error). Used by the queue to decide whether the persistent context must
// be re-created on the next request. Only real "context closed" /
// "target closed" errors qualify -- transient errors like
// ERR_NAME_NOT_RESOLVED or individual page failures must NOT trigger a
// context re-creation, since tearing down the persistent profile would
// force a fresh Cloudflare challenge on the next request and frequently
// cascade into further errors.
//
// NOTE: this deliberately excludes substrings like "connection closed" or
// "ERR_CONNECTION_CLOSED" that appear in transient network errors. Those
// are per-request failures, not signals that the Playwright context
// itself is dead; matching them here would re-introduce the over-eager
// teardown bug that this helper was added to fix.
//
// "target page, context or browser has been closed" is the upstream
// playwright-go phrasing emitted in several paths (e.g. when navigating
// via a context that has been torn down mid-flight); without it those
// errors would be treated as transient and the dead context would be
// returned to the next caller, producing a confusing error chain.
func isContextClosedErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "target closed") ||
		strings.Contains(s, "browser closed") ||
		strings.Contains(s, "browser has been closed") ||
		strings.Contains(s, "context disposed") ||
		strings.Contains(s, "context has been closed") ||
		strings.Contains(s, "target page, context or browser has been closed")
}
