package thunderscans

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/user/comic-scraper/pkg/scraper/browser"
)

const (
	cloudflareWaitTimeout  = 120 * time.Second
	cloudflarePollInterval = 3 * time.Second
	postResolutionWait     = 3 * time.Second
	dnsRetryAttempts       = 3
	// dnsRetryBaseDelay is the first retry wait; subsequent retries double
	// up to dnsRetryMaxDelay. The Windows DNS Client service caches
	// negative answers (NXDOMAIN) for a TTL that defaults to 5s
	// (MaxNegativeCacheTtl), so the first delay must beat that base
	// TTL to force a fresh lookup. Longer delays target pathological
	// cases (large negative-cache TTLs, ISP-specific resolvers).
	dnsRetryBaseDelay = 2 * time.Second
	dnsRetryMaxDelay  = 15 * time.Second
	// dnsPreflightTimeout caps the pre-flight net.LookupHost call. The
	// Windows resolver can hang for its full DnsQueryTimeout (default
	// ~4s for the first attempt) when unhealthy; without this bound a
	// single bad fetch could stall the scraper longer than the entire
	// Playwright retry loop it is meant to short-circuit.
	dnsPreflightTimeout = 2 * time.Second
)

// FetchWithPlaywright fetches a page using Playwright, routed through the
// shared browser queue so that only one browser operation runs at a time.
// The browser context is reused across calls — only a new tab (page) is
// created and closed per request.
func FetchWithPlaywright(pageURL string, cfg browser.Config, q *browser.Queue) (string, []*http.Cookie, string, error) {
	log.Printf("[THUNDERSCANS-PLAYWRIGHT] Queuing browser request for: %s", pageURL)

	// Cheap pre-flight: confirm the OS-level resolver can see the host.
	// If the resolver fails or hangs (network down, DNS dead), no
	// amount of Playwright retry will help -- fail fast and skip the
	// queue entirely so we don't burn cloudflareWaitTimeout on every
	// series. The lookup is bounded by dnsPreflightTimeout because the
	// Windows resolver can hang for its full DnsQueryTimeout (~4s)
	// when unhealthy.
	if u, parseErr := url.Parse(pageURL); parseErr == nil && u.Host != "" {
		ctx, cancel := context.WithTimeout(context.Background(), dnsPreflightTimeout)
		// Hostname() strips the port and IPv6 brackets — LookupHost needs a
		// bare host name.
		_, lookupErr := (&net.Resolver{}).LookupHost(ctx, u.Hostname())
		cancel()
		if lookupErr != nil {
			log.Printf("[THUNDERSCANS-PLAYWRIGHT] Pre-flight DNS lookup failed for %s: %v (skipping Playwright retry)", u.Hostname(), lookupErr)
			return "", nil, "", fmt.Errorf("DNS lookup failed for %s: %w", u.Hostname(), lookupErr)
		}
	}

	var content string
	var cookies []*http.Cookie
	var userAgentStr string

	err := q.Run(func(ctx playwright.BrowserContext) error {
		log.Printf("[THUNDERSCANS-PLAYWRIGHT] Executing browser request for: %s", pageURL)

		// Retry transient DNS failures (e.g. Windows DNS resolver returning
		// a stale NXDOMAIN, or Chromium's IPv6-first race) by re-creating the
		// page on each attempt so a stuck connection from a prior navigation
		// can't poison the next one. Uses exponential backoff (2s, 4s, 8s,
		// capped at dnsRetryMaxDelay) to outlast Windows' configurable
		// negative-cache TTL -- a fixed delay would either be too short for
		// pathological resolvers or waste time on a true outage.
		var page playwright.Page
		for attempt := 1; attempt <= dnsRetryAttempts; attempt++ {
			p, err := ctx.NewPage()
			if err != nil {
				return err
			}
			p.SetDefaultTimeout(60000)

			log.Printf("[THUNDERSCANS-PLAYWRIGHT] Navigating to page (attempt %d/%d)...", attempt, dnsRetryAttempts)
			_, err = p.Goto(pageURL, playwright.PageGotoOptions{
				WaitUntil: playwright.WaitUntilStateDomcontentloaded,
			})
			if err == nil {
				page = p
				break
			}

			// Always close the failed page before deciding whether to retry.
			p.Close()

			if !isTransientDNSError(err) || attempt == dnsRetryAttempts {
				return err
			}
			delay := dnsRetryBaseDelay * time.Duration(1<<(attempt-1))
			if delay > dnsRetryMaxDelay || delay < 0 {
				delay = dnsRetryMaxDelay
			}
			log.Printf("[THUNDERSCANS-PLAYWRIGHT] Transient DNS error on attempt %d/%d (%v); retrying in %v",
				attempt, dnsRetryAttempts, err, delay)
			time.Sleep(delay)
		}
		defer page.Close()

		log.Printf("[THUNDERSCANS-PLAYWRIGHT] Waiting for Cloudflare challenge to resolve...")
		deadline := time.Now().Add(cloudflareWaitTimeout)
		var lastLoggedTitle string
		for time.Now().Before(deadline) {
			title, err := page.Title()
			if err != nil {
				time.Sleep(cloudflarePollInterval)
				continue
			}
			// Log the page title whenever it changes so stuck challenges are
			// diagnosable from the logs (Turnstile interstitials and blocks
			// present with distinct titles).
			if title != lastLoggedTitle {
				log.Printf("[THUNDERSCANS-PLAYWRIGHT] Page title: %q", title)
				lastLoggedTitle = title
			}

			if !isCloudflareChallenge(title) {
				log.Printf("[THUNDERSCANS-PLAYWRIGHT] Cloudflare challenge passed, page title: %s", title)
				break
			}

			time.Sleep(cloudflarePollInterval)
		}

		title, _ := page.Title()
		if isCloudflareChallenge(title) {
			// The challenge page repeatedly 401s on the Private Access Token
			// endpoint (/pat/), which means Cloudflare is enforcing device
			// attestation for this site and the container's Linux Chrome
			// cannot satisfy it. Retrying longer does not help — the title
			// never leaves "Just a moment...".
			return fmt.Errorf("Cloudflare challenge did not resolve within %v: en-thunderscans.com enforces device-attestation challenges that this environment cannot pass (see Docs/DOCKER.md#troubleshooting)", cloudflareWaitTimeout)
		}

		time.Sleep(postResolutionWait)

		pageContent, err := page.Content()
		if err != nil {
			return fmt.Errorf("failed to get page content: %w", err)
		}
		content = pageContent

		// Extract the User-Agent from the browser context
		userAgent, _ := page.Evaluate("() => navigator.userAgent")
		if userAgent != nil {
			userAgentStr, _ = userAgent.(string)
		}
		log.Printf("[THUNDERSCANS-PLAYWRIGHT] Browser User-Agent: %s", userAgentStr)

		// Extract cookies from the browser context for reuse in HTTP requests
		playwrightCookies, err := ctx.Cookies()
		if err != nil {
			log.Printf("[THUNDERSCANS-PLAYWRIGHT] Warning: failed to extract cookies: %v", err)
			// Non-fatal — continue without cookies
		}

		cookies = browser.ConvertPlaywrightCookies(playwrightCookies)
		log.Printf("[THUNDERSCANS-PLAYWRIGHT] Extracted %d cookies for image downloads", len(cookies))

		log.Printf("[THUNDERSCANS-PLAYWRIGHT] Successfully retrieved page content (%d bytes)", len(content))
		return nil
	})

	if err != nil {
		return "", nil, "", err
	}
	return content, cookies, userAgentStr, nil
}

func isCloudflareChallenge(title string) bool {
	return title == "Just a moment..." || title == "Attention Required! | Cloudflare"
}

// isTransientDNSError reports whether err looks like a Chromium DNS-resolution
// failure (e.g. net::ERR_NAME_NOT_RESOLVED) that may succeed on retry.
func isTransientDNSError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "ERR_NAME_NOT_RESOLVED")
}
