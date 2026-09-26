package drake

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
	log.Printf("[DRAKE-PLAYWRIGHT] Queuing browser request for: %s", pageURL)

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
			log.Printf("[DRAKE-PLAYWRIGHT] Pre-flight DNS lookup failed for %s: %v (skipping Playwright retry)", u.Hostname(), lookupErr)
			return "", nil, "", fmt.Errorf("DNS lookup failed for %s: %w", u.Hostname(), lookupErr)
		}
	}

	var content string
	var cookies []*http.Cookie
	var userAgentStr string

	err := q.RunWithPage(func(page playwright.Page) error {
		log.Printf("[DRAKE-PLAYWRIGHT] Executing browser request for: %s", pageURL)

		page.SetDefaultTimeout(60000)

		// Retry transient DNS failures (e.g. Windows DNS resolver returning
		// a stale NXDOMAIN, or Chromium's IPv6-first race) by re-navigating
		// in place on each attempt. Uses exponential backoff (2s, 4s, 8s,
		// capped at dnsRetryMaxDelay) to outlast Windows' configurable
		// negative-cache TTL -- a fixed delay would either be too short for
		// pathological resolvers or waste time on a true outage.
		var navErr error
		for attempt := 1; attempt <= dnsRetryAttempts; attempt++ {
			log.Printf("[DRAKE-PLAYWRIGHT] Navigating to page (attempt %d/%d)...", attempt, dnsRetryAttempts)
			if _, err := page.Goto(pageURL, playwright.PageGotoOptions{
				WaitUntil: playwright.WaitUntilStateDomcontentloaded,
			}); err == nil {
				navErr = nil
				break
			} else {
				navErr = err
			}

			if !isTransientDNSError(navErr) || attempt == dnsRetryAttempts {
				return navErr
			}
			delay := dnsRetryBaseDelay * time.Duration(1<<(attempt-1))
			if delay > dnsRetryMaxDelay || delay < 0 {
				delay = dnsRetryMaxDelay
			}
			log.Printf("[DRAKE-PLAYWRIGHT] Transient DNS error on attempt %d/%d (%v); retrying in %v",
				attempt, dnsRetryAttempts, navErr, delay)
			time.Sleep(delay)
		}
		_ = navErr

		log.Printf("[DRAKE-PLAYWRIGHT] Waiting for Cloudflare challenge to resolve...")
		deadline := time.Now().Add(cloudflareWaitTimeout)
		for time.Now().Before(deadline) {
			title, err := page.Title()
			if err != nil {
				time.Sleep(cloudflarePollInterval)
				continue
			}

			if !isCloudflareChallenge(title) {
				log.Printf("[DRAKE-PLAYWRIGHT] Cloudflare challenge passed, page title: %s", title)
				break
			}

			time.Sleep(cloudflarePollInterval)
		}

		title, _ := page.Title()
		if isCloudflareChallenge(title) {
			return fmt.Errorf("Cloudflare challenge did not resolve within %v", cloudflareWaitTimeout)
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
		log.Printf("[DRAKE-PLAYWRIGHT] Browser User-Agent: %s", userAgentStr)

		// Extract cookies from the browser context for reuse in HTTP requests
		playwrightCookies, err := page.Context().Cookies()
		if err != nil {
			log.Printf("[DRAKE-PLAYWRIGHT] Warning: failed to extract cookies: %v", err)
			// Non-fatal — continue without cookies
		}

		cookies = browser.ConvertPlaywrightCookies(playwrightCookies)
		log.Printf("[DRAKE-PLAYWRIGHT] Extracted %d cookies for image downloads", len(cookies))

		log.Printf("[DRAKE-PLAYWRIGHT] Successfully retrieved page content (%d bytes)", len(content))
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
