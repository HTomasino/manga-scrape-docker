package manhuaus

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/user/comic-scraper/pkg/scraper/browser"
)

const (
	cloudflareWaitTimeout  = 120 * time.Second
	cloudflarePollInterval = 3 * time.Second
	postResolutionWait     = 2 * time.Second
)

// FetchWithPlaywright fetches a page using Playwright, routed through the
// shared browser queue so that only one browser operation runs at a time.
// The operation reuses the queue's single persistent tab and navigates it in
// place (page.Goto) — no new tab per request, so the Cloudflare session is
// not churned by repeated page creation.
func FetchWithPlaywright(pageURL string, cfg browser.Config, q *browser.Queue) (string, []*http.Cookie, string, error) {
	log.Printf("[MANHUAUS-PLAYWRIGHT] Queuing browser request for: %s", pageURL)

	type fetchResult struct {
		content string
		cookies []*http.Cookie
		ua      string
	}
	res, err := browser.RunWithPageKeyedVal(q, pageURL, func(page playwright.Page) (fetchResult, error) {
		log.Printf("[MANHUAUS-PLAYWRIGHT] Executing browser request for: %s", pageURL)

		page.SetDefaultTimeout(60000)

		log.Printf("[MANHUAUS-PLAYWRIGHT] Navigating to page...")
		if _, err := page.Goto(pageURL, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		}); err != nil {
			return fetchResult{}, err
		}

		log.Printf("[MANHUAUS-PLAYWRIGHT] Waiting for Cloudflare challenge to resolve...")
		deadline := time.Now().Add(cloudflareWaitTimeout)
		for time.Now().Before(deadline) {
			title, err := page.Title()
			if err != nil {
				time.Sleep(cloudflarePollInterval)
				continue
			}

			if !isCloudflareChallenge(title) {
				log.Printf("[MANHUAUS-PLAYWRIGHT] Cloudflare challenge passed, page title: %s", title)
				break
			}

			time.Sleep(cloudflarePollInterval)
		}

		title, _ := page.Title()
		if isCloudflareChallenge(title) {
			currentURL := page.URL()
			log.Printf("[MANHUAUS-PLAYWRIGHT] Challenge unresolved at timeout: title=%q url=%s", title, currentURL)
			return fetchResult{}, fmt.Errorf("Cloudflare challenge did not resolve within %v (title: %q)", cloudflareWaitTimeout, title)
		}

		time.Sleep(postResolutionWait)

		pageContent, err := page.Content()
		if err != nil {
			return fetchResult{}, fmt.Errorf("failed to get page content: %w", err)
		}

		// Extract the User-Agent from the browser context
		userAgent, _ := page.Evaluate("() => navigator.userAgent")
		ua, _ := userAgent.(string)
		log.Printf("[MANHUAUS-PLAYWRIGHT] Browser User-Agent: %s", ua)

		// Extract cookies from the browser context for reuse in HTTP requests
		playwrightCookies, err := page.Context().Cookies()
		if err != nil {
			log.Printf("[MANHUAUS-PLAYWRIGHT] Warning: failed to extract cookies: %v", err)
		}

		cookies := browser.ConvertPlaywrightCookies(playwrightCookies)
		log.Printf("[MANHUAUS-PLAYWRIGHT] Extracted %d cookies for image downloads", len(cookies))

		log.Printf("[MANHUAUS-PLAYWRIGHT] Successfully retrieved page content (%d bytes)", len(pageContent))
		return fetchResult{content: pageContent, cookies: cookies, ua: ua}, nil
	})

	if err != nil {
		return "", nil, "", err
	}
	return res.content, res.cookies, res.ua, nil
}

func isCloudflareChallenge(title string) bool {
	return title == "Just a moment..." ||
		title == "Attention Required! | Cloudflare" ||
		title == "Verify you are human"
}

// FetchImageBytesWithPlaywright fetches raw image bytes through the browser
// session. The image CDN (img.manhuaus.com) sits behind a Cloudflare
// challenge that rejects the plain HTTP client with 403 regardless of
// cookies/UA — Cloudflare fingerprints the Go TLS client — so the bytes are
// fetched from inside the persistent tab via the JS fetch API (same
// origin/session as the challenge-passing browser) and returned as base64.
// Routed through the shared queue like every other browser op.
func FetchImageBytesWithPlaywright(imgURL string, cfg browser.Config, q *browser.Queue) ([]byte, error) {
	type imgResult struct {
		data    []byte
		content string
	}
	res, err := browser.RunWithPageKeyedVal(q, imgURL, func(page playwright.Page) (imgResult, error) {
		log.Printf("[MANHUAUS-PLAYWRIGHT] Fetching image via browser: %s", imgURL)
		page.SetDefaultTimeout(60000)

		// Make sure the tab is on a manhuaus.com page so the fetch runs with
		// the site's origin, cookies, and Cloudflare clearance. The persistent
		// tab is parked on about:blank between ops.
		if current := page.URL(); current == "about:blank" || current == "" {
			if _, err := page.Goto("https://manhuaus.com/", playwright.PageGotoOptions{
				WaitUntil: playwright.WaitUntilStateDomcontentloaded,
			}); err != nil {
				return imgResult{}, fmt.Errorf("failed to navigate to site origin: %w", err)
			}
		}

		const fetchJS = `
async (url) => {
	const resp = await fetch(url, { credentials: 'include' });
	if (!resp.ok) { throw new Error('HTTP ' + resp.status + ': ' + url); }
	const buf = await resp.arrayBuffer();
	const bytes = new Uint8Array(buf);
	let binary = '';
	const chunk = 0x8000;
	for (let i = 0; i < bytes.length; i += chunk) {
		binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
	}
	return btoa(binary);
}
`
		raw, err := page.Evaluate(fetchJS, imgURL)
		if err != nil {
			return imgResult{}, fmt.Errorf("browser image fetch failed: %w", err)
		}
		b64, ok := raw.(string)
		if !ok || b64 == "" {
			return imgResult{}, fmt.Errorf("browser image fetch returned no data")
		}
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return imgResult{}, fmt.Errorf("failed to decode image data: %w", err)
		}
		log.Printf("[MANHUAUS-PLAYWRIGHT] Fetched %d image bytes via browser", len(data))
		return imgResult{data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return res.data, nil
}
