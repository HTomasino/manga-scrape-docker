package manhuaus

import (
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
// cookies/UA — Cloudflare fingerprints the Go TLS client — so the bytes must
// come from the challenge-passing browser. The tab navigates DIRECTLY to the
// image URL and the response body is read via Playwright's Response.Body():
// a JS in-page fetch() cannot be used because the CDN sends no
// Access-Control-Allow-Origin headers, so a cross-origin fetch from the
// manhuaus.com origin is blocked by CORS ("TypeError: Failed to fetch").
// Top-level navigation has no CORS restriction. Routed through the shared
// queue like every other browser op.
func FetchImageBytesWithPlaywright(imgURL string, cfg browser.Config, q *browser.Queue) ([]byte, error) {
	type imgResult struct {
		data []byte
	}
	res, err := browser.RunWithPageKeyedVal(q, imgURL, func(page playwright.Page) (imgResult, error) {
		log.Printf("[MANHUAUS-PLAYWRIGHT] Fetching image via browser: %s", imgURL)
		page.SetDefaultTimeout(60000)

		resp, err := page.Goto(imgURL, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateCommit,
		})
		if err != nil {
			return imgResult{}, fmt.Errorf("browser image navigation failed: %w", err)
		}
		if resp == nil {
			return imgResult{}, fmt.Errorf("browser image navigation returned no response")
		}
		if status := resp.Status(); status >= 400 {
			return imgResult{}, fmt.Errorf("HTTP %d fetching image: %s", status, imgURL)
		}
		data, err := resp.Body()
		if err != nil {
			return imgResult{}, fmt.Errorf("failed to read image body: %w", err)
		}
		log.Printf("[MANHUAUS-PLAYWRIGHT] Fetched %d image bytes via browser", len(data))
		return imgResult{data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return res.data, nil
}
