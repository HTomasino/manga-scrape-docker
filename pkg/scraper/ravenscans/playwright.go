package ravenscans

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
	log.Printf("[RAVENSCANS-PLAYWRIGHT] Queuing browser request for: %s", pageURL)

	type fetchResult struct {
		content string
		cookies []*http.Cookie
		ua      string
	}
	res, err := browser.RunWithPageKeyedVal(q, pageURL, func(page playwright.Page) (fetchResult, error) {
		log.Printf("[RAVENSCANS-PLAYWRIGHT] Executing browser request for: %s", pageURL)

		page.SetDefaultTimeout(60000)

		log.Printf("[RAVENSCANS-PLAYWRIGHT] Navigating to page...")
		if _, err := page.Goto(pageURL, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		}); err != nil {
			return fetchResult{}, err
		}

		log.Printf("[RAVENSCANS-PLAYWRIGHT] Waiting for Cloudflare challenge to resolve...")
		deadline := time.Now().Add(cloudflareWaitTimeout)
		for time.Now().Before(deadline) {
			title, err := page.Title()
			if err != nil {
				time.Sleep(cloudflarePollInterval)
				continue
			}

			if !isCloudflareChallenge(title) {
				log.Printf("[RAVENSCANS-PLAYWRIGHT] Cloudflare challenge passed, page title: %s", title)
				break
			}

			time.Sleep(cloudflarePollInterval)
		}

		title, _ := page.Title()
		if isCloudflareChallenge(title) {
			log.Printf("[RAVENSCANS-PLAYWRIGHT] Challenge unresolved at timeout: title=%q url=%s", title, page.URL())
			return fetchResult{}, fmt.Errorf("Cloudflare challenge did not resolve within %v (title: %q)", cloudflareWaitTimeout, title)
		}

		time.Sleep(postResolutionWait)

		pageContent, err := page.Content()
		if err != nil {
			return fetchResult{}, fmt.Errorf("failed to get page content: %w", err)
		}

		userAgent, _ := page.Evaluate("() => navigator.userAgent")
		ua, _ := userAgent.(string)
		log.Printf("[RAVENSCANS-PLAYWRIGHT] Browser User-Agent: %s", ua)

		playwrightCookies, err := page.Context().Cookies()
		if err != nil {
			log.Printf("[RAVENSCANS-PLAYWRIGHT] Warning: failed to extract cookies: %v", err)
		}

		cookies := browser.ConvertPlaywrightCookies(playwrightCookies)
		log.Printf("[RAVENSCANS-PLAYWRIGHT] Extracted %d cookies", len(cookies))

		log.Printf("[RAVENSCANS-PLAYWRIGHT] Successfully retrieved page content (%d bytes)", len(pageContent))
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
// session. cdn*.ravenscans.org sits behind the same Cloudflare challenge as
// the site and rejects the plain HTTP client with 403 regardless of UA, so the
// request must carry the challenge-passing session's cf_clearance cookie.
// APIRequestContext runs in the Playwright driver rather than Chromium, but it
// shares the browser context's cookie jar, which is what Cloudflare accepts. A
// JS in-page fetch() would be blocked by CORS, and navigating the tab to the
// image is unreliable: on dead-origin CDNs (522) the navigation hangs even
// with WaitUntilStateCommit because no response ever commits.
func FetchImageBytesWithPlaywright(imgURL string, cfg browser.Config, q *browser.Queue) ([]byte, error) {
	type imgResult struct {
		data []byte
	}
	res, err := browser.RunWithPageKeyedVal(q, imgURL, func(page playwright.Page) (imgResult, error) {
		log.Printf("[RAVENSCANS-PLAYWRIGHT] Fetching image via browser: %s", imgURL)

		resp, err := page.Context().Request().Get(imgURL)
		if err != nil {
			return imgResult{}, fmt.Errorf("browser image request failed: %w", err)
		}
		defer resp.Dispose()
		if !resp.Ok() {
			return imgResult{}, fmt.Errorf("HTTP %d fetching image: %s", resp.Status(), imgURL)
		}
		data, err := resp.Body()
		if err != nil {
			return imgResult{}, fmt.Errorf("failed to read image body: %w", err)
		}
		log.Printf("[RAVENSCANS-PLAYWRIGHT] Fetched %d image bytes via browser", len(data))
		return imgResult{data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return res.data, nil
}
