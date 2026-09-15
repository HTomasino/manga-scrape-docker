package asura

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
	postResolutionWait     = 3 * time.Second
)

// FetchWithPlaywright fetches a page using Playwright, routed through the
// shared browser queue so that only one browser operation runs at a time.
// AsuraScans uses lazy loading for images, so this function scrolls the page
// to trigger all image loads before returning the fully rendered content.
//
// The browser context is reused across calls — only a new tab (page) is
// created and closed per request, keeping the browser window alive.
func FetchWithPlaywright(pageURL string, cfg browser.Config, q *browser.Queue) (string, []*http.Cookie, string, error) {
	log.Printf("[ASURA-PLAYWRIGHT] Queuing browser request for: %s", pageURL)

	var content string
	var cookies []*http.Cookie
	var userAgentStr string

	err := q.Run(func(ctx playwright.BrowserContext) error {
		log.Printf("[ASURA-PLAYWRIGHT] Executing browser request for: %s", pageURL)

		page, err := ctx.NewPage()
		if err != nil {
			return err
		}
		defer page.Close()

		page.SetDefaultTimeout(60000)

		log.Printf("[ASURA-PLAYWRIGHT] Navigating to page...")
		_, err = page.Goto(pageURL, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		})
		if err != nil {
			return err
		}

		// Wait for Cloudflare challenge to resolve (AsuraScans uses Cloudflare).
		log.Printf("[ASURA-PLAYWRIGHT] Waiting for Cloudflare challenge to resolve...")
		deadline := time.Now().Add(cloudflareWaitTimeout)
		for time.Now().Before(deadline) {
			title, err := page.Title()
			if err != nil {
				time.Sleep(cloudflarePollInterval)
				continue
			}

			if !isCloudflareChallenge(title) {
				log.Printf("[ASURA-PLAYWRIGHT] Cloudflare challenge passed, page title: %s", title)
				break
			}

			time.Sleep(cloudflarePollInterval)
		}

		title, _ := page.Title()
		if isCloudflareChallenge(title) {
			return fmt.Errorf("Cloudflare challenge did not resolve within %v", cloudflareWaitTimeout)
		}

		time.Sleep(postResolutionWait)

		// AsuraScans uses lazy loading for chapter images. Scroll the page
		// progressively to trigger all lazy-loaded images.
		log.Printf("[ASURA-PLAYWRIGHT] Scrolling page to trigger lazy-loaded images...")
		if err := browser.ScrollPageForLazyImages(page, 800.0, 100); err != nil {
			log.Printf("[ASURA-PLAYWRIGHT] Warning: scroll failed: %v", err)
		}

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
		log.Printf("[ASURA-PLAYWRIGHT] Browser User-Agent: %s", userAgentStr)

		// Extract cookies from the browser context for reuse in HTTP requests
		playwrightCookies, err := ctx.Cookies()
		if err != nil {
			log.Printf("[ASURA-PLAYWRIGHT] Warning: failed to extract cookies: %v", err)
			// Non-fatal — continue without cookies
		}

		cookies = browser.ConvertPlaywrightCookies(playwrightCookies)
		log.Printf("[ASURA-PLAYWRIGHT] Extracted %d cookies for image downloads", len(cookies))

		log.Printf("[ASURA-PLAYWRIGHT] Successfully retrieved page content (%d bytes)", len(content))
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
