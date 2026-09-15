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
// The browser context is reused across calls — only a new tab (page) is
// created and closed per request.
func FetchWithPlaywright(pageURL string, cfg browser.Config, q *browser.Queue) (string, []*http.Cookie, string, error) {
	log.Printf("[MANHUAUS-PLAYWRIGHT] Queuing browser request for: %s", pageURL)

	var content string
	var cookies []*http.Cookie
	var userAgentStr string

	err := q.Run(func(ctx playwright.BrowserContext) error {
		log.Printf("[MANHUAUS-PLAYWRIGHT] Executing browser request for: %s", pageURL)

		page, err := ctx.NewPage()
		if err != nil {
			return err
		}
		defer page.Close()

		page.SetDefaultTimeout(60000)

		log.Printf("[MANHUAUS-PLAYWRIGHT] Navigating to page...")
		_, err = page.Goto(pageURL, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		})
		if err != nil {
			return err
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
		log.Printf("[MANHUAUS-PLAYWRIGHT] Browser User-Agent: %s", userAgentStr)

		// Extract cookies from the browser context for reuse in HTTP requests
		playwrightCookies, err := ctx.Cookies()
		if err != nil {
			log.Printf("[MANHUAUS-PLAYWRIGHT] Warning: failed to extract cookies: %v", err)
		}

		cookies = browser.ConvertPlaywrightCookies(playwrightCookies)
		log.Printf("[MANHUAUS-PLAYWRIGHT] Extracted %d cookies for image downloads", len(cookies))

		log.Printf("[MANHUAUS-PLAYWRIGHT] Successfully retrieved page content (%d bytes)", len(content))
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
