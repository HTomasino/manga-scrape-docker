package http

import (
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sync"
	"time"
)

// Client wraps an HTTP client with rate limiting and cookie management
type Client struct {
	client      *http.Client
	rateDelay   time.Duration
	userAgent   string
	lastRequest time.Time
	mu          sync.Mutex
}

// NewClient creates a new HTTP client with rate limiting
func NewClient(rateDelayMs int, userAgent string) *Client {
	jar, _ := cookiejar.New(nil)
	return &Client{
		client: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return nil
			},
			Jar: jar,
		},
		rateDelay: time.Duration(rateDelayMs) * time.Millisecond,
		userAgent: userAgent,
	}
}

// waitForRateLimit enforces rate limiting between requests.
// It reserves the time slot by setting lastRequest to the future fire time
// before releasing the lock, so concurrent callers properly queue behind.
func (c *Client) waitForRateLimit() {
	c.mu.Lock()
	var wait time.Duration
	if !c.lastRequest.IsZero() {
		elapsed := time.Since(c.lastRequest)
		if elapsed < c.rateDelay {
			wait = c.rateDelay - elapsed
		}
	}
	// Reserve the time slot immediately so the next caller sees the updated lastRequest
	c.lastRequest = time.Now().Add(wait)
	c.mu.Unlock()

	if wait > 0 {
		time.Sleep(wait)
	}
}

// FetchHTML fetches HTML content from a URL with rate limiting
func (c *Client) FetchHTML(url string) (string, error) {
	c.waitForRateLimit()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", c.getUserAgent())
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// FetchBytes fetches raw bytes from a URL with rate limiting
func (c *Client) FetchBytes(url string) ([]byte, error) {
	c.waitForRateLimit()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", c.getUserAgent())

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, url)
	}

	return io.ReadAll(resp.Body)
}

// getUserAgent reads the user agent under the lock so SetUserAgent updates
// are not raced by concurrent fetches.
func (c *Client) getUserAgent() string {
	c.mu.Lock()
	ua := c.userAgent
	c.mu.Unlock()
	return ua
}

// SetUserAgent updates the user agent string
func (c *Client) SetUserAgent(userAgent string) {
	c.mu.Lock()
	c.userAgent = userAgent
	c.mu.Unlock()
}

// SetRateDelay updates the rate delay
func (c *Client) SetRateDelay(delayMs int) {
	c.mu.Lock()
	c.rateDelay = time.Duration(delayMs) * time.Millisecond
	c.mu.Unlock()
}

// SetDomainCookies injects cookies for a specific domain into the client's cookie jar.
// This is used to transfer Cloudflare clearance cookies from a Playwright session
// to the HTTP client so image downloads can bypass Cloudflare.
func (c *Client) SetDomainCookies(domainURL string, cookies []*http.Cookie) {
	parsedURL, err := url.Parse(domainURL)
	if err != nil {
		return
	}
	c.client.Jar.SetCookies(parsedURL, cookies)
}
