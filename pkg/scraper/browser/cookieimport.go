package browser

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/playwright-community/playwright-go"
)

// importedCookie mirrors the JSON layout produced by common browser cookie
// export extensions ("Get cookies.txt LOCALLY" JSON export). Only the fields
// Playwright's AddCookies understands are carried over; expires uses a Unix
// timestamp in seconds (0 = session cookie).
type importedCookie struct {
	Name     string  `json:"name"`
	Value    string  `json:"value"`
	Domain   string  `json:"domain"`
	Path     string  `json:"path"`
	Expires  float64 `json:"expires"`
	HTTPOnly bool    `json:"httpOnly"`
	Secure   bool    `json:"secure"`
	SameSite string  `json:"sameSite"`
	// Netscape-format extras (parsed by LoadCookieFile when the file starts
	// with "# Netscape HTTP Cookie File").
	TrueFalse string `json:"-"`
}

// LoadCookieFile reads a cookie file and converts it to Playwright cookies.
// Both formats are accepted:
//
//  1. JSON array export: [{"name": "...", "value": "...", "domain": "..."}]
//  2. Netscape cookies.txt export (# Netscape HTTP Cookie File header), the
//     format produced by most "export cookies" browser extensions.
//
// A nil slice with a nil error means the file exists but contains no
// usable cookies; a non-nil error means the file could not be read/parsed.
func LoadCookieFile(path string) ([]playwright.OptionalCookie, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read cookie file: %w", err)
	}

	// Netscape format starts with a comment header; JSON starts with '['.
	trimmed := data
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\n' || trimmed[0] == '\r' || trimmed[0] == '\t') {
		trimmed = trimmed[1:]
	}
	if len(trimmed) > 0 && trimmed[0] == '#' {
		return parseNetscapeCookies(string(trimmed))
	}
	return parseJSONCookies(trimmed)
}

// parseJSONCookies parses a JSON array of exported cookies.
func parseJSONCookies(data []byte) ([]playwright.OptionalCookie, error) {
	var raw []importedCookie
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("cookie file is not valid JSON: %w", err)
	}
	out := make([]playwright.OptionalCookie, 0, len(raw))
	for _, c := range raw {
		if c.Name == "" || (c.Value == "" && c.Domain == "") {
			continue
		}
		pc := playwright.OptionalCookie{
			Name:     c.Name,
			Value:    c.Value,
			Domain:   playwright.String(c.Domain),
			Path:     playwright.String("/"),
			Secure:   playwright.Bool(c.Secure),
			HttpOnly: playwright.Bool(c.HTTPOnly),
		}
		if c.Path != "" {
			pc.Path = playwright.String(c.Path)
		}
		if c.Expires > 0 {
			pc.Expires = playwright.Float(c.Expires)
		}
		// Playwright wants "Strict" | "Lax" | "None"; the exporter may
		// provide lowercase or "no_restriction"/"lax"/"strict" styles.
		switch c.SameSite {
		case "Strict", "strict", "strictly", "no_restriction":
			pc.SameSite = playwright.SameSiteAttributeStrict
		case "Lax", "lax":
			pc.SameSite = playwright.SameSiteAttributeLax
		case "None", "none":
			pc.SameSite = playwright.SameSiteAttributeNone
		}
		out = append(out, pc)
	}
	return out, nil
}

// parseNetscapeCookies parses the classic Netscape cookies.txt format:
//
//	# Netscape HTTP Cookie File
//	.tsreader.net	TRUE	/	TRUE	1790000000	cookie_name	value
func parseNetscapeCookies(text string) ([]playwright.OptionalCookie, error) {
	out := make([]playwright.OptionalCookie, 0, 32)
	for _, line := range splitLines(text) {
		if line == "" || line[0] == '#' {
			continue
		}
		fields := splitTabFields(line)
		if len(fields) < 7 {
			continue
		}
		domain, path, secure, expires, name, value := fields[0], fields[2], fields[3], fields[4], fields[5], fields[6]
		if name == "" {
			continue
		}
		pc := playwright.OptionalCookie{
			Name:   name,
			Value:  value,
			Domain: playwright.String(domain),
			Path:   playwright.String(path),
			Secure: playwright.Bool(secure == "TRUE" || secure == "true"),
		}
		// A leading dot on the domain marks it as a domain cookie — Playwright
		// wants the leading dot preserved so subdomains match, which the
		// exporter already provides.
		if exp, err := parseNetscapeExpiry(expires); err == nil && exp > 0 {
			pc.Expires = playwright.Float(exp)
		}
		out = append(out, pc)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no cookies parsed from Netscape-format file")
	}
	return out, nil
}

// splitTabFields splits on tabs and trims trailing whitespace per field.
func splitTabFields(line string) []string {
	fields := make([]string, 0, 7)
	start := 0
	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == '\t' {
			fields = append(fields, trimSpaces(line[start:i]))
			start = i + 1
		}
	}
	return fields
}

func splitLines(text string) []string {
	lines := make([]string, 0, 32)
	start := 0
	for i := 0; i <= len(text); i++ {
		if i == len(text) || text[i] == '\n' {
			l := text[start:i]
			l = trimCR(l)
			l = trimSpaces(l)
			if l != "" {
				lines = append(lines, l)
			}
			start = i + 1
		}
	}
	return lines
}

func trimSpaces(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func trimCR(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// parseNetscapeExpiry parses the Unix-seconds expiry field; "0" means
// session cookie — return 0 so the caller treats it as session cookie.
func parseNetscapeExpiry(s string) (float64, error) {
	var v float64
	if _, err := fmt.Sscanf(trimSpaces(s), "%f", &v); err != nil {
		return 0, err
	}
	return v, nil
}

var _ = log.Println
var _ = time.Now
