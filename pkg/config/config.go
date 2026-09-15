package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/user/comic-scraper/pkg/fileutil"
)

// Config holds application configuration
type Config struct {
	DownloadPath      string `json:"downloadPath"`
	RateDelay         int    `json:"rateDelay"`
	MinImageSizeKB    int    `json:"minImageSizeKB"`
	MinImageWidth     int    `json:"minImageWidth"`
	MinImageHeight    int    `json:"minImageHeight"`
	UserAgent         string `json:"userAgent"`
	BrowserBackground bool   `json:"browserBackground"`

	HMangaDownloadPath  string `json:"hmangaDownloadPath"`
	HentaiNexusUsername string `json:"hentaiNexusUsername"`
	HentaiNexusPassword string `json:"hentaiNexusPassword"`

	// Scraping pause state persisted across restarts.
	MangaScrapingPaused  bool `json:"mangaScrapingPaused"`
	HMangaScrapingPaused bool `json:"hmangaScrapingPaused"`

	// VerifyHMDownloads enables disk verification of H-Manga downloads on startup
	// and manual scans. When false, the server trusts .books.json state.
	VerifyHMDownloads bool `json:"verifyHMDownloads"`

	// HMangaZipNameRegex is a regular expression applied to the ZIP file
	// name of every downloaded H-Manga book. Every match is removed, so
	// "[Tag] Name_123.zip" with pattern "\[[^\]]*\]|_" becomes
	// "Name123.zip". Empty disables renaming.
	HMangaZipNameRegex string `json:"hmangaZipNameRegex"`

	// HMangaExtractZips enables extraction of each downloaded H-Manga ZIP
	// into <hmangaDownloadPath>/<ArtistFolder>/<bookID>/ and deletion of the
	// ZIP once extraction succeeds.
	HMangaExtractZips bool `json:"hmangaExtractZips"`

	// ShutdownTimeoutSeconds is the maximum time (in seconds) the server waits
	// for in-flight downloads to finish on a graceful shutdown (Ctrl+C, SIGTERM).
	// The shutdown sequence stops accepting new work immediately, then waits up
	// to this long for currently-running downloads to complete naturally. If
	// the timeout elapses, downloads are force-killed (existing behaviour).
	// 0 falls back to defaultShutdownTimeoutSeconds.
	ShutdownTimeoutSeconds int `json:"shutdownTimeoutSeconds"`

	// hmangaZipNameRegexKeyMissing is a decode-time marker (not persisted):
	// Load sets it when the raw config JSON lacks the hmangaZipNameRegex key,
	// so Validate can distinguish "never configured" (apply the default
	// pattern) from "explicitly disabled" (empty string = no renaming).
	hmangaZipNameRegexKeyMissing bool `json:"-"`
}

// DefaultHMangaZipNameRegex is the default H-Manga ZIP filename pattern.
// It removes bracketed prefixes ([tag], [artist], etc.) and literal
// underscores from downloaded book ZIP names.
const DefaultHMangaZipNameRegex = `\[[^\]]*\]|_`

// DefaultShutdownTimeoutSeconds bounds how long a graceful shutdown waits for
// in-flight downloads. Configurable via ShutdownTimeoutSeconds. 10 minutes is
// long enough for a multi-GB H-Manga ZIP at typical broadband speeds while
// keeping the upper bound on user-perceived shutdown delay reasonable.
const DefaultShutdownTimeoutSeconds = 600

// DefaultUserAgent is the default browser user agent
const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

// legacyConfigPaths defines the order to check for old config locations
var legacyConfigPaths = []string{
	`comic-scraper-web\config.json`,
	`comic-scraper-go\config.json`,
	`comic-scraper\config.json`,
}

// GetConfigPath returns the unified config path
func GetConfigPath() string {
	return filepath.Join(ConfigBaseDir(), "config.json")
}

// ConfigBaseDir returns the directory that holds config.json and other
// app-level state (H-Manga registry, global book cache). Precedence:
// CONFIG_DIR env > DATA_DIR/config > Windows %APPDATA%\comic-scraper >
// $XDG_CONFIG_HOME/comic-scraper > ~/.config/comic-scraper.
func ConfigBaseDir() string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return dir
	}
	if data := os.Getenv("DATA_DIR"); data != "" {
		return filepath.Join(data, "config")
	}
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("APPDATA"), "comic-scraper")
	}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "comic-scraper")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "comic-scraper"
	}
	return filepath.Join(home, ".config", "comic-scraper")
}

// downloadBaseDir returns the default directory for manga downloads.
// Precedence: DOWNLOAD_DIR env > DATA_DIR/downloads > <configBaseDir>/downloads.
func downloadBaseDir() string {
	if dir := os.Getenv("DOWNLOAD_DIR"); dir != "" {
		return dir
	}
	if data := os.Getenv("DATA_DIR"); data != "" {
		return filepath.Join(data, "downloads")
	}
	return filepath.Join(ConfigBaseDir(), "downloads")
}

// hmangaDownloadBaseDir returns the default directory for H-Manga downloads.
// Precedence: DATA_DIR/hmanga-downloads > <configBaseDir>/hmanga-downloads.
func hmangaDownloadBaseDir() string {
	if data := os.Getenv("DATA_DIR"); data != "" {
		return filepath.Join(data, "hmanga-downloads")
	}
	return filepath.Join(ConfigBaseDir(), "hmanga-downloads")
}

// markZipRegexKeyPresence sets cfg.hmangaZipNameRegexKeyMissing when the raw
// config JSON does not contain the hmangaZipNameRegex key. A missing key is
// never configured; an explicit "" disables the regex renaming.
func markZipRegexKeyPresence(data []byte, cfg *Config) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}
	if _, ok := raw["hmangaZipNameRegex"]; !ok {
		cfg.hmangaZipNameRegexKeyMissing = true
	}
}

// Load loads configuration with migration support
func Load() (*Config, error) {
	cfg := &Config{
		RateDelay:         500,
		MinImageSizeKB:    0,
		MinImageWidth:     0,
		MinImageHeight:    0,
		UserAgent:         DefaultUserAgent,
		BrowserBackground: true,
	}

	// Try to load from unified location first
	configPath := GetConfigPath()
	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, cfg); err != nil {
			log.Printf("[CONFIG] Failed to parse %s (%v); falling back to defaults/legacy", configPath, err)
		} else {
			markZipRegexKeyPresence(data, cfg)
			cfg.applyDownloadPathDefaults()
			return cfg, nil
		}
	}

	// Try legacy locations for migration
	if runtime.GOOS == "windows" {
		appData := os.Getenv("APPDATA")
		for _, legacyPath := range legacyConfigPaths {
			fullPath := filepath.Join(appData, legacyPath)
			if data, err := os.ReadFile(fullPath); err == nil {
				if err := json.Unmarshal(data, cfg); err == nil {
					// Successfully loaded from legacy, migrate to new location
					_ = Save(cfg) // Best effort migration
					markZipRegexKeyPresence(data, cfg)
					cfg.applyDownloadPathDefaults()
					return cfg, nil
				}
			}
		}
	}

	// No config file anywhere: apply the default zip-name pattern.
	cfg.hmangaZipNameRegexKeyMissing = true
	cfg.applyDownloadPathDefaults()

	return cfg, nil
}

// applyDownloadPathDefaults sets default download paths if not configured.
func (c *Config) applyDownloadPathDefaults() {
	if c.DownloadPath == "" {
		c.DownloadPath = downloadBaseDir()
	}
	if c.HMangaDownloadPath == "" {
		c.HMangaDownloadPath = hmangaDownloadBaseDir()
	}
}

// Save persists configuration to disk
func Save(cfg *Config) error {
	configPath := GetConfigPath()
	dir := filepath.Dir(configPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// WriteFileAtomic: temp + rename so a crash mid-write cannot truncate
	// config.json.
	if err := fileutil.WriteFileAtomic(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	return nil
}

// Validate checks configuration values
func (c *Config) Validate() error {
	c.applyEnvOverrides()
	if c.DownloadPath == "" {
		c.DownloadPath = downloadBaseDir()
	}
	if c.RateDelay < 0 {
		c.RateDelay = 500
	}
	if c.MinImageSizeKB < 0 {
		c.MinImageSizeKB = 0
	}
	if c.MinImageWidth < 0 {
		c.MinImageWidth = 0
	}
	if c.MinImageHeight < 0 {
		c.MinImageHeight = 0
	}
	if c.UserAgent == "" {
		c.UserAgent = DefaultUserAgent
	}
	if c.HMangaDownloadPath == "" {
		c.HMangaDownloadPath = hmangaDownloadBaseDir()
	}
	// An unset (never-configured) H-Manga zip regex gets the shipped default
	// pattern, which strips "[Tag]" groups and literal underscores. An
	// explicitly configured empty string ("hmangaZipNameRegex": "") disables
	// renaming — callers that want to opt out must set the key explicitly.
	if c.hmangaZipNameRegexKeyMissing && c.HMangaZipNameRegex == "" {
		c.HMangaZipNameRegex = DefaultHMangaZipNameRegex
	}
	if c.ShutdownTimeoutSeconds <= 0 {
		c.ShutdownTimeoutSeconds = DefaultShutdownTimeoutSeconds
	}
	return nil
}

// applyEnvOverrides applies SCRAPE_* environment variables on top of the
// loaded config. Env wins over config.json so Docker users can pin settings
// without editing the persisted file. Applied during Validate().
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("SCRAPE_DOWNLOAD_PATH"); v != "" {
		c.DownloadPath = v
	}
	if v := os.Getenv("SCRAPE_HMANGA_DOWNLOAD_PATH"); v != "" {
		c.HMangaDownloadPath = v
	}
	if v := os.Getenv("SCRAPE_RATE_DELAY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.RateDelay = n
		}
	}
	if v := os.Getenv("SCRAPE_MIN_IMAGE_SIZE_KB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.MinImageSizeKB = n
		}
	}
	if v := os.Getenv("SCRAPE_MIN_IMAGE_WIDTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.MinImageWidth = n
		}
	}
	if v := os.Getenv("SCRAPE_MIN_IMAGE_HEIGHT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.MinImageHeight = n
		}
	}
	if v := os.Getenv("SCRAPE_USER_AGENT"); v != "" {
		c.UserAgent = v
	}
	if v := strings.ToLower(os.Getenv("SCRAPE_BROWSER_BACKGROUND")); v != "" {
		c.BrowserBackground = v == "true" || v == "1" || v == "yes"
	}
	if v := os.Getenv("SCRAPE_SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.ShutdownTimeoutSeconds = n
		}
	}
}
