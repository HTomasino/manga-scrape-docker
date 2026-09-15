package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/user/comic-scraper/pkg/config"
	"github.com/user/comic-scraper/pkg/download"
	"github.com/user/comic-scraper/pkg/fileutil"
	httpclient "github.com/user/comic-scraper/pkg/http"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
	"github.com/user/comic-scraper/pkg/scraper/asura"
	"github.com/user/comic-scraper/pkg/scraper/browser"
	"github.com/user/comic-scraper/pkg/scraper/demonic"
	"github.com/user/comic-scraper/pkg/scraper/drake"
	"github.com/user/comic-scraper/pkg/scraper/lua"
	"github.com/user/comic-scraper/pkg/scraper/manhuaplus"
	"github.com/user/comic-scraper/pkg/scraper/manhuaus"
	"github.com/user/comic-scraper/pkg/scraper/ravenscans"
	"github.com/user/comic-scraper/pkg/scraper/thunderscans"
)

// SeriesStore holds tracked series
type SeriesStore struct {
	Series []models.Series `json:"series"`
}

// Commands holds CLI dependencies
type Commands struct {
	config     *config.Config
	httpClient *httpclient.Client
	registry   *scraper.Registry
	fileOps    *fileutil.Operations
	storePath  string
}

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load config: %v\n", err)
		cfg = &config.Config{
			RateDelay: 500,
			UserAgent: config.DefaultUserAgent,
		}
	}
	// Apply defaults / validation so a config file missing newer fields (e.g.
	// HMangaDownloadPath) still gets sane values, matching the web server.
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: invalid config: %v\n", err)
	}

	// Ensure download directory exists
	if err := os.MkdirAll(cfg.DownloadPath, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create download directory: %v\n", err)
		os.Exit(1)
	}

	// Initialize dependencies
	httpClient := httpclient.NewClient(cfg.RateDelay, cfg.UserAgent)
	fileOps := fileutil.NewOperations(httpClient)

	// Create scraper registry and register scrapers
	browserCfg := browser.Config{Background: cfg.BrowserBackground}
	browserQueue := browser.NewQueue(browserCfg)
	defer browserQueue.Stop()

	registry := scraper.NewRegistry()
	registry.Register(asura.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(demonic.NewScraper())
	registry.Register(drake.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(lua.NewScraper())
	registry.Register(manhuaus.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(manhuaplus.NewScraper(httpClient))
	registry.Register(thunderscans.NewScraper())
	registry.Register(ravenscans.NewScraper())

	// Determine store path
	storePath := filepath.Join(config.ConfigBaseDir(), "series.json")

	cmds := &Commands{
		config:     cfg,
		httpClient: httpClient,
		registry:   registry,
		fileOps:    fileOps,
		storePath:  storePath,
	}

	// Parse subcommands
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]

	switch subcommand {
	case "add":
		if err := cmds.add(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "download":
		if err := cmds.download(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "list":
		if err := cmds.list(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "config":
		if err := cmds.showConfig(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "config-set":
		if err := cmds.setConfig(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "remove":
		if err := cmds.remove(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "sync":
		if err := cmds.sync(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "edit":
		if err := cmds.edit(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Comic Scraper CLI")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  cli <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  add <url> [-name <custom_name>]   Add a series by URL")
	fmt.Println("  download <series_id> [-ch <num>]  Download chapters")
	fmt.Println("  list                              List tracked series")
	fmt.Println("  config                            Show current configuration")
	fmt.Println("  config-set -key <key> -val <val>  Update configuration")
	fmt.Println("  remove <series_id>                Remove a tracked series")
	fmt.Println("  edit <series_id> [-name <name>] [-url <url>]")
	fmt.Println("                                    Edit series name or URL")
	fmt.Println("  sync                              Scan download folder for series")
	fmt.Println("  help                              Show this help message")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  cli add https://asurascans.com/manga/series-name")
	fmt.Println("  cli add https://luacomic.org/series/123 -name \"My Custom Name\"")
	fmt.Println("  cli download <uuid> -ch 5")
	fmt.Println("  cli edit <uuid> -name \"New Name\"")
	fmt.Println("  cli sync")
}

// add adds a new series by URL
func (c *Commands) add(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: add <url> [-name <custom_name>]")
	}

	url := args[0]

	// Parse flags
	var customName string
	for i := 1; i < len(args); i++ {
		if args[i] == "-name" && i+1 < len(args) {
			customName = args[i+1]
			i++
		}
	}

	// Find appropriate scraper
	scr := c.registry.GetScraper(url)
	if scr == nil {
		return fmt.Errorf("no scraper found for URL: %s", url)
	}

	fmt.Printf("Fetching series info from %s...\n", scr.Name())

	// Fetch series page
	html, err := scraper.FetchHTML(scr, c.httpClient, url)
	if err != nil {
		return fmt.Errorf("failed to fetch series page: %w", err)
	}

	// Extract title from HTML
	title := scraper.ExtractText(html, "h1, .series-title, .manga-title")
	if title == "" {
		title = "Unknown Series"
	}

	// Extract chapters
	chapters, err := scr.ExtractChapters(html, url)
	if err != nil {
		return fmt.Errorf("failed to extract chapters: %w", err)
	}

	fmt.Printf("Found %d chapters for '%s'\n", len(chapters), title)

	// Load existing store
	store, err := c.loadSeriesStore()
	if err != nil {
		store = &SeriesStore{Series: []models.Series{}}
	}

	// Create new series
	series := models.NewSeries(title, url)
	series.ID = uuid.New().String()
	if customName != "" {
		safeName := fileutil.SanitizeFolderName(customName)
		series.CustomName = safeName
		series.Title = safeName
	}
	series.ChapterCount = len(chapters)
	series.UpdatedAt = time.Now()

	// Add to store
	store.Series = append(store.Series, *series)

	// Save store
	if err := c.saveSeriesStore(store); err != nil {
		return fmt.Errorf("failed to save series: %w", err)
	}

	// Persist series metadata and cover
	folderName := fileutil.SanitizeFolderName(series.CustomName)
	if folderName == "unnamed" {
		folderName = series.Title
	}
	folderName = fileutil.SanitizeFolderName(folderName)
	if err := c.saveSeriesMetadata(folderName, scr, html); err != nil {
		log.Printf("Warning: failed to save series metadata: %v", err)
	}

	fmt.Printf("Added series '%s' (ID: %s)\n", series.Title, series.ID)
	return nil
}

func (c *Commands) saveSeriesMetadata(folderName string, scr scraper.Scraper, html string) error {
	seriesPath := filepath.Join(c.config.DownloadPath, folderName)
	if err := os.MkdirAll(seriesPath, 0755); err != nil {
		return fmt.Errorf("failed to create series folder: %w", err)
	}

	info := models.SeriesInfo{Source: scr.Name()}
	if sip, ok := scr.(scraper.SeriesInfoProvider); ok {
		extracted, err := sip.ExtractSeriesInfo(html, folderName)
		if err != nil {
			log.Printf("Warning: metadata extraction failed: %v", err)
		} else {
			info = extracted
		}
	}

	if info.Title == "" {
		info.Title = folderName
	}

	infoPath := filepath.Join(seriesPath, "series-info.json")
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal series info: %w", err)
	}
	if err := os.WriteFile(infoPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write series info: %w", err)
	}

	if info.CoverURL != "" {
		scraper.SetCookiesOnHttpClient(scr, c.httpClient, info.CoverURL)
		coverPath := filepath.Join(seriesPath, "cover")
		if err := c.fileOps.SaveCoverImage(info.CoverURL, coverPath); err != nil {
			log.Printf("Warning: failed to save cover image: %v", err)
		}
	}

	return nil
}

// download downloads chapters for a series
func (c *Commands) download(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: download <series_id> [-ch <number|all>]")
	}

	seriesID := args[0]

	// Parse flags
	chapterNum := "all"
	for i := 1; i < len(args); i++ {
		if args[i] == "-ch" && i+1 < len(args) {
			chapterNum = args[i+1]
			i++
		}
	}

	// Load store
	store, err := c.loadSeriesStore()
	if err != nil {
		return fmt.Errorf("failed to load series store: %w", err)
	}

	// Find series
	var series *models.Series
	for i := range store.Series {
		if store.Series[i].ID == seriesID {
			series = &store.Series[i]
			break
		}
	}

	if series == nil {
		return fmt.Errorf("series not found: %s", seriesID)
	}

	// Find scraper
	scr := c.registry.GetScraper(series.URL)
	if scr == nil {
		return fmt.Errorf("no scraper found for series URL: %s", series.URL)
	}

	// Fetch chapters
	html, err := scraper.FetchHTML(scr, c.httpClient, series.URL)
	if err != nil {
		return fmt.Errorf("failed to fetch series page: %w", err)
	}

	// Inject Cloudflare cookies from the browser session into the HTTP client
	// so image downloads can bypass Cloudflare protection
	scraper.SetCookiesOnHttpClient(scr, c.httpClient, series.URL)

	chapters, err := scr.ExtractChapters(html, series.URL)
	if err != nil {
		return fmt.Errorf("failed to extract chapters: %w", err)
	}

	// Create download manager
	dm := download.NewManager(c.config, c.httpClient)

	// Determine which chapters to download
	var chaptersToDownload []models.Chapter
	if chapterNum == "all" {
		chaptersToDownload = chapters
	} else {
		num, err := strconv.ParseFloat(chapterNum, 64)
		if err != nil {
			return fmt.Errorf("invalid chapter number: %s", chapterNum)
		}
		for _, ch := range chapters {
			if ch.Number == num {
				chaptersToDownload = append(chaptersToDownload, ch)
				break
			}
		}
	}

	if len(chaptersToDownload) == 0 {
		return fmt.Errorf("no chapters found to download")
	}

	// Sort chapters ascending (lowest first) for proper download order
	scraper.SortChaptersAscending(chaptersToDownload)

	fmt.Printf("Downloading %d chapter(s) for '%s'...\n", len(chaptersToDownload), series.Title)

	// Download each chapter
	downloaded := 0
	failed := 0

	// Determine folder name (use custom name if set, otherwise title).
	// Both are sanitized to a single safe path component so a hand-edited
	// store.json can't escape the download root via ".." or separators.
	folderName := fileutil.SanitizeFolderName(series.CustomName)
	if folderName == "unnamed" {
		folderName = fileutil.SanitizeFolderName(series.Title)
	}

	for _, chapter := range chaptersToDownload {
		// Check if chapter already exists
		chapterDir := filepath.Join(c.config.DownloadPath, folderName, "Chapter "+strconv.FormatFloat(chapter.Number, 'f', -1, 64))
		if _, err := os.Stat(chapterDir); err == nil {
			entries, _ := os.ReadDir(chapterDir)
			if len(entries) > 0 {
				fmt.Printf("Chapter %.1f already exists with %d images, skipping\n", chapter.Number, len(entries))
				continue
			}
		}
		fmt.Printf("Downloading Chapter %.1f: %s\n", chapter.Number, chapter.Title)

		// Fetch chapter page
		chHTML, err := scraper.FetchHTML(scr, c.httpClient, chapter.URL)
		if err != nil {
			fmt.Printf("  Failed to fetch chapter page: %v\n", err)
			failed++
			continue
		}

		// Re-inject cookies after each chapter fetch (cookies may refresh)
		scraper.SetCookiesOnHttpClient(scr, c.httpClient, chapter.URL)

		// Extract images
		images, err := scr.ExtractImages(chHTML, chapter.URL)
		if err != nil {
			fmt.Printf("  Failed to extract images: %v\n", err)
			failed++
			continue
		}

		fmt.Printf("  Found %d images\n", len(images))

		// Create download task
		task := &download.Task{
			ID:            uuid.New().String(),
			SeriesID:      series.ID,
			ChapterID:     chapter.ID,
			SeriesTitle:   series.Title,
			CustomName:    folderName,
			ChapterTitle:  chapter.Title,
			ChapterNumber: chapter.Number,
			URL:           chapter.URL,
			Images:        images,
		}

		dl := models.NewDownload(series.ID, chapter.ID, series.Title, chapter.Title)

		// Download chapter
		if err := dm.DownloadChapter(task, dl); err != nil {
			fmt.Printf("  Download failed: %v\n", err)
			failed++
			continue
		}

		if dl.Status == models.StatusCompleted {
			downloaded++
			fmt.Printf("  Download complete!\n")
		} else if dl.Status == models.StatusPartial {
			fmt.Printf("  Partial download (some images may have failed)\n")
			downloaded++
		} else {
			failed++
		}
	}

	// Update series stats
	series.ChaptersDownloaded += downloaded
	series.UpdatedAt = time.Now()
	c.saveSeriesStore(store)

	fmt.Printf("\nDownload summary: %d succeeded, %d failed\n", downloaded, failed)
	fmt.Printf("Downloaded to: %s\n", c.config.DownloadPath)
	return nil
}

// list lists all tracked series
func (c *Commands) list() error {
	store, err := c.loadSeriesStore()
	if err != nil {
		return fmt.Errorf("no tracked series found (run 'add' first)")
	}

	if len(store.Series) == 0 {
		fmt.Println("No tracked series. Use 'add' command to track a series.")
		return nil
	}

	fmt.Println("Tracked Series:")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-36s %-30s %10s %10s\n", "ID", "Title", "Chapters", "Downloaded")
	fmt.Println(strings.Repeat("-", 80))

	for _, series := range store.Series {
		title := series.Title
		if len(title) > 28 {
			title = title[:25] + "..."
		}
		fmt.Printf("%-36s %-30s %10d %10d\n",
			series.ID,
			title,
			series.ChapterCount,
			series.ChaptersDownloaded,
		)
	}

	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("Total: %d series\n", len(store.Series))
	return nil
}

// showConfig displays current configuration
func (c *Commands) showConfig() error {
	fmt.Println("Current Configuration:")
	fmt.Println(strings.Repeat("-", 40))
	fmt.Printf("Download Path:    %s\n", c.config.DownloadPath)
	fmt.Printf("Rate Delay:       %d ms\n", c.config.RateDelay)
	fmt.Printf("Min Image Size:   %d KB\n", c.config.MinImageSizeKB)
	fmt.Printf("Min Image Width:  %d px\n", c.config.MinImageWidth)
	fmt.Printf("Min Image Height: %d px\n", c.config.MinImageHeight)
	fmt.Printf("User Agent:      %s\n", c.config.UserAgent)
	fmt.Println(strings.Repeat("-", 40))
	fmt.Printf("Config File:   %s\n", config.GetConfigPath())
	fmt.Printf("Series Store:  %s\n", c.storePath)
	return nil
}

// setConfig updates configuration values
func (c *Commands) setConfig(args []string) error {
	fs := flag.NewFlagSet("config-set", flag.ContinueOnError)
	key := fs.String("key", "", "Configuration key to set")
	val := fs.String("val", "", "Value to set")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *key == "" || *val == "" {
		return fmt.Errorf("usage: config-set -key <key> -val <value>")
	}

	switch *key {
	case "downloadPath":
		c.config.DownloadPath = *val
		// Ensure directory exists
		if err := os.MkdirAll(*val, 0755); err != nil {
			return fmt.Errorf("failed to create download directory: %w", err)
		}
	case "rateDelay":
		delay, err := strconv.Atoi(*val)
		if err != nil {
			return fmt.Errorf("rateDelay must be an integer (milliseconds)")
		}
		c.config.RateDelay = delay
		c.httpClient.SetRateDelay(delay)
	case "minImageSizeKB":
		size, err := strconv.Atoi(*val)
		if err != nil {
			return fmt.Errorf("minImageSizeKB must be an integer")
		}
		c.config.MinImageSizeKB = size
	case "minImageWidth":
		w, err := strconv.Atoi(*val)
		if err != nil {
			return fmt.Errorf("minImageWidth must be an integer")
		}
		c.config.MinImageWidth = w
	case "minImageHeight":
		h, err := strconv.Atoi(*val)
		if err != nil {
			return fmt.Errorf("minImageHeight must be an integer")
		}
		c.config.MinImageHeight = h
	case "userAgent":
		c.config.UserAgent = *val
		c.httpClient.SetUserAgent(*val)
	case "browserBackground":
		valLower := strings.ToLower(*val)
		c.config.BrowserBackground = valLower == "true" || valLower == "1"
	default:
		return fmt.Errorf("unknown configuration key: %s", *key)
	}

	if err := config.Save(c.config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	fmt.Printf("Updated %s to %s\n", *key, *val)
	return nil
}

// remove removes a series from the store
func (c *Commands) remove(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: remove <series_id>")
	}

	seriesID := args[0]

	store, err := c.loadSeriesStore()
	if err != nil {
		return fmt.Errorf("failed to load series store: %w", err)
	}

	// Find and remove series
	found := false
	for i, series := range store.Series {
		if series.ID == seriesID {
			store.Series = append(store.Series[:i], store.Series[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("series not found: %s", seriesID)
	}

	if err := c.saveSeriesStore(store); err != nil {
		return fmt.Errorf("failed to save series store: %w", err)
	}

	fmt.Printf("Removed series %s\n", seriesID)
	return nil
}

// edit edits an existing series
func (c *Commands) edit(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: edit <series_id> [-name <new_name>] [-url <new_url>]")
	}

	seriesID := args[0]

	// Parse flags
	var newName, newURL string
	for i := 1; i < len(args); i++ {
		if args[i] == "-name" && i+1 < len(args) {
			newName = args[i+1]
			i++
		} else if args[i] == "-url" && i+1 < len(args) {
			newURL = args[i+1]
			i++
		}
	}

	if newName == "" && newURL == "" {
		return fmt.Errorf("no changes specified. Use -name or -url")
	}

	store, err := c.loadSeriesStore()
	if err != nil {
		return fmt.Errorf("failed to load series store: %w", err)
	}

	// Find series
	found := false
	for i := range store.Series {
		if store.Series[i].ID == seriesID {
			if newName != "" {
				safeName := fileutil.SanitizeFolderName(newName)
				store.Series[i].CustomName = safeName
				store.Series[i].Title = safeName
				newName = safeName
			}
			if newURL != "" {
				store.Series[i].URL = newURL
			}
			store.Series[i].UpdatedAt = time.Now()
			found = true
			fmt.Printf("Updated series %s\n", seriesID)
			if newName != "" {
				fmt.Printf("  Name: %s\n", newName)
			}
			if newURL != "" {
				fmt.Printf("  URL: %s\n", newURL)
			}
			break
		}
	}

	if !found {
		return fmt.Errorf("series not found: %s", seriesID)
	}

	if err := c.saveSeriesStore(store); err != nil {
		return fmt.Errorf("failed to save series store: %w", err)
	}

	return nil
}

// sync scans the download folder and adds discovered series
func (c *Commands) sync() error {
	fmt.Println("Scanning download folder for existing series...")

	discovered, err := c.scanDownloadFolder()
	if err != nil {
		return fmt.Errorf("failed to scan download folder: %w", err)
	}

	if len(discovered) == 0 {
		fmt.Println("No series found in download folder")
		return nil
	}

	store, err := c.loadSeriesStore()
	if err != nil {
		return fmt.Errorf("failed to load series store: %w", err)
	}

	// Add discovered series that don't already exist
	added := 0
	for _, disc := range discovered {
		exists := false
		for _, existing := range store.Series {
			if existing.Title == disc.Title || existing.CustomName == disc.CustomName {
				exists = true
				break
			}
		}
		if !exists {
			store.Series = append(store.Series, disc)
			added++
			fmt.Printf("Discovered: %s (%d chapters)\n", disc.Title, disc.ChapterCount)
		}
	}

	if err := c.saveSeriesStore(store); err != nil {
		return fmt.Errorf("failed to save series store: %w", err)
	}

	fmt.Printf("\nSync complete: Added %d new series, %d total in library\n", added, len(store.Series))
	return nil
}

// scanDownloadFolder scans the download directory for existing series
func (c *Commands) scanDownloadFolder() ([]models.Series, error) {
	var discovered []models.Series

	entries, err := os.ReadDir(c.config.DownloadPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read download directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		seriesPath := filepath.Join(c.config.DownloadPath, entry.Name())
		chapterDirs, err := os.ReadDir(seriesPath)
		if err != nil {
			continue
		}

		chapterCount := 0
		for _, ch := range chapterDirs {
			if ch.IsDir() && strings.HasPrefix(strings.ToLower(ch.Name()), "chapter") {
				chapterCount++
			}
		}

		if chapterCount > 0 {
			now := time.Now()
			series := models.Series{
				ID:                 uuid.New().String(),
				Title:              entry.Name(),
				CustomName:         entry.Name(),
				URL:                "", // Unknown - downloaded manually
				ChapterCount:       chapterCount,
				ChaptersDownloaded: chapterCount,
				CreatedAt:          now,
				UpdatedAt:          now,
			}
			discovered = append(discovered, series)
		}
	}

	return discovered, nil
}
func (c *Commands) saveSeriesStore(store *SeriesStore) error {
	dir := filepath.Dir(c.storePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal series store: %w", err)
	}

	if err := os.WriteFile(c.storePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write series store: %w", err)
	}

	return nil
}

// loadSeriesStore loads series data from JSON
func (c *Commands) loadSeriesStore() (*SeriesStore, error) {
	data, err := os.ReadFile(c.storePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &SeriesStore{Series: []models.Series{}}, nil
		}
		return nil, err
	}

	var store SeriesStore
	if err := json.Unmarshal(data, &store); err != nil {
		return nil, fmt.Errorf("failed to unmarshal series store: %w", err)
	}

	return &store, nil
}
