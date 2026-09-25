package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/user/comic-scraper/pkg/config"
	"github.com/user/comic-scraper/pkg/download"
	"github.com/user/comic-scraper/pkg/fileutil"
	"github.com/user/comic-scraper/pkg/hmanga"
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

// Server holds all dependencies for the web application
type Server struct {
	config       *config.Config
	httpClient   *httpclient.Client
	scraperReg   *scraper.Registry
	dlManager    *download.Manager
	browserQueue *browser.Queue

	// In-memory storage with mutex protection
	series    map[string]*models.Series
	chapters  map[string][]*models.Chapter
	downloads map[string]*models.Download

	mu sync.RWMutex

	// chapterStateMu serializes all read-modify-save operations on chapter state
	// files. Without this, autoDownloadMissingChapters (synchronous) and
	// updateChapterState (async goroutine from handleDownloadProgress) can
	// race: one loads the file, the other saves, and the first's save overwrites
	// the second's changes.
	chapterStateMu sync.Mutex

	// chapterStateCache holds the last-known chapter state for each series
	// folder in memory. loadChapterState serves reads from this cache and
	// only reads the .chapters.json file the first time a folder is touched.
	// saveChapterState deep-compares against the last persisted snapshot and
	// skips the disk write when nothing changed, so periodic scrapes that
	// only refresh timestamps no longer hammer the storage server.
	// All access must hold chapterStateMu (or use the wrapper functions).
	chapterStateCache map[string]*ChapterStateFile

	// hmangaStateSnapshots stores the last-persisted JSON of each artist's
	// .books.json so saveHMangaBookState can skip writes when the serialized
	// content is unchanged. Access requires hmangaStateMu.
	hmangaStateSnapshots map[string][]byte

	// hmangaGlobalSnapshots stores the last-persisted JSON of the global
	// H-Manga book cache so saveHMangaGlobalCache can skip unchanged writes.
	// Access requires hmangaGlobalMu.
	hmangaGlobalSnapshots []byte

	// hmangaRegistrySnapshots stores the last-persisted JSON of the H-Manga
	// artist registry so saveHMangaRegistry can skip unchanged writes.
	// Access requires s.mu.
	hmangaRegistrySnapshots []byte

	// Scheduler for periodic updates
	schedulerRunning bool
	schedulerStop    chan bool

	// httpServer is the running HTTP server, set by Start for graceful shutdown.
	httpServer *http.Server

	// Tracks which series are currently being checked to prevent duplicate refreshes
	checking map[string]bool

	// Tracks which series are currently auto-downloading to prevent duplicate downloads
	downloading map[string]bool

	// scrapingPaused stops the global manga scrape/sync scheduler when true.
	scrapingPaused atomic.Bool

	// Central registry (loaded once at startup, kept in memory)
	registry *SeriesRegistry

	// ==================== H-Manga (HentaiNexus) subsystem ====================
	hmangaMgr *hmanga.HentaiNexusManager

	// hmangaArtists maps artistID -> *hmanga.Artist (in-memory mirror of the registry)
	hmangaArtists map[string]*hmanga.Artist
	// hmangaBooks maps artistID -> []book id we know about (cache of ExtractBooks results)
	hmangaBooks map[string][]hmanga.Book
	// hmangaBookStates maps artistID -> in-memory .books.json state.
	hmangaBookStates map[string]*hmanga.BookStateFile
	// hmangaRegistry is the central H-Manga registry on disk
	hmangaRegistry *hmanga.ArtistRegistry

	// hmangaStateMu serializes read-modify-save of per-artist .books.json files.
	hmangaStateMu sync.Mutex

	// hmangaGlobalMu protects the cross-artist global book cache and the
	// transient download/retry maps. Lock order when both are required:
	// acquire hmangaStateMu first, then hmangaGlobalMu, then s.mu.
	// Any nested acquisition MUST follow this order or risk deadlock against
	// the operations in this file that hold all three (see recordHMBookDownloaded,
	// rebuildHMangaGlobalCache, verifyHMBookZips). Functions that only acquire
	// these locks sequentially (e.g. renameHMangaArtistFolders) are exempt
	// from the order requirement.
	hmangaGlobalMu    sync.Mutex
	hmangaGlobalBooks map[string]hmanga.GlobalBookEntry
	// hmangaArtistDownloading tracks artists currently being synced to prevent
	// duplicate sync goroutines. Protected by s.mu (not hmangaGlobalMu).
	hmangaArtistDownloading map[string]bool
	// hmangaBookDownloading tracks individual books currently downloading to
	// prevent duplicate download goroutines. Protected by hmangaGlobalMu.
	hmangaBookDownloading map[string]bool
	hmangaRetrying        map[string]int
	// hmangaLastLoggedBytes suppresses repeated identical progress log lines.
	// Keyed by DownloadID; protected by s.mu.
	hmangaLastLoggedBytes map[string]int64

	// Zip cleanup scan single-flight guard, protected by hmangaCleanupMu.
	hmangaCleanupMu      sync.Mutex
	hmangaCleanupRunning bool

	// hmangaScrapingPaused stops the global H-Manga scrape/sync scheduler when true.
	hmangaScrapingPaused atomic.Bool

	// hmangaOpQueue serialises the Playwright "starting" window across all
	// H-Manga operations. Only one item may be in statusStarting at a time;
	// items in statusInProgress may run concurrently up to the manager's own
	// 2-slot cap.
	hmangaOpQueue     []*hmangaOpItem
	hmangaOpQueueMu   sync.Mutex
	hmangaOpQueueCond *sync.Cond // gates the dispatcher
	hmangaOpByDlID    map[string]string
	hmangaOpByID      map[string]*hmangaOpItem
	pendingReap       bool

	// lifecycleShutdown is set once the server begins graceful shutdown. New
	// scheduled/auto downloads are blocked, but in-flight downloads are
	// allowed to finish.
	lifecycleShutdown atomic.Bool
	// lifecycleActive tracks goroutines that participate in safe shutdown
	// (startup syncs, auto-download loops, etc.). Shutdown waits for these.
	// countedWaitGroup exposes the current count so the shutdown wait can
	// log progress without exposing sync.WaitGroup internals.
	lifecycleActive countedWaitGroup
}

// securityHeaders is the set of browser security headers applied to HTML and
// static responses. The CSP allows same-origin scripts/styles/images and the
// inline handlers the UI uses ('unsafe-inline' for script/style), but blocks
// external script/style/image loads, framing, and form submissions to other
// origins. This is defense-in-depth: the UI already escapes user-controlled
// strings, but the CSP limits the blast radius of any future escaping miss.
const securityHeaders = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'self'"

// setSecurityHeaders applies the standard security headers to a response.
func setSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Security-Policy", securityHeaders)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
}

// ChapterInfo tracks download progress for a single chapter in the series folder
type ChapterInfo struct {
	Number         float64              `json:"number"`
	Title          string               `json:"title"`
	URL            string               `json:"url"` // chapter URL
	Downloaded     bool                 `json:"downloaded"`
	ImageCount     int                  `json:"imageCount"`
	DownloadedAt   time.Time            `json:"downloadedAt,omitempty"`
	PerImageStatus []models.ImageResult `json:"perImageStatus,omitempty"`
}

// ChapterStateFile is stored in each series folder to track chapter downloads
type ChapterStateFile struct {
	SeriesID   string                 `json:"seriesId"`
	FolderName string                 `json:"folderName"`
	URL        string                 `json:"url"`        // cached URL from registry
	LastSynced time.Time              `json:"lastSynced"` // when chapters were last fetched
	Chapters   map[string]ChapterInfo `json:"chapters"`   // chapter number -> info
}

// RegistryEntry represents a single series in the central registry
type RegistryEntry struct {
	ID                 string    `json:"id"`
	FolderName         string    `json:"folderName"` // matches download folder name
	Title              string    `json:"title"`
	CustomName         string    `json:"customName"`
	URL                string    `json:"url"`
	CheckInterval      string    `json:"checkInterval"`
	LastCheckedAt      time.Time `json:"lastCheckedAt"`
	ChapterCount       int       `json:"chapterCount"`       // total chapters found online
	ChaptersDownloaded int       `json:"chaptersDownloaded"` // cached count
	AddedAt            time.Time `json:"addedAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// SeriesRegistry is the central registry containing all series metadata
type SeriesRegistry struct {
	Version int             `json:"version"`
	Series  []RegistryEntry `json:"series"`
}

// Legacy types for migration
// ChapterState tracks download progress for a chapter (old format)
type ChapterState struct {
	Number     float64 `json:"number"`
	Title      string  `json:"title"`
	URL        string  `json:"url"`
	Downloaded bool    `json:"downloaded"`
	ImageCount int     `json:"imageCount"`
	FilePath   string  `json:"filePath"`
}

// SeriesState tracks download progress for a series (old format)
type SeriesState struct {
	SeriesID      string                  `json:"seriesId"`
	SeriesTitle   string                  `json:"seriesTitle"`
	URL           string                  `json:"url"`
	CustomName    string                  `json:"customName"`
	Chapters      map[string]ChapterState `json:"chapters"` // chapter number -> state
	CheckInterval string                  `json:"checkInterval,omitempty"`
	LastCheckedAt time.Time               `json:"lastCheckedAt,omitempty"`
	LastUpdated   time.Time               `json:"lastUpdated"`
}

// NewServer creates a new server instance with all dependencies
func NewServer() (*Server, error) {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	// Apply defaults / validation (Load only defaults DownloadPath/HMangaDownloadPath
	// when the config file is missing or lacks them; existing config files without
	// the new H-Manga fields would otherwise keep HMangaDownloadPath empty).
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create HTTP client
	httpClient := httpclient.NewClient(cfg.RateDelay, cfg.UserAgent)

	// Create shared browser queue for serialising Playwright requests
	browserQueue := browser.NewQueue(browser.Config{Background: cfg.BrowserBackground})

	// Create scraper registry and register scrapers
	browserCfg := browser.Config{Background: cfg.BrowserBackground}

	registry := scraper.NewRegistry()
	registry.Register(asura.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(demonic.NewScraper())
	registry.Register(drake.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(lua.NewScraper())
	registry.Register(manhuaus.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(manhuaplus.NewScraper(httpClient))
	registry.Register(thunderscans.NewScraperWithPlaywright(browserCfg, browserQueue))
	registry.Register(ravenscans.NewScraper())

	// Create download manager
	dlManager := download.NewManager(cfg, httpClient)

	server := &Server{
		config:                  cfg,
		httpClient:              httpClient,
		scraperReg:              registry,
		dlManager:               dlManager,
		browserQueue:            browserQueue,
		series:                  make(map[string]*models.Series),
		chapters:                make(map[string][]*models.Chapter),
		downloads:               make(map[string]*models.Download),
		checking:                make(map[string]bool),
		downloading:             make(map[string]bool),
		chapterStateCache:       make(map[string]*ChapterStateFile),
		hmangaStateSnapshots:    make(map[string][]byte),
		hmangaArtists:           make(map[string]*hmanga.Artist),
		hmangaBooks:             make(map[string][]hmanga.Book),
		hmangaBookStates:        make(map[string]*hmanga.BookStateFile),
		hmangaGlobalBooks:       make(map[string]hmanga.GlobalBookEntry),
		hmangaArtistDownloading: make(map[string]bool),
		hmangaBookDownloading:   make(map[string]bool),
		hmangaRetrying:          make(map[string]int),
		hmangaLastLoggedBytes:   make(map[string]int64),
		hmangaOpByDlID:          make(map[string]string),
		hmangaOpByID:            make(map[string]*hmangaOpItem),
	}
	// Restore persisted scraping pause states so the server boots into the same
	// state it was shut down in. Both default to running (false) if unset.
	server.scrapingPaused.Store(cfg.MangaScrapingPaused)
	server.hmangaScrapingPaused.Store(cfg.HMangaScrapingPaused)

	// Set up progress callback
	dlManager.SetProgressCallback(server.handleDownloadProgress)

	// Load central registry
	if err := server.loadOrCreateRegistry(); err != nil {
		log.Printf("Warning: Failed to load registry: %v", err)
	}

	// Create H-Manga (HentaiNexus) manager and load its registry.
	// The manager reuses the shared browserQueue so it shares one Chrome
	// process tree and one persistent profile with the manga scrapers.
	// Disk scans and startup syncs are deferred to runStartupTasks so the
	// HTTP listener opens as quickly as possible.
	server.hmangaMgr = hmanga.NewManager(cfg, browserQueue)
	server.registerHMangaCallback()
	if err := server.loadOrCreateHMangaRegistry(); err != nil {
		log.Printf("Warning: Failed to load H-Manga registry: %v", err)
	}

	return server, nil
}

// loadOrCreateRegistry loads the central registry, migrating old state files if necessary
func (s *Server) loadOrCreateRegistry() error {
	// Load registry from disk
	registry, err := s.loadRegistry()
	if err != nil {
		return err
	}

	// Check for old state files and migrate them
	if err := s.migrateOldStateFiles(registry); err != nil {
		log.Printf("Warning: Failed to migrate old state files: %v", err)
	}

	// Ensure registry is saved (creates file if new)
	if err := s.saveRegistry(registry); err != nil {
		return err
	}

	s.mu.Lock()
	s.registry = registry
	s.mu.Unlock()
	return nil
}

// scanDownloadFolder scans the download directory for existing series
func (s *Server) scanDownloadFolder() error {
	if s.registry == nil {
		return fmt.Errorf("registry not initialized")
	}

	entries, err := os.ReadDir(s.config.DownloadPath)
	if err != nil {
		return fmt.Errorf("failed to read download directory: %w", err)
	}

	discovered := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		folderName := entry.Name()
		seriesPath := filepath.Join(s.config.DownloadPath, folderName)

		// Skip hidden directories like .state
		if strings.HasPrefix(folderName, ".") {
			continue
		}

		// Count actual chapter folders on disk, reading each chapter directory
		// exactly once and reusing the file count for both counting and state
		// reconciliation.
		chapterDirs, err := os.ReadDir(seriesPath)
		if err != nil {
			continue
		}
		chapterCount := 0
		chapterImageCounts := map[string]int{}
		for _, ch := range chapterDirs {
			if !ch.IsDir() || !strings.HasPrefix(strings.ToLower(ch.Name()), "chapter") {
				continue
			}
			chNumStr := strings.TrimPrefix(strings.ToLower(ch.Name()), "chapter")
			chNumStr = strings.TrimSpace(chNumStr)
			chPath := filepath.Join(seriesPath, ch.Name())
			files, _ := os.ReadDir(chPath)
			imageCount := 0
			for _, f := range files {
				if !f.IsDir() {
					imageCount++
				}
			}
			chapterImageCounts[chNumStr] = imageCount
			chapterCount++
		}

		if chapterCount == 0 {
			continue
		}

		// Get or create registry entry
		regEntry := s.getOrCreateRegistryEntry(s.registry, folderName)
		regEntry.UpdatedAt = time.Now()

		// Load/mutate/save under one chapterStateMu hold: the state cache
		// aliases objects, so this reconcile must be synchronized against
		// concurrent writers (updateChapterState, autoDownload, ...).
		s.chapterStateMu.Lock()
		chapterState, _ := s.loadChapterStateLocked(folderName)

		// Count downloaded chapters from state file and verify against disk
		downloadedCount := 0
		if chapterState != nil {
			stateChanged := false
			for chNumStr, imageCount := range chapterImageCounts {
				if chInfo, exists := chapterState.Chapters[chNumStr]; exists {
					if chInfo.Downloaded && imageCount > 0 {
						downloadedCount++
					} else if !chInfo.Downloaded && imageCount > 0 {
						// Chapter exists on disk but state says not downloaded — reconcile
						chInfo.Downloaded = true
						chInfo.ImageCount = imageCount
						chapterState.Chapters[chNumStr] = chInfo
						downloadedCount++
						stateChanged = true
					}
				}
			}
			if stateChanged {
				s.saveChapterStateLocked(folderName, chapterState)
			}
		} else {
			// No chapter state file yet, but folders exist - mark all as downloaded
			downloadedCount = chapterCount
			chapterState = &ChapterStateFile{
				SeriesID:   regEntry.ID,
				FolderName: folderName,
				URL:        regEntry.URL,
				Chapters:   make(map[string]ChapterInfo),
			}
			for chNumStr, imageCount := range chapterImageCounts {
				chapterState.Chapters[chNumStr] = ChapterInfo{
					Number:     0, // Will be parsed from key
					Title:      "Chapter " + chNumStr,
					Downloaded: imageCount > 0,
					ImageCount: imageCount,
				}
			}
			s.saveChapterStateLocked(folderName, chapterState)
		}
		s.chapterStateMu.Unlock()

		// Update registry with latest counts
		regEntry.ChapterCount = chapterCount
		regEntry.ChaptersDownloaded = downloadedCount

		// Create in-memory series object
		series := &models.Series{
			ID:                 regEntry.ID,
			Title:              regEntry.Title,
			CustomName:         regEntry.CustomName,
			URL:                regEntry.URL,
			ChapterCount:       chapterCount,
			ChaptersDownloaded: downloadedCount,
			CheckInterval:      regEntry.CheckInterval,
			LastCheckedAt:      regEntry.LastCheckedAt,
			CreatedAt:          regEntry.AddedAt,
			UpdatedAt:          regEntry.UpdatedAt,
		}
		s.series[series.ID] = series
		discovered++
	}

	// Save updated registry
	if err := s.saveRegistry(s.registry); err != nil {
		log.Printf("Warning: Failed to save registry after scan: %v", err)
	}

	if discovered > 0 {
		log.Printf("Discovered %d series from download folder", discovered)
	}
	return nil
}

// handleDownloadProgress updates download status when progress changes
func (s *Server) handleDownloadProgress(dl *models.Download) {
	s.mu.Lock()
	s.downloads[dl.ID] = dl
	s.mu.Unlock()

	// Update series chapter count if completed
	if dl.Status == models.StatusCompleted || dl.Status == models.StatusPartial {
		s.mu.Lock()
		if series, ok := s.series[dl.SeriesID]; ok {
			series.ChaptersDownloaded++
			series.UpdatedAt = time.Now()
		}
		s.mu.Unlock()

		// Update chapter state file asynchronously, but track it for safe shutdown.
		s.safeGoTrack("updateChapterState", func() {
			s.updateChapterState(dl.SeriesID, dl.ChapterID, dl)
		})
	}
}

// updateChapterState updates the chapter state in the series folder
func (s *Server) updateChapterState(seriesID, chapterID string, dl *models.Download) {
	s.mu.RLock()
	series, ok := s.series[seriesID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	// Get folder name
	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	// Hold chapterStateMu across the load-modify-save to prevent races with
	// autoDownloadMissingChapters which also saves the same file.
	s.chapterStateMu.Lock()
	defer s.chapterStateMu.Unlock()

	// Load chapter state from series folder (Locked variant: lock already held)
	chapterState, err := s.loadChapterStateLocked(folderName)
	if err != nil {
		log.Printf("Failed to load chapter state for update: %v", err)
		return
	}
	if chapterState == nil {
		chapterState = &ChapterStateFile{
			SeriesID:   seriesID,
			FolderName: folderName,
			URL:        series.URL,
			Chapters:   make(map[string]ChapterInfo),
		}
	}

	s.mu.RLock()
	var chapter *models.Chapter
	for _, ch := range s.chapters[seriesID] {
		if ch.ID == chapterID {
			chapter = ch
			break
		}
	}
	s.mu.RUnlock()

	if chapter == nil {
		return
	}

	chapterKey := strconv.FormatFloat(chapter.Number, 'f', -1, 64)
	// applyChapterCompletion is the single owner of the completion predicate
	// (completed, or all remaining images intentionally filtered) shared by
	// every completion-writing site.
	downloaded := isDownloadComplete(dl)
	if chInfo, ok := chapterState.Chapters[chapterKey]; ok {
		chInfo.Downloaded = downloaded
		chInfo.ImageCount = dl.ImageCount
		chInfo.DownloadedAt = time.Now()
		if dl.PerImageStatus != nil {
			chInfo.PerImageStatus = dl.PerImageStatus
		}
		chapterState.Chapters[chapterKey] = chInfo
	} else {
		// Chapter not in state yet - add it
		newInfo := ChapterInfo{
			Number:       chapter.Number,
			Title:        chapter.Title,
			URL:          chapter.URL,
			Downloaded:   downloaded,
			ImageCount:   dl.ImageCount,
			DownloadedAt: time.Now(),
		}
		if dl.PerImageStatus != nil {
			newInfo.PerImageStatus = dl.PerImageStatus
		}
		chapterState.Chapters[chapterKey] = newInfo
	}

	if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
		log.Printf("Failed to save chapter state after download: %v", err)
	}

	// Update registry — only update timestamp; ChaptersDownloaded is already
	// incremented by handleDownloadProgress so we must not double-count.
	s.updateRegistryEntry(seriesID, func(e *RegistryEntry) {
		e.UpdatedAt = time.Now()
	})
}

// startUpdateScheduler starts the background goroutine that periodically checks for series updates
func (s *Server) startUpdateScheduler() {
	s.mu.Lock()
	if s.schedulerRunning {
		s.mu.Unlock()
		return
	}
	s.schedulerRunning = true
	s.schedulerStop = make(chan bool)
	s.mu.Unlock()

	log.Printf("Starting series update scheduler (checks every minute)")

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[PANIC] recovered in update scheduler: %v", r)
			}
		}()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if s.isShutdown() {
					continue
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[PANIC] recovered in checkSeriesForUpdates: %v", r)
						}
					}()
					if !s.isMangaScrapingPaused() {
						s.checkSeriesForUpdates()
					}
				}()
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[PANIC] recovered in checkHMangaArtistsForUpdates: %v", r)
						}
					}()
					if !s.isHMangaScrapingPaused() {
						s.checkHMangaArtistsForUpdates()
					}
				}()
			case <-s.schedulerStop:
				log.Printf("Update scheduler stopped")
				return
			}
		}
	}()
}

// stopUpdateScheduler stops the background update scheduler
func (s *Server) stopUpdateScheduler() {
	s.mu.Lock()
	if !s.schedulerRunning {
		s.mu.Unlock()
		return
	}
	s.schedulerRunning = false
	stopCh := s.schedulerStop
	s.mu.Unlock()

	close(stopCh)
}

// resetStaleDownloadState clears the download queue from a previous server
// session.  The downloads map is already empty on startup (it's in-memory
// only), but chapter state files on disk may contain stale entries from
// downloads that were interrupted when the server shut down.  This method
// scans each series' chapter state and resets any chapter whose image count
// on disk doesn't match the recorded ImageCount back to Downloaded: false,
// ensuring those chapters will be re-downloaded by syncAllSeriesOnStartup.
// isAllFiltered reports whether every recorded image status is Filtered.
// It returns false for an empty list so unknown states are not misclassified.
func isAllFiltered(statuses []models.ImageResult) bool {
	if len(statuses) == 0 {
		return false
	}
	for _, st := range statuses {
		if st.Status != models.ImageStatusFiltered {
			return false
		}
	}
	return true
}

// isDownloadComplete is the single owner of the chapter-completion predicate:
// a download counts as complete when it finished fully, or when every
// remaining image was intentionally filtered (so it is never retried forever).
// Every site that writes Downloaded to chapter state must use this.
func isDownloadComplete(dl *models.Download) bool {
	return dl.Status == models.StatusCompleted || isAllFiltered(dl.PerImageStatus)
}

func (s *Server) resetStaleDownloadState() {
	if s.registry == nil {
		return
	}

	// Copy values (not pointers) to avoid stale-pointer issues if another
	// goroutine modifies s.series concurrently via handleUpdateSeries etc.
	s.mu.RLock()
	seriesByID := make(map[string]models.Series, len(s.series))
	for id, series := range s.series {
		seriesByID[id] = *series
	}
	s.mu.RUnlock()

	resetCount := 0
	skippedCount := 0
	for id, series := range seriesByID {
		folderName := series.CustomName
		if folderName == "" {
			folderName = series.Title
		}

		s.chapterStateMu.Lock()
		chapterState, err := s.loadChapterStateLocked(folderName)
		if err != nil || chapterState == nil {
			s.chapterStateMu.Unlock()
			continue
		}

		seriesPath := filepath.Join(s.config.DownloadPath, folderName)
		stateChanged := false

		for chapterKey, chInfo := range chapterState.Chapters {
			if !chInfo.Downloaded {
				continue
			}

			// Check actual files on disk for this chapter
			chapterDir := filepath.Join(seriesPath, "Chapter "+chapterKey)
			entries, err := os.ReadDir(chapterDir)
			if err != nil {
				// Directory doesn't exist but state says downloaded — reset
				log.Printf("[STARTUP-RESET] Chapter %s of '%s' directory missing, resetting for re-download",
					chapterKey, folderName)
				chInfo.Downloaded = false
				chInfo.ImageCount = 0
				chInfo.DownloadedAt = time.Time{}
				chapterState.Chapters[chapterKey] = chInfo
				stateChanged = true
				resetCount++
				continue
			}

			imageCount := 0
			for _, e := range entries {
				if !e.IsDir() {
					imageCount++
				}
			}

			// Reset if: (1) directory has fewer images than recorded, or
			// (2) ImageCount was never set (0) but directory is empty — both
			// indicate an interrupted download. But don't reset if all missing
			// images were intentionally filtered (wrong dimensions, too small).
			shouldReset := false
			if chInfo.ImageCount > 0 && imageCount < chInfo.ImageCount {
				// Check per-image status: if all missing images were filtered, this is expected
				if isAllFiltered(chInfo.PerImageStatus) {
					// Intentional skip — every missing image was filtered by
					// dimension/size rules, not a real failure. Suppressed:
					// a series with hundreds of filtered chapters would flood
					// the console. The summary at the end of the loop still
					// reports how many chapters were intentionally skipped.
				} else {
					shouldReset = true
				}
			} else if imageCount == 0 && chInfo.ImageCount == 0 {
				// Empty folder with no recorded images: normally reset, but if every
				// image was intentionally filtered (no retryable failures), treat
				// the chapter as intentionally skipped rather than failed.
				if isAllFiltered(chInfo.PerImageStatus) {
					// Intentional skip — see comment above. The state is
					// updated silently and the action is reflected in the
					// final reset/skip summary at the end of the loop.
					chInfo.Downloaded = true
					chapterState.Chapters[chapterKey] = chInfo
					stateChanged = true
					skippedCount++
				} else {
					shouldReset = true
				}
			}

			if shouldReset {
				log.Printf("[STARTUP-RESET] Chapter %s of '%s' has %d/%d images on disk, resetting for re-download",
					chapterKey, folderName, imageCount, chInfo.ImageCount)
				chInfo.Downloaded = false
				chInfo.ImageCount = 0
				chInfo.DownloadedAt = time.Time{}
				chInfo.PerImageStatus = nil
				chapterState.Chapters[chapterKey] = chInfo
				stateChanged = true
				resetCount++
			}
		}

		if stateChanged {
			if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
				log.Printf("[STARTUP-RESET] Failed to save chapter state for '%s': %v", folderName, err)
			}
			// Update the in-memory series download count
			downloadedCount := 0
			for _, c := range chapterState.Chapters {
				if c.Downloaded {
					downloadedCount++
				}
			}
			s.mu.Lock()
			if sEntry, ok := s.series[id]; ok {
				sEntry.ChaptersDownloaded = downloadedCount
			}
			s.mu.Unlock()

			s.updateRegistryEntry(id, func(e *RegistryEntry) {
				e.ChaptersDownloaded = downloadedCount
			})
			s.chapterStateMu.Unlock()
		} else {
			s.chapterStateMu.Unlock()
		}
	}

	if resetCount > 0 || skippedCount > 0 {
		log.Printf("[STARTUP-RESET] Reset %d stale chapter(s); %d chapter(s) intentionally skipped (all filtered)", resetCount, skippedCount)
	}
}

// syncAllSeriesOnStartup checks all series on server start and triggers
// auto-download for any series that has undownloaded chapters.  It processes
// series sequentially (one at a time) to avoid overwhelming the download
// manager's worker pool or hitting remote sites with too many concurrent
// requests.
func (s *Server) syncAllSeriesOnStartup() {
	if s.registry == nil {
		return
	}

	// Copy values (not pointers) to avoid stale-pointer issues.
	s.mu.RLock()
	seriesByID := make(map[string]models.Series, len(s.series))
	for id, series := range s.series {
		seriesByID[id] = *series
	}
	s.mu.RUnlock()

	for id, series := range seriesByID {
		// Skip series with no URL (discovered from folder, cannot download)
		if series.URL == "" {
			continue
		}
		// Skip series without an auto-check interval on startup; they are
		// only synced when manually requested.
		if series.CheckInterval == "" || series.CheckInterval == "never" {
			continue
		}
		// Skip series whose interval is not yet due.
		if dur, ok := models.ParseCheckInterval(series.CheckInterval); ok {
			if !series.LastCheckedAt.IsZero() && time.Since(series.LastCheckedAt) < dur {
				continue
			}
		}

		// Determine if this series needs syncing
		needsSync := false

		// Check chapter state for any missing chapters
		folderName := series.CustomName
		if folderName == "" {
			folderName = series.Title
		}
		s.chapterStateMu.Lock()
		chapterState, _ := s.loadChapterStateLocked(folderName)
		if chapterState != nil {
			for _, chInfo := range chapterState.Chapters {
				if !chInfo.Downloaded {
					needsSync = true
					break
				}
			}
		}
		s.chapterStateMu.Unlock()
		if chapterState == nil && series.ChaptersDownloaded < series.ChapterCount {
			// No state file but counts suggest missing chapters
			needsSync = true
		}

		if !needsSync {
			log.Printf("[STARTUP] Series '%s' is up to date, skipping", folderName)
			continue
		}

		log.Printf("[STARTUP] Series '%s' has missing chapters, starting auto-download", folderName)
		s.autoDownloadMissingChapters(id)
	}
}

// checkSeriesForUpdates checks all series and updates those that are due
func (s *Server) checkSeriesForUpdates() {
	if s.registry == nil {
		return
	}

	s.mu.RLock()
	seriesList := make([]models.Series, 0, len(s.series))
	for _, series := range s.series {
		seriesList = append(seriesList, *series)
	}
	s.mu.RUnlock()

	checkedCount := 0
	for i := range seriesList {
		series := &seriesList[i]
		// Skip series with no check interval set
		if series.CheckInterval == "" || series.CheckInterval == "never" {
			continue
		}

		// Skip series already being checked
		s.mu.Lock()
		if s.checking[series.ID] {
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		// Parse the interval
		duration, ok := models.ParseCheckInterval(series.CheckInterval)
		if !ok {
			continue
		}

		// Check if enough time has passed since last check
		shouldCheck := false
		if series.LastCheckedAt.IsZero() {
			// Never checked before - check now
			shouldCheck = true
		} else {
			shouldCheck = time.Since(series.LastCheckedAt) >= duration
		}

		if shouldCheck {
			checkedCount++
			log.Printf("Auto-checking series '%s' (interval: %s, last checked: %v)",
				series.Title, series.CheckInterval, series.LastCheckedAt.Format("15:04:05"))
			s.mu.Lock()
			s.checking[series.ID] = true
			s.mu.Unlock()
			safeGoTrackSeries := series // capture loop variable
			s.safeGoTrack("autoCheckSeries", func() {
				newChapters := s.refreshSeriesChapters(safeGoTrackSeries)
				if newChapters > 0 {
					s.autoDownloadMissingChapters(safeGoTrackSeries.ID, true)
				}
				s.mu.Lock()
				delete(s.checking, safeGoTrackSeries.ID)
				s.mu.Unlock()
			})
		}
	}

	if checkedCount > 0 {
		log.Printf("Auto-checked %d series this cycle", checkedCount)
	}
}

// isMangaScrapingPaused reports whether the global manga scraping scheduler is paused.
func (s *Server) isMangaScrapingPaused() bool {
	return s.scrapingPaused.Load()
}

// setMangaScrapingPaused enables or disables the global manga scraping scheduler.
func (s *Server) setMangaScrapingPaused(paused bool) {
	s.scrapingPaused.Store(paused)
	s.mu.Lock()
	s.config.MangaScrapingPaused = paused
	s.mu.Unlock()
	if err := config.Save(s.config); err != nil {
		log.Printf("[SCRAPING] Failed to persist manga scraping state: %v", err)
	}
}

// consoleBuf is a size-capped in-memory copy of every log line the server
// writes, used by the web UI Console tab (GET /api/console).
type consoleBuf struct {
	mu    sync.Mutex
	lines []string
	max   int
	drop  uint64
}

var console = &consoleBuf{max: 1000}

// Write implements io.Writer. The std log package writes one full line per
// call (it appends a newline when missing), so each Write is stored as a
// single line. When the buffer is full, oldest lines are evicted in batches
// and drop counts them so the UI can show how many were discarded.
func (c *consoleBuf) Write(p []byte) (int, error) {
	text := strings.TrimRight(string(p), "\n")
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.lines) >= c.max {
		keep := c.max / 4
		c.drop += uint64(len(c.lines) - keep)
		c.lines = append([]string(nil), c.lines[len(c.lines)-keep:]...)
	}
	c.lines = append(c.lines, text)
	return len(p), nil
}

// snapshot returns the buffered lines and the number of older lines that were
// discarded to stay under the cap.
func (c *consoleBuf) snapshot() (lines []string, dropped uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...), c.drop
}

// handleConsole serves the in-memory console log buffer as JSON.
func (s *Server) handleConsole(w http.ResponseWriter, r *http.Request) {
	lines, dropped := console.snapshot()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"lines": lines, "dropped": dropped})
}

// handleGetMangaScrapingState returns whether global manga scraping is running.
func (s *Server) handleGetMangaScrapingState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"running": !s.isMangaScrapingPaused()})
}

// handleToggleMangaScraping toggles the global manga scraping state.
func (s *Server) handleToggleMangaScraping(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Running bool `json:"running"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	s.setMangaScrapingPaused(!req.Running)
	status := "resumed"
	if !req.Running {
		status = "paused"
	}
	log.Printf("[SCRAPING] Manga scraping %s (running=%v)", status, req.Running)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"running": req.Running, "status": status})
}

// runStartupTasks runs all offline disk scans and startup syncs in the
// background. It is called from Start() so the HTTP listener can open before
// scans finish.
func (s *Server) runStartupTasks() {
	defer s.lifecycleActive.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PANIC] recovered in runStartupTasks: %v", r)
		}
	}()

	totalStart := time.Now()

	// Load any existing global H-Manga cache before scanning so shared books
	// already tracked as downloaded are honored during discovery.
	s.loadOrRebuildHMangaGlobalCache()

	// Phase 1: scan download folder and H-Manga artists concurrently.
	scanStart := time.Now()
	var scanWg sync.WaitGroup
	scanWg.Add(2)
	go func() {
		defer scanWg.Done()
		if err := s.scanDownloadFolder(); err != nil {
			log.Printf("[STARTUP] Warning: Failed to scan download folder: %v", err)
		}
	}()
	go func() {
		defer scanWg.Done()
		if err := s.scanHMangaArtists(); err != nil {
			log.Printf("[STARTUP] Warning: Failed to scan H-Manga download folder: %v", err)
		}
	}()
	scanWg.Wait()
	log.Printf("[STARTUP] scanDownloadFolder + scanHMangaArtists took %v", time.Since(scanStart))

	// Phase 2: optionally verify H-Manga ZIPs asynchronously, then rebuild the
	// global book cache from the up-to-date per-artist state.
	if s.config.VerifyHMDownloads {
		verifyStart := time.Now()
		s.verifyHMBookZips()
		log.Printf("[STARTUP] verifyHMBookZips took %v", time.Since(verifyStart))
	}
	cacheStart := time.Now()
	s.rebuildHMangaGlobalCache()
	log.Printf("[STARTUP] rebuildHMangaGlobalCache took %v", time.Since(cacheStart))

	// Phase 3: reset stale download state.
	resetStart := time.Now()
	s.resetStaleDownloadState()
	log.Printf("[STARTUP] resetStaleDownloadState took %v", time.Since(resetStart))

	// Phase 4: startup syncs for manga and H-Manga, running concurrently.
	syncStart := time.Now()
	var syncWg sync.WaitGroup
	syncWg.Add(2)
	go func() {
		defer syncWg.Done()
		if !s.isMangaScrapingPaused() {
			s.syncAllSeriesOnStartup()
		}
	}()
	go func() {
		defer syncWg.Done()
		if !s.isHMangaScrapingPaused() {
			s.checkAllHMArtistsOnStartup()
		}
	}()
	syncWg.Wait()
	log.Printf("[STARTUP] startup syncs took %v", time.Since(syncStart))

	log.Printf("[STARTUP] All startup tasks completed in %v", time.Since(totalStart))
}

// Start initializes and starts the HTTP server
func (s *Server) Start(addr string) error {
	// Start the update scheduler
	s.startUpdateScheduler()

	// Run startup disk scans and syncs in the background so the HTTP listener
	// opens immediately. Track the goroutine for safe shutdown.
	s.lifecycleActive.Add(1)
	go s.runStartupTasks()

	return s.setupHTTPServer(addr)
}

// setupHTTPServer builds and starts the HTTP server. It is separate from Start
// so Start can also register the signal handler.
func (s *Server) setupHTTPServer(addr string) error {
	mux := http.NewServeMux()

	// Static files
	mux.HandleFunc("/static/", s.handleStatic)

	// Health check for Docker HEALTHCHECK and CI verification. GET-only,
	// no state, no CSRF implications.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	// API routes
	mux.HandleFunc("/api/series", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleListSeries(w, r)
		case http.MethodPost:
			s.handleAddSeries(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/series/", func(w http.ResponseWriter, r *http.Request) {
		// Bulk interval must be matched before per-series suffix handlers (its
		// path has no series ID).
		if strings.HasSuffix(r.URL.Path, "/bulk-check-interval") {
			if r.Method == http.MethodPut {
				s.handleBulkCheckInterval(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/chapters") {
			if r.Method == http.MethodGet {
				s.handleGetChapters(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/sync-all") {
			// sync-all only downloads known missing chapters and is allowed while paused.
			if r.Method == http.MethodPost {
				s.handleSyncAllSeries(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/sync") {
			if r.Method == http.MethodPost {
				s.handleSyncSeries(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/state") {
			if r.Method == http.MethodGet {
				s.handleGetState(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/scan-missing") {
			if r.Method == http.MethodPost {
				s.handleScanMissing(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/force-redownload") {
			if r.Method == http.MethodPost {
				s.handleForceRedownload(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/redownload-chapter") {
			if r.Method == http.MethodPost {
				s.handleRedownloadChapter(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/check-now") {
			if r.Method == http.MethodPost {
				s.handleCheckNow(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/check-interval") {
			if r.Method == http.MethodPut {
				s.handleUpdateCheckInterval(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(r.URL.Path, "/refresh-metadata") {
			if r.Method == http.MethodPost {
				s.handleRefreshMetadata(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if r.Method == http.MethodDelete {
			// Extract series ID from path like /api/series/{id}
			path := strings.TrimPrefix(r.URL.Path, "/api/series/")
			path = strings.Split(path, "/")[0]
			s.handleDeleteSeries(w, r, path)
		} else if r.Method == http.MethodPut || r.Method == http.MethodPatch {
			// Extract series ID and update
			path := strings.TrimPrefix(r.URL.Path, "/api/series/")
			path = strings.Split(path, "/")[0]
			s.handleUpdateSeries(w, r, path)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	})

	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			s.handleStartDownload(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/downloads", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			s.handleListDownloads(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/downloads/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			s.handleClearDownloads(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Cancel an active download: /api/downloads/{id}/cancel
	mux.HandleFunc("/api/downloads/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/cancel") || r.Method != http.MethodPost {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		s.handleCancelDownload(w, r)
	})

	mux.HandleFunc("/api/settings", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleGetSettings(w, r)
		case http.MethodPost:
			s.handleUpdateSettings(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/series/recheck-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if s.isMangaScrapingPaused() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "paused", "message": "Manga scraping is paused"})
			return
		}
		s.handleRecheckAllSeries(w, r)
	})

	mux.HandleFunc("/api/scraping/manga", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleGetMangaScrapingState(w, r)
		case http.MethodPost, http.MethodPut:
			s.handleToggleMangaScraping(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/console", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleConsole(w, r)
	})

	mux.HandleFunc("/api/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		log.Printf("[SHUTDOWN] Shutdown requested via API")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "shutting down"})
		// Give the response a moment to flush to the client before tearing down
		// the HTTP server. Shutdown waits for idle connections, but starting it
		// synchronously here would block the handler from returning.
		go func() {
			time.Sleep(250 * time.Millisecond)
			s.ShutdownAndExit()
		}()
	})

	// H-Manga (HentaiNexus) routes
	registerHMangaRoutes(mux, s)

	// Main UI
	mux.HandleFunc("/", s.handleIndex)

	log.Printf("Server starting on %s", addr)
	s.httpServer = &http.Server{Addr: addr, Handler: s.csrfGuard(mux)}
	return s.httpServer.ListenAndServe()
}

// csrfGuard rejects state-changing requests whose Origin header shows a
// cross-site origin. The API has no authentication (local-only by design), so
// without this a malicious web page could trigger destructive POST endpoints
// (force-redownload, shutdown, ...) from the user's browser via no-cors fetch.
// The Origin host must match a server-side allowlist (the configured bind
// host, loopback, and localhost variants) — NOT the client-controlled Host
// header, which an attacker controls under DNS rebinding. Requests without an
// Origin header (curl, same-origin JS in most browsers, CLI clients) pass
// through; GET/HEAD/OPTIONS are always safe methods.
func (s *Server) csrfGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			if origin := r.Header.Get("Origin"); origin != "" {
				u, err := url.Parse(origin)
				originHost := ""
				if err == nil {
					originHost = u.Hostname()
				}
				if err != nil || !isAllowedCSRFHost(originHost) {
					log.Printf("[CSRF] Blocked %s %s with cross-origin Origin=%q Host=%q", r.Method, r.URL.Path, origin, r.Host)
					http.Error(w, "cross-origin request rejected", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// isAllowedCSRFHost reports whether host is one the user's own browser would
// legitimately send as an Origin for this local server. Anything else is a
// cross-site origin (including DNS-rebound attacker hostnames).
func isAllowedCSRFHost(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return true
	}
	// Explicit allowlist (CSRF_ALLOWED_HOSTS, comma-separated IPs/hostnames):
	// needed behind Docker port-mapping, where LAN browsers send
	// Origin: http://<host-LAN-IP>:8080 and the container cannot match that
	// against its own interface IPs.
	for _, allowed := range strings.Split(os.Getenv("CSRF_ALLOWED_HOSTS"), ",") {
		if allowed = strings.TrimSpace(allowed); allowed != "" && strings.EqualFold(allowed, host) {
			return true
		}
	}
	// The configured bind address (e.g. HOST=0.0.0.0 override): when the user
	// exposes the server to a LAN, the UI is opened via the machine's own
	// address, which resolves to a local interface IP.
	return isLocalInterfaceIP(host)
}

// isLocalInterfaceIP reports whether host is an IP assigned to one of this
// machine's network interfaces. Interface enumeration happens on each
// validation call (Origins on state-changing requests only — not a hot path).
func isLocalInterfaceIP(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.Equal(ip) {
			return true
		}
	}
	return false
}

// Shutdown performs a safe shutdown: after stopping the HTTP server and
// scheduler it waits for tracked lifecycle goroutines to finish, then waits for
// the H-Manga manager and browser queue to complete their in-flight work before
// closing the browser contexts. Safe to call multiple times and from signal handlers.
func (s *Server) Shutdown() {
	log.Printf("[SHUTDOWN] Beginning safe shutdown")

	if s.lifecycleShutdown.CompareAndSwap(false, true) {
		log.Printf("[SHUTDOWN] Shutdown flag set; new scheduled work will be rejected")
	}

	// Stop accepting new HTTP requests and finish active ones.
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("[SHUTDOWN] HTTP shutdown error: %v", err)
		}
	}

	// Stop background scheduler ticks so no new periodic syncs start.
	s.stopUpdateScheduler()

	// Wait for in-flight downloads to finish. The bound is
	// config.ShutdownTimeoutSeconds (default 10 minutes) and is a SAFETY
	// NET only: under normal conditions the goroutines finish well
	// before the timeout because they are download operations that
	// complete at their natural pace (no cancellation on shutdown).
	// H-Manga downloads are tracked in the manga lifecycle via
	// safeGoTrack, so this wait covers them too.
	timeout := s.config.ShutdownTimeoutSeconds
	if timeout <= 0 {
		timeout = config.DefaultShutdownTimeoutSeconds
	}
	log.Printf("[SHUTDOWN] Waiting up to %v for in-flight downloads to finish...", time.Duration(timeout)*time.Second)
	s.waitForLifecycleWithProgress(time.Duration(timeout) * time.Second)

	// Stop the H-Manga manager FIRST. The manager shares the browser
	// queue, so any in-flight H-Manga op (Login, ExtractBooks,
	// DownloadBook's cookie extraction) is currently calling q.Run and
	// holding a queue slot. Stopping the manager waits for those ops
	// (and for the plain-HTTP stream portion of DownloadBook, which
	// the manager's active counter covers for its full duration) to
	// finish. Until that happens, the queue still has in-flight
	// Playwright requests that need the browser alive.
	s.hmangaMgr.Stop()

	// Now stop the shared browser queue. With no H-Manga ops in flight
	// the queue has nothing left to do but drain any remaining manga
	// scraper requests (which the lifecycleActive wait in step 1
	// already covered) and then tear down the persistent Chrome
	// context.
	s.browserQueue.Stop()

	log.Printf("[SHUTDOWN] Safe shutdown complete")
}

// waitForLifecycleWithProgress waits up to timeout for all tracked lifecycle
// goroutines to finish, logging a "still waiting" line every 5 seconds so the
// user can see the wait is progressing during a long graceful shutdown. If
// the timeout fires it logs and returns so shutdown can proceed instead of
// hanging forever on a stuck background goroutine.
func (s *Server) waitForLifecycleWithProgress(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.lifecycleActive.Wait()
		close(done)
	}()

	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-done:
			return
		case <-progress.C:
			remaining := time.Until(deadline).Round(time.Second)
			inFlight := s.lifecycleActive.InFlight()
			log.Printf("[SHUTDOWN] still waiting for %d in-flight goroutine(s) to finish (%v remaining)", inFlight, remaining)
		case <-time.After(time.Until(deadline)):
			if time.Now().Before(deadline) {
				continue
			}
			inFlight := s.lifecycleActive.InFlight()
			log.Printf("[SHUTDOWN] lifecycle wait timed out after %v, proceeding (%d goroutine(s) may not have finished)", timeout, inFlight)
			return
		}
	}
}

// ShutdownAndExit performs a graceful shutdown then exits the process.
// Used by signal handlers and the API shutdown endpoint where the process
// must terminate after cleanup.
func (s *Server) ShutdownAndExit() {
	s.Shutdown()
	os.Exit(0)
}

// handleIndex serves the main HTML page
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	setSecurityHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

// handleStatic serves static CSS/JS files
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/static/")
	setSecurityHeaders(w)

	switch path {
	case "style.css":
		w.Header().Set("Content-Type", "text/css")
		w.Write([]byte(styleCSS))
	case "app.js":
		w.Header().Set("Content-Type", "application/javascript")
		w.Write([]byte(appJS))
	default:
		http.NotFound(w, r)
	}
}

// handleListSeries returns all series
func (s *Server) handleListSeries(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	seriesList := make([]*models.Series, 0, len(s.series))
	for _, s := range s.series {
		seriesList = append(seriesList, s)
	}
	s.mu.RUnlock()

	// Sort alphabetically by title for consistent ordering
	sort.Slice(seriesList, func(i, j int) bool {
		titleI := seriesList[i].CustomName
		if titleI == "" {
			titleI = seriesList[i].Title
		}
		titleJ := seriesList[j].CustomName
		if titleJ == "" {
			titleJ = seriesList[j].Title
		}
		return strings.ToLower(titleI) < strings.ToLower(titleJ)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(seriesList)
}

// updateRegistryEntry safely updates a registry entry by ID with mutex protection.
// It acquires s.mu, finds the entry, applies the update function, saves, and returns.
// Returns false if the entry was not found.
func (s *Server) updateRegistryEntry(seriesID string, updateFn func(*RegistryEntry)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registry == nil {
		log.Printf("Warning: updateRegistryEntry called with nil registry")
		return false
	}
	for i := range s.registry.Series {
		if s.registry.Series[i].ID == seriesID {
			prior := s.registry.Series[i]
			updateFn(&s.registry.Series[i])
			// RegistryEntry is fully comparable (strings, ints, time.Time), so a
			// direct == check detects no-op updates and skips the disk write.
			if s.registry.Series[i] != prior {
				s.saveRegistry(s.registry)
			}
			return true
		}
	}
	return false
}

// deleteRegistryEntry safely removes a registry entry by ID with mutex protection.
func (s *Server) deleteRegistryEntry(seriesID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, entry := range s.registry.Series {
		if entry.ID == seriesID {
			s.registry.Series = append(s.registry.Series[:i], s.registry.Series[i+1:]...)
			s.saveRegistry(s.registry)
			return true
		}
	}
	return false
}

// appendRegistryEntry safely appends a new entry to the registry with mutex protection.
func (s *Server) appendRegistryEntry(entry RegistryEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registry.Series = append(s.registry.Series, entry)
	return s.saveRegistry(s.registry)
}

// handleAddSeries adds a new series
func (s *Server) handleAddSeries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL           string `json:"url"`
		CustomName    string `json:"customName,omitempty"`
		CheckInterval string `json:"checkInterval,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.URL == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	// Reject non-http(s), loopback/private hosts, and userinfo before any
	// scraper substring-matches a hostile URL and drives the HTTP client /
	// Playwright browser at an internal service.
	if err := scraper.ValidateSeriesURL(req.URL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate check interval if provided
	if req.CheckInterval != "" && req.CheckInterval != "never" {
		_, ok := models.ParseCheckInterval(req.CheckInterval)
		if !ok {
			http.Error(w, "Invalid check interval. Valid values: never, 5m, 15m, 30m, 1h, 6h, 12h, 24h", http.StatusBadRequest)
			return
		}
	}

	// Find appropriate scraper
	scr := s.scraperReg.GetScraper(req.URL)
	if scr == nil {
		http.Error(w, "No scraper available for this URL", http.StatusBadRequest)
		return
	}

	// Fetch series page
	html, err := scraper.FetchHTML(scr, s.httpClient, req.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch series: %v", err), http.StatusInternalServerError)
		return
	}

	// Inject Cloudflare cookies from browser session
	scraper.SetCookiesOnHttpClient(scr, s.httpClient, req.URL)

	// Extract chapters
	chapters, err := scr.ExtractChapters(html, req.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to extract chapters: %v", err), http.StatusInternalServerError)
		return
	}

	// Determine folder name (use custom name if provided, otherwise generate from URL/Title).
	// CustomName is user-controlled, so it MUST be sanitized into a single safe path
	// component before being joined with the download root; otherwise "..", path
	// separators, or trailing dots could escape the download dir or collide with
	// other series folders. The scraper-name fallback is sanitized the same way
	// for consistency.
	folderName := req.CustomName
	if folderName == "" {
		folderName = fileutil.SanitizeFolderName(scr.Name())
	} else {
		folderName = fileutil.SanitizeFolderName(folderName)
	}

	// If the folder already exists, reuse it (do NOT create a new suffixed
	// subdirectory). This lets the user re-add a series that was previously
	// removed or discovered from disk, downloading into the existing folder.
	seriesPath := filepath.Join(s.config.DownloadPath, folderName)
	if _, err := os.Stat(seriesPath); err == nil {
		log.Printf("Folder '%s' already exists, reusing it for new series", folderName)
	}

	// Persist series metadata and cover before scheduling downloads.
	// The series folder must exist so series-info.json and the cover can be
	// written even if no chapters have been downloaded yet.
	if err := s.saveSeriesMetadata(folderName, scr, html); err != nil {
		log.Printf("Warning: Failed to save series metadata: %v", err)
	}

	// Reject duplicates: if the series is already tracked by URL OR by folder
	// name, don't add a second registry entry. Reusing an existing folder is
	// allowed only when no other tracked series owns that folder, otherwise
	// two different-URL series sanitizing to the same folder would overwrite
	// each other's metadata and state. The check is done under the write lock
	// to avoid a TOCTOU race with concurrent adds.
	s.mu.Lock()
	for _, ser := range s.series {
		if ser.URL == req.URL {
			s.mu.Unlock()
			http.Error(w, "Series already added", http.StatusConflict)
			return
		}
	}
	for _, e := range s.registry.Series {
		if e.FolderName == folderName {
			s.mu.Unlock()
			http.Error(w, "A series with this folder name already exists", http.StatusConflict)
			return
		}
	}
	s.mu.Unlock()

	// Create series ID
	seriesID := uuid.New().String()
	now := time.Now()

	// Add to central registry. CustomName is stored sanitized so every code
	// path that uses series.CustomName as a folder name (downloads, force
	// redownload, scan-missing) joins a safe single component with the
	// download root.
	regEntry := RegistryEntry{
		ID:                 seriesID,
		FolderName:         folderName,
		Title:              scr.Name(),
		CustomName:         folderName,
		URL:                req.URL,
		CheckInterval:      req.CheckInterval,
		LastCheckedAt:      now,
		ChapterCount:       len(chapters),
		ChaptersDownloaded: 0,
		AddedAt:            now,
		UpdatedAt:          now,
	}
	if err := s.appendRegistryEntry(regEntry); err != nil {
		http.Error(w, fmt.Sprintf("Failed to save registry: %v", err), http.StatusInternalServerError)
		return
	}

	// Create chapter state file in series folder
	chapterState := &ChapterStateFile{
		SeriesID:   seriesID,
		FolderName: folderName,
		URL:        req.URL,
		LastSynced: now,
		Chapters:   make(map[string]ChapterInfo),
	}

	for i, ch := range chapters {
		ch.ID = fmt.Sprintf("ch-%s-%d", seriesID[:8], i)
		chapters[i].ID = ch.ID

		chapterState.Chapters[strconv.FormatFloat(ch.Number, 'f', -1, 64)] = ChapterInfo{
			Number:     ch.Number,
			Title:      ch.Title,
			URL:        ch.URL,
			Downloaded: false,
		}
	}

	// Save chapter state file (folder will be created on first download)
	if err := s.saveChapterState(folderName, chapterState); err != nil {
		log.Printf("Warning: Failed to save initial chapter state: %v", err)
	}

	// Create in-memory series object
	series := &models.Series{
		ID:                 seriesID,
		Title:              regEntry.Title,
		CustomName:         regEntry.CustomName,
		URL:                regEntry.URL,
		ChapterCount:       len(chapters),
		ChaptersDownloaded: 0,
		CheckInterval:      regEntry.CheckInterval,
		LastCheckedAt:      regEntry.LastCheckedAt,
		CreatedAt:          regEntry.AddedAt,
		UpdatedAt:          regEntry.UpdatedAt,
	}

	// Convert chapters to pointers
	chapterPtrs := make([]*models.Chapter, len(chapters))
	for i := range chapters {
		chapterPtrs[i] = &chapters[i]
	}

	// Store series and chapters in memory
	s.mu.Lock()
	s.series[series.ID] = series
	s.chapters[series.ID] = chapterPtrs
	s.mu.Unlock()

	// Automatically start downloading all chapters for the newly added series
	// unless global manga scraping is paused. The add itself is a manual
	// action, but honoring the pause keeps the UI state consistent.
	if !s.isMangaScrapingPaused() {
		s.safeGoTrack("autoDownloadNewSeries", func() {
			s.autoDownloadMissingChapters(seriesID, true)
		})
	} else {
		log.Printf("[SCRAPING] Manga scraping paused, skipping auto-download for newly added series %s", seriesID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(series)
}

// saveSeriesMetadata writes series-info.json and downloads the cover image.
// Failures are logged but not returned as errors to the caller.
func (s *Server) saveSeriesMetadata(folderName string, scr scraper.Scraper, html string) error {
	info := models.SeriesInfo{Source: scr.Name()}
	if sip, ok := scr.(scraper.SeriesInfoProvider); ok {
		extracted, err := sip.ExtractSeriesInfo(html, "")
		if err != nil {
			log.Printf("Warning: metadata extraction failed: %v", err)
		} else {
			info = extracted
		}
	}
	if info.Title == "" {
		info.Title = folderName
	}

	seriesPath := filepath.Join(s.config.DownloadPath, folderName)
	if err := os.MkdirAll(seriesPath, 0755); err != nil {
		return fmt.Errorf("failed to create series folder: %w", err)
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
		scraper.SetCookiesOnHttpClient(scr, s.httpClient, info.CoverURL)
		coverPath := filepath.Join(seriesPath, "cover")
		if err := s.dlManager.FileOps().SaveCoverImage(info.CoverURL, coverPath); err != nil {
			log.Printf("Warning: failed to save cover image: %v", err)
		}
	}

	return nil
}

// loadSeriesMetadata reads series-info.json from a series folder, if present.
func (s *Server) loadSeriesMetadata(folderName string) (*models.SeriesInfo, error) {
	infoPath := filepath.Join(s.config.DownloadPath, folderName, "series-info.json")
	data, err := os.ReadFile(infoPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var info models.SeriesInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal series info: %w", err)
	}
	return &info, nil
}

// handleDeleteSeries removes a series from the library
func (s *Server) handleDeleteSeries(w http.ResponseWriter, r *http.Request, seriesID string) {
	s.mu.Lock()
	series, ok := s.series[seriesID]
	delete(s.series, seriesID)
	delete(s.chapters, seriesID)
	s.mu.Unlock()

	if !ok || series == nil {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	if s.registry != nil {
		// Remove from registry
		s.deleteRegistryEntry(seriesID)

		// Note: We don't delete the chapter state file (.chapters.json)
		// or downloaded files - user may want to keep them
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

// handleUpdateSeries updates series metadata
func (s *Server) handleUpdateSeries(w http.ResponseWriter, r *http.Request, seriesID string) {
	var req struct {
		CustomName string `json:"customName,omitempty"`
		URL        string `json:"url,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate a new URL the same way handleAddSeries does, before it is
	// persisted and used to drive the HTTP client / Playwright browser.
	if req.URL != "" {
		if err := scraper.ValidateSeriesURL(req.URL); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	s.mu.Lock()
	series, ok := s.series[seriesID]
	s.mu.Unlock()

	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	// Update registry entry (callback runs under s.mu)
	needRefresh := false
	s.updateRegistryEntry(seriesID, func(e *RegistryEntry) {
		if req.CustomName != "" {
			safeName := fileutil.SanitizeFolderName(req.CustomName)
			e.CustomName = safeName
			series.CustomName = safeName
			// Do not clobber the scraped Title with the folder name; Title
			// stays the site title while CustomName is the display/folder name.
		}
		if req.URL != "" && req.URL != series.URL {
			e.URL = req.URL
			series.URL = req.URL
			needRefresh = true
		}
		e.UpdatedAt = time.Now()
		series.UpdatedAt = time.Now()
	})

	// Refresh chapters outside the registry lock if the URL changed
	if needRefresh {
		s.safeGoTrack("refreshSeriesOnUpdate", func() {
			newChapters := s.refreshSeriesChapters(series)
			if newChapters > 0 {
				s.autoDownloadMissingChapters(seriesID, true)
			}
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(series)
}

// refreshSeriesChapters fetches chapters from the series URL and updates state.
// Returns the number of newly discovered chapters (not previously in the state file).
func (s *Server) refreshSeriesChapters(series *models.Series) int {
	if series.URL == "" {
		return 0
	}

	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		log.Printf("No scraper for URL %s", series.URL)
		return 0
	}

	html, err := scraper.FetchHTML(scr, s.httpClient, series.URL)
	if err != nil {
		log.Printf("Failed to fetch series %s: %v", series.ID, err)
		return 0
	}

	scraper.SetCookiesOnHttpClient(scr, s.httpClient, series.URL)

	chapters, err := scr.ExtractChapters(html, series.URL)
	if err != nil {
		log.Printf("Failed to extract chapters for %s: %v", series.ID, err)
		return 0
	}

	// Update chapters in memory
	chapterPtrs := make([]*models.Chapter, len(chapters))
	for i := range chapters {
		chapters[i].ID = fmt.Sprintf("ch-%s-%d", series.ID[:8], i)
		chapterPtrs[i] = &chapters[i]
	}

	now := time.Now()
	s.mu.Lock()
	s.chapters[series.ID] = chapterPtrs
	// Update the in-memory series map entry (not the local copy pointer)
	if entry, ok := s.series[series.ID]; ok {
		entry.ChapterCount = len(chapters)
		entry.LastCheckedAt = now
	}
	s.mu.Unlock()

	// Update registry
	if !s.updateRegistryEntry(series.ID, func(e *RegistryEntry) {
		e.ChapterCount = len(chapters)
		e.LastCheckedAt = now
		e.UpdatedAt = now
	}) {
		log.Printf("Failed to find series %s in registry for update", series.ID)
	}

	// Get folder name for chapter state
	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	// Load or create chapter state (under lock to prevent race with concurrent
	// updateChapterState or autoDownloadMissingChapters saves)
	s.chapterStateMu.Lock()
	chapterState, _ := s.loadChapterStateLocked(folderName)
	if chapterState == nil {
		chapterState = &ChapterStateFile{
			SeriesID:   series.ID,
			FolderName: folderName,
			URL:        series.URL,
			Chapters:   make(map[string]ChapterInfo),
		}
	}

	// Always update metadata
	chapterState.URL = series.URL
	chapterState.LastSynced = now

	// Add new chapters, preserve existing download status
	newChapterCount := 0
	for _, ch := range chapters {
		key := strconv.FormatFloat(ch.Number, 'f', -1, 64)
		if existing, exists := chapterState.Chapters[key]; exists {
			// Preserve downloaded status
			existing.Number = ch.Number
			existing.Title = ch.Title
			existing.URL = ch.URL
			chapterState.Chapters[key] = existing
		} else {
			// New chapter
			newChapterCount++
			chapterState.Chapters[key] = ChapterInfo{
				Number:     ch.Number,
				Title:      ch.Title,
				URL:        ch.URL,
				Downloaded: false,
			}
		}
	}

	if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
		log.Printf("Failed to save chapter state for %s: %v", series.ID, err)
	}
	s.chapterStateMu.Unlock()

	if newChapterCount > 0 {
		seriesName := series.CustomName
		if seriesName == "" {
			seriesName = series.Title
		}
		log.Printf("Found %d new chapters for series %s", newChapterCount, seriesName)
	}

	log.Printf("Refreshed %d chapters for series %s (%d new)", len(chapters), series.ID, newChapterCount)
	return newChapterCount
}

// handleGetChapters returns chapters for a series
func (s *Server) handleGetChapters(w http.ResponseWriter, r *http.Request) {
	// Extract series ID from path: /api/series/{id}/chapters
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	s.mu.RLock()
	chapters, ok := s.chapters[seriesID]
	s.mu.RUnlock()

	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chapters)
}

// handleSyncSeries triggers auto-download of missing chapters
func (s *Server) handleSyncSeries(w http.ResponseWriter, r *http.Request) {
	// Extract series ID from path: /api/series/{id}/sync
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	s.mu.RLock()
	_, ok := s.series[seriesID]
	s.mu.RUnlock()

	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	// Start auto-download in background. Manual sync is allowed while paused
	// (documented behavior), so pass manual=true to bypass the pause check.
	s.safeGoTrack("manualSyncSeries", func() {
		s.autoDownloadMissingChapters(seriesID, false, true)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "Auto-download started",
		"seriesId": seriesID,
		"status":   "syncing",
	})
}

// handleSyncAllSeries triggers auto-download of missing chapters for every
// tracked series. Each series is dispatched to its own goroutine so a slow
// download does not block the others.
func (s *Server) handleSyncAllSeries(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	seriesList := make([]*models.Series, 0, len(s.series))
	for _, series := range s.series {
		seriesList = append(seriesList, series)
	}
	s.mu.RUnlock()

	dispatchedCount := 0
	for _, series := range seriesList {
		if series.URL == "" {
			continue
		}
		seriesID := series.ID
		s.safeGoTrack("syncAllSeries", func() {
			s.autoDownloadMissingChapters(seriesID, false, true)
		})
		dispatchedCount++
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"message": "Syncing all series",
		"status":  "syncing",
		"count":   dispatchedCount,
	})
}

// handleCheckNow triggers a manual check for updates
func (s *Server) handleCheckNow(w http.ResponseWriter, r *http.Request) {
	// Extract series ID from path: /api/series/{id}/check-now
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	s.mu.RLock()
	series, ok := s.series[seriesID]
	s.mu.RUnlock()

	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	if series.URL == "" {
		http.Error(w, "Series has no URL configured", http.StatusBadRequest)
		return
	}

	// Documented pause semantics: pause stops auto-checks, recheck-all and
	// check-now, but manual sync/force-redownload/scan-missing still work.
	if s.isMangaScrapingPaused() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "paused", "message": "Manga scraping is paused"})
		return
	}

	// Dedup concurrent checks for the same series (same as the scheduler path).
	s.mu.Lock()
	if s.checking[seriesID] {
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message":  "Update check already running",
			"seriesId": seriesID,
			"status":   "checking",
		})
		return
	}
	s.checking[seriesID] = true
	s.mu.Unlock()

	// Trigger refresh in background, and auto-download if new chapters found.
	// Pass the series by value snapshot: the background goroutine must not
	// read the shared pointer while handleUpdateSeries mutates it.
	s.safeGoTrack("handleCheckNow", func() {
		defer func() {
			s.mu.Lock()
			delete(s.checking, seriesID)
			s.mu.Unlock()
		}()
		seriesCopy := *series
		newChapters := s.refreshSeriesChapters(&seriesCopy)
		if newChapters > 0 {
			s.autoDownloadMissingChapters(seriesID, true)
		}
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "Update check started",
		"seriesId": seriesID,
		"status":   "checking",
	})
}

// handleRefreshMetadata fetches fresh series metadata and overwrites
// series-info.json and the cover image in the series folder.
func (s *Server) handleRefreshMetadata(w http.ResponseWriter, r *http.Request) {
	// Extract series ID from path: /api/series/{id}/refresh-metadata
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	seriesID := parts[3]

	s.mu.RLock()
	series, ok := s.series[seriesID]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	if series.URL == "" {
		http.Error(w, "Series has no URL configured", http.StatusBadRequest)
		return
	}

	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		http.Error(w, "No scraper available", http.StatusInternalServerError)
		return
	}

	html, err := scraper.FetchHTML(scr, s.httpClient, series.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch series: %v", err), http.StatusInternalServerError)
		return
	}

	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	if err := s.saveSeriesMetadata(folderName, scr, html); err != nil {
		log.Printf("[REFRESH-METADATA] Failed to save metadata for %s: %v", seriesID, err)
		http.Error(w, fmt.Sprintf("Failed to save metadata: %v", err), http.StatusInternalServerError)
		return
	}

	info, _ := s.loadSeriesMetadata(folderName)
	if info == nil {
		info = &models.SeriesInfo{Source: scr.Name()}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// handleUpdateCheckInterval updates the check frequency for a series
func (s *Server) handleUpdateCheckInterval(w http.ResponseWriter, r *http.Request) {
	// Extract series ID from path: /api/series/{id}/check-interval
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	var req struct {
		CheckInterval string `json:"checkInterval"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate interval
	validIntervals := []string{"", "never", "5m", "15m", "30m", "1h", "6h", "12h", "24h"}
	isValid := false
	for _, v := range validIntervals {
		if req.CheckInterval == v {
			isValid = true
			break
		}
	}
	if !isValid {
		http.Error(w, "Invalid check interval. Valid values: never, 5m, 15m, 30m, 1h, 6h, 12h, 24h", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	series, ok := s.series[seriesID]
	if ok {
		series.CheckInterval = req.CheckInterval
		series.UpdatedAt = time.Now()
	}
	s.mu.Unlock()

	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	// Update registry
	s.updateRegistryEntry(seriesID, func(e *RegistryEntry) {
		e.CheckInterval = req.CheckInterval
		e.UpdatedAt = time.Now()
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Check interval updated",
		"seriesId":      seriesID,
		"checkInterval": req.CheckInterval,
	})
}

// handleBulkCheckInterval sets the auto-check interval for every series that
// is not already set to "never". Series set to never (manual only) are
// deliberately ignored so a bulk change cannot re-enable auto-checking for
// items the user explicitly opted out of. Returns how many were updated.
func (s *Server) handleBulkCheckInterval(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CheckInterval string `json:"checkInterval"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if _, ok := models.ParseCheckInterval(req.CheckInterval); !ok {
		http.Error(w, "Invalid check interval", http.StatusBadRequest)
		return
	}
	updated := 0
	s.mu.Lock()
	for i := range s.registry.Series {
		e := &s.registry.Series[i]
		if e.CheckInterval == "never" {
			continue // user opted out; bulk change must not re-enable
		}
		if e.CheckInterval != req.CheckInterval {
			e.CheckInterval = req.CheckInterval
			e.UpdatedAt = time.Now()
			// Mirror to the in-memory series map so the UI dropdowns agree.
			if series, ok := s.series[e.ID]; ok {
				series.CheckInterval = req.CheckInterval
			}
			updated++
		}
	}
	if updated > 0 {
		s.saveRegistry(s.registry)
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Bulk check interval updated",
		"updated":       updated,
		"checkInterval": req.CheckInterval,
	})
}

// handleGetState returns the download state for a series (using new format)
func (s *Server) handleGetState(w http.ResponseWriter, r *http.Request) {
	// Extract series ID from path: /api/series/{id}/state
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	s.mu.RLock()
	series, ok := s.series[seriesID]
	s.mu.RUnlock()

	if !ok {
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	// Load chapter state file
	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}
	s.chapterStateMu.Lock()
	chapterState, err := s.loadChapterStateLocked(folderName)
	if err != nil {
		s.chapterStateMu.Unlock()
		http.Error(w, fmt.Sprintf("Failed to load chapter state: %v", err), http.StatusInternalServerError)
		return
	}
	if chapterState == nil {
		// Return empty state if file doesn't exist
		chapterState = &ChapterStateFile{
			SeriesID:   seriesID,
			FolderName: folderName,
			URL:        series.URL,
			Chapters:   make(map[string]ChapterInfo),
		}
	} else {
		// Snapshot-copy so the JSON encode below reads a stable map even
		// while writers mutate the cached original.
		cp := *chapterState
		cp.Chapters = make(map[string]ChapterInfo, len(chapterState.Chapters))
		for k, v := range chapterState.Chapters {
			v.PerImageStatus = append([]models.ImageResult(nil), v.PerImageStatus...)
			cp.Chapters[k] = v
		}
		chapterState = &cp
	}
	s.chapterStateMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chapterState)
}

// handleScanMissing scans for missing/incomplete images in downloaded chapters and re-downloads them
func (s *Server) handleScanMissing(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	s.mu.RLock()
	series, ok := s.series[seriesID]
	if !ok {
		s.mu.RUnlock()
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}
	chapters := s.chapters[seriesID]
	s.mu.RUnlock()

	if series.URL == "" {
		http.Error(w, "Series has no URL configured", http.StatusBadRequest)
		return
	}

	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		http.Error(w, "No scraper available", http.StatusInternalServerError)
		return
	}

	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	s.chapterStateMu.Lock()
	chapterState, err := s.loadChapterStateLocked(folderName)
	if err != nil || chapterState == nil {
		s.chapterStateMu.Unlock()
		http.Error(w, "No chapter state found", http.StatusNotFound)
		return
	}

	// Snapshot the scan plan under the lock, then release it: the fetches and
	// downloads below are long-running network operations and must not block
	// concurrent chapter-state writers. The state file is re-merged on save.
	type missingChapter struct {
		key           string
		chapter       *models.Chapter
		existingFiles int
	}
	var work []missingChapter

	// Scan for missing/incomplete images
	missingCount := 0
	redownloadedCount := 0

	// Iterate chapters in sorted order (ascending by chapter number) for deterministic processing
	sortedChapterKeys := make([]string, 0, len(chapterState.Chapters))
	for chapterKey := range chapterState.Chapters {
		sortedChapterKeys = append(sortedChapterKeys, chapterKey)
	}
	sort.Slice(sortedChapterKeys, func(i, j int) bool {
		ni, errI := strconv.ParseFloat(sortedChapterKeys[i], 64)
		nj, errJ := strconv.ParseFloat(sortedChapterKeys[j], 64)
		if errI == nil && errJ == nil {
			return ni < nj
		}
		return sortedChapterKeys[i] < sortedChapterKeys[j]
	})

	for _, chapterKey := range sortedChapterKeys {
		chInfo := chapterState.Chapters[chapterKey]
		if !chInfo.Downloaded {
			continue
		}

		chapterDir := filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)
		entries, err := os.ReadDir(chapterDir)
		if err != nil {
			continue
		}

		existingFiles := 0
		for _, e := range entries {
			if !e.IsDir() {
				existingFiles++
			}
		}

		// Check if we know the expected image count
		if chInfo.ImageCount > 0 && existingFiles < chInfo.ImageCount {
			// Check per-image status: only redownload when a *missing* image
			// (absent on disk) had a retryable failure. If every missing image
			// was intentionally filtered, don't redownload.
			hasRetryableMissing := false
			if len(chInfo.PerImageStatus) > 0 {
				for _, imgStatus := range chInfo.PerImageStatus {
					if imgStatus.Status != models.ImageStatusFailed {
						continue
					}
					// Retryable failure — is the file actually missing on disk?
					expectedFile := fmt.Sprintf("%03d", imgStatus.Page)
					found := false
					for _, e := range entries {
						if !e.IsDir() && strings.HasPrefix(e.Name(), expectedFile) {
							found = true
							break
						}
					}
					if !found {
						hasRetryableMissing = true
						break
					}
				}
			} else {
				// No per-image status recorded (old download before this feature) —
				// assume missing images are retryable failures.
				hasRetryableMissing = true
			}
			if !hasRetryableMissing {
				log.Printf("[SCAN-MISSING] Chapter %s has %d/%d images but all missing were filtered, skipping", chapterKey, existingFiles, chInfo.ImageCount)
				continue
			}
			missingCount++
			log.Printf("[SCAN-MISSING] Chapter %s has %d/%d images, re-downloading", chapterKey, existingFiles, chInfo.ImageCount)

			// Find the chapter in our list
			var chapter *models.Chapter
			for _, ch := range chapters {
				chNum := strconv.FormatFloat(ch.Number, 'f', -1, 64)
				if chNum == chapterKey {
					chapter = ch
					break
				}
			}

			if chapter == nil {
				continue
			}

			work = append(work, missingChapter{key: chapterKey, chapter: chapter, existingFiles: existingFiles})
		}
	}

	// Long network work happens without the state lock held.
	s.chapterStateMu.Unlock()

	for _, item := range work {
		chapterKey := item.key
		chapter := item.chapter
		existingFiles := item.existingFiles
		chInfo := chapterState.Chapters[chapterKey]

		// Fetch and extract images
		html, err := scraper.FetchHTML(scr, s.httpClient, chapter.URL)
		if err != nil {
			log.Printf("[SCAN-MISSING] Failed to fetch chapter %s: %v", chapterKey, err)
			continue
		}

		scraper.SetCookiesOnHttpClient(scr, s.httpClient, chapter.URL)

		images, err := s.extractImagesWithValidation(scr, html, chapter.URL)
		if err != nil {
			log.Printf("[SCAN-MISSING] Failed to extract images for %s: %v", chapterKey, err)
			continue
		}

		if len(images) <= existingFiles {
			// We don't have more images than what's on disk
			continue
		}

		// Delete existing incomplete files and re-download
		if entries, err := os.ReadDir(filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)); err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					os.Remove(filepath.Join(filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey), e.Name()))
				}
			}
		}

		dl := models.NewDownload(series.ID, chapter.ID, series.Title, chapter.Title)
		dl.ID = uuid.New().String()
		dl.CustomName = series.CustomName

		s.mu.Lock()
		s.downloads[dl.ID] = dl
		s.mu.Unlock()

		task := &download.Task{
			ID:            dl.ID,
			SeriesID:      series.ID,
			ChapterID:     chapter.ID,
			SeriesTitle:   series.Title,
			CustomName:    series.CustomName,
			ChapterTitle:  chapter.Title,
			ChapterNumber: chapter.Number,
			URL:           chapter.URL,
			Images:        images,
		}

		s.dlManager.DownloadChapter(task, dl)
		redownloadedCount++

		// Update state via the shared completion helper: only mark downloaded
		// when the download actually succeeded (or all remaining images were
		// filtered), otherwise the chapter would be retried forever.
		if isDownloadComplete(dl) {
			if dl.Status == models.StatusCompleted {
				if actualFiles := countFilesInDir(filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)); actualFiles == 0 {
					log.Printf("[SCAN-MISSING] Chapter %s reported completed but folder has 0 files, marking as partial", chapterKey)
					dl.Status = models.StatusPartial
				}
			}
			chInfo.Downloaded = true
			if actualFiles := countFilesInDir(filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)); actualFiles > 0 {
				chInfo.ImageCount = actualFiles
			} else {
				chInfo.ImageCount = dl.DownloadedCount
			}
		} else {
			log.Printf("[SCAN-MISSING] Download failed for chapter %s (status: %s), not marking as downloaded", chapterKey, dl.Status)
			chInfo.Downloaded = false
			chInfo.ImageCount = 0
		}
		if dl.PerImageStatus != nil {
			chInfo.PerImageStatus = dl.PerImageStatus
		}
		chInfo.DownloadedAt = time.Now()
		chapterState.Chapters[chapterKey] = chInfo
	}

	s.chapterStateMu.Lock()
	// Re-load under lock to merge any concurrent writes before saving.
	if latestState, loadErr := s.loadChapterStateLocked(folderName); loadErr == nil && latestState != nil {
		for chapterKey, chInfo := range chapterState.Chapters {
			latestState.Chapters[chapterKey] = chInfo
		}
		chapterState = latestState
	}
	if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
		log.Printf("[SCAN-MISSING] Failed to save chapter state: %v", err)
	}
	s.chapterStateMu.Unlock()

	// Update series stats
	s.mu.Lock()
	if series, ok := s.series[seriesID]; ok {
		series.UpdatedAt = time.Now()
	}
	s.mu.Unlock()

	s.updateRegistryEntry(seriesID, func(e *RegistryEntry) {
		e.UpdatedAt = time.Now()
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":              fmt.Sprintf("Found %d incomplete chapters, redownloading %d", missingCount, redownloadedCount),
		"missingChapters":      missingCount,
		"redownloadedChapters": redownloadedCount,
	})
}

// handleForceRedownload deletes and re-downloads chapters in a specified range
func (s *Server) handleForceRedownload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	var req struct {
		FromChapter float64 `json:"fromChapter"`
		ToChapter   float64 `json:"toChapter"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	series, ok := s.series[seriesID]
	if !ok {
		s.mu.RUnlock()
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}
	s.mu.RUnlock()

	if series.URL == "" {
		http.Error(w, "Series has no URL configured", http.StatusBadRequest)
		return
	}

	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		http.Error(w, "No scraper available", http.StatusInternalServerError)
		return
	}

	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	// Refresh chapter list synchronously, then hand the actual deletions and
	// re-downloads off to a background goroutine tracked for safe shutdown.
	s.refreshSeriesChapters(series)

	s.mu.RLock()
	series, ok = s.series[seriesID]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Series not found after refresh", http.StatusNotFound)
		return
	}

	s.chapterStateMu.Lock()
	chapterState, err := s.loadChapterStateLocked(folderName)
	if err != nil || chapterState == nil {
		s.chapterStateMu.Unlock()
		http.Error(w, "No chapter state found", http.StatusNotFound)
		return
	}

	// Find chapters in the specified range, mark for redownload.
	toRedownload := make([]string, 0)
	for chapterKey, chInfo := range chapterState.Chapters {
		num := chInfo.Number
		if num >= req.FromChapter && num <= req.ToChapter {
			toRedownload = append(toRedownload, chapterKey)
			chapterDir := filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)
			os.RemoveAll(chapterDir)
			chInfo.Downloaded = false
			chInfo.ImageCount = 0
			chInfo.DownloadedAt = time.Time{}
			chInfo.PerImageStatus = nil
			chapterState.Chapters[chapterKey] = chInfo
		}
	}

	if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
		log.Printf("[FORCE-REDOWNLOAD] Failed to save chapter state before redownload: %v", err)
	}
	s.chapterStateMu.Unlock()

	// Sort range chapters ascending for proper download order.
	sort.Slice(toRedownload, func(i, j int) bool {
		ni, _ := strconv.ParseFloat(toRedownload[i], 64)
		nj, _ := strconv.ParseFloat(toRedownload[j], 64)
		return ni < nj
	})

	s.safeGoTrack("forceRedownload", func() {
		s.forceRedownloadChapters(seriesID, folderName, scr, toRedownload, req.FromChapter, req.ToChapter)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":              fmt.Sprintf("Force redownload started for %d chapters (range: %.1f - %.1f)", len(toRedownload), req.FromChapter, req.ToChapter),
		"redownloadedChapters": len(toRedownload),
		"status":               "redownloading",
	})
}

// forceRedownloadChapters performs the actual chapter deletion + redownload for
// a range. It runs inside a safeGoTrack goroutine.
func (s *Server) forceRedownloadChapters(seriesID, folderName string, scr scraper.Scraper, toRedownload []string, fromChapter, toChapter float64) {
	s.mu.RLock()
	series, ok := s.series[seriesID]
	if !ok {
		s.mu.RUnlock()
		return
	}
	chapters := s.chapters[seriesID]
	s.mu.RUnlock()

	redownloadedCount := 0
	// Load the state briefly (no long lock hold): fetches and full chapter
	// downloads below are long-running and must not block concurrent
	// chapter-state writers.
	var chapterState *ChapterStateFile
	s.chapterStateMu.Lock()
	if st, err := s.loadChapterStateLocked(folderName); err == nil && st != nil {
		chapterState = st
	}
	s.chapterStateMu.Unlock()
	if chapterState == nil {
		return
	}

	for _, chapterKey := range toRedownload {
		chInfo := chapterState.Chapters[chapterKey]

		var chapter *models.Chapter
		for _, ch := range chapters {
			chNum := strconv.FormatFloat(ch.Number, 'f', -1, 64)
			if chNum == chapterKey {
				chapter = ch
				break
			}
		}
		if chapter == nil {
			log.Printf("[FORCE-REDOWNLOAD] Chapter %s not found in memory", chapterKey)
			continue
		}

		html, err := scraper.FetchHTML(scr, s.httpClient, chapter.URL)
		if err != nil {
			log.Printf("[FORCE-REDOWNLOAD] Failed to fetch chapter %s: %v", chapterKey, err)
			continue
		}
		scraper.SetCookiesOnHttpClient(scr, s.httpClient, chapter.URL)

		images, err := s.extractImagesWithValidation(scr, html, chapter.URL)
		if err != nil {
			log.Printf("[FORCE-REDOWNLOAD] Failed to extract images for %s: %v", chapterKey, err)
			continue
		}
		if len(images) == 0 {
			continue
		}

		dl := models.NewDownload(series.ID, chapter.ID, series.Title, chapter.Title)
		dl.ID = uuid.New().String()
		dl.CustomName = series.CustomName

		s.mu.Lock()
		s.downloads[dl.ID] = dl
		s.mu.Unlock()

		task := &download.Task{
			ID:            dl.ID,
			SeriesID:      series.ID,
			ChapterID:     chapter.ID,
			SeriesTitle:   series.Title,
			CustomName:    series.CustomName,
			ChapterTitle:  chapter.Title,
			ChapterNumber: chapter.Number,
			URL:           chapter.URL,
			Images:        images,
		}

		s.dlManager.DownloadChapter(task, dl)
		redownloadedCount++

		chInfo.Downloaded = (dl.Status == models.StatusCompleted) || isAllFiltered(dl.PerImageStatus)
		if chInfo.Downloaded {
			if actualFiles := countFilesInDir(filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)); actualFiles > 0 {
				chInfo.ImageCount = actualFiles
			} else {
				chInfo.ImageCount = dl.DownloadedCount
			}
		} else {
			chInfo.ImageCount = 0
		}
		chInfo.DownloadedAt = time.Now()
		if dl.PerImageStatus != nil {
			chInfo.PerImageStatus = dl.PerImageStatus
		}

		s.chapterStateMu.Lock()
		// Re-load under lock to merge concurrent writes before saving.
		latestState, loadErr := s.loadChapterStateLocked(folderName)
		if loadErr == nil && latestState != nil {
			latestState.Chapters[chapterKey] = chInfo
			chapterState = latestState
		}
		if saveErr := s.saveChapterStateLocked(folderName, chapterState); saveErr != nil {
			log.Printf("[FORCE-REDOWNLOAD] Failed to save chapter state after %s: %v", chapterKey, saveErr)
		}
		s.chapterStateMu.Unlock()
	}

	// Update series stats. chapterState may alias the cached object after the
	// merge above, so count totals while chapterStateMu is held.
	totalDownloaded := 0
	s.chapterStateMu.Lock()
	for _, c := range chapterState.Chapters {
		if c.Downloaded {
			totalDownloaded++
		}
	}
	s.chapterStateMu.Unlock()
	s.mu.Lock()
	if sEntry, ok := s.series[seriesID]; ok {
		sEntry.ChaptersDownloaded = totalDownloaded
		sEntry.UpdatedAt = time.Now()
	}
	s.mu.Unlock()

	s.updateRegistryEntry(seriesID, func(e *RegistryEntry) {
		e.ChaptersDownloaded = totalDownloaded
		e.UpdatedAt = time.Now()
	})

	log.Printf("[FORCE-REDOWNLOAD] Completed redownload of %d/%d chapters (range: %.1f - %.1f)", redownloadedCount, len(toRedownload), fromChapter, toChapter)
}

func (s *Server) handleRedownloadChapter(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	seriesID := parts[3]

	var req struct {
		ChapterNumber float64 `json:"chapterNumber"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.ChapterNumber <= 0 {
		http.Error(w, "Invalid chapter number", http.StatusBadRequest)
		return
	}

	chapterKey := strconv.FormatFloat(req.ChapterNumber, 'f', -1, 64)

	s.mu.RLock()
	series, ok := s.series[seriesID]
	if !ok {
		s.mu.RUnlock()
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}
	chapters := s.chapters[seriesID]
	s.mu.RUnlock()

	if series.URL == "" {
		http.Error(w, "Series has no URL configured", http.StatusBadRequest)
		return
	}

	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		http.Error(w, "No scraper available", http.StatusInternalServerError)
		return
	}

	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	s.chapterStateMu.Lock()
	chapterState, err := s.loadChapterStateLocked(folderName)
	if err != nil || chapterState == nil {
		s.chapterStateMu.Unlock()
		http.Error(w, "No chapter state found", http.StatusNotFound)
		return
	}

	chInfo, exists := chapterState.Chapters[chapterKey]
	if !exists {
		s.chapterStateMu.Unlock()
		http.Error(w, fmt.Sprintf("Chapter %s not found in state", chapterKey), http.StatusNotFound)
		return
	}

	chapterDir := filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)
	os.RemoveAll(chapterDir)

	chInfo.Downloaded = false
	chInfo.ImageCount = 0
	chInfo.DownloadedAt = time.Time{}
	chInfo.PerImageStatus = nil
	chapterState.Chapters[chapterKey] = chInfo

	if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
		log.Printf("[REDOWNLOAD-CHAPTER] Failed to save chapter state before redownload: %v", err)
	}
	s.chapterStateMu.Unlock()

	var chapter *models.Chapter
	for _, ch := range chapters {
		chNum := strconv.FormatFloat(ch.Number, 'f', -1, 64)
		if chNum == chapterKey {
			chapter = ch
			break
		}
	}

	if chapter == nil {
		http.Error(w, fmt.Sprintf("Chapter %s not found in memory", chapterKey), http.StatusNotFound)
		return
	}

	html, err := scraper.FetchHTML(scr, s.httpClient, chapter.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch chapter: %v", err), http.StatusInternalServerError)
		return
	}

	scraper.SetCookiesOnHttpClient(scr, s.httpClient, chapter.URL)

	images, err := s.extractImagesWithValidation(scr, html, chapter.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to extract images: %v", err), http.StatusInternalServerError)
		return
	}

	if len(images) == 0 {
		http.Error(w, "No images found for chapter", http.StatusInternalServerError)
		return
	}

	dl := models.NewDownload(series.ID, chapter.ID, series.Title, chapter.Title)
	dl.ID = uuid.New().String()
	dl.CustomName = series.CustomName

	s.mu.Lock()
	s.downloads[dl.ID] = dl
	s.mu.Unlock()

	task := &download.Task{
		ID:            dl.ID,
		SeriesID:      series.ID,
		ChapterID:     chapter.ID,
		SeriesTitle:   series.Title,
		CustomName:    series.CustomName,
		ChapterTitle:  chapter.Title,
		ChapterNumber: chapter.Number,
		URL:           chapter.URL,
		Images:        images,
	}

	// Start download in background and track it for safe shutdown.
	s.safeGoTrack("redownloadChapter", func() {
		s.dlManager.DownloadChapter(task, dl)

		if dl.Status == models.StatusCompleted || dl.Status == models.StatusPartial {
			s.chapterStateMu.Lock()
			latestState, loadErr := s.loadChapterStateLocked(folderName)
			if loadErr == nil && latestState != nil {
				updated := latestState.Chapters[chapterKey]
				updated.Downloaded = isDownloadComplete(dl)
				if actualFiles := countFilesInDir(filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)); actualFiles > 0 {
					updated.ImageCount = actualFiles
				} else {
					updated.ImageCount = dl.DownloadedCount
				}
				updated.DownloadedAt = time.Now()
				if dl.PerImageStatus != nil {
					updated.PerImageStatus = dl.PerImageStatus
				}
				latestState.Chapters[chapterKey] = updated
				if saveErr := s.saveChapterStateLocked(folderName, latestState); saveErr != nil {
					log.Printf("[REDOWNLOAD-CHAPTER] Failed to save chapter state: %v", saveErr)
				}
			}
			s.chapterStateMu.Unlock()

			// If the state file could not be loaded, skip the count update —
			// recomputing from a nil state would silently zero ChaptersDownloaded.
			if latestState == nil {
				return
			}

			// latestState aliases the cached object; count totals under the
			// state lock so concurrent writers can't mutate during iteration.
			totalDownloaded := 0
			s.chapterStateMu.Lock()
			for _, c := range latestState.Chapters {
				if c.Downloaded {
					totalDownloaded++
				}
			}
			s.chapterStateMu.Unlock()

			s.mu.Lock()
			if sEntry, ok := s.series[seriesID]; ok {
				sEntry.ChaptersDownloaded = totalDownloaded
				sEntry.UpdatedAt = time.Now()
			}
			s.mu.Unlock()

			s.updateRegistryEntry(seriesID, func(e *RegistryEntry) {
				e.ChaptersDownloaded = totalDownloaded
				e.UpdatedAt = time.Now()
			})
		}
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":       fmt.Sprintf("Redownloading chapter %s", chapterKey),
		"chapterNumber": req.ChapterNumber,
		"downloadId":    dl.ID,
	})
}

// handleRecheckAllSeries scans download folders for all series and updates
// .chapters.json state files to reflect what's actually on disk. It also
// refreshes the chapter list from the website for each series that has a URL.
func (s *Server) handleRecheckAllSeries(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	seriesList := make([]*models.Series, 0, len(s.series))
	for _, series := range s.series {
		seriesList = append(seriesList, series)
	}
	s.mu.RUnlock()

	checkedCount := 0
	for _, series := range seriesList {
		folderName := series.CustomName
		if folderName == "" {
			folderName = series.Title
		}

		// Refresh chapter list from the website if URL is configured
		if series.URL != "" {
			s.refreshSeriesChapters(series)
		}

		// Re-read series after refresh; skip if it was deleted concurrently.
		s.mu.RLock()
		reread, ok := s.series[series.ID]
		s.mu.RUnlock()
		if !ok || reread == nil {
			continue
		}
		series = reread

		// Recompute folderName from the fresh series object — a concurrent
		// rename must not make us scan/save into the old folder.
		folderName = series.CustomName
		if folderName == "" {
			folderName = series.Title
		}

		seriesPath := filepath.Join(s.config.DownloadPath, folderName)
		chapterDirs, err := os.ReadDir(seriesPath)
		if err != nil {
			continue // folder doesn't exist yet
		}

		s.chapterStateMu.Lock()
		chapterState, _ := s.loadChapterStateLocked(folderName)
		if chapterState == nil {
			// No state file — create from disk folders
			chapterState = &ChapterStateFile{
				SeriesID:   series.ID,
				FolderName: folderName,
				URL:        series.URL,
				Chapters:   make(map[string]ChapterInfo),
			}
			for _, ch := range chapterDirs {
				if !ch.IsDir() || !strings.HasPrefix(strings.ToLower(ch.Name()), "chapter") {
					continue
				}
				chNumStr := strings.TrimPrefix(strings.ToLower(ch.Name()), "chapter")
				chNumStr = strings.TrimSpace(chNumStr)
				chPath := filepath.Join(seriesPath, ch.Name())
				files, _ := os.ReadDir(chPath)
				imageCount := 0
				for _, f := range files {
					if !f.IsDir() {
						imageCount++
					}
				}
				if imageCount > 0 {
					chapterState.Chapters[chNumStr] = ChapterInfo{
						Number:     0,
						Title:      ch.Name(),
						Downloaded: true,
						ImageCount: imageCount,
					}
				}
			}
			s.saveChapterStateLocked(folderName, chapterState)
		} else {
			// State file exists — reconcile disk vs state
			stateChanged := false
			for _, ch := range chapterDirs {
				if !ch.IsDir() || !strings.HasPrefix(strings.ToLower(ch.Name()), "chapter") {
					continue
				}
				chNumStr := strings.TrimPrefix(strings.ToLower(ch.Name()), "chapter")
				chNumStr = strings.TrimSpace(chNumStr)
				chPath := filepath.Join(seriesPath, ch.Name())
				files, _ := os.ReadDir(chPath)
				imageCount := 0
				for _, f := range files {
					if !f.IsDir() {
						imageCount++
					}
				}
				if imageCount > 0 {
					if chInfo, exists := chapterState.Chapters[chNumStr]; exists {
						if !chInfo.Downloaded {
							chInfo.Downloaded = true
							chInfo.ImageCount = imageCount
							// Don't discard any existing per-image status from a prior
							// download; keep it so auto-sync still knows which images were
							// intentionally filtered.
							chapterState.Chapters[chNumStr] = chInfo
							stateChanged = true
						}
					} else {
						chapterState.Chapters[chNumStr] = ChapterInfo{
							Number:     0,
							Title:      ch.Name(),
							Downloaded: true,
							ImageCount: imageCount,
						}
						stateChanged = true
					}
				}
			}
			if stateChanged {
				s.saveChapterStateLocked(folderName, chapterState)
			}
		}
		// Count totals while still holding the lock: chapterState may alias
		// the cached object and writers may mutate it after we release.
		totalDownloaded := 0
		for _, c := range chapterState.Chapters {
			if c.Downloaded {
				totalDownloaded++
			}
		}
		s.chapterStateMu.Unlock()

		// Update in-memory series count
		s.mu.Lock()
		if sEntry, ok := s.series[series.ID]; ok {
			sEntry.ChaptersDownloaded = totalDownloaded
			sEntry.UpdatedAt = time.Now()
		}
		s.mu.Unlock()

		s.updateRegistryEntry(series.ID, func(e *RegistryEntry) {
			e.ChaptersDownloaded = totalDownloaded
			e.UpdatedAt = time.Now()
		})

		checkedCount++
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":       fmt.Sprintf("Rechecked %d series", checkedCount),
		"checkedSeries": checkedCount,
	})
}

// handleStartDownload starts a chapter download
func (s *Server) handleStartDownload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SeriesID  string `json:"seriesId"`
		ChapterID string `json:"chapterId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Get series and chapter
	s.mu.RLock()
	series, ok := s.series[req.SeriesID]
	if !ok {
		s.mu.RUnlock()
		http.Error(w, "Series not found", http.StatusNotFound)
		return
	}

	var chapter *models.Chapter
	for _, ch := range s.chapters[req.SeriesID] {
		if ch.ID == req.ChapterID {
			chapter = ch
			break
		}
	}
	s.mu.RUnlock()

	if chapter == nil {
		http.Error(w, "Chapter not found", http.StatusNotFound)
		return
	}

	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}
	chapterKey := strconv.FormatFloat(chapter.Number, 'f', -1, 64)
	chapterDir := filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)

	// Find scraper and fetch/extract BEFORE deleting anything: if the fetch or
	// extraction fails, the existing chapter must remain intact on disk.
	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		http.Error(w, "Scraper not available", http.StatusInternalServerError)
		return
	}

	// Fetch chapter page
	html, err := scraper.FetchHTML(scr, s.httpClient, chapter.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch chapter: %v", err), http.StatusInternalServerError)
		return
	}

	scraper.SetCookiesOnHttpClient(scr, s.httpClient, chapter.URL)

	// Extract images with validation and optional re-fetch
	images, err := s.extractImagesWithValidation(scr, html, chapter.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to extract images: %v", err), http.StatusInternalServerError)
		return
	}

	// Only now that images are ready, delete the existing chapter data.
	if _, err := os.Stat(chapterDir); err == nil {
		os.RemoveAll(chapterDir)
		s.chapterStateMu.Lock()
		if state, loadErr := s.loadChapterStateLocked(folderName); loadErr == nil && state != nil {
			if chInfo, ok := state.Chapters[chapterKey]; ok {
				chInfo.Downloaded = false
				chInfo.ImageCount = 0
				chInfo.DownloadedAt = time.Time{}
				chInfo.PerImageStatus = nil
				state.Chapters[chapterKey] = chInfo
				s.saveChapterStateLocked(folderName, state)
			}
		}
		s.chapterStateMu.Unlock()
	}

	// Create download
	dl := models.NewDownload(series.ID, chapter.ID, series.Title, chapter.Title)
	dl.ID = uuid.New().String()
	dl.CustomName = series.CustomName

	s.mu.Lock()
	s.downloads[dl.ID] = dl
	s.mu.Unlock()

	// Create download task
	task := &download.Task{
		ID:            dl.ID,
		SeriesID:      series.ID,
		ChapterID:     chapter.ID,
		SeriesTitle:   series.Title,
		CustomName:    series.CustomName,
		ChapterTitle:  chapter.Title,
		ChapterNumber: chapter.Number,
		URL:           chapter.URL,
		Images:        images,
	}

	// Start download in background and track it for safe shutdown.
	s.safeGoTrack("manualDownloadChapter", func() {
		s.dlManager.DownloadChapter(task, dl)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	// Snapshot under the read lock: the manager mutates the live Download
	// concurrently, so encoding the raw pointer races.
	s.mu.RLock()
	dlCopy := *dl
	s.mu.RUnlock()
	json.NewEncoder(w).Encode(dlCopy)
}

// handleListDownloads returns active downloads
func (s *Server) handleListDownloads(w http.ResponseWriter, r *http.Request) {
	// Copy by value under the read lock: the download manager mutates the
	// live structs concurrently, so encoding raw pointers afterwards races.
	s.mu.RLock()
	downloads := make([]models.Download, 0, len(s.downloads))
	for _, dl := range s.downloads {
		downloads = append(downloads, *dl)
	}
	s.mu.RUnlock()

	// Sort: completed first (most recent first by UpdatedAt), then
	// downloading, then everything else (pending/failed/partial/cancelled)
	// alphabetical by series title. Matches the client-side sort in
	// loadDownloads() so the API order is consistent with the rendered order
	// if either side is consumed independently.
	sort.Slice(downloads, func(i, j int) bool {
		ri := downloadSortRank(downloads[i].Status)
		rj := downloadSortRank(downloads[j].Status)
		if ri != rj {
			return ri < rj
		}
		if ri == 0 {
			// Completed: most recent first.
			return downloads[i].UpdatedAt.After(downloads[j].UpdatedAt)
		}
		titleI := downloads[i].CustomName
		if titleI == "" {
			titleI = downloads[i].SeriesTitle
		}
		titleJ := downloads[j].CustomName
		if titleJ == "" {
			titleJ = downloads[j].SeriesTitle
		}
		if titleI != titleJ {
			return strings.ToLower(titleI) < strings.ToLower(titleJ)
		}
		return downloads[i].ChapterTitle < downloads[j].ChapterTitle
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(downloads)
}

// downloadSortRank returns the bucket index used to sort downloads on the
// downloads page. 0 = completed (top), 1 = downloading, 2 = everything else.
// Mirrors the rank function in loadDownloads() in the embedded JS.
func downloadSortRank(status models.DownloadStatus) int {
	switch status {
	case models.StatusCompleted:
		return 0
	case models.StatusDownloading:
		return 1
	default:
		return 2
	}
}

// handleClearDownloads removes completed/failed downloads
func (s *Server) handleClearDownloads(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	for id, dl := range s.downloads {
		if dl.Status == models.StatusCompleted || dl.Status == models.StatusFailed || dl.Status == models.StatusPartial {
			delete(s.downloads, id)
			delete(s.hmangaLastLoggedBytes, id)
		}
	}
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

// handleCancelDownload cancels an active manga download by ID:
// POST /api/downloads/{id}/cancel
func (s *Server) handleCancelDownload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/downloads/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	dlID := parts[0]

	s.mu.RLock()
	dl, ok := s.downloads[dlID]
	active := ok && (dl.Status == models.StatusPending || dl.Status == models.StatusDownloading)
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Download not found", http.StatusNotFound)
		return
	}
	if !active {
		http.Error(w, "Download is not active", http.StatusConflict)
		return
	}

	if s.dlManager.CancelTask(dlID) {
		s.mu.Lock()
		if existing, ok := s.downloads[dlID]; ok && existing.Status != models.StatusCompleted {
			existing.Status = models.StatusCancelled
			existing.UpdatedAt = time.Now()
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelled", "downloadId": dlID})
		return
	}
	// Not tracked by the manga manager: it may be an H-Manga download, whose
	// lifecycle is owned by the H-Manga manager's progress phases.
	http.Error(w, "Download is not cancellable", http.StatusConflict)
}

// handleGetSettings returns current settings. The stored HentaiNexus password
// is redacted: GET responses must not leak the credential. The UI leaves the
// password field blank and only overwrites the stored password when a new
// non-empty value is POSTed.
func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	cfgCopy := *s.config
	s.mu.RUnlock()

	cfgCopy.HentaiNexusPassword = ""

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cfgCopy)
}

// handleUpdateSettings updates configuration
func (s *Server) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var req config.Config
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Check for active downloads before replacing download manager
	s.mu.RLock()
	activeCount := len(s.dlManager.GetActiveTasks())
	s.mu.RUnlock()

	if activeCount > 0 {
		http.Error(w, "Cannot update settings while downloads are active", http.StatusConflict)
		return
	}

	s.mu.Lock()
	s.config.DownloadPath = req.DownloadPath
	s.config.RateDelay = req.RateDelay
	s.config.MinImageSizeKB = req.MinImageSizeKB
	s.config.MinImageWidth = req.MinImageWidth
	s.config.MinImageHeight = req.MinImageHeight
	s.config.UserAgent = req.UserAgent
	s.config.BrowserBackground = req.BrowserBackground
	s.config.HMangaDownloadPath = req.HMangaDownloadPath
	s.config.HentaiNexusUsername = req.HentaiNexusUsername
	s.config.VerifyHMDownloads = req.VerifyHMDownloads
	s.config.HMangaZipNameRegex = req.HMangaZipNameRegex
	s.config.HMangaExtractZips = req.HMangaExtractZips
	// The GET response redacts the password, so a settings round-trip POSTs
	// an empty value. Only overwrite the stored password when a new non-empty
	// one is explicitly provided.
	if req.HentaiNexusPassword != "" {
		s.config.HentaiNexusPassword = req.HentaiNexusPassword
	}

	// Update HTTP client
	s.httpClient.SetRateDelay(req.RateDelay)
	s.httpClient.SetUserAgent(req.UserAgent)

	// Update H-Manga manager in-place (do NOT recreate — its browser profile persists)
	if s.hmangaMgr != nil {
		s.hmangaMgr.UpdateConfig(s.config)
	}

	// Update download manager in place rather than replacing it: download
	// goroutines read s.dlManager without s.mu, so swapping the pointer
	// races them. The manager snapshots its config at the start of each
	// download, so reconfiguring the live instance is race-free.
	s.dlManager.SetDownloadPath(s.config.DownloadPath)
	s.dlManager.SetMinImageSizeKB(s.config.MinImageSizeKB)
	s.dlManager.SetMinImageWidth(s.config.MinImageWidth)
	s.dlManager.SetMinImageHeight(s.config.MinImageHeight)
	s.mu.Unlock()

	// Persist config
	if err := config.Save(s.config); err != nil {
		http.Error(w, fmt.Sprintf("Failed to save config: %v", err), http.StatusInternalServerError)
		return
	}

	respCfg := *s.config
	respCfg.HentaiNexusPassword = ""

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(respCfg)
}

// generateIDFromString creates a consistent ID from a string (for discovered series)
func generateIDFromString(name string) string {
	// Use hash of the name to create a consistent ID
	h := fnv.New64a()
	h.Write([]byte(name))
	return fmt.Sprintf("discovered-%x", h.Sum64())
}

// ensureStateDir creates the .state directory if it doesn't exist
func (s *Server) ensureStateDir() error {
	stateDir := filepath.Join(s.config.DownloadPath, ".state")
	return os.MkdirAll(stateDir, 0755)
}

// ==================== NEW REGISTRY SYSTEM ====================

// getRegistryPath returns the path for the central registry file
func (s *Server) getRegistryPath() string {
	return filepath.Join(s.config.DownloadPath, ".state", "series.json")
}

// getChapterStatePath returns the path for a series' chapter state file
func (s *Server) getChapterStatePath(folderName string) string {
	return filepath.Join(s.config.DownloadPath, folderName, ".chapters.json")
}

// loadRegistry loads the central series registry from disk
func (s *Server) loadRegistry() (*SeriesRegistry, error) {
	registryPath := s.getRegistryPath()

	data, err := os.ReadFile(registryPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Return empty registry
			return &SeriesRegistry{
				Version: 1,
				Series:  []RegistryEntry{},
			}, nil
		}
		return nil, fmt.Errorf("failed to read registry file: %w", err)
	}

	var registry SeriesRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal registry: %w", err)
	}

	// Migration: handle legacy registry without version
	if registry.Version == 0 {
		registry.Version = 1
	}

	// Defense-in-depth: sanitize every entry's FolderName/CustomName/Title so
	// a hand-edited registry.json (or one written by an older, unsanitizing
	// build) can't smuggle ".." or path separators into download paths derived
	// from these fields. Log and fix in place rather than rejecting, since the
	// user may have legitimately added series with names the old code accepted.
	for i := range registry.Series {
		e := &registry.Series[i]
		if !fileutil.IsSafeFolderName(e.FolderName) {
			log.Printf("[REGISTRY] sanitizing unsafe folderName %q for series %s", e.FolderName, e.ID)
			e.FolderName = fileutil.SanitizeFolderName(e.FolderName)
		}
		if e.CustomName != "" {
			e.CustomName = fileutil.SanitizeFolderName(e.CustomName)
		}
	}

	return &registry, nil
}

// saveRegistry saves the central series registry to disk
func (s *Server) saveRegistry(registry *SeriesRegistry) error {
	if err := s.ensureStateDir(); err != nil {
		return err
	}

	registryPath := s.getRegistryPath()

	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal registry: %w", err)
	}

	if err := os.WriteFile(registryPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write registry file: %w", err)
	}

	return nil
}

// loadChapterState returns the in-memory chapter state for a series folder,
// reading the .chapters.json file from disk only on first access. It acquires
// chapterStateMu itself; callers already holding the lock must call
// loadChapterStateLocked instead.
func (s *Server) loadChapterState(folderName string) (*ChapterStateFile, error) {
	s.chapterStateMu.Lock()
	defer s.chapterStateMu.Unlock()
	return s.loadChapterStateLocked(folderName)
}

// loadChapterStateLocked is loadChapterState without locking. The returned
// pointer aliases the cached object: callers mutating it must hold
// chapterStateMu across the mutation (and any subsequent save) so the
// write-on-change snapshot comparison is race-free.
// Caller must hold chapterStateMu.
func (s *Server) loadChapterStateLocked(folderName string) (*ChapterStateFile, error) {
	if cached, ok := s.chapterStateCache[folderName]; ok {
		return cached, nil
	}

	statePath := s.getChapterStatePath(folderName)

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read chapter state file: %w", err)
	}

	var state ChapterStateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chapter state: %w", err)
	}

	s.chapterStateCache[folderName] = &state
	return &state, nil
}

// saveChapterState persists the chapter state to disk only when its content
// differs from the last persisted snapshot. It acquires chapterStateMu
// itself; callers already holding the lock must call saveChapterStateLocked.
func (s *Server) saveChapterState(folderName string, state *ChapterStateFile) error {
	s.chapterStateMu.Lock()
	defer s.chapterStateMu.Unlock()
	return s.saveChapterStateLocked(folderName, state)
}

// saveChapterStateLocked is saveChapterState without locking.
// Caller must hold chapterStateMu.
func (s *Server) saveChapterStateLocked(folderName string, state *ChapterStateFile) error {
	// Ensure the series directory exists before writing the chapter state file
	seriesPath := filepath.Join(s.config.DownloadPath, folderName)
	if err := os.MkdirAll(seriesPath, 0755); err != nil {
		return fmt.Errorf("failed to create series directory: %w", err)
	}

	statePath := s.getChapterStatePath(folderName)

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal chapter state: %w", err)
	}

	if prev, ok := s.chapterStateCache[folderName]; ok && prev != state {
		prevData, err := json.MarshalIndent(prev, "", "  ")
		if err == nil && bytes.Equal(prevData, data) {
			// Content unchanged -- skip the disk write. Update the cache entry
			// to alias the caller's object so future mutations are detected.
			s.chapterStateCache[folderName] = state
			return nil
		}
	}

	// WriteFileAtomic: temp + rename so a crash mid-write can't truncate
	// .chapters.json and a concurrent unlocked reader never sees a partial file.
	if err := fileutil.WriteFileAtomic(statePath, data, 0644); err != nil {
		return err
	}

	s.chapterStateCache[folderName] = state
	return nil
}

// countFilesInDir counts non-directory entries in dir, or -1 if unreadable.
func countFilesInDir(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count
}

// getOrCreateRegistryEntry finds or creates a registry entry by folder name
func (s *Server) getOrCreateRegistryEntry(registry *SeriesRegistry, folderName string) *RegistryEntry {
	for i := range registry.Series {
		if registry.Series[i].FolderName == folderName {
			return &registry.Series[i]
		}
	}

	// Create new entry
	newEntry := RegistryEntry{
		ID:            generateIDFromString(folderName),
		FolderName:    folderName,
		Title:         folderName,
		CustomName:    folderName,
		CheckInterval: "never",
		AddedAt:       time.Now(),
		UpdatedAt:     time.Now(),
	}
	registry.Series = append(registry.Series, newEntry)
	return &registry.Series[len(registry.Series)-1]
}

// migrateOldStateFiles migrates from old per-series state files to new system
func (s *Server) migrateOldStateFiles(registry *SeriesRegistry) error {
	stateDir := filepath.Join(s.config.DownloadPath, ".state")

	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return nil // .state may not exist yet
	}

	migrated := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if entry.Name() == "series.json" {
			continue // Skip the new registry file
		}

		// Load old state file
		oldPath := filepath.Join(stateDir, entry.Name())
		data, err := os.ReadFile(oldPath)
		if err != nil {
			continue
		}

		var oldState SeriesState
		if err := json.Unmarshal(data, &oldState); err != nil {
			continue
		}

		// Create or update registry entry
		var regEntry *RegistryEntry
		for i := range registry.Series {
			if registry.Series[i].ID == oldState.SeriesID {
				regEntry = &registry.Series[i]
				break
			}
		}

		if regEntry == nil {
			// Use custom name as folder name if available
			folderName := oldState.CustomName
			if folderName == "" {
				folderName = oldState.SeriesTitle
			}

			registry.Series = append(registry.Series, RegistryEntry{
				ID:            oldState.SeriesID,
				FolderName:    folderName,
				Title:         oldState.SeriesTitle,
				CustomName:    oldState.CustomName,
				URL:           oldState.URL,
				CheckInterval: oldState.CheckInterval,
				LastCheckedAt: oldState.LastCheckedAt,
				AddedAt:       time.Now(),
				UpdatedAt:     time.Now(),
			})
			regEntry = &registry.Series[len(registry.Series)-1]
		}

		// Create chapter state file in series folder
		chapterState := &ChapterStateFile{
			SeriesID:   oldState.SeriesID,
			FolderName: regEntry.FolderName,
			URL:        oldState.URL,
			LastSynced: oldState.LastUpdated,
			Chapters:   make(map[string]ChapterInfo),
		}

		for key, ch := range oldState.Chapters {
			chapterState.Chapters[key] = ChapterInfo{
				Number:     ch.Number,
				Title:      ch.Title,
				URL:        ch.URL,
				Downloaded: ch.Downloaded,
				ImageCount: ch.ImageCount,
			}
		}

		// Save chapter state file
		s.saveChapterState(regEntry.FolderName, chapterState)

		// Rename old file as backup
		backupPath := oldPath + ".backup"
		os.Rename(oldPath, backupPath)

		migrated++
		log.Printf("Migrated old state file for series: %s", regEntry.FolderName)
	}

	if migrated > 0 {
		log.Printf("Migrated %d old state files to new format", migrated)
	}

	return nil
}

// extractImagesWithValidation extracts images from a chapter page and
// performs sequence validation. If gaps are detected and the scraper
// supports BrowserFetcher, it attempts one re-fetch with aggressive
// scrolling. The re-fetched images are used only if they yield more
// pages than the original extraction.
func (s *Server) extractImagesWithValidation(scr scraper.Scraper, html string, chapterURL string) ([]models.Image, error) {
	images, err := scr.ExtractImages(html, chapterURL)
	if err != nil {
		return nil, err
	}

	expectedCount := 0
	if pcp, ok := scr.(scraper.PageCountProvider); ok {
		expectedCount = pcp.ExtractExpectedPageCount(html)
	}

	report := scraper.InspectImageSequence(images, expectedCount)

	if len(report.Warnings) > 0 {
		for _, w := range report.Warnings {
			log.Printf("[SEQ-CHECK] %s: %s", chapterURL, w)
		}
	}

	needsRetry := len(report.Gaps) > 0 ||
		(expectedCount > 0 && report.TotalImages < expectedCount)

	if !needsRetry {
		urlGaps := scraper.DetectURLGaps(images)
		if len(urlGaps) > 0 {
			log.Printf("[SEQ-CHECK] URL-index gaps detected: %v for %s", urlGaps, chapterURL)
			needsRetry = true
		}
	}

	if needsRetry {
		if bf, ok := scr.(scraper.BrowserFetcher); ok {
			log.Printf("[SEQ-CHECK] Attempting re-fetch with aggressive scrolling for: %s", chapterURL)
			newHTML, retryErr := bf.FetchHTML(chapterURL)
			if retryErr != nil {
				log.Printf("[SEQ-CHECK] Re-fetch failed for %s: %v", chapterURL, retryErr)
				return images, nil
			}

			// Re-fetch produced new cookies/User-Agent - inject them into the
			// HTTP client so subsequent image downloads use the fresh session.
			scraper.SetCookiesOnHttpClient(scr, s.httpClient, chapterURL)

			newImages, retryErr := scr.ExtractImages(newHTML, chapterURL)
			if retryErr != nil {
				log.Printf("[SEQ-CHECK] Re-extraction failed for %s: %v", chapterURL, retryErr)
				return images, nil
			}

			if len(newImages) > len(images) {
				log.Printf("[SEQ-CHECK] Re-fetch yielded %d images (was %d), using re-fetched set for %s",
					len(newImages), len(images), chapterURL)

				newExpectedCount := 0
				if pcp, ok := scr.(scraper.PageCountProvider); ok {
					newExpectedCount = pcp.ExtractExpectedPageCount(newHTML)
				}
				newReport := scraper.InspectImageSequence(newImages, newExpectedCount)
				if len(newReport.Warnings) > 0 {
					for _, w := range newReport.Warnings {
						log.Printf("[SEQ-CHECK] Post-retry: %s: %s", chapterURL, w)
					}
				}
				return newImages, nil
			}

			log.Printf("[SEQ-CHECK] Re-fetch yielded %d images (was %d), keeping original for %s",
				len(newImages), len(images), chapterURL)
		} else {
			log.Printf("[SEQ-CHECK] Scraper does not support BrowserFetcher, cannot re-fetch for: %s", chapterURL)
		}
	}

	return images, nil
}

// autoDownloadMissingChapters downloads all chapters not marked as downloaded.
// If skipRefresh is true, the chapter list is assumed to already be up-to-date
// (e.g. because refreshSeriesChapters was just called) and the refresh step is
// skipped to avoid redundant network requests.
// If manual is true (user-invoked /sync, /sync-all), the manga scraping pause
// does not abort the loop — manual sync is documented to work while paused.
// The pause check still applies to scheduler/auto-download invocations.
func (s *Server) autoDownloadMissingChapters(seriesID string, args ...bool) {
	skipRefresh := len(args) > 0 && args[0]
	manual := len(args) > 1 && args[1]
	seriesName := ""
	s.mu.RLock()
	if series, ok := s.series[seriesID]; ok {
		seriesName = series.CustomName
		if seriesName == "" {
			seriesName = series.Title
		}
	}
	s.mu.RUnlock()

	// Prevent concurrent auto-downloads for the same series
	s.mu.Lock()
	if s.downloading[seriesID] {
		s.mu.Unlock()
		log.Printf("Auto-download already in progress for series %s, skipping", seriesName)
		return
	}
	s.downloading[seriesID] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.downloading, seriesID)
		s.mu.Unlock()
	}()

	s.mu.RLock()
	series, ok := s.series[seriesID]
	s.mu.RUnlock()

	if !ok {
		log.Printf("Series %s not found in memory", seriesName)
		return
	}

	// Get folder name
	folderName := series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	// Load chapter state from series folder. Copy it: the cache aliases the
	// live object, and this function mutates its copy while unheld.
	chapterState, err := s.loadChapterState(folderName)
	if err == nil && chapterState != nil {
		s.chapterStateMu.Lock()
		cp := *chapterState
		cp.Chapters = make(map[string]ChapterInfo, len(chapterState.Chapters))
		for k, v := range chapterState.Chapters {
			v.PerImageStatus = append([]models.ImageResult(nil), v.PerImageStatus...)
			cp.Chapters[k] = v
		}
		chapterState = &cp
		s.chapterStateMu.Unlock()
	}
	if err != nil {
		log.Printf("Failed to load chapter state for %s: %v", seriesName, err)
		return
	}
	if chapterState == nil {
		log.Printf("No chapter state found for series %s", seriesName)
		return
	}

	// Check if series has a URL (discovered series from folder may not)
	if series.URL == "" {
		log.Printf("Series %s has no URL configured, cannot auto-download", seriesName)
		return
	}

	scr := s.scraperReg.GetScraper(series.URL)
	if scr == nil {
		log.Printf("No scraper available for series %s (URL: %s)", seriesName, series.URL)
		return
	}

	// Refresh the chapter list from the website so we discover any newly
	// published chapters before deciding what to download. refreshSeriesChapters
	// also updates the chapter state file with any new chapters.
	// Skip this step if the caller already performed a refresh (e.g. the
	// scheduler or check-now handler calls refreshSeriesChapters before
	// invoking autoDownloadMissingChapters).
	if !skipRefresh {
		s.refreshSeriesChapters(series)
	}

	// (Re-)read the series pointer and chapter state. If we refreshed, this
	// picks up any new chapters. If we skipped refresh, the caller's refresh
	// already updated in-memory state, and we just need the latest pointers.
	s.mu.RLock()
	series, ok = s.series[seriesID]
	s.mu.RUnlock()
	if !ok {
		log.Printf("Series %s not found after refresh", seriesName)
		return
	}

	folderName = series.CustomName
	if folderName == "" {
		folderName = series.Title
	}

	// Re-load + copy after refresh (locked read, private copy for mutation).
	chapterState, err = s.loadChapterState(folderName)
	if err == nil && chapterState != nil {
		s.chapterStateMu.Lock()
		cp := *chapterState
		cp.Chapters = make(map[string]ChapterInfo, len(chapterState.Chapters))
		for k, v := range chapterState.Chapters {
			v.PerImageStatus = append([]models.ImageResult(nil), v.PerImageStatus...)
			cp.Chapters[k] = v
		}
		chapterState = &cp
		s.chapterStateMu.Unlock()
	}
	if err != nil {
		log.Printf("Failed to load chapter state after refresh for %s: %v", seriesName, err)
		return
	}
	if chapterState == nil {
		log.Printf("No chapter state found after refresh for series %s", seriesName)
		return
	}

	// Iterate chapters in sorted order (ascending by chapter number) for proper download order.
	// The .chapters.json file is the authoritative source of truth for what has been
	// downloaded.  If a chapter entry has Downloaded: true, it is skipped.  If it has
	// Downloaded: false or the chapter key is absent, it needs downloading.
	sortedChapterKeys := make([]string, 0, len(chapterState.Chapters))
	for chapterKey := range chapterState.Chapters {
		sortedChapterKeys = append(sortedChapterKeys, chapterKey)
	}
	sort.Slice(sortedChapterKeys, func(i, j int) bool {
		ni, errI := strconv.ParseFloat(sortedChapterKeys[i], 64)
		nj, errJ := strconv.ParseFloat(sortedChapterKeys[j], 64)
		if errI == nil && errJ == nil {
			return ni < nj
		}
		return sortedChapterKeys[i] < sortedChapterKeys[j]
	})

	downloadedCount := 0

	// First pass: verify that chapters marked as downloaded actually have files
	// on disk. Batch state resets into a single lock-load-save cycle.
	// When a chapter has fewer files than expected, check PerImageStatus to decide
	// whether to redownload: only redownload if there are retryable failures
	// (ImageStatusFailed). Don't redownload if missing images were intentionally
	// filtered (ImageStatusFiltered) — those were skipped for valid reasons.
	needsReset := false
	for _, chapterKey := range sortedChapterKeys {
		chInfo := chapterState.Chapters[chapterKey]
		if !chInfo.Downloaded {
			continue
		}
		chapterDir := filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)
		entries, readErr := os.ReadDir(chapterDir)
		if readErr != nil {
			// Folder missing entirely — always redownload
			log.Printf("[SYNC] Chapter %s marked as downloaded but folder missing, re-downloading", chapterKey)
			chInfo.Downloaded = false
			chInfo.ImageCount = 0
			chInfo.DownloadedAt = time.Time{}
			chInfo.PerImageStatus = nil
			chapterState.Chapters[chapterKey] = chInfo
			needsReset = true
		} else {
			fileCount := 0
			for _, e := range entries {
				if !e.IsDir() {
					fileCount++
				}
			}
			if fileCount == 0 {
				// Empty folder — check PerImageStatus: if ALL images were filtered, this
				// is expected (no pages passed dimension/size checks) and we shouldn't
				// keep retrying forever. Otherwise, redownload.
				if isAllFiltered(chInfo.PerImageStatus) {
					log.Printf("[SYNC] Chapter %s folder empty but all images were filtered, skipping", chapterKey)
					continue
				}
				log.Printf("[SYNC] Chapter %s marked as downloaded but folder empty, re-downloading", chapterKey)
				os.RemoveAll(chapterDir)
				chInfo.Downloaded = false
				chInfo.ImageCount = 0
				chInfo.DownloadedAt = time.Time{}
				chInfo.PerImageStatus = nil
				chapterState.Chapters[chapterKey] = chInfo
				needsReset = true
			} else if chInfo.ImageCount > 0 && fileCount < chInfo.ImageCount {
				// Fewer files than expected — check if missing images have retryable errors
				hasRetryableFailures := false
				if len(chInfo.PerImageStatus) > 0 {
					for _, imgStatus := range chInfo.PerImageStatus {
						if imgStatus.IsRetryable() {
							// File doesn't exist on disk AND the failure was retryable
							expectedFile := fmt.Sprintf("%03d", imgStatus.Page)
							found := false
							for _, e := range entries {
								if !e.IsDir() && strings.HasPrefix(e.Name(), expectedFile) {
									found = true
									break
								}
							}
							if !found {
								hasRetryableFailures = true
								break
							}
						}
					}
				} else {
					// No per-image status recorded (old download before this feature) —
					// assume missing images are retryable failures
					hasRetryableFailures = true
				}
				if hasRetryableFailures {
					log.Printf("[SYNC] Chapter %s has %d/%d files with retryable failures, re-downloading", chapterKey, fileCount, chInfo.ImageCount)
					os.RemoveAll(chapterDir)
					chInfo.Downloaded = false
					chInfo.ImageCount = 0
					chInfo.DownloadedAt = time.Time{}
					chInfo.PerImageStatus = nil
					chapterState.Chapters[chapterKey] = chInfo
					needsReset = true
				} else {
					log.Printf("[SYNC] Chapter %s has %d/%d files but all missing images were filtered, skipping", chapterKey, fileCount, chInfo.ImageCount)
				}
			}
		}
	}

	if needsReset {
		s.chapterStateMu.Lock()
		latestState, loadErr := s.loadChapterStateLocked(folderName)
		if loadErr == nil && latestState != nil {
			for chapterKey, chInfo := range chapterState.Chapters {
				if !chInfo.Downloaded {
					latestState.Chapters[chapterKey] = chInfo
				}
			}
			chapterState = latestState
		}
		s.saveChapterStateLocked(folderName, chapterState)
		s.chapterStateMu.Unlock()
	}

	for _, chapterKey := range sortedChapterKeys {
		chInfo := chapterState.Chapters[chapterKey]
		if chInfo.Downloaded {
			continue
		}

		// If shutdown or a non-manual pause is signaled mid-sync, stop starting
		// new chapter downloads. The current chapter (if any) will finish.
		// Manual syncs keep going while paused (documented behavior).
		if s.isShutdown() || (!manual && s.isMangaScrapingPaused()) {
			log.Printf("[SYNC] Manga scraping paused or shutting down, stopping auto-download for series %s", folderName)
			break
		}

		s.mu.RLock()
		var chapter *models.Chapter
		for _, ch := range s.chapters[seriesID] {
			if ch.URL == chInfo.URL {
				chapter = ch
				break
			}
		}
		s.mu.RUnlock()

		if chapter == nil {
			log.Printf("Chapter %s not found in memory", chInfo.URL)
			continue
		}

		html, err := scraper.FetchHTML(scr, s.httpClient, chapter.URL)
		if err != nil {
			log.Printf("Failed to fetch chapter %s (%s): %v", chapter.Title, seriesName, err)
			continue
		}

		scraper.SetCookiesOnHttpClient(scr, s.httpClient, chapter.URL)

		images, err := s.extractImagesWithValidation(scr, html, chapter.URL)
		if err != nil {
			log.Printf("Failed to extract images for %s (%s): %v", chapter.Title, seriesName, err)
			continue
		}

		log.Printf("[SYNC] Downloading chapter %s for series %s", chapter.Title, seriesName)

		dl := models.NewDownload(series.ID, chapter.ID, series.Title, chapter.Title)
		dl.ID = uuid.New().String()
		dl.CustomName = series.CustomName

		s.mu.Lock()
		s.downloads[dl.ID] = dl
		s.mu.Unlock()

		task := &download.Task{
			ID:            dl.ID,
			SeriesID:      series.ID,
			ChapterID:     chapter.ID,
			SeriesTitle:   series.Title,
			CustomName:    series.CustomName,
			ChapterTitle:  chapter.Title,
			ChapterNumber: chapter.Number,
			URL:           chapter.URL,
			Images:        images,
		}

		s.dlManager.DownloadChapter(task, dl)

		// Only mark as downloaded if the download actually succeeded
		if dl.Status == models.StatusCompleted || dl.Status == models.StatusPartial {
			chapterResultDir := filepath.Join(s.config.DownloadPath, folderName, "Chapter "+chapterKey)
			actualFiles := 0
			if entries, err := os.ReadDir(chapterResultDir); err == nil {
				for _, e := range entries {
					if !e.IsDir() {
						actualFiles++
					}
				}
			}

			if dl.Status == models.StatusCompleted && actualFiles == 0 {
				log.Printf("[SYNC] Chapter %s (%s) reported completed but folder has 0 files, marking as partial", chapterKey, seriesName)
				dl.Status = models.StatusPartial
			}

			// Mark as downloaded if complete, or if all remaining images were
			// intentionally filtered (so auto-sync won't retry it forever).
			chInfo.Downloaded = isDownloadComplete(dl)
			if actualFiles > 0 {
				chInfo.ImageCount = actualFiles
			} else {
				chInfo.ImageCount = dl.DownloadedCount
			}
			chInfo.DownloadedAt = time.Now()
			if dl.PerImageStatus != nil {
				chInfo.PerImageStatus = dl.PerImageStatus
			}
			task.Mu.Lock()
			if task.ImageResults != nil {
				chInfo.PerImageStatus = task.ImageResults
			}
			task.Mu.Unlock()
			chapterState.Chapters[chapterKey] = chInfo
			downloadedCount++

			s.chapterStateMu.Lock()
			// Re-load under lock to merge any concurrent writes from updateChapterState
			latestState, loadErr := s.loadChapterStateLocked(folderName)
			if loadErr == nil && latestState != nil {
				// Apply our change on top of the latest saved state
				latestState.Chapters[chapterKey] = chInfo
				chapterState = latestState
			}
			if err := s.saveChapterStateLocked(folderName, chapterState); err != nil {
				log.Printf("Failed to save chapter state after downloading %s (%s): %v", chapter.Title, seriesName, err)
			}
			s.chapterStateMu.Unlock()
		} else {
			log.Printf("Download failed for chapter %s (%s) (status: %s), not marking as downloaded", chapter.Title, seriesName, dl.Status)
		}

		// Note: ChaptersDownloaded is incremented by handleDownloadProgress callback,
		// so we do not increment it here to avoid double-counting.
	}

	if downloadedCount > 0 {
		log.Printf("Auto-download: %d new chapters downloaded for series %s", downloadedCount, seriesName)
	}

	log.Printf("Auto-download completed for series %s", seriesName)
}

func main() {
	background := flag.Bool("d", false, "Run in background (detached from terminal)")
	flag.Parse()

	// Mirror all log output into the in-memory console buffer (web UI
	// Console tab) while still writing to stderr.
	log.SetFlags(log.LstdFlags)
	log.SetOutput(io.MultiWriter(os.Stderr, console))

	if *background {
		runDetached()
	}

	server, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Bind to localhost by default so the unauthenticated local admin API
	// (series/artists/settings mutation) is not exposed to the LAN. Override
	// with HOST=0.0.0.0 to listen on all interfaces.
	host := os.Getenv("HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	// Catch Ctrl+C and Windows close events so background goroutines, browser
	// queues, and the H-Manga manager are stopped cleanly instead of being killed.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("[SHUTDOWN] Received signal %v", sig)
		server.ShutdownAndExit()
	}()

	if err := server.Start(host + ":" + port); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// Embedded HTML template
const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comic Scraper</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>Comic Scraper</h1>
            <nav>
                <button class="nav-btn active" data-tab="series">Series</button>
                <button class="nav-btn" data-tab="hmanga">H-Manga</button>
                <button class="nav-btn" data-tab="downloads">Downloads</button>
                <button class="nav-btn" data-tab="settings">Settings</button>
                <button class="nav-btn" data-tab="console">Console</button>
                <button class="btn btn-secondary" id="theme-toggle" style="margin-left: auto;">🌙 Dark Mode</button>
            </nav>
            <div class="global-actions">
                <button class="btn btn-secondary" id="manga-scrape-toggle" onclick="toggleMangaScraping()" title="Start or stop all manga scraping" disabled>Loading...</button>
                <button class="btn btn-secondary" id="hmanga-scrape-toggle" onclick="toggleHMangaScraping()" title="Start or stop all H-Manga scraping" disabled>Loading...</button>
            </div>
        </header>

        <main>
            <!-- Series Tab -->
            <section id="series-tab" class="tab-content active">
                <div class="card">
                    <h2>Add New Series</h2>
                    <form id="add-series-form">
                        <div class="form-group">
                            <label for="series-url">Series URL</label>
                            <input type="url" id="series-url" placeholder="https://..." required>
                        </div>
                        <div class="form-group">
                            <label for="custom-name">Custom Name (optional)</label>
                            <input type="text" id="custom-name" placeholder="My Comic Name">
                        </div>
                        <div class="form-group">
                            <label for="check-interval">Auto-Check Frequency</label>
                            <select id="check-interval">
                                <option value="never">Never (manual only)</option>
                                <option value="5m">Every 5 minutes</option>
                                <option value="15m">Every 15 minutes</option>
                                <option value="30m">Every 30 minutes</option>
                                <option value="1h">Every hour</option>
                                <option value="6h">Every 6 hours</option>
                                <option value="12h">Every 12 hours</option>
                                <option value="24h">Every 24 hours</option>
                            </select>
                            <small class="help-text">How often to automatically check for new chapters</small>
                        </div>
                        <button type="submit" class="btn btn-primary">Add Series</button>
                    </form>
                </div>

                <div class="card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                        <h2 style="margin: 0;">Your Series</h2>
                        <div style="display: flex; gap: 0.5rem; align-items: center;">
					<label for="bulk-manga-interval" class="help-text" style="margin: 0;">Rescan all:</label>
					<select id="bulk-manga-interval" class="interval-select" onchange="bulkSetInterval('manga', this.value); this.selectedIndex = 0;" title="Set rescan interval for all series except those set to Never">
						<option value="">Choose...</option>
						<option value="5m">Every 5 minutes</option>
						<option value="15m">Every 15 minutes</option>
						<option value="30m">Every 30 minutes</option>
						<option value="1h">Every hour</option>
						<option value="6h">Every 6 hours</option>
						<option value="12h">Every 12 hours</option>
						<option value="24h">Every 24 hours</option>
					</select>
					<button class="btn btn-success" onclick="syncAllSeries()" title="Download missing chapters for all series">Sync All</button>
					<button class="btn btn-secondary" onclick="recheckAllSeries()" title="Scan all series folders and update download status">Recheck All</button>
				</div>
                    </div>
                    <div id="series-list" class="series-list">
                        <p class="empty">No series added yet.</p>
                    </div>
                </div>
            </section>

            <!-- H-Manga Tab -->
            <section id="hmanga-tab" class="tab-content">
                <div class="card">
                    <h2>Add New Artist (HentaiNexus)</h2>
                    <form id="add-hmanga-form">
                        <div class="form-group">
                            <label for="hmanga-url">Artist URL</label>
                            <input type="url" id="hmanga-url" placeholder="https://hentainexus.com/?q=artist:Prime" required>
                            <small class="help-text">HentaiNexus artist search URL. The artist name is parsed from the ?q=artist:NAME parameter.</small>
                        </div>
                        <div class="form-group">
                            <label for="hmanga-check-interval">Auto-Check Frequency</label>
                            <select id="hmanga-check-interval">
                                <option value="never">Never (manual only)</option>
                                <option value="5m">Every 5 minutes</option>
                                <option value="15m">Every 15 minutes</option>
                                <option value="30m">Every 30 minutes</option>
                                <option value="1h">Every hour</option>
                                <option value="6h">Every 6 hours</option>
                                <option value="12h">Every 12 hours</option>
                                <option value="24h">Every 24 hours</option>
                            </select>
                            <small class="help-text">How often to automatically check for new books</small>
                        </div>
                        <button type="submit" class="btn btn-primary">Add Artist</button>
                    </form>
                </div>

                <div class="card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                        <h2 style="margin: 0;">Your Artists</h2>
                        <div style="display: flex; gap: 0.5rem; align-items: center;">
					<label for="bulk-hmanga-interval" class="help-text" style="margin: 0;">Rescan all:</label>
					<select id="bulk-hmanga-interval" class="interval-select" onchange="bulkSetInterval('hmanga', this.value); this.selectedIndex = 0;" title="Set rescan interval for all artists except those set to Never">
						<option value="">Choose...</option>
						<option value="5m">Every 5 minutes</option>
						<option value="15m">Every 15 minutes</option>
						<option value="30m">Every 30 minutes</option>
						<option value="1h">Every hour</option>
						<option value="6h">Every 6 hours</option>
						<option value="12h">Every 12 hours</option>
						<option value="24h">Every 24 hours</option>
					</select>
					<button class="btn btn-warning" onclick="checkAllHMArtists()" title="Check all artists for new books now">Check All</button>
					<button class="btn btn-success" onclick="syncAllHMArtists()" title="Download missing books for all artists">Sync All</button>
					<button class="btn btn-secondary" onclick="cleanupHMZips()" title="Rename existing ZIPs per the configured name regex and extract them if extraction is enabled">Clean Up Zips</button>
				</div>
                    </div>
                    <div id="hmanga-list" class="series-list">
                        <p class="empty">No artists added yet.</p>
                    </div>
                </div>
            </section>

            <!-- Downloads Tab -->
            <section id="downloads-tab" class="tab-content">
                <div class="card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h2 style="margin: 0;">Active Downloads</h2>
                        <button class="btn btn-secondary" onclick="clearFinishedDownloads()">Clear Finished</button>
                    </div>
                    <div id="downloads-list">
                        <p class="empty">No active downloads.</p>
                    </div>
                </div>
            </section>

            <!-- Console Tab -->
            <section id="console-tab" class="tab-content">
                <div class="card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                        <h2 style="margin: 0;">Console Output</h2>
                        <span id="console-meta" class="help-text"></span>
                    </div>
                    <pre id="console-output" class="console-output">Loading console output...</pre>
                </div>
            </section>

            <!-- Settings Tab -->
            <section id="settings-tab" class="tab-content">
                <div class="card">
                    <h2>Settings</h2>
                    <form id="settings-form">
                        <div class="form-group">
                            <label for="download-path">Download Path</label>
                            <input type="text" id="download-path" required>
                        </div>
                        <div class="form-group">
                            <label for="rate-delay">Rate Delay (ms)</label>
                            <input type="number" id="rate-delay" min="0" max="5000" required>
                        </div>
                        <div class="form-group">
                            <label for="min-image-size">Min Image Size (KB)</label>
                            <input type="number" id="min-image-size" min="0" required>
                            <small class="help-text">Used as fallback when image dimensions cannot be determined.</small>
                        </div>
                        <div class="form-group">
                            <label for="min-image-width">Min Image Width (px)</label>
                            <input type="number" id="min-image-width" min="0" required>
                        </div>
                        <div class="form-group">
                            <label for="min-image-height">Min Image Height (px)</label>
                            <input type="number" id="min-image-height" min="0" required>
                        </div>
                        <div class="form-group">
                            <label for="user-agent">User Agent</label>
                            <input type="text" id="user-agent" required>
                        </div>
                        <div class="section-title">Browser</div>
                        <div class="form-group">
                            <label for="browser-background">
                                <input type="checkbox" id="browser-background" checked> Run Browser in Background
                            </label>
                            <small class="help-text">When enabled, the Playwright browser window is positioned off-screen so it doesn't steal focus. Disable to see the browser window for debugging.</small>
                        </div>
                        <div class="section-title">H-Manga</div>
                        <div class="form-group">
                            <label for="hmanga-download-path">H-Manga Download Path</label>
                            <input type="text" id="hmanga-download-path" required>
                            <small class="help-text">Directory where HentaiNexus book ZIPs are saved (separate from manga downloads).</small>
                        </div>
                        <div class="form-group">
                            <label for="verify-hm-downloads">
                                <input type="checkbox" id="verify-hm-downloads"> Verify H-Manga Downloads on Disk
                            </label>
                            <small class="help-text">When enabled, the server checks that ZIP files (or extracted folders) exist on disk during startup and manual scans. When disabled, the server trusts .books.json state.</small>
                        </div>
                        <div class="form-group">
                            <label for="hmanga-zip-regex">H-Manga ZIP Name Regex</label>
                            <input type="text" id="hmanga-zip-regex" placeholder="\[[^\]]*\]|_">
                            <small class="help-text">Regular expression applied to downloaded book ZIP names; every match is removed. Default removes [tag] groups and underscores. Leave empty (and save) to disable renaming.</small>
                        </div>
                        <div class="form-group">
                            <label for="hmanga-extract-zips">
                                <input type="checkbox" id="hmanga-extract-zips"> Extract H-Manga ZIPs After Download
                            </label>
                            <small class="help-text">When enabled, each downloaded ZIP is extracted into a folder with the same name as the ZIP (in the artist folder) and the ZIP is deleted once extraction succeeds.</small>
                        </div>
                        <div class="form-group">
                            <label for="hn-username">HentaiNexus Username</label>
                            <input type="text" id="hn-username" placeholder="username">
                            <small class="help-text">Required to download books. The login session persists in a separate browser profile.</small>
                        </div>
                        <div class="form-group">
                            <label for="hn-password">HentaiNexus Password</label>
                            <input type="password" id="hn-password" placeholder="password">
                            <small class="help-text">Stored in plaintext in config.json (local single-user tool).</small>
                        </div>
                        <button type="submit" class="btn btn-primary">Save Settings</button>
                    </form>
                </div>
            </section>
        </main>
    </div>

    <div id="toast" class="toast"></div>
    <script src="/static/app.js"></script>
</body>
</html>`

// Embedded CSS
const styleCSS = `
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f5f5f5;
    color: #333;
    line-height: 1.6;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
}

header {
    background: #fff;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

header h1 {
    margin-bottom: 15px;
    color: #2c3e50;
}

nav {
    display: flex;
    gap: 10px;
}

.global-actions {
    display: flex;
    gap: 10px;
    margin-top: 15px;
    padding-top: 15px;
    border-top: 1px solid #e9ecef;
}

[data-theme="dark"] .global-actions {
    border-top-color: #1a4b7a;
}

.nav-btn {
    padding: 10px 20px;
    border: none;
    background: #ecf0f1;
    color: #2c3e50;
    border-radius: 4px;
    cursor: pointer;
    transition: all 0.2s;
}

.nav-btn:hover {
    background: #bdc3c7;
}

.nav-btn.active {
    background: #3498db;
    color: white;
}

.tab-content {
    display: none;
}

.tab-content.active {
    display: block;
}

.card {
    background: #fff;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.card h2 {
    margin-bottom: 15px;
    color: #2c3e50;
}

.form-group {
    margin-bottom: 15px;
}

.form-group label {
    display: block;
    margin-bottom: 5px;
    font-weight: 500;
    color: #555;
}

.form-group input {
    width: 100%;
    padding: 10px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 14px;
}

.form-group input:focus {
    outline: none;
    border-color: #3498db;
}

.btn {
    padding: 10px 20px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.2s;
}

.btn-primary {
    background: #3498db;
    color: white;
}

.btn-primary:hover {
    background: #2980b9;
}

.btn-success {
    background: #27ae60;
    color: white;
}

.btn-success:hover {
    background: #219a52;
}

.btn-warning {
    background: #ff9800;
    color: white;
}

.btn-warning:hover {
    background: #e68900;
}

.btn-danger {
    background: #e74c3c;
    color: white;
}

.btn-danger:hover {
    background: #c0392b;
}

.btn-error {
    background: #f44336;
    color: white;
}

.btn-error:hover {
    background: #d32f2f;
}

.btn-secondary {
    background: #95a5a6;
    color: white;
}

.btn-secondary:hover {
    background: #7f8c8d;
}

.status-badge {
    margin-right: 8px;
    font-size: 14px;
    font-weight: bold;
}

.status-badge.completed {
    color: #27ae60;
}

.status-badge.pending {
    color: #95a5a6;
}

.chapter-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px;
    background: #fff;
    border-radius: 4px;
    margin-bottom: 5px;
    font-size: 14px;
}

.chapter-item span {
    flex: 1;
}

.series-list {
    display: flex;
    flex-direction: column;
    gap: 0;
    border: 1px solid #e9ecef;
    border-radius: 6px;
    overflow: hidden;
}

.series-list-header {
    display: grid;
    grid-template-columns: 2fr 100px 100px 100px 180px;
    gap: 10px;
    padding: 12px 15px;
    background: #f0f0f0;
    font-weight: 600;
    font-size: 13px;
    color: #555;
    border-bottom: 1px solid #e9ecef;
}

.series-item {
    display: grid;
    grid-template-columns: 2fr 100px 100px 100px 180px;
    gap: 10px;
    padding: 12px 15px;
    border-bottom: 1px solid #e9ecef;
    cursor: pointer;
    transition: background 0.2s;
    align-items: center;
}

.series-item:last-child {
    border-bottom: none;
}

.series-item:hover {
    background: #f8f9fa;
}

.series-item.expanded {
    background: #f0f7ff;
}

.series-item .title {
    font-weight: 500;
    color: #2c3e50;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.series-item .meta {
    font-size: 13px;
    color: #666;
    text-align: center;
}

.series-item .actions {
    display: flex;
    gap: 8px;
    justify-content: flex-end;
    align-items: center;
}

.series-item .actions .action-group {
    display: flex;
    gap: 8px;
    align-items: center;
}

.series-item .actions .interval-select {
    padding: 5px 10px;
    font-size: 12px;
    border: 1px solid #ddd;
    border-radius: 4px;
    background: #fff;
    color: #333;
    cursor: pointer;
    min-width: 70px;
}

.series-item .actions .check-status {
    font-size: 11px;
    color: #888;
    white-space: nowrap;
}

.series-item .actions .btn {
    padding: 5px 10px;
    font-size: 12px;
}

.form-group select {
    width: 100%;
    padding: 10px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 14px;
    background: #fff;
    color: #333;
    cursor: pointer;
}

.form-group select:focus {
    outline: none;
    border-color: #3498db;
}

.help-text {
    display: block;
    margin-top: 5px;
    font-size: 12px;
    color: #666;
}

.section-title {
    font-size: 16px;
    font-weight: 600;
    color: #2c3e50;
    margin: 20px 0 10px 0;
    padding-top: 15px;
    border-top: 1px solid #e9ecef;
}

.series-item-details {
    grid-column: 1 / -1;
    padding: 15px;
    background: #fff;
    border-top: 1px dashed #ddd;
    display: none;
}

.series-item.expanded .series-item-details {
    display: block;
}

.chapters-list {
    margin-top: 10px;
    max-height: 200px;
    overflow-y: auto;
}

.chapter-item .status {
    font-size: 12px;
    padding: 2px 8px;
    border-radius: 12px;
    background: #ecf0f1;
}

.chapter-item .status.downloaded {
    background: #d5f4e6;
    color: #27ae60;
}


.download-item .header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 10px;
}

.download-item .title {
    font-weight: 500;
}

.download-item .status {
    font-size: 12px;
    padding: 2px 8px;
    border-radius: 12px;
    background: #ecf0f1;
}

.download-item .progress-bar {
    height: 6px;
    background: #ecf0f1;
    border-radius: 3px;
    overflow: hidden;
}

.download-item .progress-fill {
    height: 100%;
    background: #3498db;
    transition: width 0.3s;
}

.download-item.completed .progress-fill {
    background: #27ae60;
}

.empty {
    color: #888;
    text-align: center;
    padding: 40px;
}

.console-output {
    background: #1e1e1e;
    color: #d4d4d4;
    font-family: Consolas, Menlo, monospace;
    font-size: 12px;
    line-height: 1.45;
    padding: 12px;
    border-radius: 6px;
    height: 70vh;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-all;
    margin: 0;
}

[data-theme="dark"] .console-output {
    border: 1px solid #1a4b7a;
}

.toast {
    position: fixed;
    bottom: 20px;
    right: 20px;
    padding: 15px 20px;
    background: #333;
    color: white;
    border-radius: 4px;
    opacity: 0;
    transform: translateY(20px);
    transition: all 0.3s;
    z-index: 1000;
}

.toast.show {
    opacity: 1;
    transform: translateY(0);
}

.toast.success {
    background: #27ae60;
}

.toast.error {
    background: #e74c3c;
}

/* Dark mode */
[data-theme="dark"] body {
    background: #1a1a2e;
    color: #eee;
}

[data-theme="dark"] header,
[data-theme="dark"] .card {
    background: #16213e;
    color: #eee;
}

[data-theme="dark"] header h1 {
    color: #eee;
}

[data-theme="dark"] .nav-btn {
    background: #0f3460;
    color: #eee;
}

[data-theme="dark"] .nav-btn:hover {
    background: #1a4b7a;
}

[data-theme="dark"] .nav-btn.active {
    background: #e94560;
}

[data-theme="dark"] .series-card {
    background: #0f3460;
    border-color: #1a4b7a;
}

[data-theme="dark"] .series-card h3 {
    color: #eee;
}

[data-theme="dark"] .chapter-item {
    background: #16213e;
}

[data-theme="dark"] .status-badge.completed {
    color: #4caf50;
}

[data-theme="dark"] .status-badge.pending {
    color: #888;
}

[data-theme="dark"] .series-item:hover {
    background: #1a4b7a;
}

[data-theme="dark"] .series-item.expanded {
    background: #1a3a5c;
}

[data-theme="dark"] .series-item-details {
    background: #16213e;
}

[data-theme="dark"] .series-item .title {
    color: #eee;
}

[data-theme="dark"] .series-item .meta {
    color: #aaa;
}

[data-theme="dark"] .series-list-header {
    background: #0f3460;
    color: #eee;
    border-bottom-color: #1a4b7a;
}

[data-theme="dark"] .series-list {
    border-color: #1a4b7a;
}

[data-theme="dark"] .series-item {
    border-bottom-color: #1a4b7a;
}

[data-theme="dark"] .empty {
    color: #aaa;
}

/* Downloads list styling */
.downloads-list-header {
    display: grid;
    grid-template-columns: 2fr 1.5fr 120px 100px 80px;
    gap: 15px;
    padding: 12px 15px;
    background: #f0f0f0;
    font-weight: 600;
    font-size: 13px;
    color: #555;
    border-bottom: 2px solid #ddd;
    margin-bottom: 10px;
}

.download-item {
    display: grid;
    grid-template-columns: 2fr 1.5fr 120px 100px 80px;
    gap: 15px;
    padding: 12px 15px;
    background: #f8f9fa;
    border-radius: 6px;
    margin-bottom: 8px;
    border-left: 4px solid #3498db;
    align-items: center;
}

.download-item.completed {
    border-left-color: #27ae60;
}

.download-item.failed {
    border-left-color: #e74c3c;
}

.download-item .download-series,
.download-item .download-chapter {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: 14px;
}

.download-item .progress-bar {
    height: 8px;
    background: #ecf0f1;
    border-radius: 4px;
    overflow: hidden;
}

.download-item .progress-fill {
    height: 100%;
    background: #3498db;
    transition: width 0.3s;
}

.download-item.completed .progress-fill {
    background: #27ae60;
}

.status-badge {
    font-size: 11px;
    padding: 4px 10px;
    border-radius: 12px;
    text-transform: uppercase;
    font-weight: 600;
    text-align: center;
    display: inline-block;
}

.status-badge.downloading {
    background: #3498db;
    color: white;
}

.status-badge.partial {
    background: #f39c12;
    color: white;
}

.status-badge.pending {
    background: #95a5a6;
    color: white;
}

.status-badge.completed {
    background: #27ae60;
    color: white;
}

.status-badge.failed {
    background: #e74c3c;
    color: white;
}

.time-since {
    font-size: 12px;
    color: #888;
}

/* Dark mode for downloads list */
[data-theme="dark"] .downloads-list-header {
    background: #0f3460;
    border-bottom-color: #1a4b7a;
    color: #eee;
}

[data-theme="dark"] .download-item {
    background: #0f3460;
}

[data-theme="dark"] .download-item .progress-bar {
    background: #1a4b7a;
}

[data-theme="dark"] .time-since {
    color: #aaa;
}

[data-theme="dark"] input {
    background: #0f3460;
    color: #eee;
    border-color: #1a4b7a;
}

[data-theme="dark"] select {
    background: #0f3460;
    color: #eee;
    border-color: #1a4b7a;
}

[data-theme="dark"] .help-text {
    color: #aaa;
}

[data-theme="dark"] .series-item .actions .interval-select {
    background: #0f3460;
    color: #eee;
    border-color: #1a4b7a;
}

[data-theme="dark"] .series-item .actions .check-status {
    color: #aaa;
}

[data-theme="dark"] .section-title {
    color: #eee;
    border-top-color: #1a4b7a;
}

[data-theme="dark"] input[type="checkbox"] {
    accent-color: #e94560;
}
`

// Embedded JavaScript
const appJS = `
// Dark mode toggle
const themeToggle = document.getElementById('theme-toggle');
const currentTheme = localStorage.getItem('theme') || 'light';

document.documentElement.setAttribute('data-theme', currentTheme);
updateThemeButton(currentTheme);

themeToggle.addEventListener('click', () => {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';

    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeButton(newTheme);
});

function updateThemeButton(theme) {
    themeToggle.textContent = theme === 'light' ? '🌙 Dark Mode' : '☀️ Light Mode';
}

// Tab navigation
document.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        
        btn.classList.add('active');
        document.getElementById(btn.dataset.tab + '-tab').classList.add('active');
        
        if (btn.dataset.tab === 'series') loadSeries();
        if (btn.dataset.tab === 'hmanga') loadHMArtists();
        if (btn.dataset.tab === 'downloads') loadDownloads();
        if (btn.dataset.tab === 'settings') loadSettings();
        if (btn.dataset.tab === 'console') startConsolePolling(); else stopConsolePolling();
    });
});

// Console tab: polls /api/console every 3s while the tab is active. Polling
// starts when the tab is opened and stops when any other tab is opened, so
// the server does not serve console snapshots that nobody is watching.
let consolePollTimer = null;

function startConsolePolling() {
    if (consolePollTimer) return;
    refreshConsole();
    consolePollTimer = setInterval(refreshConsole, 3000);
}

function stopConsolePolling() {
    if (consolePollTimer) {
        clearInterval(consolePollTimer);
        consolePollTimer = null;
    }
}

async function refreshConsole() {
    const out = document.getElementById('console-output');
    const meta = document.getElementById('console-meta');
    try {
        const response = await fetch('/api/console');
        const data = await response.json();
        const atBottom = out.scrollTop + out.clientHeight >= out.scrollHeight - 30;
        const text = data.lines.length ? data.lines.join('\n') : '';
        out.textContent = text || 'No console output yet.';
        meta.textContent = data.lines.length + ' lines' + (data.dropped > 0 ? ' (' + data.dropped + ' older lines discarded)' : '');
        if (atBottom) out.scrollTop = out.scrollHeight;
    } catch (err) {
        meta.textContent = 'Failed to refresh: ' + err;
    }
}

// Show toast notification
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = 'toast ' + type;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3000);
}

// Escape helpers used throughout the UI for safe HTML/attribute/onclick string construction
function escapeHtml(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
function escapeAttr(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
function escapeOnclick(s) { return (s || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '&quot;').replace(/</g, '&lt;'); }
function escapeJsString(s) { return escapeOnclick(s).replace(/\$/g, '\\$'); }
// Add series form
document.getElementById('add-series-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const url = document.getElementById('series-url').value;
    const customName = document.getElementById('custom-name').value;
    const checkInterval = document.getElementById('check-interval').value;
    
    try {
        const response = await fetch('/api/series', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url, customName, checkInterval })
        });
        
        if (response.ok) {
            showToast('Series added successfully!', 'success');
            document.getElementById('add-series-form').reset();
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
});

// Load series list
async function loadSeries() {
    try {
        const response = await fetch('/api/series');
        let series = await response.json();

        // Sort alphabetically by display name for consistent ordering
        series.sort((a, b) => {
            const nameA = (a.customName || a.title).toLowerCase();
            const nameB = (b.customName || b.title).toLowerCase();
            return nameA.localeCompare(nameB);
        });

        const container = document.getElementById('series-list');
        if (series.length === 0) {
            container.innerHTML = '<p class="empty">No series added yet.</p>';
            return;
        }

        let html = '<div class="series-list-header">' +
            '<span>Title</span>' +
            '<span style="text-align:center">Chapters</span>' +
            '<span style="text-align:center">Downloaded</span>' +
            '<span style="text-align:center">Status</span>' +
            '<span style="text-align:right">Actions</span>' +
        '</div>';

        html += series.map(s => {
            const status = s.chaptersDownloaded >= s.chapterCount ? 'Complete' :
                          s.chaptersDownloaded > 0 ? 'Partial' : 'Pending';
            const intervalDisplay = getIntervalDisplay(s.checkInterval);
            const lastCheckedDisplay = getLastCheckedDisplay(s.lastCheckedAt);
            const displayName = s.customName || s.title || '';
            const displayUrl = s.url || '';
            return '<div class="series-item" data-id="' + escapeJsString(s.id) + '" onclick="toggleSeriesDetails(\'' + escapeJsString(s.id) + '\')">' +
                '<span class="title" title="' + escapeAttr(displayName) + '">' + escapeHtml(displayName) + '</span>' +
                '<span class="meta">' + escapeHtml(String(s.chapterCount)) + '</span>' +
                '<span class="meta">' + escapeHtml(String(s.chaptersDownloaded)) + '</span>' +
                '<span class="meta">' + escapeHtml(status) + '</span>' +
                '<div class="actions" onclick="event.stopPropagation()">' +
                    '<div class="action-group">' +
                        '<select class="interval-select" id="interval-' + escapeJsString(s.id) + '" name="interval-' + escapeJsString(s.id) + '" onchange="updateCheckInterval(\'' + escapeJsString(s.id) + '\', this.value)" title="Auto-check frequency">' +
                            '<option value="never"' + (s.checkInterval === 'never' || !s.checkInterval ? ' selected' : '') + '>Never</option>' +
                            '<option value="5m"' + (s.checkInterval === '5m' ? ' selected' : '') + '>5m</option>' +
                            '<option value="15m"' + (s.checkInterval === '15m' ? ' selected' : '') + '>15m</option>' +
                            '<option value="30m"' + (s.checkInterval === '30m' ? ' selected' : '') + '>30m</option>' +
                            '<option value="1h"' + (s.checkInterval === '1h' ? ' selected' : '') + '>1h</option>' +
                            '<option value="6h"' + (s.checkInterval === '6h' ? ' selected' : '') + '>6h</option>' +
                            '<option value="12h"' + (s.checkInterval === '12h' ? ' selected' : '') + '>12h</option>' +
                            '<option value="24h"' + (s.checkInterval === '24h' ? ' selected' : '') + '>24h</option>' +
                        '</select>' +
                        '<span class="check-status" title="' + escapeAttr(lastCheckedDisplay.full) + '" >' + escapeHtml(lastCheckedDisplay.short) + '</span>' +
                    '</div>' +
                    '<button class="btn btn-primary" onclick="loadChapters(\'' + escapeJsString(s.id) + '\')" title="View Chapters">View</button>' +
                    '<button class="btn btn-success" onclick="downloadAllChapters(\'' + escapeJsString(s.id) + '\')" title="Download Missing">Sync</button>' +
                    '<button class="btn btn-warning" onclick="checkNow(\'' + escapeJsString(s.id) + '\')" title="Check for new chapters now">Check Now</button>' +
                        '<button class="btn btn-secondary" onclick="scanMissing(\'' + escapeJsString(s.id) + '\')" title="Scan for missing images in downloaded chapters and re-download them">Scan Missing</button>' +
                        '<button class="btn btn-danger" onclick="forceRedownload(\'' + escapeJsString(s.id) + '\', \'' + escapeJsString(displayName) + '\')" title="Delete all downloads and re-download from scratch">Force Redownload</button>' +
                        '<button class="btn btn-secondary" onclick="editSeriesURL(\'' + escapeJsString(s.id) + '\', \'' + escapeJsString(displayUrl) + '\')" title="Edit Source URL">Edit URL</button>' +
                        '<button class="btn btn-secondary" onclick="refreshMetadata(\'' + escapeJsString(s.id) + '\')" title="Refresh series metadata and cover">Refresh Metadata</button>' +
                        '<button class="btn btn-danger" onclick="removeSeries(\'' + escapeJsString(s.id) + '\', \'' + escapeJsString(displayName) + '\')" title="Remove Series">Remove</button>' +
                '</div>' +
                '<div class="series-item-details" id="chapters-' + escapeJsString(s.id) + '" ></div>' +
            '</div>';
        }).join('');

        container.innerHTML = html;
    } catch (err) {
        console.error('Failed to load series:', err);
    }
}

// Toggle series details expansion
function toggleSeriesDetails(seriesId) {
    const item = document.querySelector('.series-item[data-id="' + seriesId + '"]');
    if (item) {
        item.classList.toggle('expanded');
        if (item.classList.contains('expanded')) {
            loadChapters(seriesId);
        }
    }
}

// Load chapters for a series
async function loadChapters(seriesId) {
    const container = document.getElementById('chapters-' + seriesId);
    
    try {
        const response = await fetch('/api/series/' + seriesId + '/chapters');
        const chapters = await response.json();
        
        const stateResp = await fetch('/api/series/' + seriesId + '/state');
        const state = stateResp.ok ? await stateResp.json() : null;
        const stateChapters = state && state.chapters ? state.chapters : {};
        
        container.innerHTML = chapters.map(ch => {
            const chKey = String(ch.number);
            const chState = stateChapters[chKey];
            const isDownloaded = chState && chState.downloaded;
            const allFiltered = isDownloaded && chState.imageCount === 0 && chState.perImageStatus && chState.perImageStatus.length > 0 &&
                chState.perImageStatus.every(s => s.status === 'filtered');

            let btnClass, btnText, btnAction;
            const chNumNum = Number(ch.number);
            const chNumSafe = isFinite(chNumNum) ? chNumNum : JSON.stringify(ch.number);
            if (allFiltered) {
                btnClass = 'btn btn-warning';
                btnText = 'Retry Filtered';
                btnAction = 'redownloadChapter(\'' + escapeJsString(seriesId) + '\', ' + chNumSafe + ')';
            } else if (isDownloaded) {
                btnClass = 'btn btn-warning';
                btnText = 'Redownload';
                btnAction = 'redownloadChapter(\'' + escapeJsString(seriesId) + '\', ' + chNumSafe + ')';
            } else {
                btnClass = 'btn btn-success';
                btnText = 'Download';
                btnAction = 'downloadChapter(\'' + escapeJsString(seriesId) + '\', \'' + escapeJsString(ch.id) + '\')';
            }

            let statusBadge;
            if (allFiltered) {
                statusBadge = '<span class="status-badge pending" title="Skipped: all images filtered">&#8709;</span>';
            } else if (isDownloaded) {
                statusBadge = '<span class="status-badge completed" title="Downloaded">&#10003;</span>';
            } else {
                statusBadge = '<span class="status-badge pending" title="Not downloaded">&#9711;</span>';
            }

            // Build a note for intentionally filtered/skipped images so the user
            // can see why a chapter has fewer files than expected.
            let filterNote = '';
            if (chState && chState.perImageStatus && chState.perImageStatus.length > 0) {
                const filtered = chState.perImageStatus.filter(s => s.status === 'filtered');
                const failed = chState.perImageStatus.filter(s => s.status === 'failed');
                if (filtered.length > 0) {
                    const reasons = [...new Set(filtered.map(s => s.reason).filter(Boolean))];
                    const reasonText = reasons.length > 0 ? reasons.join('; ') : 'filtered by dimension/size checks';
                    filterNote = '<small class="help-text" style="color:var(--warning);margin-left:0.5rem;" title="' + escapeAttr(reasonText) + '">' + escapeHtml(String(filtered.length)) + ' skipped</small>';
                }
                if (failed.length > 0) {
                    const reasons = [...new Set(failed.map(s => s.reason).filter(Boolean))];
                    const reasonText = reasons.length > 0 ? reasons.join('; ') : 'download failed';
                    filterNote += '<small class="help-text" style="color:var(--error);margin-left:0.5rem;" title="' + escapeAttr(reasonText) + '">' + escapeHtml(String(failed.length)) + ' failed</small>';
                }
            }

            return '<div class="chapter-item">' +
                statusBadge +
                '<span>' + escapeHtml(ch.title || '') + filterNote + '</span>' +
                '<button class="' + escapeHtml(btnClass) + '" onclick="' + btnAction + '">' + escapeHtml(btnText) + '</button>' +
            '</div>';
        }).join('');
    } catch (err) {
        console.error('Failed to load chapters:', err);
    }
}

// Download all chapters for a series
async function downloadAllChapters(seriesId) {
    try {
        const response = await fetch('/api/series/' + seriesId + '/sync', {
            method: 'POST'
        });

        if (response.ok) {
            showToast('Downloading all chapters...', 'success');
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Get interval display text
function getIntervalDisplay(interval) {
    const displayMap = {
        'never': 'Never',
        '5m': '5 min',
        '15m': '15 min',
        '30m': '30 min',
        '1h': '1 hr',
        '6h': '6 hrs',
        '12h': '12 hrs',
        '24h': '24 hrs'
    };
    return displayMap[interval] || 'Never';
}

// Get last checked display
function getLastCheckedDisplay(lastCheckedAt) {
    if (!lastCheckedAt) {
        return { short: 'Never checked', full: 'Never checked' };
    }
    const date = new Date(lastCheckedAt);
    const now = Date.now();
    const diff = now - date.getTime();
    const minutes = Math.floor(diff / (1000 * 60));
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));

    let shortText, fullText = date.toLocaleString();

    if (minutes < 1) shortText = 'Just now';
    else if (minutes < 60) shortText = minutes + 'm ago';
    else if (hours < 24) shortText = hours + 'h ago';
    else shortText = days + 'd ago';

    return { short: shortText, full: fullText };
}

// Update check interval for a series
async function updateCheckInterval(seriesId, interval) {
    try {
        const response = await fetch('/api/series/' + seriesId + '/check-interval', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ checkInterval: interval })
        });

        if (response.ok) {
            showToast('Check interval updated to ' + getIntervalDisplay(interval), 'success');
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to update check interval:', err);
        showToast('Network error', 'error');
    }
}

// Check for updates now
async function checkNow(seriesId) {
    try {
        const response = await fetch('/api/series/' + seriesId + '/check-now', {
            method: 'POST'
        });

        if (response.ok) {
            showToast('Checking for updates...', 'success');
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to check now:', err);
        showToast('Network error', 'error');
    }
}

// Scan for missing images in downloaded chapters
// Recheck all series - refresh chapter lists and reconcile disk state
async function recheckAllSeries() {
    if (!confirm('Recheck all series?\n\nThis will refresh chapter lists from the website and update download status from disk for every series.')) {
        return;
    }

    try {
        showToast('Rechecking all series...', 'success');
        const response = await fetch('/api/series/recheck-all', {
            method: 'POST'
        });

        if (response.ok) {
            const result = await response.json();
            showToast(result.message || 'Recheck complete', 'success');
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to recheck all series:', err);
        showToast('Network error', 'error');
    }
}

// Sync all series - download missing chapters for every series
async function syncAllSeries() {
    if (!confirm('Sync all series?\n\nThis will download missing chapters for every tracked series.')) {
        return;
    }

    try {
        showToast('Syncing all series...', 'success');
        const response = await fetch('/api/series/sync-all', {
            method: 'POST'
        });

        if (response.ok) {
            showToast('All series sync started', 'success');
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to sync all series:', err);
        showToast('Network error', 'error');
    }
}

async function scanMissing(seriesId) {
    if (!confirm('Scan all downloaded chapters for missing images and re-download them?\n\nThis will check each chapter folder for gaps in image files and re-download any that are missing.')) {
        return;
    }

    try {
        showToast('Scanning for missing images...', 'success');
        const response = await fetch('/api/series/' + seriesId + '/scan-missing', {
            method: 'POST'
        });

        if (response.ok) {
            const result = await response.json();
            showToast(result.message || 'Scan complete', 'success');
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to scan missing images:', err);
        showToast('Network error', 'error');
    }
}

// Force redownload - delete and redownload a range of chapters
async function forceRedownload(seriesId, seriesName) {
    const rangeStr = prompt('Enter chapter range to force redownload for "' + seriesName + '":\n(e.g. "1-50" or "150-200")', '');
    if (rangeStr === null || rangeStr.trim() === '') {
        return;
    }

    const match = rangeStr.trim().match(/^(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)$/);
    if (!match) {
        showToast('Invalid range format. Use: start-end (e.g. 1-50)', 'error');
        return;
    }

    const fromChapter = parseFloat(match[1]);
    const toChapter = parseFloat(match[2]);
    if (isNaN(fromChapter) || isNaN(toChapter) || fromChapter > toChapter) {
        showToast('Invalid range: start must be <= end', 'error');
        return;
    }

    if (!confirm('Force redownload chapters ' + fromChapter + ' to ' + toChapter + ' of "' + seriesName + '"?\n\nThis will DELETE those chapter folders and re-download them. This cannot be undone!')) {
        return;
    }

    try {
        showToast('Force redownload started for chapters ' + fromChapter + '-' + toChapter + '...', 'success');
        const response = await fetch('/api/series/' + seriesId + '/force-redownload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ fromChapter, toChapter })
        });

        if (response.ok) {
            const result = await response.json();
            showToast(result.message || 'Force redownload started', 'success');
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to force redownload:', err);
        showToast('Network error', 'error');
    }
}

// Edit series URL
async function editSeriesURL(seriesId, currentURL) {
    const newURL = prompt('Enter new source URL for this series:', currentURL || '');
    if (newURL === null || newURL === currentURL || newURL.trim() === '') {
        return;
    }
    
    try {
        const response = await fetch('/api/series/' + seriesId, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url: newURL.trim() })
        });
        
        if (response.ok) {
            showToast('Series URL updated! Refreshing chapters...', 'success');
            // Refresh the series to get new chapters from new URL
            setTimeout(() => loadSeries(), 500);
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to edit series URL:', err);
        showToast('Network error', 'error');
    }
}

// Remove series
async function removeSeries(seriesId, seriesName) {
    if (!confirm('Are you sure you want to remove "' + seriesName + '"?\n\nThis will remove it from the library but NOT delete downloaded files.')) {
        return;
    }
    
    try {
        const response = await fetch('/api/series/' + seriesId, {
            method: 'DELETE'
        });
        
        if (response.ok) {
            showToast('Series removed!', 'success');
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to remove series:', err);
        showToast('Network error', 'error');
    }
}

// Refresh series metadata and cover
async function refreshMetadata(seriesId) {
    try {
        const response = await fetch('/api/series/' + seriesId + '/refresh-metadata', { method: 'POST' });
        if (response.ok) {
            const info = await response.json();
            showToast('Metadata refreshed: ' + (info.title || 'series'), 'success');
            loadSeries();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        console.error('Failed to refresh metadata:', err);
        showToast('Network error', 'error');
    }
}

// Download a chapter
async function downloadChapter(seriesId, chapterId) {
    try {
        const response = await fetch('/api/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ seriesId, chapterId })
        });
        
        if (response.ok) {
            showToast('Download started!', 'success');
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Redownload a single chapter (delete existing and re-download)
async function redownloadChapter(seriesId, chapterNumber) {
    if (!confirm('Redownload chapter ' + chapterNumber + '? This will delete existing files and re-download.')) {
        return;
    }
    try {
        const response = await fetch('/api/series/' + seriesId + '/redownload-chapter', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chapterNumber: chapterNumber })
        });
        if (response.ok) {
            showToast('Redownload started for chapter ' + chapterNumber, 'success');
            loadChapters(seriesId);
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Format time since update
function getTimeSince(dateString) {
    const date = new Date(dateString);
    const now = Date.now();
    const diff = now - date.getTime();
    const minutes = Math.floor(diff / (1000 * 60));
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));

    if (minutes < 1) return 'Just now';
    if (minutes < 60) return minutes + 'm ago';
    if (hours < 24) return hours + 'h ago';
    return days + 'd ago';
}

// Load downloads
async function loadDownloads() {
    try {
        const response = await fetch('/api/downloads');
        let downloads = await response.json();

        // Auto-cleanup: mark downloads as completed if no update in > 1 minute
        const now = Date.now();
        downloads = downloads.map(d => {
            const lastUpdate = new Date(d.updatedAt || d.createdAt).getTime();
            const minutesSinceUpdate = (now - lastUpdate) / (1000 * 60);

            // H-Manga book downloads report imageCount=1 and may take minutes.
            // Only auto-cleanup stalled downloads after 10 minutes with no update.
            const staleThreshold = d.imageCount === 1 ? 10 : 1;
            if (minutesSinceUpdate > staleThreshold && d.status === 'downloading') {
                // Check if actually complete based on progress
                if (d.progress >= 100 || d.downloadedCount >= d.imageCount) {
                    d.status = 'completed';
                } else if (d.downloadedCount > 0) {
                    d.status = 'partial';
                } else {
                    d.status = 'failed';
                }
            }
            return d;
        });

        // Sort: completed first (most recent first by updatedAt), then in-progress
        // (downloading), then pending -- alphabetical by series title within
        // pending. The server also sorts this way; the client-side sort here
        // is what actually drives rendering order, so both must agree.
        const isCompleted = d => d.status === 'completed';
        const isInProgress = d => d.status === 'downloading';
        const rank = d => isCompleted(d) ? 0 : isInProgress(d) ? 1 : 2;
        downloads.sort((a, b) => {
            const ra = rank(a);
            const rb = rank(b);
            if (ra !== rb) {
                return ra - rb;
            }
            if (ra === 0) {
                // Completed: most recent first.
                const ta = new Date(a.updatedAt || a.createdAt).getTime();
                const tb = new Date(b.updatedAt || b.createdAt).getTime();
                if (ta !== tb) {
                    return tb - ta;
                }
            }
            // In-progress and pending: alphabetical by series, then chapter.
            const titleA = (a.customName || a.seriesTitle).toLowerCase();
            const titleB = (b.customName || b.seriesTitle).toLowerCase();
            if (titleA !== titleB) {
                return titleA.localeCompare(titleB);
            }
            return (a.chapterTitle || '').localeCompare(b.chapterTitle || '');
        });

        const container = document.getElementById('downloads-list');
        if (downloads.length === 0) {
            container.innerHTML = '<p class="empty">No active downloads.</p>';
            return;
        }

        // Header row
        let html = '<div class="downloads-list-header">' +
            '<span>Series</span>' +
            '<span>Chapter</span>' +
            '<span>Progress</span>' +
            '<span>Status</span>' +
            '<span>Updated</span>' +
        '</div>';

        html += downloads.map(d => {
            const statusClass = d.status === 'completed' ? 'completed' : d.status === 'failed' ? 'failed' : '';
            const statusBadgeClass = 'status-badge ' + escapeHtml(d.status);
            const timeSince = getTimeSince(d.updatedAt || d.createdAt);
            const progress = isFinite(Number(d.progress)) ? Number(d.progress) : 0;
            return '<div class="download-item ' + escapeHtml(statusClass) + '">' +
                '<span class="download-series">' + escapeHtml(d.seriesTitle || '') + '</span>' +
                '<span class="download-chapter">' + escapeHtml(d.chapterTitle || '') + '</span>' +
                '<div class="progress-bar">' +
                    '<div class="progress-fill" style="width: ' + progress + '%"></div>' +
                '</div>' +
                '<span class="' + statusBadgeClass + '">' + escapeHtml(d.status || '') + '</span>' +
                '<span class="time-since">' + escapeHtml(timeSince) + '</span>' +
            '</div>';
        }).join('');

        container.innerHTML = html;
    } catch (err) {
        console.error('Failed to load downloads:', err);
    }
}

// Load settings
async function loadSettings() {
    try {
        const response = await fetch('/api/settings');
        const settings = await response.json();
        
        document.getElementById('download-path').value = settings.downloadPath;
        document.getElementById('rate-delay').value = settings.rateDelay;
        document.getElementById('min-image-size').value = settings.minImageSizeKB;
        document.getElementById('min-image-width').value = settings.minImageWidth;
        document.getElementById('min-image-height').value = settings.minImageHeight;
        document.getElementById('user-agent').value = settings.userAgent;
        document.getElementById('browser-background').checked = settings.browserBackground;
        document.getElementById('hmanga-download-path').value = settings.hmangaDownloadPath || '';
        document.getElementById('verify-hm-downloads').checked = settings.verifyHMDownloads || false;
        document.getElementById('hmanga-zip-regex').value = settings.hmangaZipNameRegex || '';
        document.getElementById('hmanga-extract-zips').checked = settings.hmangaExtractZips || false;
        document.getElementById('hn-username').value = settings.hentaiNexusUsername || '';
        // Password is redacted by the API; leave the field blank. A blank
        // field on save means "keep the stored password".
        document.getElementById('hn-password').value = '';
    } catch (err) {
        console.error('Failed to load settings:', err);
    }
}

// Update settings
document.getElementById('settings-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const settings = {
        downloadPath: document.getElementById('download-path').value,
        rateDelay: parseInt(document.getElementById('rate-delay').value),
        minImageSizeKB: parseInt(document.getElementById('min-image-size').value),
        minImageWidth: parseInt(document.getElementById('min-image-width').value),
        minImageHeight: parseInt(document.getElementById('min-image-height').value),
        userAgent: document.getElementById('user-agent').value,
        browserBackground: document.getElementById('browser-background').checked,
        hmangaDownloadPath: document.getElementById('hmanga-download-path').value,
        verifyHMDownloads: document.getElementById('verify-hm-downloads').checked,
        hmangaZipNameRegex: document.getElementById('hmanga-zip-regex').value,
        hmangaExtractZips: document.getElementById('hmanga-extract-zips').checked,
        hentaiNexusUsername: document.getElementById('hn-username').value,
        hentaiNexusPassword: document.getElementById('hn-password').value
    };
    
    try {
        const response = await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(settings)
        });
        
        if (response.ok) {
            showToast('Settings saved!', 'success');
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
});

// Clear finished downloads
async function clearFinishedDownloads() {
    try {
        const response = await fetch('/api/downloads/clear', { method: 'POST' });
        if (response.ok) {
            showToast('Finished downloads cleared', 'success');
            loadDownloads();
        } else {
            showToast('Failed to clear downloads', 'error');
        }
    } catch (err) {
        console.error('Failed to clear downloads:', err);
        showToast('Network error', 'error');
    }
}

// Poll downloads every 2 seconds
setInterval(() => {
    if (document.getElementById('downloads-tab').classList.contains('active')) {
        loadDownloads();
    }
}, 2000);

// Poll H-Manga list every 3 seconds while the tab is visible (so the
// background scrape/download progress is reflected without a manual refresh)
setInterval(() => {
    if (document.getElementById('hmanga-tab') && document.getElementById('hmanga-tab').classList.contains('active')) {
        loadHMArtists();
        document.querySelectorAll('#hmanga-list .series-item.expanded').forEach(item => {
            renderHMBooks(item.dataset.id);
        });
    }
}, 3000);

// ==================== H-Manga (HentaiNexus) ====================

// Add H-Manga artist form
document.getElementById('add-hmanga-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const url = document.getElementById('hmanga-url').value;
    const checkInterval = document.getElementById('hmanga-check-interval').value;
    try {
        const response = await fetch('/api/hmanga/artists', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url, checkInterval })
        });
        if (response.ok) {
            showToast('Artist added! Downloading books...', 'success');
            document.getElementById('add-hmanga-form').reset();
            loadHMArtists();
        } else {
            const error = await response.text();
            showToast('Error: ' + error, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
});

// Load H-Manga artists list
async function loadHMArtists() {
    try {
        const response = await fetch('/api/hmanga/artists');
        if (!response.ok) {
            console.error('Failed to load H-Manga artists:', response.status, response.statusText);
            return;
        }
        let artists = await response.json();
        if (!Array.isArray(artists)) {
            console.error('Invalid H-Manga artists response:', artists);
            return;
        }
        artists.sort((a, b) => (a.folderName || '').toLowerCase().localeCompare((b.folderName || '').toLowerCase()));

        renderHMArtists(artists);
    } catch (err) {
        console.error('Failed to load H-Manga artists:', err);
    }
}

// Toggle artist book list
function toggleHMArtistDetails(artistId) {
    const item = document.querySelector('.series-item[data-id="' + artistId + '"]');
    if (!item) return;
    item.classList.toggle('expanded');
    if (item.classList.contains('expanded')) {
        renderHMBooks(artistId);
    }
}

// Sync (download missing books)
async function syncHMArtist(artistId) {
    try {
        const response = await fetch('/api/hmanga/artists/' + artistId + '/sync', { method: 'POST' });
        if (response.ok) {
            showToast('Downloading missing books...', 'success');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Check now
async function checkHMArtistNow(artistId) {
    try {
        const response = await fetch('/api/hmanga/artists/' + artistId + '/check-now', { method: 'POST' });
        if (response.ok) {
            showToast('Checking for new books...', 'success');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Check all artists for new books
async function checkAllHMArtists() {
    if (!confirm('Check all artists?\n\nThis will re-scrape every artist for new books and start downloading them.')) {
        return;
    }

    try {
        showToast('Checking all artists...', 'success');
        const response = await fetch('/api/hmanga/artists/check-all', { method: 'POST' });
        if (response.ok) {
            showToast('All artists check started', 'success');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        console.error('Failed to check all artists:', err);
        showToast('Network error', 'error');
    }
}

// Sync all artists - download missing books for every artist
async function syncAllHMArtists() {
    if (!confirm('Sync all artists?\n\nThis will download missing books for every tracked artist.')) {
        return;
    }

    try {
        showToast('Syncing all artists...', 'success');
        const response = await fetch('/api/hmanga/artists/sync-all', { method: 'POST' });
        if (response.ok) {
            showToast('All artists sync started', 'success');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        console.error('Failed to sync all artists:', err);
        showToast('Network error', 'error');
    }
}

// Clean Up Zips: rename existing ZIPs per the configured name regex and
// extract them (when hmangaExtractZips is enabled), deleting the ZIPs.
async function cleanupHMZips() {
    if (!confirm('Clean up existing H-Manga ZIPs?\n\nThis scans every artist folder, renames ZIPs whose name still matches the configured regex (skipping when the target name already exists), and — if "Extract H-Manga ZIPs" is enabled in Settings — extracts each ZIP and deletes it. Existing .books.json entries are updated to match.')) {
        return;
    }

    try {
        showToast('Zip cleanup scan started...', 'success');
        const response = await fetch('/api/hmanga/artists/cleanup-zips', { method: 'POST' });
        if (response.ok) {
            const data = await response.json();
            showToast('Zip cleanup started for ' + (data.folders || 0) + ' artist folder(s)' + (data.extractMode ? ' (extract+delete enabled)' : ' (rename only)'), 'success');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        console.error('Failed to start zip cleanup:', err);
        showToast('Network error', 'error');
    }
}

// Download a single book
async function downloadHMBook(artistId, bookId) {
    try {
        const response = await fetch('/api/hmanga/artists/' + artistId + '/download-book', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bookId })
        });
        if (response.ok) {
            showToast('Download started!', 'success');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Scan missing
async function scanHMArtistMissing(artistId) {
    try {
        showToast('Scanning for missing books...', 'success');
        const response = await fetch('/api/hmanga/artists/' + artistId + '/scan-missing', { method: 'POST' });
        if (response.ok) {
            showToast('Scan complete', 'success');
            loadHMArtists();
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Force redownload
async function forceHMRedownload(artistId, artistName) {
    if (!confirm('Force redownload ALL books for "' + artistName + '"?\n\nThis will DELETE every downloaded ZIP and re-download them. This cannot be undone!')) {
        return;
    }
    try {
        showToast('Redownloading all books...', 'success');
        const response = await fetch('/api/hmanga/artists/' + artistId + '/force-redownload', { method: 'POST' });
        if (response.ok) {
            loadHMArtists();
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Remove artist
async function removeHMArtist(artistId, artistName) {
    if (!confirm('Remove artist "' + artistName + '"?\n\nThis removes the artist from tracking but keeps downloaded files on disk.')) {
        return;
    }
    try {
        const response = await fetch('/api/hmanga/artists/' + artistId, { method: 'DELETE' });
        if (response.ok) {
            showToast('Artist removed', 'success');
            loadHMArtists();
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Bulk-set rescan interval for a whole section. The backend skips items
// set to Never, so explicit opt-outs are preserved.
async function bulkSetInterval(section, interval) {
	if (!interval) return;
	const url = section === 'manga' ? '/api/series/bulk-check-interval' : '/api/hmanga/artists/bulk-check-interval';
	try {
		const response = await fetch(url, {
			method: 'PUT',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ checkInterval: interval })
		});
		if (response.ok) {
			const result = await response.json();
			showToast('Rescan interval set to ' + getIntervalDisplay(interval) + ' for ' + result.updated + ' item(s) (Never excluded)', 'success');
			if (section === 'manga') loadSeries(); else loadHMArtists();
		} else {
			const error = await response.text();
			showToast('Error: ' + error, 'error');
		}
	} catch (err) {
		showToast('Network error', 'error');
	}
}

// Update check interval
async function updateHMCheckInterval(artistId, interval) {
    try {
        const response = await fetch('/api/hmanga/artists/' + artistId + '/check-interval', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ checkInterval: interval })
        });
        if (response.ok) {
            showToast('Check interval updated to ' + getIntervalDisplay(interval), 'success');
            loadHMArtists();
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

// Global scraping toggles
async function toggleMangaScraping() {
    const btn = document.getElementById('manga-scrape-toggle');
    const currentlyRunning = btn.textContent.includes('Stop');
    try {
        const response = await fetch('/api/scraping/manga', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ running: !currentlyRunning })
        });
        if (response.ok) {
            const result = await response.json();
            updateScrapeButton(btn, result.running);
            showToast('Manga scraping ' + (result.running ? 'started' : 'stopped'), result.running ? 'success' : 'info');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

async function toggleHMangaScraping() {
    const btn = document.getElementById('hmanga-scrape-toggle');
    const currentlyRunning = btn.textContent.includes('Stop');
    try {
        const response = await fetch('/api/scraping/hmanga', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ running: !currentlyRunning })
        });
        if (response.ok) {
            const result = await response.json();
            updateScrapeButton(btn, result.running);
            showToast('H-Manga scraping ' + (result.running ? 'started' : 'stopped'), result.running ? 'success' : 'info');
        } else {
            const errText = await response.text();
            let errMsg = errText;
            try {
                const errJson = JSON.parse(errText);
                if (errJson.message) errMsg = errJson.message;
            } catch (e) {}
            showToast('Error: ' + errMsg, 'error');
        }
    } catch (err) {
        showToast('Network error', 'error');
    }
}

function updateScrapeButton(btn, running) {
    if (!btn) return;
    const label = btn.id === 'manga-scrape-toggle' ? 'Manga' : 'H-Manga';
    btn.textContent = (running ? 'Stop ' : 'Start ') + label + ' Scraping';
    btn.className = running ? 'btn btn-primary' : 'btn btn-secondary';
    btn.disabled = false;
}

async function loadScrapingStates() {
    try {
        const [mangaResp, hmangaResp] = await Promise.all([
            fetch('/api/scraping/manga'),
            fetch('/api/scraping/hmanga')
        ]);
        const mangaBtn = document.getElementById('manga-scrape-toggle');
        const hmangaBtn = document.getElementById('hmanga-scrape-toggle');
        if (mangaResp.ok) {
            const state = await mangaResp.json();
            updateScrapeButton(mangaBtn, state.running);
        } else {
            updateScrapeButton(mangaBtn, true);
        }
        if (hmangaResp.ok) {
            const state = await hmangaResp.json();
            updateScrapeButton(hmangaBtn, state.running);
        } else {
            updateScrapeButton(hmangaBtn, true);
        }
        if (mangaBtn) mangaBtn.disabled = false;
        if (hmangaBtn) hmangaBtn.disabled = false;
    } catch (err) {
        console.error('Failed to load scraping states:', err);
        const mangaBtn = document.getElementById('manga-scrape-toggle');
        const hmangaBtn = document.getElementById('hmanga-scrape-toggle');
        if (mangaBtn) { updateScrapeButton(mangaBtn, true); mangaBtn.disabled = false; }
        if (hmangaBtn) { updateScrapeButton(hmangaBtn, true); hmangaBtn.disabled = false; }
    }
}

function renderHMArtists(artists) {
    const container = document.getElementById('hmanga-list');
    if (!Array.isArray(artists) || artists.length === 0) {
        container.innerHTML = '<p class="empty">No artists added yet.</p>';
        window._hmLastArtistsJson = JSON.stringify(artists);
        return;
    }

    const json = JSON.stringify(artists);
    if (window._hmLastArtistsJson === json) return;
    window._hmLastArtistsJson = json;

    container.querySelectorAll('.empty').forEach(e => e.remove());

    let header = container.querySelector('.series-list-header');
    if (!header) {
        header = document.createElement('div');
        header.className = 'series-list-header';
        header.innerHTML = '<span>Artist</span><span style="text-align:center">Books</span><span style="text-align:center">Downloaded</span><span style="text-align:center">Status</span><span style="text-align:right">Actions</span>';
        container.appendChild(header);
    }

    const seen = new Set();
    const rows = Array.from(container.querySelectorAll('.series-item'));
    const rowMap = {};
    rows.forEach(r => { rowMap[r.dataset.id] = r; });

    let next = header.nextElementSibling;

    for (const a of artists) {
        seen.add(a.id);
        let item = rowMap[a.id];
        const status = a.scraping ? 'Scraping...' :
            (a.booksDownloaded >= a.bookCount && a.bookCount > 0 ? 'Complete' :
            a.booksDownloaded > 0 ? 'Partial' : 'Pending');
        const lastCheckedDisplay = getLastCheckedDisplay(a.lastCheckedAt);

        if (!item) {
            item = document.createElement('div');
            item.className = 'series-item';
            item.dataset.id = a.id;
            item.onclick = () => toggleHMArtistDetails(a.id);
            item.innerHTML =
                '<span class="title" title="' + escapeAttr(a.folderName) + '">' + escapeHtml(a.folderName) + '</span>' +
                '<span class="meta" data-field="bookCount">' + a.bookCount + '</span>' +
                '<span class="meta" data-field="booksDownloaded">' + a.booksDownloaded + '</span>' +
                '<span class="meta" data-field="status">' + status + '</span>' +
                '<div class="actions" onclick="event.stopPropagation()">' +
                    '<div class="action-group">' +
                        '<select class="interval-select" id="hm-interval-' + escapeJsString(a.id) + '" name="hm-interval-' + escapeJsString(a.id) + '" onchange="updateHMCheckInterval(\'' + escapeJsString(a.id) + '\', this.value)" title="Auto-check frequency">' +
                            '<option value="never"' + (a.checkInterval === 'never' || !a.checkInterval ? ' selected' : '') + '>Never</option>' +
                            '<option value="5m"' + (a.checkInterval === '5m' ? ' selected' : '') + '>5m</option>' +
                            '<option value="15m"' + (a.checkInterval === '15m' ? ' selected' : '') + '>15m</option>' +
                            '<option value="30m"' + (a.checkInterval === '30m' ? ' selected' : '') + '>30m</option>' +
                            '<option value="1h"' + (a.checkInterval === '1h' ? ' selected' : '') + '>1h</option>' +
                            '<option value="6h"' + (a.checkInterval === '6h' ? ' selected' : '') + '>6h</option>' +
                            '<option value="12h"' + (a.checkInterval === '12h' ? ' selected' : '') + '>12h</option>' +
                            '<option value="24h"' + (a.checkInterval === '24h' ? ' selected' : '') + '>24h</option>' +
                        '</select>' +
                        '<span class="check-status" data-full-ts="' + (a.lastCheckedAt || '') + '" title="' + lastCheckedDisplay.full + '">' + lastCheckedDisplay.short + '</span>' +
                    '</div>' +
                    '<button class="btn btn-success" onclick="syncHMArtist(\'' + escapeJsString(a.id) + '\')" title="Download missing books">Sync</button>' +
                    '<button class="btn btn-warning" onclick="checkHMArtistNow(\'' + escapeJsString(a.id) + '\')" title="Check for new books now">Check Now</button>' +
                    '<button class="btn btn-secondary" onclick="scanHMArtistMissing(\'' + escapeJsString(a.id) + '\')" title="Verify zip files on disk vs state">Scan Missing</button>' +
                    '<button class="btn btn-danger" onclick="forceHMRedownload(\'' + escapeJsString(a.id) + '\', \'' + escapeOnclick(a.folderName) + '\')" title="Delete all downloads and re-download from scratch">Force Redownload</button>' +
                    '<button class="btn btn-danger" onclick="removeHMArtist(\'' + escapeJsString(a.id) + '\', \'' + escapeOnclick(a.folderName) + '\')" title="Remove artist (keeps files)">Remove</button>' +
                '</div>' +
                '<div class="series-item-details" id="hmbooks-' + escapeJsString(a.id) + '"></div>';
        } else {
            const title = item.querySelector('.title');
            if (title) { title.textContent = a.folderName; title.title = a.folderName; }
            const bc = item.querySelector('[data-field="bookCount"]');
            if (bc) bc.textContent = a.bookCount;
            const bd = item.querySelector('[data-field="booksDownloaded"]');
            if (bd) bd.textContent = a.booksDownloaded;
            const st = item.querySelector('[data-field="status"]');
            if (st) st.textContent = status;
            const sel = item.querySelector('select');
            if (sel) sel.value = a.checkInterval || 'never';
            const cs = item.querySelector('.check-status');
            if (cs) {
                cs.setAttribute('data-full-ts', a.lastCheckedAt || '');
                cs.textContent = lastCheckedDisplay.short;
                cs.title = lastCheckedDisplay.full;
            }
            const btns = item.querySelectorAll('.actions button');
            btns.forEach(btn => {
                const oc = btn.getAttribute('onclick') || '';
                if (oc.indexOf('forceHMRedownload') === 0 || oc.indexOf('removeHMArtist') === 0) {
                    btn.setAttribute('onclick', oc.replace(/,\s*'[^']*'\s*\)$/, () => ', \'' + escapeJsString(a.folderName) + '\')'));
                }
            });
        }

        if (item !== next) {
            container.insertBefore(item, next);
        }
        next = item.nextElementSibling;
    }

    rows.forEach(r => { if (!seen.has(r.dataset.id)) r.remove(); });
}

async function renderHMBooks(artistId) {
    const container = document.getElementById('hmbooks-' + artistId);
    if (!container) return;
    try {
        const [booksResp, stateResp] = await Promise.all([
            fetch('/api/hmanga/artists/' + artistId + '/books'),
            fetch('/api/hmanga/artists/' + artistId + '/state')
        ]);
        const books = booksResp.ok ? await booksResp.json() : [];
        const state = stateResp.ok ? await stateResp.json() : null;
        const stateBooks = state && state.books ? state.books : {};

        if (!Array.isArray(books) || books.length === 0) {
            if (!container.querySelector('.empty')) {
                container.innerHTML = '<p class="empty">No books found.</p>';
            }
            delete container.dataset.lastJson;
            return;
        }

        const emptyMsg = container.querySelector('.empty');
        if (emptyMsg) emptyMsg.remove();

        const json = JSON.stringify(books) + '|' + JSON.stringify(stateBooks);
        if (container.dataset.lastJson === json) return;
        container.dataset.lastJson = json;

        const seen = new Set();
        const items = Array.from(container.querySelectorAll('.chapter-item'));
        const itemMap = {};
        items.forEach(el => { itemMap[el.dataset.bookId] = el; });

        let next = container.firstElementChild;

        for (const b of books) {
            seen.add(b.id);
            const bs = stateBooks[b.id];
            const isDownloaded = bs && bs.downloaded;
            const btnClass = isDownloaded ? 'btn btn-warning' : 'btn btn-success';
            const btnText = isDownloaded ? 'Redownload' : 'Download';
            const statusBadge = isDownloaded
                ? '<span class="status-badge completed" title="Downloaded">&#10003;</span>'
                : '<span class="status-badge pending" title="Not downloaded">&#9711;</span>';
            const html = statusBadge +
                '<span title="' + escapeAttr(b.title || '') + '">' + escapeHtml(b.title || b.id) + '</span>' +
                '<button class="' + btnClass + '" onclick="downloadHMBook(\'' + escapeJsString(artistId) + '\', \'' + escapeJsString(b.id) + '\')">' + btnText + '</button>';

            let el = itemMap[b.id];
            if (!el) {
                el = document.createElement('div');
                el.className = 'chapter-item';
                el.dataset.bookId = b.id;
                el.innerHTML = html;
                el.setAttribute('data-last-html', html);
            } else if (el.getAttribute('data-last-html') !== html) {
                el.setAttribute('data-last-html', html);
                el.innerHTML = html;
            }

            if (el !== next) {
                container.insertBefore(el, next);
            }
            next = el.nextElementSibling;
        }

        items.forEach(el => { if (!seen.has(el.dataset.bookId)) el.remove(); });
    } catch (err) {
        console.error('Failed to load H-Manga books:', err);
    }
}

// Initial load
loadSeries();
loadSettings();
loadScrapingStates();

// Recompute relative times for artist rows every minute so "3m ago" keeps ticking
// even when the underlying artist JSON hasn't changed.
setInterval(() => {
    if (!document.getElementById('hmanga-list')) return;
    document.querySelectorAll('#hmanga-list .series-item').forEach(item => {
        const cs = item.querySelector('.check-status');
        if (!cs) return;
        const full = cs.getAttribute('data-full-ts');
        if (!full) return;
        const disp = getLastCheckedDisplay(full);
        cs.textContent = disp.short;
        cs.title = disp.full;
    });
}, 60000);
`
