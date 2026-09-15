package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/user/comic-scraper/pkg/config"
	"github.com/user/comic-scraper/pkg/fileutil"
	"github.com/user/comic-scraper/pkg/hmanga"
	"github.com/user/comic-scraper/pkg/models"
)

// registerHMangaCallback wires the idle-timeout active-download callback and
// the live download progress callback. It is called during server startup
// after the manager exists.
func (s *Server) registerHMangaCallback() {
	s.hmangaMgr.SetProgressCallback(s.handleHMangaDownloadProgress)
	s.startHMangaOpDispatcher()
}

// extractHMangaArtistName parses the artist name from a HentaiNexus artist URL,
// delegating to the shared helper in the hmanga package.
func extractHMangaArtistName(artistURL string) string {
	return hmanga.ExtractArtistName(artistURL)
}

// handleHMangaDownloadProgress updates the shared Download record from
// lifecycle ticks emitted by the H-Manga manager. It is safe to call from any
// goroutine and tolerates missing records.
func (s *Server) handleHMangaDownloadProgress(p hmanga.HentaiNexusManagerDownloadProgress) {
	defer func() { recover() }()
	if p.Phase == "" || p.DownloadID == "" {
		return
	}

	// Download-record mutation happens under s.mu; queue signaling happens
	// after s.mu is released so the hot per-byte-tick callback doesn't pin
	// the server write lock while waiting on the queue lock.
	queuePhase := p.Phase
	queueDownloadID := p.DownloadID

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		dl, ok := s.downloads[p.DownloadID]
		if !ok || (dl.Status != models.StatusPending && dl.Status != models.StatusDownloading) {
			queuePhase = "" // suppress queue signaling for unknown/stale records
			return
		}

		// Ignore stale "downloading" ticks that arrive after a final phase has
		// already been applied to this record.
		if p.Phase == "downloading" && (dl.Phase == "verifying" || dl.Phase == "done" || dl.Phase == "failed" || dl.Phase == "cancelled" || dl.Phase == "superseded") {
			queuePhase = ""
			return
		}

		dl.Phase = p.Phase
		dl.BytesDownloaded = p.BytesDownloaded
		dl.TotalBytes = p.TotalBytes
		if dl.TotalBytes > 0 && dl.Status != models.StatusCompleted {
			prog := float64(dl.BytesDownloaded) / float64(dl.TotalBytes) * 100
			if prog > 99 {
				prog = 99
			}
			dl.Progress = prog
		}
		dl.UpdatedAt = time.Now()
		if p.SuggestedFilename != "" && dl.ChapterTitle == "" {
			dl.ChapterTitle = p.SuggestedFilename
		}

		switch p.Phase {
		case "starting", "downloading", "verifying":
			// Transition from pending to downloading once the first lifecycle
			// tick arrives, so the Downloads tab shows the active state
			// instead of "pending" for the whole download duration.
			if dl.Status == models.StatusPending {
				dl.Status = models.StatusDownloading
			}
		case "done":
			// The manager reports "done" once Playwright's SaveAs returns.
			// The Download record is intentionally NOT marked Completed
			// here: downloadHMBookWithID owns the Completed transition and
			// only fires it after the file is verified on disk and the
			// .books.json bookkeeping has been persisted. We still capture
			// the manager-reported size and progress so the UI shows the
			// final byte count.
			if p.TotalBytes > 0 {
				dl.TotalBytes = p.TotalBytes
			}
			if p.BytesDownloaded > 0 {
				dl.BytesDownloaded = p.BytesDownloaded
			}
		case "failed", "cancelled", "superseded":
			if p.Phase == "failed" {
				dl.Status = models.StatusFailed
			} else {
				dl.Status = models.StatusCancelled
			}
			delete(s.hmangaLastLoggedBytes, p.DownloadID)
		}

		if p.Phase == "downloading" && dl.BytesDownloaded != s.hmangaLastLoggedBytes[p.DownloadID] {
			s.hmangaLastLoggedBytes[p.DownloadID] = dl.BytesDownloaded
			title := dl.ChapterTitle
			if title == "" {
				title = "Book " + dl.ChapterID
			}
			if dl.TotalBytes > 0 {
				log.Printf("[HMANGA] Downloading %s: %.1f%% (%.2f MB / %.2f MB)", title, dl.Progress, float64(dl.BytesDownloaded)/(1024*1024), float64(dl.TotalBytes)/(1024*1024))
			} else {
				log.Printf("[HMANGA] Downloading %s: %.2f MB downloaded", title, float64(dl.BytesDownloaded)/(1024*1024))
			}
		}
	}()

	if queuePhase == "" {
		return
	}

	if queuePhase == "downloading" {
		s.hmangaOpQueueMu.Lock()
		opID, ok := s.hmangaOpByDlID[queueDownloadID]
		s.hmangaOpQueueMu.Unlock()
		if ok {
			s.markHMangaOpInProgress(opID)
			// Refresh the reaper's idle clock so a slow-but-alive download
			// with ticks still arriving is never force-failed by the
			// max-idle reaper.
			s.touchHMangaOpActivity(opID)
		}
	} else if queuePhase == "done" || queuePhase == "failed" || queuePhase == "cancelled" || queuePhase == "superseded" {
		s.hmangaOpQueueMu.Lock()
		opID, ok := s.hmangaOpByDlID[queueDownloadID]
		s.hmangaOpQueueMu.Unlock()
		if ok {
			status := hmangaOpDone
			if queuePhase != "done" {
				status = hmangaOpFailed
			}
			s.markHMangaOpFinished(opID, status)
		}
	}
}

// startHMangaProgressLogger and logActiveHMDownloads were removed: the
// live progress callback (handleHMangaDownloadProgress) already logs
// progress every 500ms during a download, making the 5-second polling
// logger redundant.

// safeGoTrack runs fn in a new goroutine and registers it with s.lifecycleActive
// so safe shutdown can wait for it to finish. Use this for background downloads.
func (s *Server) safeGoTrack(label string, fn func()) {
	s.lifecycleActive.Add(1)
	go func() {
		defer s.lifecycleActive.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[PANIC] recovered in %s: %v", label, r)
			}
		}()
		fn()
	}()
}

// registerHMangaRoutes wires all /api/hmanga/* endpoints onto mux.
func registerHMangaRoutes(mux *http.ServeMux, s *Server) {
	mux.HandleFunc("/api/hmanga/artists", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleListHMArtists(w, r)
		case http.MethodPost:
			s.handleAddHMArtist(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Global all-artist actions must be registered before the generic
	// per-artist prefix handler, otherwise "check-all" is parsed as an artist ID.
	mux.HandleFunc("/api/hmanga/artists/check-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if s.isHMangaScrapingPaused() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "paused", "message": "H-Manga scraping is paused"})
			return
		}
		s.handleCheckAllHMArtists(w, r)
	})
	mux.HandleFunc("/api/hmanga/artists/sync-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// sync-all only downloads known missing books and is allowed while paused.
		s.handleSyncAllHMArtists(w, r)
	})
	// Post-download cleanup over existing files: rename zips per the configured
	// hmangaZipNameRegex (only when the name still matches) and, when
	// hmangaExtractZips is enabled, extract them and delete the zip. Runs
	// while paused: it only touches files already on disk.
	mux.HandleFunc("/api/hmanga/artists/cleanup-zips", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleCleanupHMZips(w, r)
	})

	mux.HandleFunc("/api/scraping/hmanga", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleGetHMangaScrapingState(w, r)
		case http.MethodPost, http.MethodPut:
			s.handleToggleHMangaScraping(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/hmanga/artists/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/hmanga/artists/")
		parts := strings.Split(rest, "/")
		if len(parts) == 0 || parts[0] == "" {
			http.NotFound(w, r)
			return
		}
		artistID := parts[0]
		action := ""
		if len(parts) >= 2 {
			action = parts[1]
		}

		switch action {
		case "":
			if r.Method == http.MethodDelete {
				s.handleDeleteHMArtist(w, r, artistID)
				return
			}
			// The artist resource exists; only the method is wrong.
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		case "books":
			if r.Method == http.MethodGet {
				s.handleListHMBooks(w, r, artistID)
				return
			}
		case "state":
			if r.Method == http.MethodGet {
				s.handleGetHMArtistState(w, r, artistID)
				return
			}
		case "sync":
			// sync only downloads known missing books; it does not scrape the artist page,
			// so it is allowed even when global H-Manga scraping is paused.
			if r.Method == http.MethodPost {
				s.handleSyncHMArtist(w, r, artistID)
				return
			}
		case "check-now":
			// check-now re-scrapes the artist page, which is an auto/scraping
			// action, so it respects the global H-Manga pause.
			if r.Method == http.MethodPost {
				if s.isHMangaScrapingPaused() {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusServiceUnavailable)
					json.NewEncoder(w).Encode(map[string]string{"status": "paused", "message": "H-Manga scraping is paused"})
					return
				}
				s.handleCheckHMArtistNow(w, r, artistID)
				return
			}
		case "scan-missing", "force-redownload", "download-book":
			// scan-missing, force-redownload, and download-book only touch known
			// books and are manual actions; they are allowed while paused.
			if r.Method == http.MethodPost {
				switch action {
				case "scan-missing":
					s.handleScanHMArtistMissing(w, r, artistID)
				case "force-redownload":
					s.handleForceHMRedownload(w, r, artistID)
				case "download-book":
					s.handleDownloadHMBook(w, r, artistID)
				}
				return
			}
		case "check-interval":
			if r.Method == http.MethodPut {
				s.handleUpdateHMCheckInterval(w, r, artistID)
				return
			}
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})
}

// ==================== Registry load / save ====================

// getHMangaRegistryPath returns the path for the central H-Manga registry file.
func (s *Server) getHMangaRegistryPath() string {
	return filepath.Join(config.ConfigBaseDir(), "hmanga-registry.json")
}

// loadOrCreateHMangaRegistry loads the H-Manga registry from disk, creating an
// empty one if missing. Populates s.hmangaArtists from registry entries.
func (s *Server) loadOrCreateHMangaRegistry() error {
	path := s.getHMangaRegistryPath()
	registry := &hmanga.ArtistRegistry{Version: 1, Artists: []hmanga.Artist{}}

	// Always install empty in-memory state first so that even if the on-disk
	// file is corrupt, the server degrades gracefully instead of panicking
	// in the subsequent scanHMangaArtists.
	s.hmangaRegistry = registry
	s.hmangaArtists = make(map[string]*hmanga.Artist)

	needsSave := false // true only when the file is missing, corrupt, or migrated
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, registry); err != nil {
			log.Printf("Warning: corrupt H-Manga registry (%v), starting with empty registry", err)
			registry = &hmanga.ArtistRegistry{Version: 1, Artists: []hmanga.Artist{}}
			s.hmangaRegistry = registry
			s.hmangaArtists = make(map[string]*hmanga.Artist)
			needsSave = true
		} else {
			if registry.Version == 0 {
				registry.Version = 1
				needsSave = true
			}
		}
	} else {
		// File missing Ã¢â‚¬â€ will be created on first save.
		needsSave = true
	}

	for i := range registry.Artists {
		a := &registry.Artists[i]
		// Scraping is transient: a value written mid-scrape must not survive
		// a restart (the UI would show a stuck "Scraping..." status).
		if a.Scraping {
			a.Scraping = false
			needsSave = true
		}
		// Defense-in-depth: a hand-edited or migrated registry could contain a
		// non-HentaiNexus URL. Log and clear it so a later check-now / sync
		// doesn't drive the logged-in browser at an arbitrary host; the artist
		// entry is preserved so the user can re-add a valid URL via the UI.
		if a.URL != "" && !hmanga.IsHentaiNexusURL(a.URL) {
			log.Printf("[HMANGA] Registry entry %s has non-HentaiNexus URL %q, clearing", a.ID, a.URL)
			a.URL = ""
			needsSave = true
		}
		s.hmangaArtists[a.ID] = a
	}
	if needsSave {
		return s.saveHMangaRegistry()
	}
	return nil
}

// saveHMangaRegistry persists the H-Manga registry to disk. Caller must hold
// s.mu OR otherwise serialize. Most callers use s.mu.
func (s *Server) saveHMangaRegistry() error {
	path := s.getHMangaRegistryPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.hmangaRegistry, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// updateHMangaRegistryEntry safely updates an artist registry entry by ID.
func (s *Server) updateHMangaRegistryEntry(artistID string, updateFn func(*hmanga.Artist)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.hmangaRegistry.Artists {
		if s.hmangaRegistry.Artists[i].ID == artistID {
			updateFn(&s.hmangaRegistry.Artists[i])
			if a, ok := s.hmangaArtists[artistID]; ok {
				*a = s.hmangaRegistry.Artists[i]
			} else {
				s.hmangaArtists[artistID] = &s.hmangaRegistry.Artists[i]
			}
			s.saveHMangaRegistry()
			return true
		}
	}
	return false
}

// getHMangaGlobalCachePath returns the path for the global H-Manga book cache.
func (s *Server) getHMangaGlobalCachePath() string {
	return filepath.Join(s.getGlobalBooksDataDir(), "hmanga-global-books.json")
}

// loadHMangaGlobalCache loads the global H-Manga book cache from disk, if present
// and of a supported version. Unsupported versions are treated as missing so
// the caller rebuilds.
func (s *Server) loadHMangaGlobalCache() (*hmanga.GlobalBookCache, error) {
	path := s.getHMangaGlobalCachePath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cache hmanga.GlobalBookCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}
	if cache.Version != 1 {
		return nil, fmt.Errorf("unsupported global cache version %d", cache.Version)
	}
	return &cache, nil
}

// saveHMangaGlobalCache persists the global H-Manga book cache to disk.
func (s *Server) saveHMangaGlobalCache(cache *hmanga.GlobalBookCache) error {
	path := s.getHMangaGlobalCachePath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// rebuildHMangaGlobalCache rebuilds the global cache from all in-memory
// per-artist .books.json state. It preserves entries for artists no longer
// tracked (e.g. deleted) so shared-book dedup remains valid, but drops entries
// whose owning artist is still tracked and no longer marks the book as
// downloaded (so VerifyHMDownloads can actually re-download missing books).
// Existing entries are kept on conflict (first owner wins).
func (s *Server) rebuildHMangaGlobalCache() {
	s.hmangaStateMu.Lock()
	defer s.hmangaStateMu.Unlock()
	s.hmangaGlobalMu.Lock()
	defer s.hmangaGlobalMu.Unlock()

	cache := &hmanga.GlobalBookCache{Version: 1, Books: map[string]hmanga.GlobalBookEntry{}}
	trackedArtists := map[string]bool{}
	for id := range s.hmangaBookStates {
		trackedArtists[id] = true
	}

	// Start from the existing in-memory global cache so entries for deleted
	// artists are not lost. Entries are overwritten below from current artist
	// state only when the per-artist state says the book is downloaded.
	for bookID, entry := range s.hmangaGlobalBooks {
		cache.Books[bookID] = entry
	}

	for _, st := range s.hmangaBookStates {
		if st == nil {
			continue
		}
		for bookID, b := range st.Books {
			if !b.Downloaded || b.Filename == "" {
				// If this artist owns the global entry but the book is no longer
				// downloaded, remove the stale global entry so it can be re-downloaded.
				if existing, ok := cache.Books[bookID]; ok && existing.OwnerArtistID == st.ArtistID {
					delete(cache.Books, bookID)
					delete(s.hmangaGlobalBooks, bookID)
				}
				continue
			}
			if existing, ok := cache.Books[bookID]; ok {
				// Keep first owner. Update the in-memory copy to match.
				s.hmangaGlobalBooks[bookID] = existing
				continue
			}
			entry := hmanga.GlobalBookEntry{
				Downloaded:      true,
				Filename:        b.Filename,
				OwnerArtistID:   st.ArtistID,
				OwnerFolderName: st.FolderName,
				DownloadedAt:    b.DownloadedAt,
				FileSize:        b.FileSize,
				Extracted:       b.Extracted,
				DirName:         b.DirName,
			}
			cache.Books[bookID] = entry
			s.hmangaGlobalBooks[bookID] = entry
		}
	}
	// Also drop global entries whose owning artist is tracked but no longer
	// present in any .books.json (all books removed by verification).
	for bookID, entry := range cache.Books {
		if trackedArtists[entry.OwnerArtistID] {
			owned := false
			st := s.hmangaBookStates[entry.OwnerArtistID]
			if st != nil {
				for _, b := range st.Books {
					if b.Downloaded && b.Filename == entry.Filename {
						owned = true
						break
					}
				}
			}
			if !owned {
				delete(cache.Books, bookID)
				delete(s.hmangaGlobalBooks, bookID)
			}
		}
	}
	if err := s.saveHMangaGlobalCache(cache); err != nil {
		log.Printf("[HMANGA] Failed to save global cache: %v", err)
	}
}

// syncGlobalCacheForArtist mirrors an artist's .books.json state into the
// in-memory global cache. Caller must hold hmangaStateMu and hmangaGlobalMu in
// that order.
//
// This helper updates only the in-memory cache. Persistence to
// hmanga-global-books.json is the caller's responsibility: most callers use
// saveAndSyncHMangaBookState (which writes after the locks are released),
// while recordHMBookDownloaded writes the global cache itself before
// releasing hmangaGlobalMu.
func (s *Server) syncGlobalCacheForArtist(st *hmanga.BookStateFile) {
	if st == nil {
		return
	}
	for bookID, b := range st.Books {
		if !b.Downloaded {
			continue
		}
		if existing, ok := s.hmangaGlobalBooks[bookID]; ok {
			if existing.OwnerArtistID == st.ArtistID {
				existing.OwnerFolderName = st.FolderName
				s.hmangaGlobalBooks[bookID] = existing
			}
			continue
		}
		s.hmangaGlobalBooks[bookID] = hmanga.GlobalBookEntry{
			Downloaded:      true,
			Filename:        b.Filename,
			OwnerArtistID:   st.ArtistID,
			OwnerFolderName: st.FolderName,
			DownloadedAt:    b.DownloadedAt,
			FileSize:        b.FileSize,
			Extracted:       b.Extracted,
			DirName:         b.DirName,
		}
	}
}

// saveGlobalBookCache writes the current in-memory global cache to disk. Callers
// that already hold hmangaGlobalMu can call this helper; otherwise use
// saveAndPersistHMangaGlobalCache which acquires the lock.
func (s *Server) saveGlobalBookCache() {
	cache := &hmanga.GlobalBookCache{Version: 1, Books: map[string]hmanga.GlobalBookEntry{}}
	for bookID, entry := range s.hmangaGlobalBooks {
		cache.Books[bookID] = entry
	}
	if err := s.saveHMangaGlobalCache(cache); err != nil {
		log.Printf("[HMANGA] Failed to save global cache: %v", err)
	}
}

// getHMangaBookState returns the in-memory .books.json for an artist,
// loading from disk once and caching it if needed. It may return nil.
func (s *Server) getHMangaBookState(folderName string) *hmanga.BookStateFile {
	s.hmangaStateMu.Lock()
	defer s.hmangaStateMu.Unlock()
	for _, st := range s.hmangaBookStates {
		if st != nil && st.FolderName == folderName {
			return st
		}
	}
	st, err := s.loadHMangaBookState(folderName)
	if err != nil {
		log.Printf("[HMANGA] Failed to load .books.json for %s: %v", folderName, err)
		return nil
	}
	if st == nil {
		return nil
	}
	st.FolderName = folderName
	s.hmangaBookStates[st.ArtistID] = st
	return st
}

// saveAndSyncHMangaBookState writes the per-artist state to disk, updates the
// in-memory cache, mirrors any downloaded entries to the global cache, and
// persists the global cache. Acquires hmangaStateMu then hmangaGlobalMu.
func (s *Server) saveAndSyncHMangaBookState(st *hmanga.BookStateFile) {
	s.hmangaStateMu.Lock()
	defer s.hmangaStateMu.Unlock()
	if err := s.saveHMangaBookState(st.FolderName, st); err != nil {
		log.Printf("[HMANGA] Failed to save .books.json for %s: %v", st.FolderName, err)
		return
	}
	if st.ArtistID != "" {
		s.hmangaBookStates[st.ArtistID] = st
	}
	s.hmangaGlobalMu.Lock()
	defer s.hmangaGlobalMu.Unlock()
	s.syncGlobalCacheForArtist(st)
	s.saveGlobalBookCache()
}

// ==================== Per-artist book state (.books.json) ====================

// getHMangaBookStatePath returns the path for an artist's .books.json file.
func (s *Server) getHMangaBookStatePath(folderName string) string {
	return filepath.Join(s.config.HMangaDownloadPath, folderName, ".books.json")
}

// loadHMangaBookState loads the .books.json for an artist folder.
func (s *Server) loadHMangaBookState(folderName string) (*hmanga.BookStateFile, error) {
	data, err := os.ReadFile(s.getHMangaBookStatePath(folderName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var st hmanga.BookStateFile
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// saveHMangaBookState saves the .books.json for an artist folder.
func (s *Server) saveHMangaBookState(folderName string, st *hmanga.BookStateFile) error {
	dir := filepath.Join(s.config.HMangaDownloadPath, folderName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.getHMangaBookStatePath(folderName), data, 0644)
}

// ==================== Folder scanning ====================

// renameHMangaArtistFolders renames legacy artist folders whose names still
// contain URL-encoding artifacts (e.g. "%22Kurosu+Gatari%22" -> "Kurosu Gatari")
// or other erroneous characters. It cleans each folder name via
// hmanga.CleanArtistName + fileutil.SanitizeFileName and renames on disk if the
// cleaned name differs. Collisions (cleaned name already exists) are skipped
// to avoid overwriting. After a successful rename, both the .books.json inside
// the folder, any matching registry entry, and the global cache's
// OwnerFolderName are updated so cross-artist references stay valid.
//
// Lock discipline: this function takes hmangaStateMu, s.mu, and
// hmangaGlobalMu SEQUENTIALLY (never nested). The other H-Manga paths that
// hold them nested (e.g. recordHMBookDownloaded,
// rebuildHMangaGlobalCache) always use the order
// hmangaStateMu -> hmangaGlobalMu -> s.mu, so sequential-only acquisition
// here cannot deadlock them regardless of the order chosen.
func (s *Server) renameHMangaArtistFolders(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		oldName := e.Name()
		// Clean the raw name: URL-decode, strip quotes, then re-sanitize for
		// the filesystem.
		cleaned := hmanga.CleanArtistName(oldName)
		cleaned = fileutil.SanitizeFileName(cleaned)
		if cleaned == "" || cleaned == "." || cleaned == ".." || strings.ContainsAny(cleaned, `/\`) {
			continue
		}
		if cleaned == oldName {
			continue // already clean
		}
		newPath := filepath.Join(root, cleaned)
		// Don't clobber an existing clean folder.
		if _, err := os.Stat(newPath); err == nil {
			log.Printf("[HMANGA] Skipping rename of %q -> %q (target already exists)", oldName, cleaned)
			continue
		}
		oldPath := filepath.Join(root, oldName)
		if err := os.Rename(oldPath, newPath); err != nil {
			log.Printf("[HMANGA] Failed to rename %q -> %q: %v", oldName, cleaned, err)
			continue
		}
		log.Printf("[HMANGA] Renamed artist folder %q -> %q", oldName, cleaned)

		// Update the .books.json inside the renamed folder so its FolderName
		// matches the new directory name; otherwise the backend will keep
		// looking for the old folder name and lose track of downloads.
		st, err := s.loadHMangaBookState(cleaned)
		if err == nil && st != nil {
			st.FolderName = cleaned
			s.hmangaStateMu.Lock()
			s.saveHMangaBookState(cleaned, st)
			s.hmangaStateMu.Unlock()
		}

		// Determine the renamed artist's ID under s.mu while we still need it
		// for the global cache update. Per-artist state was already saved
		// above; this block updates the registry/in-memory artist map.
		s.mu.Lock()
		var renamedArtistID string
		for i := range s.hmangaRegistry.Artists {
			if s.hmangaRegistry.Artists[i].FolderName == oldName {
				s.hmangaRegistry.Artists[i].FolderName = cleaned
				renamedArtistID = s.hmangaRegistry.Artists[i].ID
			}
		}
		for _, a := range s.hmangaArtists {
			if a.FolderName == oldName {
				a.FolderName = cleaned
			}
		}
		s.saveHMangaRegistry()
		s.mu.Unlock()

		// Update global cache entries owned by the renamed artist.
		if renamedArtistID != "" {
			s.hmangaGlobalMu.Lock()
			for bookID, entry := range s.hmangaGlobalBooks {
				if entry.OwnerArtistID == renamedArtistID {
					entry.OwnerFolderName = cleaned
					s.hmangaGlobalBooks[bookID] = entry
				}
			}
			s.saveGlobalBookCache()
			s.hmangaGlobalMu.Unlock()
			if st != nil {
				s.saveAndSyncHMangaBookState(st)
			}
		}
	}
}

// scanHMangaArtists walks the H-Manga download directory for artist folders
// with a .books.json and rebuilds in-memory state. It first renames any
// legacy folders whose names still contain URL-encoding artifacts (e.g.
// "%22Kurosu+Gatari%22" -> "Kurosu Gatari"). ZIP verification is deferred to
// verifyHMBookZips so the startup scan stays cheap.
func (s *Server) scanHMangaArtists() error {
	root := s.config.HMangaDownloadPath
	// Pass 1: rename legacy folders with erroneous characters.
	s.renameHMangaArtistFolders(root)

	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		folderName := e.Name()
		st := s.getHMangaBookState(folderName)
		if st == nil {
			continue
		}

		// Read/derive everything from st.Books under hmangaStateMu — this scan
		// runs concurrently with startup ops that mutate the same state files.
		s.hmangaStateMu.Lock()
		// Trust .books.json for the initial scan; verification runs separately
		// when enabled so this scan is a single cheap read.
		downloaded := 0
		for _, b := range st.Books {
			if b.Downloaded {
				downloaded++
			}
		}
		// Ensure registry entry exists for this folder.
		artistID := st.ArtistID
		if artistID == "" {
			artistID = generateHMangaID(folderName)
			st.ArtistID = artistID
			s.saveHMangaBookState(folderName, st)
		}
		// The folder on disk is the source of truth; make sure the state file
		// and registry both use the actual folder name (covers legacy renames).
		st.FolderName = folderName
		s.hmangaBookStates[artistID] = st

		// Build an in-memory book cache from the loaded state so the UI can list
		// books for existing artists without waiting for a fresh scrape. Sort by
		// book ID descending so the order matches a fresh scrape (newest first).
		ids := make([]string, 0, len(st.Books))
		for id := range st.Books {
			ids = append(ids, id)
		}
		bookCache := make([]hmanga.Book, 0, len(st.Books))
		for _, id := range ids {
			b := st.Books[id]
			bookCache = append(bookCache, hmanga.Book{ID: id, Title: b.Title, URL: b.URL})
		}
		bookCount := len(st.Books)
		s.hmangaStateMu.Unlock()

		sort.Slice(bookCache, func(i, j int) bool { return bookCache[i].ID > bookCache[j].ID })
		s.mu.Lock()
		s.hmangaBooks[artistID] = bookCache
		if a, ok := s.hmangaArtists[artistID]; ok {
			a.BookCount = bookCount
			a.BooksDownloaded = downloaded
			a.FolderName = folderName
			a.UpdatedAt = time.Now()
		} else {
			s.hmangaRegistry.Artists = append(s.hmangaRegistry.Artists, hmanga.Artist{
				ID:              artistID,
				FolderName:      folderName,
				URL:             st.URL,
				BookCount:       bookCount,
				BooksDownloaded: downloaded,
				AddedAt:         time.Now(),
				UpdatedAt:       time.Now(),
			})
			s.hmangaArtists[artistID] = &s.hmangaRegistry.Artists[len(s.hmangaRegistry.Artists)-1]
		}
		s.mu.Unlock()
	}
	// Persist the registry once after the scan instead of once per folder.
	s.mu.Lock()
	s.saveHMangaRegistry()
	s.mu.Unlock()
	return nil
}

// verifyHMBookZips checks that every downloaded book's ZIP file actually exists
// on disk and resets missing ones to not-downloaded. It updates both the
// in-memory state and the global cache.
func (s *Server) verifyHMBookZips() {
	if !s.config.VerifyHMDownloads {
		return
	}
	s.hmangaStateMu.Lock()
	root := s.config.HMangaDownloadPath
	for artistID, st := range s.hmangaBookStates {
		if st == nil {
			continue
		}
		if !s.reconcileHMArtistStateLocked(st, root) {
			continue
		}
		if err := s.saveHMangaBookState(st.FolderName, st); err != nil {
			log.Printf("[HMANGA] Failed to save verified state for %s: %v", st.FolderName, err)
			continue
		}
		s.hmangaBookStates[artistID] = st
		downloaded := countDownloadedBooks(st)
		s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
			a.BooksDownloaded = downloaded
			a.UpdatedAt = time.Now()
		})
	}
	s.hmangaStateMu.Unlock()

	// Rebuild the global cache so books reset above are removed from cross-artist
	// deduplication state until they are re-downloaded.
	s.rebuildHMangaGlobalCache()
}

// reconcileHMArtistStateLocked resets any downloaded book entries whose
// artifact (ZIP, or extracted directory for Extracted entries) is missing on
// disk. A ZIP that is missing but whose same-named extracted directory exists
// is adopted as extracted instead of reset, so manually extracted books are
// not re-downloaded. Returns true if any book was changed.
// Caller must hold s.hmangaStateMu.
func (s *Server) reconcileHMArtistStateLocked(st *hmanga.BookStateFile, root string) bool {
	if st == nil {
		return false
	}
	changed := false
	for id, b := range st.Books {
		if !b.Downloaded || b.Filename == "" {
			continue
		}
		if b.Extracted {
			// The artifact is the extracted directory named after the ZIP.
			if s.bookExtractedDirExists(st.FolderName, b.DirName) {
				continue
			}
			// Directory gone but the ZIP reappeared (e.g. user re-added it)?
			// Fall back to the zip check below with Extracted cleared.
			b.Extracted = false
			b.DirName = ""
		}
		zipPath := filepath.Join(root, st.FolderName, b.Filename)
		if _, err := os.Stat(zipPath); err == nil {
			if b.Extracted {
				// Zip reappeared; persist the Extracted/DirName reset above.
				st.Books[id] = b
				changed = true
			}
			continue
		}
		// ZIP missing: adopt the same-named extracted directory if present.
		dirName := strings.TrimSuffix(b.Filename, filepath.Ext(b.Filename))
		if dirName != "" && s.bookExtractedDirExists(st.FolderName, dirName) {
			b.Extracted = true
			b.DirName = dirName
			st.Books[id] = b
			changed = true
			continue
		}
		b.Downloaded = false
		b.Filename = ""
		b.DownloadedAt = time.Time{}
		b.FileSize = 0
		b.Extracted = false
		b.DirName = ""
		st.Books[id] = b
		changed = true
	}
	return changed
}

// countDownloadedBooks returns how many books in the state file are marked
// as downloaded. Caller must hold s.hmangaStateMu (so Books can't be
// mutated concurrently) — not asserted, but required for correctness.
func countDownloadedBooks(st *hmanga.BookStateFile) int {
	if st == nil {
		return 0
	}
	n := 0
	for _, b := range st.Books {
		if b.Downloaded {
			n++
		}
	}
	return n
}

// generateHMangaID creates a consistent ID from a string (FNV-64a hash).
func generateHMangaID(name string) string {
	h := fnv.New64a()
	h.Write([]byte(name))
	return fmt.Sprintf("hmn-%x", h.Sum64())
}

// ==================== Handlers ====================

// handleListHMArtists returns all tracked H-Manga artists.
func (s *Server) handleListHMArtists(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	list := make([]*hmanga.Artist, 0, len(s.hmangaArtists))
	for _, a := range s.hmangaArtists {
		list = append(list, a)
	}
	s.mu.RUnlock()

	sort.Slice(list, func(i, j int) bool {
		return strings.ToLower(list[i].FolderName) < strings.ToLower(list[j].FolderName)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// findExistingHMArtistFolder looks for a folder on disk whose sanitized name
// matches the candidate sanitized name. If one exists, that folder name is
// returned so the artist re-uses the existing subdirectory. A matching folder
// that also contains a .books.json is preferred over one without it.
func (s *Server) findExistingHMArtistFolder(candidate string) string {
	root := s.config.HMangaDownloadPath
	entries, err := os.ReadDir(root)
	if err != nil {
		return candidate
	}
	want := strings.ToLower(fileutil.SanitizeFileName(candidate))
	fallback := ""
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		name := e.Name()
		clean := strings.ToLower(fileutil.SanitizeFileName(name))
		if clean != want {
			continue
		}
		statePath := filepath.Join(root, name, ".books.json")
		if _, err := os.Stat(statePath); err == nil {
			return name
		}
		if fallback == "" {
			fallback = name
		}
	}
	if fallback != "" {
		return fallback
	}
	return candidate
}

// handleAddHMArtist adds a new artist by URL, scrapes the book list, writes the
// initial .books.json, and starts downloading all books.
func (s *Server) handleAddHMArtist(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL           string `json:"url"`
		CheckInterval string `json:"checkInterval,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	artistName := extractHMangaArtistName(req.URL)
	if artistName == "" {
		http.Error(w, "URL must be an artist search URL like https://hentainexus.com/?q=artist:NAME", http.StatusBadRequest)
		return
	}

	// Validate the URL host is HentaiNexus before driving the logged-in
	// persistent browser to it (the download-origin guard only covers /zip/).
	if !hmanga.IsHentaiNexusURL(req.URL) {
		http.Error(w, "URL must be on hentainexus.com", http.StatusBadRequest)
		return
	}

	if req.CheckInterval != "" && req.CheckInterval != "never" {
		if _, ok := models.ParseCheckInterval(req.CheckInterval); !ok {
			http.Error(w, "Invalid check interval", http.StatusBadRequest)
			return
		}
	}

	folderName := fileutil.SanitizeFolderName(artistName)
	if folderName == "unnamed" {
		folderName = "unnamed_artist"
	}

	// Prefer an existing folder whose sanitized name matches the candidate.
	// If it contains a .books.json, adopt that download index instead of
	// creating a new subdirectory.
	folderName = s.findExistingHMArtistFolder(folderName)
	seriesPath := filepath.Join(s.config.HMangaDownloadPath, folderName)
	if _, err := os.Stat(seriesPath); err == nil {
		log.Printf("[HMANGA] Folder '%s' already exists, reusing it for new artist", folderName)
	}

	// If the adopted folder already has a download index, preserve its artist
	// ID and previously downloaded book state instead of starting from scratch.
	artistID := generateHMangaID(folderName)
	var existingState *hmanga.BookStateFile
	var initialBookCount, initialDownloaded int
	if st, _ := s.loadHMangaBookState(folderName); st != nil {
		existingState = st
		if st.ArtistID != "" {
			artistID = st.ArtistID
		}
		initialBookCount = len(st.Books)
		for _, b := range st.Books {
			if b.Downloaded {
				initialDownloaded++
			}
		}
	}

	now := time.Now()

	// Reject duplicates: if the artist is already tracked (by ID or by URL),
	// don't create a second registry entry. The existing folder is reused by
	// design, but the registry must keep a single entry per artist. The check
	// and append are done under a single write lock to avoid a TOCTOU race
	// where two concurrent adds both pass the check and both append.
	s.mu.Lock()
	for _, a := range s.hmangaArtists {
		if a.ID == artistID || (req.URL != "" && a.URL == req.URL) {
			s.mu.Unlock()
			http.Error(w, "Artist already added", http.StatusConflict)
			return
		}
	}
	s.hmangaRegistry.Artists = append(s.hmangaRegistry.Artists, hmanga.Artist{
		ID:              artistID,
		FolderName:      folderName,
		URL:             req.URL,
		CheckInterval:   req.CheckInterval,
		LastCheckedAt:   now,
		BookCount:       initialBookCount,
		BooksDownloaded: initialDownloaded,
		AddedAt:         now,
		UpdatedAt:       now,
		Scraping:        true,
	})
	s.hmangaArtists[artistID] = &s.hmangaRegistry.Artists[len(s.hmangaRegistry.Artists)-1]
	s.hmangaBooks[artistID] = []hmanga.Book{}
	s.saveHMangaRegistry()
	artistCopy := *s.hmangaArtists[artistID]
	s.mu.Unlock()

	// Prepare state. If we adopted an existing download index, keep its books
	// and downloaded flags; otherwise start with an empty map.
	initState := existingState
	if initState == nil {
		initState = &hmanga.BookStateFile{
			ArtistID:   artistID,
			FolderName: folderName,
			URL:        req.URL,
			LastSynced: now,
			Books:      map[string]hmanga.BookInfo{},
		}
	} else {
		initState.ArtistID = artistID
		initState.FolderName = folderName
		initState.URL = req.URL
	}
	s.saveAndSyncHMangaBookState(initState)

	// Scrape the book list + auto-download in the background unless global
	// H-Manga scraping is paused. The add itself is a manual action, but
	// honoring the pause keeps the UI state consistent.
	if !s.isHMangaScrapingPaused() {
		s.enqueueHMangaOp(&hmangaOpItem{
			kind:       hmangaOpRefresh,
			artistID:   artistID,
			folderName: folderName,
			runRefresh: func(opID string) error {
				defer s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
					a.Scraping = false
					a.UpdatedAt = time.Now()
				})
				s.scrapeAndDownloadHMArtist(artistID, req.URL, folderName, opID)
				return nil
			},
		})
	} else {
		// Paused: clear Scraping immediately since we are NOT enqueuing the
		// refresh op. Otherwise the flag would stay true until next restart.
		s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
			a.Scraping = false
		})
		log.Printf("[SCRAPING] H-Manga scraping paused, skipping auto-scrape for newly added artist %s", folderName)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(artistCopy)
}

// scrapeAndDownloadHMArtist runs in its own goroutine to scrape an artist's
// book list via Playwright, persist the discovered books to .books.json +
// registry, then auto-download missing books. It runs independently of the
// manga download worker pool. opID is the queue operation id, signaled
// in-progress once ExtractBooks returns.
func (s *Server) scrapeAndDownloadHMArtist(artistID, artistURL, folderName, opID string) {
	log.Printf("[HMANGA] Background scrape started for artist %s", folderName)

	// Always clear the scraping flag when this goroutine exits, even on panic.
	defer func() {
		s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
			a.Scraping = false
			a.UpdatedAt = time.Now()
		})
	}()

	books, err := s.hmangaMgr.ExtractBooks(artistURL)
	if err != nil {
		s.markHMangaOpFinished(opID, hmangaOpFailed)
		log.Printf("[HMANGA] Failed to scrape artist %s: %v", folderName, err)
		return
	}
	s.markHMangaOpInProgress(opID)

	now := time.Now()
	st := s.getHMangaBookState(folderName)
	if st == nil {
		st = &hmanga.BookStateFile{ArtistID: artistID, FolderName: folderName, URL: artistURL, Books: map[string]hmanga.BookInfo{}}
	}
	if st.Books == nil {
		st.Books = map[string]hmanga.BookInfo{}
	}
	st.URL = artistURL
	st.LastSynced = now
	s.mergeHMBooksIntoState(st, books)

	// Update in-memory book cache + registry counts.
	s.mu.Lock()
	bookList := make([]hmanga.Book, len(books))
	copy(bookList, books)
	s.hmangaBooks[artistID] = bookList
	s.mu.Unlock()

	bookCount := s.mergeHMBookCount(st)
	s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
		a.BookCount = bookCount
		a.LastCheckedAt = now
		a.UpdatedAt = now
	})

	log.Printf("[HMANGA] Scrape complete for artist %s: %d books found, starting downloads", folderName, len(books))

	// Auto-download everything in the background (its own 2-concurrent pool).
	s.autoDownloadMissingBooks(artistID)
}

// enqueueHMDownloadForBook builds a download queue item for a single book.
// The caller must already own hmangaBookDownloading[bookID]. wg, if non-nil,
// is released when the op completes or is dropped on shutdown.
func (s *Server) enqueueHMDownloadForBook(artistID, folderName, bookID, title string, wg *sync.WaitGroup) {
	dlID := uuid.New().String()
	s.enqueueHMangaOp(&hmangaOpItem{
		kind:       hmangaOpDownload,
		artistID:   artistID,
		folderName: folderName,
		bookID:     bookID,
		title:      title,
		dlID:       dlID,
		wg:         wg,
		runDownload: func(opID string) error {
			// Signal "in-progress" immediately so the dispatcher doesn't
			// have to wait for the manager's "downloading" progress tick.
			// The download's actual concurrency is bounded by the
			// manager's 2-slot download semaphore (m.sem), so it's safe
			// for the dispatcher to advance to the next op as soon as
			// this body's bookkeeping is set up. Without this, the
			// dispatcher timed out before the manager emitted its
			// first "downloading" tick (which only fires after
			// triggerDownload returns), producing misleading
			// "starting... advancing" logs on cold-start downloads.
			s.markHMangaOpInProgress(opID)
			s.downloadHMBookWithID(artistID, folderName, bookID, title, dlID, true)
			return nil
		},
	})
}

// handleDeleteHMArtist removes an artist from the registry (keeps files).
func (s *Server) handleDeleteHMArtist(w http.ResponseWriter, r *http.Request, artistID string) {
	s.mu.Lock()
	found := false
	for i, a := range s.hmangaRegistry.Artists {
		if a.ID == artistID {
			s.hmangaRegistry.Artists = append(s.hmangaRegistry.Artists[:i], s.hmangaRegistry.Artists[i+1:]...)
			found = true
			break
		}
	}
	delete(s.hmangaArtists, artistID)
	delete(s.hmangaBooks, artistID)
	s.saveHMangaRegistry()
	s.mu.Unlock()

	if !found {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	// Preserve global cache entries per plan: deleting an artist must not remove
	// shared book entries so re-adding the artist later keeps them tracked.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

// handleListHMBooks returns the known books for an artist.
func (s *Server) handleListHMBooks(w http.ResponseWriter, r *http.Request, artistID string) {
	s.mu.RLock()
	artist, artistOK := s.hmangaArtists[artistID]
	books, cacheOK := s.hmangaBooks[artistID]
	s.mu.RUnlock()
	if !artistOK {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	out := make([]hmanga.Book, 0, len(books))
	if cacheOK {
		out = append(out, books...)
	} else {
		// Fall back to the persisted .books.json state if the in-memory cache
		// hasn't been populated yet (e.g. after startup scan without a scrape).
		// Read st.Books under hmangaStateMu and copy FolderName under s.mu to
		// avoid racing concurrent scrapes / artist renames.
		st := s.getHMangaBookState(artist.FolderName)
		if st != nil {
			s.hmangaStateMu.Lock()
			ids := make([]string, 0, len(st.Books))
			for id := range st.Books {
				ids = append(ids, id)
			}
			sorted := make([]hmanga.Book, 0, len(ids))
			for _, id := range ids {
				b := st.Books[id]
				sorted = append(sorted, hmanga.Book{ID: id, Title: b.Title, URL: b.URL})
			}
			s.hmangaStateMu.Unlock()
			sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID > sorted[j].ID })
			out = append(out, sorted...)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// handleGetHMArtistState returns the .books.json state for an artist.
func (s *Server) handleGetHMArtistState(w http.ResponseWriter, r *http.Request, artistID string) {
	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	st := s.getHMangaBookState(artist.FolderName)
	if st == nil {
		st = &hmanga.BookStateFile{ArtistID: artistID, FolderName: artist.FolderName, URL: artist.URL, Books: map[string]hmanga.BookInfo{}}
	}
	// Snapshot the shared state under hmangaStateMu so the JSON encode does
	// not race concurrent mutations of st.Books.
	s.hmangaStateMu.Lock()
	stCopy := *st
	stCopy.Books = make(map[string]hmanga.BookInfo, len(st.Books))
	for id, info := range st.Books {
		stCopy.Books[id] = info
	}
	s.hmangaStateMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stCopy)
}

// handleSyncHMArtist triggers auto-download of all missing books. Sync only
// touches already-known books, so it is allowed while paused (manual path).
func (s *Server) handleSyncHMArtist(w http.ResponseWriter, r *http.Request, artistID string) {
	if !s.hmangaArtistExists(artistID) {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	if s.isShutdown() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "paused", "artistId": artistID, "message": "Server is shutting down"})
		return
	}
	s.safeGoTrack("autoDownloadMissingBooks", func() {
		s.autoDownloadMissingBooks(artistID, true)
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "syncing", "artistId": artistID})
}

// handleCheckHMArtistNow re-scrapes the book list and enqueues new books.
func (s *Server) handleCheckHMArtistNow(w http.ResponseWriter, r *http.Request, artistID string) {
	if !s.hmangaArtistExists(artistID) {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	s.enqueueHMangaOp(&hmangaOpItem{
		kind:     hmangaOpRefresh,
		artistID: artistID,
		runRefresh: func(opID string) error {
			s.refreshAndDownloadHMArtist(artistID, opID)
			return nil
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "checking", "artistId": artistID})
}

// handleCheckAllHMArtists re-scrapes the book list for every tracked artist
// and auto-downloads any newly discovered books. The per-artist refresh ops are
// serialised by the shared H-Manga operation queue.
func (s *Server) handleCheckAllHMArtists(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	list := make([]string, 0, len(s.hmangaArtists))
	for id := range s.hmangaArtists {
		list = append(list, id)
	}
	s.mu.RUnlock()

	for _, artistID := range list {
		artistID := artistID
		s.enqueueHMangaOp(&hmangaOpItem{
			kind:     hmangaOpRefresh,
			artistID: artistID,
			runRefresh: func(opID string) error {
				s.refreshAndDownloadHMArtist(artistID, opID)
				return nil
			},
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{"status": "checking", "count": len(list)})
}

// handleSyncAllHMArtists triggers auto-download of missing books for every
// tracked artist. Each artist is dispatched to its own goroutine so a slow
// download does not block the rest.
func (s *Server) handleSyncAllHMArtists(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	list := make([]string, 0, len(s.hmangaArtists))
	for id := range s.hmangaArtists {
		list = append(list, id)
	}
	s.mu.RUnlock()

	if s.isShutdown() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "paused", "message": "Server is shutting down"})
		return
	}

	for _, artistID := range list {
		artistID := artistID
		s.safeGoTrack("syncAllHMArtists", func() {
			s.autoDownloadMissingBooks(artistID, true)
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{"status": "syncing", "count": len(list)})
}

// handleCleanupHMZips starts a background scan over every artist folder in the
// H-Manga download path. For each ZIP on disk it:
//  1. Applies the configured hmangaZipNameRegex to the name — if the name
//     changes (i.e. the rename hasn't been done yet), the ZIP is renamed,
//     unless the target name already exists (skipped to avoid clobbering).
//  2. When hmangaExtractZips is enabled, extracts the (possibly renamed) ZIP
//     into a same-named directory and deletes the ZIP on success.
//  3. Updates .books.json entries whose Filename referred to a renamed file.
//
// The response returns immediately; the result is reported via the log.
func (s *Server) handleCleanupHMZips(w http.ResponseWriter, r *http.Request) {
	if s.isShutdown() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "paused", "message": "Server is shutting down"})
		return
	}

	// Prevent concurrent cleanup runs: the scan mutates files and .books.json.
	s.hmangaCleanupMu.Lock()
	if s.hmangaCleanupRunning {
		s.hmangaCleanupMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"status": "busy", "message": "A zip cleanup scan is already running"})
		return
	}
	s.hmangaCleanupRunning = true
	s.hmangaCleanupMu.Unlock()

	// Snapshot artist folder names under the lock so the scan works against a
	// stable list even if artists are added/removed mid-scan.
	s.mu.RLock()
	folders := make([]string, 0, len(s.hmangaArtists))
	seen := map[string]bool{}
	for _, a := range s.hmangaArtists {
		if a.FolderName != "" && !seen[a.FolderName] {
			folders = append(folders, a.FolderName)
			seen[a.FolderName] = true
		}
	}
	s.mu.RUnlock()

	pattern := s.config.HMangaZipNameRegex
	extractEnabled := s.config.HMangaExtractZips

	s.safeGoTrack("cleanupHMZips", func() {
		defer func() {
			s.hmangaCleanupMu.Lock()
			s.hmangaCleanupRunning = false
			s.hmangaCleanupMu.Unlock()
		}()
		// Compile once; an invalid pattern disables renaming for this scan.
		re, _ := compileHMZipNameRegex(pattern)
		var renamed, extracted, skipped, failed int
		for _, folder := range folders {
			rn, ex, sk, fl := s.cleanupHMZips(folder, extractEnabled, re)
			renamed += rn
			extracted += ex
			skipped += sk
			failed += fl
		}
		log.Printf("[HMANGA-CLEANUP] Finished: %d renamed, %d extracted, %d skipped, %d failed", renamed, extracted, skipped, failed)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"status":      "started",
		"folders":     len(folders),
		"extractMode": extractEnabled,
		"message":     "Zip cleanup scan started; see the server log for the summary",
	})
}

// compileHMZipNameRegex compiles the configured zip-name regex, falling back
// to the shipped default when unset, and returns nil when the pattern is
// invalid (the caller then skips renaming).
func compileHMZipNameRegex(pattern string) (*regexp.Regexp, bool) {
	if pattern == "" {
		pattern = config.DefaultHMangaZipNameRegex
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Printf("[HMANGA-CLEANUP] Invalid zip name regex %q (%v); skipping renaming", pattern, err)
		return nil, false
	}
	return re, true
}

// cleanupHMZips processes one artist folder: rename zips per the regex, then
// extract them if enabled. It returns per-category counts.
func (s *Server) cleanupHMZips(folder string, extractEnabled bool, re *regexp.Regexp) (renamed, extracted, skipped, failed int) {
	dir := filepath.Join(s.config.HMangaDownloadPath, folder)
	entries, err := os.ReadDir(dir)
	if err != nil {
		// No folder on disk (artist added but nothing downloaded yet).
		return 0, 0, 0, 0
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.EqualFold(filepath.Ext(name), ".zip") {
			continue
		}
		current := name
		// Step 1: rename when the regex still changes the name.
		if re != nil {
			cleaned := strings.TrimSpace(re.ReplaceAllString(name, ""))
			base := strings.TrimSuffix(cleaned, filepath.Ext(cleaned))
			if cleaned != "" && base != "" && strings.EqualFold(filepath.Ext(cleaned), ".zip") && cleaned != name {
				target := filepath.Join(dir, cleaned)
				if _, statErr := os.Stat(target); statErr == nil {
					// Target exists — do not clobber.
					log.Printf("[HMANGA-CLEANUP] %s: %s -> %s already exists, skipping rename", folder, name, cleaned)
					skipped++
					continue
				}
				if err := os.Rename(filepath.Join(dir, name), target); err != nil {
					log.Printf("[HMANGA-CLEANUP] %s: rename %s -> %s failed: %v", folder, name, cleaned, err)
					failed++
					continue
				}
				log.Printf("[HMANGA-CLEANUP] %s: renamed %s -> %s", folder, name, cleaned)
				renamed++
				s.updateHMangaStateFilename(folder, name, cleaned)
				current = cleaned
			}
		}
		// Step 2: extract when enabled. extractHMBookZip extracts into a
		// same-named directory and deletes the zip on success.
		if extractEnabled {
			zipPath := filepath.Join(dir, current)
			if err := s.extractHMBookZip(folder, "", zipPath); err != nil {
				log.Printf("[HMANGA-CLEANUP] %s: extraction of %s failed: %v", folder, current, err)
				failed++
				continue
			}
			extracted++
			s.updateHMangaStateExtracted(folder, current)
		}
	}
	return renamed, extracted, skipped, failed
}

// updateHMangaStateFilename rewrites .books.json entries whose Filename
// referred to a zip that was renamed by the cleanup scan. Bookkeeping for a
// book marked Extracted is left alone (its zip no longer exists on disk).
func (s *Server) updateHMangaStateFilename(folderName, oldName, newName string) {
	changed := func() bool {
		s.hmangaStateMu.Lock()
		defer s.hmangaStateMu.Unlock()
		st := s.findOrLoadHMangaBookStateLocked(folderName)
		if st == nil {
			return false
		}
		changed := false
		for id, b := range st.Books {
			if !b.Downloaded || b.Extracted || b.Filename != oldName {
				continue
			}
			b.Filename = newName
			st.Books[id] = b
			changed = true
		}
		if changed {
			if err := s.saveHMangaBookState(folderName, st); err != nil {
				log.Printf("[HMANGA-CLEANUP] Failed to update .books.json after rename in %s: %v", folderName, err)
				return false
			}
		}
		return changed
	}()
	// rebuildHMangaGlobalCache acquires hmangaStateMu itself; call it only
	// after the state lock is released.
	if changed {
		s.rebuildHMangaGlobalCache()
	}
}

// updateHMangaStateExtracted marks .books.json entries whose Filename matches
// a zip that the cleanup scan extracted and deleted.
func (s *Server) updateHMangaStateExtracted(folderName, zipName string) {
	dirName := strings.TrimSuffix(zipName, filepath.Ext(zipName))
	changed := func() bool {
		s.hmangaStateMu.Lock()
		defer s.hmangaStateMu.Unlock()
		st := s.findOrLoadHMangaBookStateLocked(folderName)
		if st == nil {
			return false
		}
		changed := false
		for id, b := range st.Books {
			if b.Extracted || b.Filename != zipName {
				continue
			}
			b.Extracted = true
			b.DirName = dirName
			st.Books[id] = b
			changed = true
		}
		if changed {
			if err := s.saveHMangaBookState(folderName, st); err != nil {
				log.Printf("[HMANGA-CLEANUP] Failed to update .books.json after extraction in %s: %v", folderName, err)
				return false
			}
		}
		return changed
	}()
	// rebuildHMangaGlobalCache acquires hmangaStateMu itself; call it only
	// after the state lock is released.
	if changed {
		s.rebuildHMangaGlobalCache()
	}
}

// findOrLoadHMangaBookStateLocked returns the in-memory state file for
// folderName, loading from disk and registering it if not cached yet.
// Caller must hold hmangaStateMu.
func (s *Server) findOrLoadHMangaBookStateLocked(folderName string) *hmanga.BookStateFile {
	for _, candidate := range s.hmangaBookStates {
		if candidate != nil && candidate.FolderName == folderName {
			return candidate
		}
	}
	loaded, err := s.loadHMangaBookState(folderName)
	if err != nil || loaded == nil {
		return nil
	}
	if loaded.ArtistID != "" {
		s.hmangaBookStates[loaded.ArtistID] = loaded
	}
	return loaded
}

// handleDownloadHMBook downloads a single book.
func (s *Server) handleDownloadHMBook(w http.ResponseWriter, r *http.Request, artistID string) {
	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	// Copy FolderName under the lock: the background goroutine below must not
	// read the shared pointer while a rename can mutate it.
	folderName := artist.FolderName
	var req struct {
		BookID string `json:"bookId"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.BookID == "" {
		http.Error(w, "bookId is required", http.StatusBadRequest)
		return
	}
	dlID := uuid.New().String()
	s.enqueueHMangaOp(&hmangaOpItem{
		kind:       hmangaOpDownload,
		artistID:   artistID,
		folderName: folderName,
		bookID:     req.BookID,
		dlID:       dlID,
		runDownload: func(opID string) error {
			// Signal in-progress immediately so the dispatcher
			// advances past this op's "starting" phase without waiting
			// for the manager's first progress tick. The manager's
			// 2-slot semaphore still serializes the actual
			// Playwright-touching work.
			s.markHMangaOpInProgress(opID)
			s.downloadHMBookWithID(artistID, folderName, req.BookID, "", dlID, false)
			return nil
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "downloading", "bookId": req.BookID})
}

// handleScanHMArtistMissing verifies zip files on disk vs state and re-downloads missing ones.
func (s *Server) handleScanHMArtistMissing(w http.ResponseWriter, r *http.Request, artistID string) {
	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	folderName := artist.FolderName
	s.safeGoTrack("scanHMArtistMissing", func() {
		s.reconcileHMBookState(artistID, folderName)
		s.autoDownloadMissingBooks(artistID)
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "scanning", "artistId": artistID})
}

// handleForceHMRedownload deletes all downloaded zips and re-downloads every book.
func (s *Server) handleForceHMRedownload(w http.ResponseWriter, r *http.Request, artistID string) {
	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	folderName := artist.FolderName
	s.safeGoTrack("forceHMRedownload", func() {
		s.forceHMRedownload(artistID, folderName)
	})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "redownloading", "artistId": artistID})
}

// handleUpdateHMCheckInterval sets the auto-check interval for an artist.
func (s *Server) handleUpdateHMCheckInterval(w http.ResponseWriter, r *http.Request, artistID string) {
	var req struct {
		CheckInterval string `json:"checkInterval"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	// Validate the interval using the shared helper. "" and "never" are
	// valid (no auto-check); everything else must parse.
	if req.CheckInterval != "" && req.CheckInterval != "never" {
		if _, ok := models.ParseCheckInterval(req.CheckInterval); !ok {
			http.Error(w, "Invalid check interval", http.StatusBadRequest)
			return
		}
	}
	if !s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
		a.CheckInterval = req.CheckInterval
		a.UpdatedAt = time.Now()
	}) {
		http.Error(w, "Artist not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "updated", "checkInterval": req.CheckInterval})
}

// hmangaArtistExists reports whether an artistID is tracked.
func (s *Server) hmangaArtistExists(artistID string) bool {
	s.mu.RLock()
	_, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	return ok
}

// ==================== Download orchestration ====================

// autoDownloadMissingBooks downloads every book for an artist that is not yet
// downloaded, respecting the manager's 2-concurrent semaphore. If manual is
// true (user-invoked /sync, /sync-all, download of known books), the H-Manga
// scraping pause does not abort the loop — sync of known books is documented
// to work while paused. The pause check still applies to scheduler and
// post-scrape auto-download invocations.
func (s *Server) autoDownloadMissingBooks(artistID string, manual ...bool) {
	manualSync := len(manual) > 0 && manual[0]
	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	folderName := ""
	if ok {
		folderName = artist.FolderName
	}

	s.mu.Lock()
	if s.hmangaArtistDownloading[artistID] {
		s.mu.Unlock()
		log.Printf("[HMANGA] Sync already in progress for artist %s, skipping", folderName)
		return
	}
	s.hmangaArtistDownloading[artistID] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.hmangaArtistDownloading, artistID)
		s.mu.Unlock()
	}()

	if !ok {
		return
	}

	// Copy the FolderName under the lock so a concurrent artist rename
	// cannot race this read; the rest of this goroutine uses the copy.
	s.mu.RLock()
	folderName = artist.FolderName
	s.mu.RUnlock()

	st := s.getHMangaBookState(folderName)
	if st == nil {
		log.Printf("[HMANGA] No book state for artist %s", folderName)
		return
	}

	// Snapshot the book list under hmangaStateMu — st.Books is mutated
	// concurrently by downloads and scrapes.
	s.hmangaStateMu.Lock()
	ids := make([]string, 0, len(st.Books))
	for id := range st.Books {
		ids = append(ids, id)
	}
	booksSnapshot := make(map[string]hmanga.BookInfo, len(st.Books))
	for id, info := range st.Books {
		booksSnapshot[id] = info
	}
	s.hmangaStateMu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	var wg sync.WaitGroup
	for _, id := range ids {
		b := booksSnapshot[id]
		if b.Downloaded {
			continue
		}
		bookID := id
		title := b.Title

		// If shutdown or a non-manual pause is signaled mid-sync, stop queuing
		// new books. Any book already downloading will finish. Manual syncs
		// keep going while paused (documented behavior).
		if s.isShutdown() || (!manualSync && s.isHMangaScrapingPaused()) {
			log.Printf("[HMANGA] Scraping paused or shutting down, stopping auto-download for artist %s", folderName)
			break
		}

		// Skip books already downloaded or currently downloading elsewhere.
		// Claim the in-flight flag atomically here before launching the
		// goroutine, so another concurrent dispatch (or a later iteration of
		// this loop) cannot enqueue the same book twice.
		s.hmangaGlobalMu.Lock()
		globalEntry := s.hmangaGlobalBooks[bookID]
		alreadyDownloading := s.hmangaBookDownloading[bookID]
		if globalEntry.Downloaded || alreadyDownloading {
			s.hmangaGlobalMu.Unlock()
			continue
		}
		s.hmangaBookDownloading[bookID] = true
		s.hmangaGlobalMu.Unlock()

		log.Printf("[HMANGA] Queued download of book %s for artist %s", bookID, folderName)
		wg.Add(1)
		s.enqueueHMDownloadForBook(artistID, folderName, bookID, title, &wg)
	}
	wg.Wait()
}

// bookFileExists reports whether the expected H-Manga zip file is present on disk.
func (s *Server) bookFileExists(folderName, filename string) bool {
	if filename == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(s.config.HMangaDownloadPath, folderName, filename))
	return err == nil
}

// bookExtractedDirExists reports whether the extracted directory for a book
// (same basename as the original ZIP) exists in the artist folder and is
// non-empty.
func (s *Server) bookExtractedDirExists(folderName, dirName string) bool {
	if dirName == "" {
		return false
	}
	entries, err := os.ReadDir(filepath.Join(s.config.HMangaDownloadPath, folderName, dirName))
	if err != nil {
		return false
	}
	return len(entries) > 0
}

// extractHMBookZip extracts the downloaded ZIP into a directory with the same
// name as the ZIP (same parent directory, basename minus ".zip"), then deletes
// the ZIP. Returns an error if extraction fails or extracts zero files; in
// that case the ZIP is left untouched so the caller can retry later.
// bookID is used only for log messages.
func (s *Server) extractHMBookZip(folderName, bookID, zipPath string) error {
	if strings.EqualFold(filepath.Ext(zipPath), "") {
		return fmt.Errorf("not a zip path: %s", zipPath)
	}
	dirName := strings.TrimSuffix(filepath.Base(zipPath), filepath.Ext(zipPath))
	if dirName == "" || dirName == "." || dirName == ".." {
		return fmt.Errorf("cannot derive extraction directory from %q", zipPath)
	}
	destDir := filepath.Join(filepath.Dir(zipPath), dirName)

	// Guard against colliding with an existing non-directory artifact.
	if fi, err := os.Stat(destDir); err == nil && !fi.IsDir() {
		return fmt.Errorf("extraction destination %s exists and is not a directory", destDir)
	}

	count, err := fileutil.ExtractZip(zipPath, destDir)
	if err != nil {
		// Remove the partial extraction so a retry starts clean; the ZIP
		// itself is untouched.
		func() { defer func() { recover() }(); os.RemoveAll(destDir) }()
		return err
	}
	if count == 0 {
		func() { defer func() { recover() }(); os.RemoveAll(destDir) }()
		return fmt.Errorf("zip contained no files")
	}

	if err := os.Remove(zipPath); err != nil {
		// The extraction succeeded but the ZIP couldn't be removed (locked,
		// permissions). The ZIP stays and the extraction remains; a later
		// sync would re-extract, so clean up the folder now to avoid
		// duplicated artifacts.
		func() { defer func() { recover() }(); os.RemoveAll(destDir) }()
		return fmt.Errorf("delete zip after extraction: %w", err)
	}

	log.Printf("[HMANGA] Extracted %d file(s) from %s -> %s (book %s, artist %s)", count, filepath.Base(zipPath), dirName, bookID, folderName)
	return nil
}

// downloadHMBookWithID is the internal implementation that optionally reuses
// an existing download record ID (e.g. during a retry). If dlID is empty, a new
// record is created. If ownsFlag is true, the caller has already claimed
// hmangaBookDownloading[bookID]; otherwise this function claims it and skips if
// another download of the same book is already active.
func (s *Server) downloadHMBookWithID(artistID, folderName, bookID, title, dlID string, ownsFlag bool) {
	if !ownsFlag {
		s.hmangaGlobalMu.Lock()
		if s.hmangaBookDownloading[bookID] {
			s.hmangaGlobalMu.Unlock()
			log.Printf("[HMANGA] Skipping duplicate download of book %s for artist %s", bookID, folderName)
			return
		}
		s.hmangaBookDownloading[bookID] = true
		s.hmangaGlobalMu.Unlock()
	}
	defer func() {
		s.hmangaGlobalMu.Lock()
		delete(s.hmangaBookDownloading, bookID)
		delete(s.hmangaRetrying, bookID)
		s.hmangaGlobalMu.Unlock()
	}()

	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	// Resolve the display title. The bulk path passes the title already in
	// memory; the single-book handler passes "" and we fall back.
	bookTitle := title
	if bookTitle == "" {
		bookTitle = "Book " + bookID
	}

	// Reuse the existing download record for retries, otherwise create one.
	// Create the record BEFORE any short-circuit (e.g. VerifyHMDownloads
	// early-return) so the Downloads-tab record exists in either path.
	dl := models.NewDownload(artistID, bookID, artist.FolderName, bookTitle)
	dl.Phase = "pending"
	if dlID != "" {
		dl.ID = dlID
		s.mu.Lock()
		if existing, ok := s.downloads[dlID]; ok {
			dl = existing
			dl.Phase = "pending"
			dl.Status = models.StatusPending
			dl.Progress = 0
			dl.BytesDownloaded = 0
			dl.TotalBytes = 0
			dl.UpdatedAt = time.Now()
		} else {
			// Pre-generated dlID (queue path) with no existing record: register
			// it so the progress callback can find it and signal the queue.
			dl.CustomName = artist.FolderName
			dl.ImageCount = 1
			dl.Status = models.StatusPending
			dl.Progress = 0
			s.downloads[dl.ID] = dl
		}
		s.mu.Unlock()
	} else {
		dl.ID = uuid.New().String()
		dl.CustomName = artist.FolderName
		dl.ImageCount = 1
		dl.Status = models.StatusPending
		dl.Progress = 0
		s.mu.Lock()
		s.downloads[dl.ID] = dl
		s.mu.Unlock()
	}

	// When disk verification is enabled, trust the file on disk over .books.json:
	// if the zip already exists, record it as downloaded without hitting the site.
	// Books already extracted (artifact is a folder) are recognized by the
	// extracted-directory check instead of the zip check.
	if s.config.VerifyHMDownloads {
		st := s.getHMangaBookState(folderName)
		if st != nil {
			// Read st.Books under hmangaStateMu — the map is mutated
			// concurrently by scrapes and other downloads.
			s.hmangaStateMu.Lock()
			b, ok := st.Books[bookID]
			s.hmangaStateMu.Unlock()
			if ok && b.Downloaded && b.Filename != "" {
				artifactExists := false
				if b.Extracted {
					artifactExists = s.bookExtractedDirExists(folderName, b.DirName)
				} else {
					artifactExists = s.bookFileExists(folderName, b.Filename)
				}
				if artifactExists {
					log.Printf("[HMANGA] Book %s already on disk for artist %s", bookID, folderName)
					if _, recErr := s.recordHMBookDownloaded(artistID, folderName, bookID, b.Filename, 0, b.Extracted); recErr != nil {
						s.markHMangaDownloadFailed(dl.ID)
						s.handleHMBookDownloadFailure(artistID, folderName, bookID, dl.ID, recErr)
						return
					}
					s.markHMangaDownloadCompleted(dl.ID, 0)
					return
				}
			}
		}
	}

	log.Printf("[HMANGA] Downloading book %s for artist %s", bookID, folderName)

	// If this goroutine panics, mark the download as failed so it doesn't
	// stay stuck in the Downloads tab as "Downloading" forever.
	defer func() {
		if rec := recover(); rec != nil {
			s.mu.Lock()
			if existing, ok := s.downloads[dl.ID]; ok {
				existing.Status = models.StatusFailed
				existing.UpdatedAt = time.Now()
			}
			s.mu.Unlock()
			log.Printf("[PANIC] recovered in downloadHMBook for book %s: %v", bookID, rec)
		}
	}()

	destDir := filepath.Join(s.config.HMangaDownloadPath, folderName)
	filename, size, err := s.hmangaMgr.DownloadBook(artistID, bookID, destDir, dl.ID)

	if err != nil {
		// The callback already marked the record failed; ensure it is visible.
		s.markHMangaDownloadFailed(dl.ID)
		s.handleHMBookDownloadFailure(artistID, folderName, bookID, dl.ID, err)
		return
	}

	// Verify the file is actually on disk in the expected location before we
	// commit the bookkeeping. The manager's "done" tick fires once SaveAs
	// returns, but Playwright's temp staging has been a source of "disappeared
	// downloads" in the past, so we re-stat the destination path here. If the
	// file is missing or the wrong size, treat the download as failed and
	// trigger the normal retry path instead of marking the record complete.
	expectedPath := filepath.Join(destDir, filename)
	info, statErr := os.Stat(expectedPath)
	if statErr != nil || info.Size() == 0 {
		reason := "file missing on disk after SaveAs"
		if statErr == nil {
			reason = "downloaded file is empty (0 bytes)"
		} else {
			reason = "file missing on disk after SaveAs: " + statErr.Error()
		}
		log.Printf("[HMANGA] Post-save verification failed for book %s: %s (path=%s)", bookID, reason, expectedPath)
		// Roll the manager-emitted "done" tick back to "failed" so the UI does
		// not flash completed right before the failure appears.
		s.markHMangaDownloadFailed(dl.ID)
		s.handleHMBookDownloadFailure(artistID, folderName, bookID, dl.ID, fmt.Errorf("%s", reason))
		return
	}
	actualSize := info.Size()
	if size == 0 {
		size = actualSize
	}

	// Extract the ZIP into a directory with the same name as the ZIP
	// (<ArtistFolder>/<zip basename>/) when enabled. The ZIP is deleted only
	// after extraction fully succeeds; on failure the intact ZIP remains and
	// the book is recorded normally (extraction is best-effort, retried on
	// the next sync).
	extracted := false
	if s.config.HMangaExtractZips {
		if exErr := s.extractHMBookZip(folderName, bookID, expectedPath); exErr != nil {
			log.Printf("[HMANGA] Extraction failed for book %s (%s): %v — keeping ZIP", bookID, folderName, exErr)
			s.markHMangaDownloadFailed(dl.ID)
			s.handleHMBookDownloadFailure(artistID, folderName, bookID, dl.ID, fmt.Errorf("extraction failed: %w", exErr))
			return
		}
		extracted = true
	}

	// Persist the .books.json entry + global cache ownership. This is the
	// source-of-truth update; if it fails the file is still on disk and a
	// later scan will reconcile it, but the Download record must NOT be
	// marked Completed until this returns without error — otherwise the
	// book would silently "disappear" on the next startup.
	wasNew, recErr := s.recordHMBookDownloaded(artistID, folderName, bookID, filename, size, extracted)
	if recErr != nil {
		log.Printf("[HMANGA] Bookkeeping failed for book %s: %v", bookID, recErr)
		s.markHMangaDownloadFailed(dl.ID)
		s.handleHMBookDownloadFailure(artistID, folderName, bookID, dl.ID, recErr)
		return
	}

	if wasNew {
		log.Printf("[HMANGA] Downloaded book %s -> %s (%d bytes) for artist %s", bookID, expectedPath, size, folderName)
	} else {
		log.Printf("[HMANGA] Verified existing book %s at %s (%d bytes) for artist %s", bookID, expectedPath, size, folderName)
	}
	if extracted {
		dirName := strings.TrimSuffix(filename, filepath.Ext(filename))
		log.Printf("[HMANGA] Book %s extracted to %s and ZIP removed", bookID, filepath.Join(destDir, dirName))
	}

	s.markHMangaDownloadCompleted(dl.ID, size)
}

// markHMangaDownloadCompleted transitions the Download record to
// StatusCompleted. It also clears the per-record logging throttle. Safe to
// call when the record no longer exists (e.g. the user cleared it). The
// caller is responsible for ensuring all preconditions (file on disk +
// .books.json persisted) have been met before invoking this helper.
func (s *Server) markHMangaDownloadCompleted(dlID string, size int64) {
	s.mu.Lock()
	if existing, ok := s.downloads[dlID]; ok {
		// A size of 0 (unknown) must not zero out byte counters already
		// reported by the download manager.
		if size > 0 {
			existing.BytesDownloaded = size
			existing.TotalBytes = size
		}
		existing.Phase = "done"
		existing.Status = models.StatusCompleted
		existing.Progress = 100
		existing.DownloadedCount = 1
		existing.ImageCount = 1
		existing.UpdatedAt = time.Now()
	}
	// The map is documented as protected by s.mu; the delete must run inside
	// the locked section so concurrent progress ticks cannot race it.
	delete(s.hmangaLastLoggedBytes, dlID)
	s.mu.Unlock()
}

// markHMangaDownloadFailed transitions the Download record to
// StatusFailed/Phase=failed. Safe to call when the record no longer exists.
func (s *Server) markHMangaDownloadFailed(dlID string) {
	s.mu.Lock()
	if existing, ok := s.downloads[dlID]; ok {
		existing.Phase = "failed"
		existing.Status = models.StatusFailed
		existing.UpdatedAt = time.Now()
	}
	// Protected by s.mu — must run inside the locked section.
	delete(s.hmangaLastLoggedBytes, dlID)
	s.mu.Unlock()
}

// handleHMBookDownloadFailure implements one non-blocking retry per book. It
// checks global dedup state to avoid retrying a book downloaded by another
// artist or currently owned by another goroutine. dlID is the original
// Downloads-tab record, which is updated or marked failed on every exit path.
func (s *Server) handleHMBookDownloadFailure(artistID, folderName, bookID, dlID string, err error) {
	s.hmangaGlobalMu.Lock()
	globalEntry := s.hmangaGlobalBooks[bookID]
	ownsDownload := s.hmangaBookDownloading[bookID]
	s.hmangaGlobalMu.Unlock()

	markFailed := func(reason string) {
		s.mu.Lock()
		if existing, ok := s.downloads[dlID]; ok {
			existing.Status = models.StatusFailed
			existing.UpdatedAt = time.Now()
		}
		s.mu.Unlock()
		s.hmangaGlobalMu.Lock()
		delete(s.hmangaBookDownloading, bookID)
		delete(s.hmangaRetrying, bookID)
		s.hmangaGlobalMu.Unlock()
		log.Printf("[HMANGA] Failed to download book %s for artist %s (%s): %v", bookID, folderName, reason, err)
	}

	if globalEntry.Downloaded {
		markFailed("downloaded by another artist")
		return
	}
	if !ownsDownload {
		// Another goroutine is now responsible for this book; suppress this
		// attempt's failure report but still mark our record failed.
		markFailed("superseded")
		return
	}

	s.hmangaGlobalMu.Lock()
	retries := s.hmangaRetrying[bookID]
	if retries < 1 {
		s.hmangaRetrying[bookID] = retries + 1
		// Do not retry if shutdown is in progress; finalize the failure so the
		// safe shutdown path isn't blocked by a sleeping retry.
		if s.isShutdown() {
			s.hmangaGlobalMu.Unlock()
			markFailed("shutdown")
			return
		}
		s.hmangaGlobalMu.Unlock()
		log.Printf("[HMANGA] Retry download of book %s for artist %s", bookID, folderName)
		time.Sleep(2 * time.Second)
		// Re-verify ownership and global state after the sleep.
		s.hmangaGlobalMu.Lock()
		if s.hmangaGlobalBooks[bookID].Downloaded {
			s.hmangaGlobalMu.Unlock()
			markFailed("downloaded by another artist during retry wait")
			return
		}
		if s.isShutdown() || !s.hmangaBookDownloading[bookID] {
			s.hmangaGlobalMu.Unlock()
			markFailed("shutdown after sleep")
			return
		}
		s.hmangaGlobalMu.Unlock()
		s.downloadHMBookWithID(artistID, folderName, bookID, "", dlID, true)
		return
	}
	s.hmangaGlobalMu.Unlock()
	markFailed("final")
}

// recordHMBookDownloaded marks a book as downloaded in .books.json and updates
// the registry counters, then mirrors the change into the global cache.
//
// The persistence step is keyed by (artistID, folderName, bookID) — NOT by
// the Download record — so the bookkeeping is correct even if the user has
// already cleared the download from the Downloads tab.
//
// If the per-artist .books.json has no entry for bookID (e.g. a single-book
// download initiated before any scrape populated the index), a minimal entry
// is created on the spot so the file we just saved is not lost.
//
// Returns (wasNew, nil) on success where wasNew reports whether this call
// flipped the entry from not-downloaded to downloaded (true for a fresh
// download, false when re-persisting an already-downloaded entry). On save
// failure the in-memory state is rolled back to its prior value and the
// returned error explains why. Callers MUST treat a non-nil error as a
// hard failure and avoid marking the Download record Completed — otherwise
// the file will be on disk but .books.json will not reflect it and the
// book will "disappear" on the next startup.
func (s *Server) recordHMBookDownloaded(artistID, folderName, bookID, filename string, size int64, extracted bool) (bool, error) {
	// Skip if the artist was deleted mid-download: re-creating state here
	// would re-insert stale entries for an artist the user removed.
	if !s.hmangaArtistExists(artistID) {
		return false, fmt.Errorf("artist %s no longer exists; skipping bookkeeping for book %s", artistID, bookID)
	}

	s.hmangaStateMu.Lock()
	defer s.hmangaStateMu.Unlock()

	st := s.hmangaBookStates[artistID]
	if st == nil {
		st, _ = s.loadHMangaBookState(folderName)
		if st == nil {
			st = &hmanga.BookStateFile{
				ArtistID:   artistID,
				FolderName: folderName,
				Books:      map[string]hmanga.BookInfo{},
			}
		}
		st.FolderName = folderName
		s.hmangaBookStates[artistID] = st
	}
	if st.Books == nil {
		st.Books = map[string]hmanga.BookInfo{}
	}

	// Snapshot the prior entry so a save failure leaves the in-memory map
	// consistent with the unchanged on-disk .books.json.
	prior, hadPrior := st.Books[bookID]
	b := prior
	if !hadPrior {
		b = hmanga.BookInfo{
			ID:    bookID,
			Title: "Book " + bookID,
			URL:   "/view/" + bookID,
		}
	}
	wasNew := !hadPrior || !prior.Downloaded
	now := time.Now()
	b.Downloaded = true
	b.Filename = filename
	b.DownloadedAt = now
	b.FileSize = size
	if extracted {
		// The artifact is the extracted folder, not the ZIP: record its name
		// (same basename as the ZIP) and clear the size, which referred to
		// the now-deleted archive.
		b.Extracted = true
		b.DirName = strings.TrimSuffix(filename, filepath.Ext(filename))
		b.FileSize = size
	} else {
		b.Extracted = false
		b.DirName = ""
	}
	st.Books[bookID] = b

	if err := s.saveHMangaBookState(folderName, st); err != nil {
		// Roll back the in-memory mutation so on-disk and in-memory state
		// stay consistent. The file is still on disk; the next disk-verify
		// scan or reconcileHMBookState will reconcile it.
		if hadPrior {
			st.Books[bookID] = prior
		} else {
			delete(st.Books, bookID)
		}
		log.Printf("[HMANGA] Failed to save .books.json for %s: %v", folderName, err)
		return false, fmt.Errorf("save .books.json: %w", err)
	}

	s.hmangaGlobalMu.Lock()
	defer s.hmangaGlobalMu.Unlock()
	s.syncGlobalCacheForArtist(st)
	s.saveGlobalBookCache()

	// Recount and update registry.
	downloaded := 0
	for _, bb := range st.Books {
		if bb.Downloaded {
			downloaded++
		}
	}
	s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
		a.BooksDownloaded = downloaded
		a.UpdatedAt = now
	})
	log.Printf("[HMANGA] Updated .books.json for artist %s: %d/%d", folderName, downloaded, len(st.Books))
	return wasNew, nil
}

// reconcileHMBookState verifies zip files on disk vs state and resets missing
// ones to not-downloaded so they get re-downloaded. It only runs when the
// VerifyHMDownloads setting is enabled; otherwise it is a no-op.
func (s *Server) reconcileHMBookState(artistID, folderName string) {
	if !s.config.VerifyHMDownloads {
		return
	}
	s.hmangaStateMu.Lock()

	st := s.hmangaBookStates[artistID]
	if st == nil {
		st, _ = s.loadHMangaBookState(folderName)
		if st == nil {
			s.hmangaStateMu.Unlock()
			return
		}
		st.FolderName = folderName
		s.hmangaBookStates[artistID] = st
	}
	if !s.reconcileHMArtistStateLocked(st, s.config.HMangaDownloadPath) {
		s.hmangaStateMu.Unlock()
		return
	}
	if err := s.saveHMangaBookState(folderName, st); err != nil {
		log.Printf("[HMANGA] Failed to save reconciled state for %s: %v", folderName, err)
		s.hmangaStateMu.Unlock()
		return
	}

	// Count while still holding hmangaStateMu — st.Books is shared state.
	downloaded := 0
	for _, bb := range st.Books {
		if bb.Downloaded {
			downloaded++
		}
	}
	s.hmangaStateMu.Unlock()

	// Rebuild the global cache so the reset books are no longer treated as
	// downloaded by other artists until they are re-downloaded.
	s.rebuildHMangaGlobalCache()

	s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
		a.BooksDownloaded = downloaded
	})
}

// forceHMRedownload deletes all downloaded zips for an artist and resets state,
// then re-downloads everything. It waits for any in-progress sync to finish
// first so its own autoDownloadMissingBooks isn't silently dropped.
//
// Files are only deleted AFTER the reset .books.json is successfully saved.
// If the save fails, in-memory state and the global cache are restored to
// their pre-reset values so the user can retry without losing the
// "downloaded" markers (the files are still on disk at that point).
func (s *Server) forceHMRedownload(artistID, folderName string) {
	if !s.waitHMangaDownloadDone(artistID) {
		// A sync/download for this artist is still running after the wait
		// timeout. Proceeding would delete zips mid-download and record
		// stale state — abort instead.
		log.Printf("[HMANGA] Force redownload aborted for %s: a sync/download is still in progress after wait timeout", folderName)
		return
	}

	// Snapshot the prior in-memory state so we can roll back on save failure.
	var priorBooks map[string]hmanga.BookInfo

	// Build the reset state under hmangaStateMu.
	s.hmangaStateMu.Lock()
	st := s.hmangaBookStates[artistID]
	if st == nil {
		st, _ = s.loadHMangaBookState(folderName)
		if st == nil {
			s.hmangaStateMu.Unlock()
			return
		}
		st.FolderName = folderName
		s.hmangaBookStates[artistID] = st
	}
	// Snapshot the prior per-book state for rollback.
	priorBooks = make(map[string]hmanga.BookInfo, len(st.Books))
	for id, b := range st.Books {
		priorBooks[id] = b
	}
	for id, b := range st.Books {
		if !b.Downloaded {
			continue
		}
		b.Downloaded = false
		b.Filename = ""
		b.DownloadedAt = time.Time{}
		b.FileSize = 0
		b.Extracted = false
		b.DirName = ""
		st.Books[id] = b
	}
	if err := s.saveHMangaBookState(folderName, st); err != nil {
		log.Printf("[HMANGA] Failed to save reset state for %s, rolling back: %v", folderName, err)
		// Roll back in-memory state so the on-disk .books.json (unchanged)
		// and the in-memory map stay consistent.
		for id, b := range priorBooks {
			st.Books[id] = b
		}
		s.hmangaStateMu.Unlock()
		return
	}
	if st.ArtistID != "" {
		s.hmangaBookStates[st.ArtistID] = st
	}
	s.hmangaStateMu.Unlock()

	// State is now safely persisted as "all reset". Delete the on-disk zips
	// (or extracted folders for books recorded as extracted). If a delete
	// fails (file in use, etc.) the disk verification scan on next startup
	// or manual scan-missing will catch it.
	root := s.config.HMangaDownloadPath
	for _, b := range priorBooks {
		if !b.Downloaded || b.Filename == "" {
			continue
		}
		if b.Extracted && b.DirName != "" {
			dirPath := filepath.Join(root, folderName, b.DirName)
			func() { defer func() { recover() }(); os.RemoveAll(dirPath) }()
			continue
		}
		path := filepath.Join(root, folderName, b.Filename)
		func() { defer func() { recover() }(); os.Remove(path) }()
	}

	// Clear this artist's ownership from the global cache so the re-download
	// can claim ownership. Shared books owned by other artists are left intact
	// and will still be skipped during the subsequent auto-download.
	//
	// On a save failure we PUSH FORWARD rather than roll back: at this point
	// the per-artist .books.json is already persisted as "all reset" and the
	// zip files are already deleted, so restoring the prior global cache would
	// leave the system in an inconsistent state where the artist still claims
	// ownership of books that no longer exist on disk — autoDownloadMissingBooks
	// would then skip them and the force-redownload would silently no-op.
	// Rebuilding the global cache from the (now-reset) per-artist state is the
	// only way to reach a consistent on-disk + in-memory state. If even that
	// fails we surface the error and the user can retry.
	s.hmangaGlobalMu.Lock()
	for bookID, entry := range s.hmangaGlobalBooks {
		if entry.OwnerArtistID == artistID {
			delete(s.hmangaGlobalBooks, bookID)
		}
	}
	cache, _ := s.loadHMangaGlobalCache()
	if cache != nil {
		for bookID, entry := range cache.Books {
			if entry.OwnerArtistID == artistID {
				delete(cache.Books, bookID)
			}
		}
		if err := s.saveHMangaGlobalCache(cache); err != nil {
			log.Printf("[HMANGA] Failed to save reset global cache for %s, falling back to rebuild: %v", folderName, err)
			s.hmangaGlobalMu.Unlock()
			// Rebuild the cache from per-artist state. This drops our
			// artist (no .books.json entries claim ownership) and
			// preserves any other artists' entries.
			s.rebuildHMangaGlobalCache()
			s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) { a.BooksDownloaded = 0 })
			s.autoDownloadMissingBooks(artistID)
			return
		}
	}
	s.hmangaGlobalMu.Unlock()

	// Reset registry count separately.
	s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) { a.BooksDownloaded = 0 })

	s.autoDownloadMissingBooks(artistID)
}

// waitHMangaDownloadDone blocks until no sync is running for the artist (or the
// server is stopping). Bounded by a short poll loop. Returns false if the wait
// timed out with a sync still running.
func (s *Server) waitHMangaDownloadDone(artistID string) bool {
	for i := 0; i < 600; i++ { // up to ~5 minutes
		s.mu.RLock()
		running := s.hmangaArtistDownloading[artistID]
		s.mu.RUnlock()
		if !running {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// refreshAndDownloadHMArtist re-scrapes the book list and then downloads any
// missing books. It guards against concurrent refresh/download operations for
// the same artist. opID is the queue operation id; after ExtractBooks returns
// the op is marked in-progress so the dispatcher can start the next op.
func (s *Server) refreshAndDownloadHMArtist(artistID, opID string) {
	s.mu.Lock()
	if s.hmangaArtistDownloading[artistID] {
		s.mu.Unlock()
		return
	}
	s.hmangaArtistDownloading[artistID] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.hmangaArtistDownloading, artistID)
		s.mu.Unlock()
	}()

	s.refreshHMArtistBooks(artistID, opID)
	s.autoDownloadMissingBooks(artistID)
}

// refreshHMArtistBooks re-scrapes the book list for an artist and merges new
// books into .books.json (preserving download status). opID is the queue
// operation id, signaled in-progress once ExtractBooks returns.
func (s *Server) refreshHMArtistBooks(artistID, opID string) {
	s.mu.RLock()
	artist, ok := s.hmangaArtists[artistID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	books, err := s.hmangaMgr.ExtractBooks(artist.URL)
	if err != nil {
		s.markHMangaOpFinished(opID, hmangaOpFailed)
		log.Printf("[HMANGA] Failed to refresh books for %s: %v", artist.FolderName, err)
		return
	}
	s.markHMangaOpInProgress(opID)

	now := time.Now()
	st := s.getHMangaBookState(artist.FolderName)
	if st == nil {
		st = &hmanga.BookStateFile{ArtistID: artistID, FolderName: artist.FolderName, URL: artist.URL, Books: map[string]hmanga.BookInfo{}}
	}
	st.URL = artist.URL
	st.LastSynced = now
	newCount := s.mergeHMBooksIntoState(st, books)
	s.saveAndSyncHMangaBookState(st)

	// Update in-memory book cache + registry.
	s.mu.Lock()
	cache := make([]hmanga.Book, len(books))
	copy(cache, books)
	s.hmangaBooks[artistID] = cache
	s.mu.Unlock()

	bookCount := s.mergeHMBookCount(st)
	s.updateHMangaRegistryEntry(artistID, func(a *hmanga.Artist) {
		a.BookCount = bookCount
		a.LastCheckedAt = now
		a.UpdatedAt = now
	})
	if newCount > 0 {
		log.Printf("[HMANGA] Found %d new books for artist %s", newCount, artist.FolderName)
	}
}

// mergeHMBooksIntoState merges a freshly scraped book list into the artist's
// state file, preserving download status and applying cross-artist dedup from
// the global cache. Returns the number of newly discovered books.
//
// Lock order follows the documented discipline: hmangaStateMu is always
// acquired before hmangaGlobalMu (or alone); hmangaGlobalMu is never held
// while acquiring hmangaStateMu.
func (s *Server) mergeHMBooksIntoState(st *hmanga.BookStateFile, books []hmanga.Book) int {
	if st == nil {
		return 0
	}
	// Snapshot the global cache entries under hmangaGlobalMu and release
	// before touching st.Books, so the two mutexes are never nested here.
	s.hmangaGlobalMu.Lock()
	downloadedEntries := make(map[string]hmanga.GlobalBookEntry, len(books))
	for _, b := range books {
		if entry, ok := s.hmangaGlobalBooks[b.ID]; ok && entry.Downloaded {
			downloadedEntries[b.ID] = entry
		}
	}
	s.hmangaGlobalMu.Unlock()

	s.hmangaStateMu.Lock()
	defer s.hmangaStateMu.Unlock()
	if st.Books == nil {
		st.Books = map[string]hmanga.BookInfo{}
	}
	newCount := 0
	for _, b := range books {
		local, exists := st.Books[b.ID]
		if !exists {
			local = hmanga.BookInfo{ID: b.ID, Title: b.Title, URL: b.URL, Downloaded: false}
			newCount++
		}
		if entry, ok := downloadedEntries[b.ID]; ok {
			local.Downloaded = true
			local.Filename = entry.Filename
			local.DownloadedAt = entry.DownloadedAt
			local.FileSize = entry.FileSize
			local.Extracted = entry.Extracted
			local.DirName = entry.DirName
		}
		st.Books[b.ID] = local
	}
	return newCount
}

// mergeHMBookCount returns the number of books in the state file under
// hmangaStateMu.
func (s *Server) mergeHMBookCount(st *hmanga.BookStateFile) int {
	if st == nil {
		return 0
	}
	s.hmangaStateMu.Lock()
	defer s.hmangaStateMu.Unlock()
	return len(st.Books)
}

// ==================== Scheduler ====================

// loadOrRebuildHMangaGlobalCache loads the global cache from disk at startup,
// or rebuilds it from per-artist .books.json if the file is missing or corrupt.
func (s *Server) loadOrRebuildHMangaGlobalCache() {
	cache, err := s.loadHMangaGlobalCache()
	if err != nil || cache == nil {
		if err != nil {
			log.Printf("[HMANGA] Global cache missing or corrupt, rebuilding: %v", err)
		} else {
			log.Printf("[HMANGA] Global cache missing, rebuilding")
		}
		s.rebuildHMangaGlobalCache()
		return
	}
	s.hmangaGlobalMu.Lock()
	defer s.hmangaGlobalMu.Unlock()
	if cache.Books == nil {
		cache.Books = map[string]hmanga.GlobalBookEntry{}
	}
	s.hmangaGlobalBooks = cache.Books
}

// getGlobalBooksDataDir returns the directory used to store the global
// H-Manga book cache, creating it if necessary.
func (s *Server) getGlobalBooksDataDir() string {
	dir := config.ConfigBaseDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[HMANGA] Failed to create global data dir %s: %v", dir, err)
	}
	return dir
}

// checkAllHMArtistsOnStartup re-scrapes tracked artists once at server
// startup and auto-downloads any newly discovered books. Only artists whose
// auto-check interval is due are included; artists set to "never" or no
// interval are skipped unless manually checked later.
func (s *Server) checkAllHMArtistsOnStartup() {
	log.Printf("[HMANGA] Startup check-all started")
	s.mu.RLock()
	list := make([]hmanga.Artist, 0, len(s.hmangaArtists))
	for _, a := range s.hmangaArtists {
		if a.CheckInterval == "" || a.CheckInterval == "never" {
			continue
		}
		// Skip artists whose URL was cleared at load (non-HentaiNexus URL):
		// refreshing them would produce a guaranteed failed op per cycle.
		if a.URL == "" {
			continue
		}
		if dur, ok := models.ParseCheckInterval(a.CheckInterval); ok {
			if !a.LastCheckedAt.IsZero() && time.Since(a.LastCheckedAt) < dur {
				continue
			}
		}
		list = append(list, *a)
	}
	s.mu.RUnlock()

	for i := range list {
		artist := list[i]
		s.enqueueHMangaOp(&hmangaOpItem{
			kind:     hmangaOpRefresh,
			artistID: artist.ID,
			runRefresh: func(opID string) error {
				s.refreshAndDownloadHMArtist(artist.ID, opID)
				return nil
			},
		})
	}
}

// checkHMangaArtistsForUpdates re-scrapes artists whose check interval is due
// and auto-downloads new books.
func (s *Server) checkHMangaArtistsForUpdates() {
	s.mu.RLock()
	list := make([]hmanga.Artist, 0, len(s.hmangaArtists))
	for _, a := range s.hmangaArtists {
		list = append(list, *a)
	}
	s.mu.RUnlock()

	for i := range list {
		a := &list[i]
		if a.CheckInterval == "" || a.CheckInterval == "never" {
			continue
		}
		// Skip artists whose URL was cleared at load (non-HentaiNexus URL):
		// refreshing them would produce a guaranteed failed op per cycle.
		if a.URL == "" {
			continue
		}
		dur, ok := models.ParseCheckInterval(a.CheckInterval)
		if !ok {
			continue
		}
		should := a.LastCheckedAt.IsZero() || time.Since(a.LastCheckedAt) >= dur
		if !should {
			continue
		}
		if s.isHMangaScrapingPaused() {
			continue
		}
		log.Printf("[HMANGA] Auto-checking artist '%s' (interval: %s)", a.FolderName, a.CheckInterval)
		artistID := a.ID
		s.enqueueHMangaOp(&hmangaOpItem{
			kind:     hmangaOpRefresh,
			artistID: artistID,
			runRefresh: func(opID string) error {
				s.refreshAndDownloadHMArtist(artistID, opID)
				return nil
			},
		})
	}
}

// isHMangaScrapingPaused reports whether the global H-Manga scraping scheduler is paused.
func (s *Server) isHMangaScrapingPaused() bool {
	return s.hmangaScrapingPaused.Load()
}

// isShutdown reports whether the server has started graceful shutdown.
func (s *Server) isShutdown() bool {
	return s.lifecycleShutdown.Load()
}

// setHMangaScrapingPaused enables or disables the global H-Manga scraping scheduler.
func (s *Server) setHMangaScrapingPaused(paused bool) {
	s.hmangaScrapingPaused.Store(paused)
	s.mu.Lock()
	s.config.HMangaScrapingPaused = paused
	s.mu.Unlock()
	if err := config.Save(s.config); err != nil {
		log.Printf("[SCRAPING] Failed to persist H-Manga scraping state: %v", err)
	}
}

// handleGetHMangaScrapingState returns whether global H-Manga scraping is running.
func (s *Server) handleGetHMangaScrapingState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"running": !s.isHMangaScrapingPaused()})
}

// handleToggleHMangaScraping toggles the global H-Manga scraping state.
func (s *Server) handleToggleHMangaScraping(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Running bool `json:"running"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	s.setHMangaScrapingPaused(!req.Running)
	status := "resumed"
	if !req.Running {
		status = "paused"
	}
	log.Printf("[SCRAPING] H-Manga scraping %s (running=%v)", status, req.Running)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"running": req.Running, "status": status})
}
