package download

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/user/comic-scraper/pkg/config"
	"github.com/user/comic-scraper/pkg/fileutil"
	httpclient "github.com/user/comic-scraper/pkg/http"
	"github.com/user/comic-scraper/pkg/models"
	"github.com/user/comic-scraper/pkg/scraper"
)

// Manager handles comic chapter downloads with worker pool
type Manager struct {
	httpClient     *httpclient.Client
	config         *config.Config
	fileOps        *fileutil.Operations
	workerPool     chan struct{}
	active         map[string]*Task
	progressCB     func(*models.Download)
	minImageSizeKB int
	minImageWidth  int
	minImageHeight int
	mu             sync.RWMutex
}

// Task represents a download task
type Task struct {
	ID            string
	SeriesID      string
	ChapterID     string
	SeriesTitle   string
	CustomName    string
	ChapterTitle  string
	ChapterNumber float64
	URL           string
	Images        []models.Image

	// ImageResults stores the per-image download status after DownloadChapter completes.
	// Each entry corresponds to the image at the same index in Images.
	Mu           sync.Mutex
	ImageResults []models.ImageResult

	// cancel is set when CancelTask is called for this specific run; the
	// download loop checks it between images so cancellation actually stops
	// the work instead of only removing the map entry.
	cancel atomic.Pointer[context.CancelCauseFunc]
}

// NewManager creates a new download manager with the given configuration
func NewManager(cfg *config.Config, httpClient *httpclient.Client) *Manager {
	// Use 3 concurrent workers by default
	workerPool := make(chan struct{}, 3)

	return &Manager{
		httpClient:     httpClient,
		config:         cfg,
		fileOps:        fileutil.NewOperations(httpClient),
		workerPool:     workerPool,
		active:         make(map[string]*Task),
		minImageSizeKB: cfg.MinImageSizeKB,
		minImageWidth:  cfg.MinImageWidth,
		minImageHeight: cfg.MinImageHeight,
	}
}

// SetProgressCallback sets the progress callback function
// BUG FIX #2: This is called BEFORE starting downloads to avoid race condition
func (m *Manager) SetProgressCallback(cb func(*models.Download)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progressCB = cb
}

// DownloadChapter downloads a chapter with the given images
// BUG FIX #2: Callback is already set before this is called
// BUG FIX #3: Verifies download completion before marking status
func (m *Manager) DownloadChapter(task *Task, download *models.Download) error {
	// Snapshot the mutable config under the lock so concurrent Set* calls
	// (settings save) cannot race the download loop's reads.
	m.mu.Lock()
	m.active[task.ID] = task
	downloadPath := m.config.DownloadPath
	minImageSizeKB := m.minImageSizeKB
	minImageWidth := m.minImageWidth
	minImageHeight := m.minImageHeight
	m.mu.Unlock()

	// Per-run cancellation: a new run of the same task ID replaces this entry,
	// so CancelTask cancels via the run's own context.
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	task.cancel.Store(&cancel)

	// Acquire worker slot
	m.workerPool <- struct{}{}
	defer func() { <-m.workerPool }()

	// Update status to downloading
	download.Status = models.StatusDownloading
	download.ImageCount = len(task.Images)
	download.DownloadedCount = 0
	download.UpdatedAt = time.Now()
	m.notifyProgress(download)

	// Validate image sequence — log warnings but do not block downloads
	if report := scraper.InspectImageSequence(task.Images, 0); len(report.Warnings) > 0 {
		for _, w := range report.Warnings {
			log.Printf("[DOWNLOAD] Sequence warning for chapter %.1f: %s", task.ChapterNumber, w)
		}
		if len(report.Gaps) > 0 {
			log.Printf("[DOWNLOAD] Gap pages detected: %v — re-extraction recommended for %s", report.Gaps, task.URL)
		}
		if report.SuspectFirst {
			log.Printf("[DOWNLOAD] First image may not be a page (cover/banner?) for %s", task.URL)
		}
		if report.SuspectLast {
			log.Printf("[DOWNLOAD] Last image may not be a page (nav/footer?) for %s", task.URL)
		}
	}

	// Create output directory
	chapterNum := strconv.FormatFloat(task.ChapterNumber, 'f', -1, 64)
	if chapterNum == "0" {
		chapterNum = "1"
	}
	chapterDir := filepath.Join(downloadPath, task.CustomName, "Chapter "+chapterNum)
	if err := m.fileOps.EnsureDir(chapterDir); err != nil {
		download.Status = models.StatusFailed
		download.UpdatedAt = time.Now()
		m.notifyProgress(download)
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Download each image
	downloadedCount := 0
	totalImages := len(task.Images)
	imageResults := make([]models.ImageResult, totalImages)
	for i, img := range task.Images {
		// Honor cancellation between images.
		select {
		case <-ctx.Done():
			download.Status = models.StatusCancelled
			download.UpdatedAt = time.Now()
			m.notifyProgress(download)
			m.mu.Lock()
			if m.active[task.ID] == task {
				delete(m.active, task.ID)
			}
			m.mu.Unlock()
			return context.Cause(ctx)
		default:
		}
		pageNum := i + 1
		ext := scraper.GetImageExtension(img.URL)
		filename := fmt.Sprintf("%03d%s", pageNum, ext)
		destPath := filepath.Join(chapterDir, filename)

		// Skip only when a real, non-empty file already exists — a 0-byte or
		// truncated file left by a crash should be re-downloaded.
		if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
			downloadedCount++
			download.DownloadedCount = downloadedCount
			download.Progress = float64(downloadedCount) / float64(totalImages) * 100
			download.UpdatedAt = time.Now()
			m.notifyProgress(download)
			imageResults[i] = models.ImageResult{
				Page:   pageNum,
				URL:    img.URL,
				Status: models.ImageStatusSkipped,
				Reason: "already exists on disk",
			}
			continue
		}

		// Only apply portrait aspect ratio check to first/last images in the
		// sequence — these are the positions where banners/covers/nav images
		// typically appear. Middle pages should always be downloaded regardless
		// of aspect ratio.
		checkPortrait := totalImages > 1 && (i == 0 || i == totalImages-1)

		if err := m.fileOps.SaveImage(img.URL, destPath, minImageSizeKB, minImageWidth, minImageHeight, checkPortrait); err != nil {
			// If the image was intentionally filtered (wrong dimensions or too small),
			// skip it without retrying the fallback URL
			if fileutil.IsImageFiltered(err) {
				fmt.Printf("Skipping image %d: %v\n", pageNum, err)
				imageResults[i] = models.ImageResult{
					Page:   pageNum,
					URL:    img.URL,
					Status: models.ImageStatusFiltered,
					Reason: err.Error(),
				}
				continue
			}
			// Try fallback URL if available (e.g., DemonicScans uses librarydm.com as fallback
			// when demoniclibs.com fails)
			if img.FallbackURL != "" {
				if fallbackErr := m.fileOps.SaveImage(img.FallbackURL, destPath, minImageSizeKB, minImageWidth, minImageHeight, checkPortrait); fallbackErr != nil {
					// Both primary and fallback failed
					fmt.Printf("Warning: Failed to download image %d (primary: %v, fallback: %v)\n", pageNum, err, fallbackErr)
					imageResults[i] = models.ImageResult{
						Page:   pageNum,
						URL:    img.URL,
						Status: models.ImageStatusFailed,
						Reason: fmt.Sprintf("primary: %v; fallback: %v", err, fallbackErr),
					}
					continue
				}
				// Fallback succeeded
				downloadedCount++
				download.DownloadedCount = downloadedCount
				download.Progress = float64(downloadedCount) / float64(totalImages) * 100
				download.UpdatedAt = time.Now()
				m.notifyProgress(download)
				imageResults[i] = models.ImageResult{
					Page:   pageNum,
					URL:    img.FallbackURL,
					Status: models.ImageStatusOK,
					Reason: "downloaded via fallback URL",
				}
				continue
			} else {
				// No fallback available
				fmt.Printf("Warning: Failed to download image %d: %v\n", pageNum, err)
				imageResults[i] = models.ImageResult{
					Page:   pageNum,
					URL:    img.URL,
					Status: models.ImageStatusFailed,
					Reason: err.Error(),
				}
				continue
			}
		}

		downloadedCount++
		download.DownloadedCount = downloadedCount
		download.Progress = float64(downloadedCount) / float64(len(task.Images)) * 100
		download.UpdatedAt = time.Now()
		m.notifyProgress(download)
		imageResults[i] = models.ImageResult{
			Page:   pageNum,
			URL:    img.URL,
			Status: models.ImageStatusOK,
		}
	}

	// Store image results on the task
	task.Mu.Lock()
	task.ImageResults = imageResults
	task.Mu.Unlock()

	// Copy image results to the Download object so progress callbacks can access them
	download.PerImageStatus = imageResults

	// Count filtered and skipped images separately so a chapter that only
	// contained filtered images (banners, wrong dimensions, too small) is not
	// treated as a failed download and retried forever. A chapter whose images
	// were already on disk is still complete.
	filteredCount := 0
	failedCount := 0
	skippedCount := 0
	for _, r := range imageResults {
		switch r.Status {
		case models.ImageStatusFiltered:
			filteredCount++
		case models.ImageStatusFailed:
			failedCount++
		case models.ImageStatusSkipped:
			skippedCount++
		}
	}

	// Determine final status. A chapter is complete if every non-filtered image
	// was downloaded or already existed on disk. If some failed, it's partial
	// (or failed if nothing usable was downloaded). If all images were
	// filtered, it's partial with 0 files so the caller can mark it
	// intentionally skipped.
	effectiveCount := downloadedCount + skippedCount
	nonFilteredCount := len(task.Images) - filteredCount
	if nonFilteredCount <= 0 {
		nonFilteredCount = len(task.Images)
	}
	if failedCount > 0 {
		download.Status = models.StatusPartial
		if effectiveCount == 0 {
			download.Status = models.StatusFailed
		}
		download.Progress = float64(effectiveCount) / float64(nonFilteredCount) * 100
	} else if filteredCount == len(task.Images) {
		download.Status = models.StatusPartial
		download.Progress = 0
	} else if effectiveCount == len(task.Images)-filteredCount {
		download.Status = models.StatusCompleted
		download.Progress = 100
	} else {
		download.Status = models.StatusPartial
		download.Progress = float64(effectiveCount) / float64(nonFilteredCount) * 100
	}
	download.UpdatedAt = time.Now()
	m.notifyProgress(download)

	m.mu.Lock()
	// Only remove this run's entry: if the same task ID was started again
	// after a CancelTask, the newer run owns the map entry now.
	if m.active[task.ID] == task {
		delete(m.active, task.ID)
	}
	m.mu.Unlock()

	if download.Status == models.StatusFailed {
		return fmt.Errorf("failed to download any images")
	}

	return nil
}

// notifyProgress calls the progress callback if set
func (m *Manager) notifyProgress(download *models.Download) {
	m.mu.RLock()
	cb := m.progressCB
	m.mu.RUnlock()
	if cb != nil {
		cb(download)
	}
}

// FileOps returns the file operations instance.
func (m *Manager) FileOps() *fileutil.Operations {
	return m.fileOps
}

// GetActiveTasks returns currently active download tasks
func (m *Manager) GetActiveTasks() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tasks := make([]*Task, 0, len(m.active))
	for _, task := range m.active {
		tasks = append(tasks, task)
	}
	return tasks
}

// IsActive checks if a task is currently active
func (m *Manager) IsActive(taskID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.active[taskID]
	return ok
}

// CancelTask cancels an active download task. The running DownloadChapter
// observes the cancellation between images and stops; only the map entry for
// this specific run is removed.
func (m *Manager) CancelTask(taskID string) bool {
	m.mu.Lock()
	task, ok := m.active[taskID]
	if ok {
		delete(m.active, taskID)
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	if cancel := task.cancel.Load(); cancel != nil {
		(*cancel)(fmt.Errorf("cancelled by user"))
	}
	return true
}

func (m *Manager) SetDownloadPath(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.DownloadPath = path
}

func (m *Manager) SetMinImageSizeKB(kb int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.minImageSizeKB = kb
}

func (m *Manager) SetMinImageWidth(w int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.minImageWidth = w
}

func (m *Manager) SetMinImageHeight(h int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.minImageHeight = h
}
