package models

import (
	"time"
)

// DownloadStatus represents the state of a download
type DownloadStatus int

const (
	StatusPending DownloadStatus = iota
	StatusDownloading
	StatusCompleted
	StatusFailed
	StatusCancelled
	StatusPartial
)

func (s DownloadStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusDownloading:
		return "downloading"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	case StatusPartial:
		return "partial"
	default:
		return "unknown"
	}
}

// MarshalText makes encoding/json marshal DownloadStatus as its human-readable
// string name (e.g. "downloading") instead of the raw int value. The web UI's
// Downloads tab compares d.status against string literals, so without this the
// status badge shows a number and stale-download auto-cleanup never fires.
func (s DownloadStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalText is the inverse of MarshalText so persisted JSON containing
// status strings ("downloading", "partial", ...) round-trips correctly.
func (s *DownloadStatus) UnmarshalText(text []byte) error {
	switch string(text) {
	case "pending":
		*s = StatusPending
	case "downloading":
		*s = StatusDownloading
	case "completed":
		*s = StatusCompleted
	case "failed":
		*s = StatusFailed
	case "cancelled":
		*s = StatusCancelled
	case "partial":
		*s = StatusPartial
	default:
		*s = StatusPending
	}
	return nil
}

// Site constants for supported comic sites
const (
	SiteAsura        = "asura"
	SiteLua          = "lua"
	SiteDemonic      = "demonic"
	SiteManhuaus     = "manhuaus"
	SiteManhuaPlus   = "manhuaplus"
	SiteDrake        = "drake"
	SiteThunderscans = "thunderscans"
	SiteRavenscans   = "ravenscans"
)

// Series represents a comic/manga series
type Series struct {
	ID                 string    `json:"id"`
	Title              string    `json:"title"`
	CustomName         string    `json:"customName,omitempty"`
	URL                string    `json:"url"`
	ChapterCount       int       `json:"chapterCount"`
	ChaptersDownloaded int       `json:"chaptersDownloaded"`
	CheckInterval      string    `json:"checkInterval,omitempty"`
	LastCheckedAt      time.Time `json:"lastCheckedAt,omitempty"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// SeriesInfo holds metadata extracted from a series page.
type SeriesInfo struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Author      string   `json:"author"`
	Status      string   `json:"status"`
	Genres      []string `json:"genres"`
	CoverURL    string   `json:"coverUrl"`
	Source      string   `json:"source"`
}

// Chapter represents a single chapter
type Chapter struct {
	ID         string  `json:"id"`
	Title      string  `json:"title"`
	URL        string  `json:"url"`
	Number     float64 `json:"number"`
	Downloaded bool    `json:"downloaded"`
}

// Download represents a download task
type Download struct {
	ID              string         `json:"id"`
	SeriesID        string         `json:"seriesId"`
	ChapterID       string         `json:"chapterId"`
	SeriesTitle     string         `json:"seriesTitle"`
	CustomName      string         `json:"customName,omitempty"`
	ChapterTitle    string         `json:"chapterTitle"`
	Status          DownloadStatus `json:"status"`
	Progress        float64        `json:"progress"`
	ImageCount      int            `json:"imageCount"`
	DownloadedCount int            `json:"downloadedCount"`
	CreatedAt       time.Time      `json:"createdAt"`
	UpdatedAt       time.Time      `json:"updatedAt"`
	PerImageStatus  []ImageResult  `json:"perImageStatus,omitempty"`

	// Phase is an H-Manga-only sub-state ("pending","starting","downloading",
	// "verifying","cancelled","superseded"). Empty for manga downloads.
	Phase string `json:"phase,omitempty"`
	// BytesDownloaded is the live byte count for H-Manga downloads.
	BytesDownloaded int64 `json:"bytesDownloaded,omitempty"`
	// TotalBytes is the expected final size when known (H-Manga only).
	TotalBytes int64 `json:"totalBytes,omitempty"`
}

// Image represents a comic page image
type Image struct {
	URL         string // Primary image URL
	FallbackURL string // Alternative URL to try if primary fails (e.g., CDN fallback)
	Page        int
	Quality     string
}

// ImageDownloadStatus represents the result of downloading a single image
type ImageDownloadStatus string

const (
	// ImageStatusOK means the image was downloaded successfully
	ImageStatusOK ImageDownloadStatus = "ok"
	// ImageStatusFiltered means the image was intentionally skipped (wrong dimensions, too small)
	ImageStatusFiltered ImageDownloadStatus = "filtered"
	// ImageStatusFailed means the image download failed (network error, server error, etc.)
	ImageStatusFailed ImageDownloadStatus = "failed"
	// ImageStatusSkipped means the image was skipped because it already existed on disk
	ImageStatusSkipped ImageDownloadStatus = "skipped"
)

// ImageResult tracks the status and reason for a single image download
type ImageResult struct {
	Page   int                 `json:"page"`
	URL    string              `json:"url"`
	Status ImageDownloadStatus `json:"status"`
	Reason string              `json:"reason,omitempty"` // Human-readable reason for filtered/failed status
}

// IsRetryable returns true if a failed image download should be retried
// (i.e., the failure was transient, not intentional)
func (r *ImageResult) IsRetryable() bool {
	return r.Status == ImageStatusFailed
}

// ProgressCallback is called during download progress
type ProgressCallback func(downloaded, total int)

// NewSeries creates a new Series with timestamps
func NewSeries(title, url string) *Series {
	now := time.Now()
	return &Series{
		Title:              title,
		URL:                url,
		ChaptersDownloaded: 0,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

// NewChapter creates a new Chapter
func NewChapter(title, url string, number float64) *Chapter {
	return &Chapter{
		Title:  title,
		URL:    url,
		Number: number,
	}
}

// NewDownload creates a new Download with timestamps
func NewDownload(seriesID, chapterID, seriesTitle, chapterTitle string) *Download {
	now := time.Now()
	return &Download{
		SeriesID:     seriesID,
		ChapterID:    chapterID,
		SeriesTitle:  seriesTitle,
		ChapterTitle: chapterTitle,
		Status:       StatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// UpdateProgress updates the download progress
func (d *Download) UpdateProgress(downloaded, total int) {
	d.DownloadedCount = downloaded
	d.ImageCount = total
	if total > 0 {
		d.Progress = float64(downloaded) / float64(total) * 100
	}
	d.UpdatedAt = time.Now()
}

// MarkCompleted marks the download as completed
func (d *Download) MarkCompleted() {
	d.Status = StatusCompleted
	d.Progress = 100
	d.UpdatedAt = time.Now()
}

// MarkFailed marks the download as failed
func (d *Download) MarkFailed() {
	d.Status = StatusFailed
	d.UpdatedAt = time.Now()
}

// MarkPartial marks the download as partially completed
func (d *Download) MarkPartial() {
	d.Status = StatusPartial
	d.UpdatedAt = time.Now()
}

// ParseCheckInterval converts interval string to duration
// Supported: "never", "5m", "15m", "30m", "1h", "6h", "12h", "24h"
func ParseCheckInterval(interval string) (time.Duration, bool) {
	if interval == "" || interval == "never" {
		return 0, false
	}

	switch interval {
	case "5m":
		return 5 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "1h":
		return 1 * time.Hour, true
	case "6h":
		return 6 * time.Hour, true
	case "12h":
		return 12 * time.Hour, true
	case "24h":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}

// ShouldCheckNow returns true if the series should be checked for updates
func (s *Series) ShouldCheckNow() bool {
	if s.CheckInterval == "" || s.CheckInterval == "never" {
		return false
	}

	duration, ok := ParseCheckInterval(s.CheckInterval)
	if !ok {
		return false
	}

	// If never checked, or enough time has passed
	if s.LastCheckedAt.IsZero() {
		return true
	}

	return time.Since(s.LastCheckedAt) >= duration
}
