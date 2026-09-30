package download

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/user/comic-scraper/pkg/config"
	httpclient "github.com/user/comic-scraper/pkg/http"
	"github.com/user/comic-scraper/pkg/models"
)

func TestDownloadChapter_ResumedCount(t *testing.T) {
	// Setup temp dir
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Setup mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("fake image data"))
	}))
	defer server.Close()

	cfg := &config.Config{
		DownloadPath:   tempDir,
		UserAgent:      "test-agent",
		MinImageSizeKB: 0,
		MinImageWidth:  0,
		MinImageHeight: 0,
	}

	httpClient := httpclient.NewClient(0, cfg.UserAgent)
	mgr := NewManager(cfg, httpClient)

	// Define a task with 5 images
	task := &Task{
		ID:            "task1",
		CustomName:    "TestSeries",
		ChapterNumber: 1,
		Images: []models.Image{
			{URL: server.URL + "/1.jpg"},
			{URL: server.URL + "/2.jpg"},
			{URL: server.URL + "/3.jpg"},
			{URL: server.URL + "/4.jpg"},
			{URL: server.URL + "/5.jpg"},
		},
	}

	// Pre-create 2 images on disk to simulate a resume scenario
	chapterDir := filepath.Join(tempDir, task.CustomName, "Chapter 1")
	if err := os.MkdirAll(chapterDir, 0755); err != nil {
		t.Fatal(err)
	}
	// Use the format expected by the manager: 001.jpg, 002.jpg...
	if err := os.WriteFile(filepath.Join(chapterDir, "001.jpg"), []byte("existing data 1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(chapterDir, "002.jpg"), []byte("existing data 2"), 0644); err != nil {
		t.Fatal(err)
	}

	download := models.NewDownload("s1", "c1", "TestSeries", "Chapter 1")

	// Execute download
	err = mgr.DownloadChapter(task, download)
	if err != nil {
		t.Fatalf("DownloadChapter failed: %v", err)
	}

	// Verify that DownloadedCount is exactly the total number of images, not double-counted
	if download.DownloadedCount != 5 {
		t.Errorf("Expected DownloadedCount to be 5, got %d", download.DownloadedCount)
	}

	// Also check that the progress is 100%
	if download.Progress != 100 {
		t.Errorf("Expected Progress to be 100, got %f", download.Progress)
	}
}
