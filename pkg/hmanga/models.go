package hmanga

import "time"

// Artist represents a tracked HentaiNexus artist in the central registry.
type Artist struct {
	ID              string    `json:"id"`
	FolderName      string    `json:"folderName"` // artist name, used as subdirectory
	URL             string    `json:"url"`        // artist search URL (?q=artist:NAME)
	CheckInterval   string    `json:"checkInterval"`
	LastCheckedAt   time.Time `json:"lastCheckedAt"`
	BookCount       int       `json:"bookCount"`       // total books found online
	BooksDownloaded int       `json:"booksDownloaded"` // cached count
	AddedAt         time.Time `json:"addedAt"`
	UpdatedAt       time.Time `json:"updatedAt"`

	// Scraping is a transient (not persisted) flag set while a background
	// scrape of the artist's book list is in progress. The UI uses it to
	// show a "Scraping..." status before the first bookCount is known.
	// The server clears it when loading the registry from disk so a value
	// written mid-scrape never survives a restart; it is still serialized
	// for the live API responses the UI reads it from.
	Scraping bool `json:"scraping,omitempty"`
}

// ArtistRegistry is the central registry of tracked H-Manga artists.
type ArtistRegistry struct {
	Version int      `json:"version"`
	Artists []Artist `json:"artists"`
}

// Book represents a single HentaiNexus book discovered from an artist's listing.
type Book struct {
	ID    string `json:"id"`    // numeric id from /view/{id}
	Title string `json:"title"` // book title
	URL   string `json:"url"`   // /view/{id}
}

// BookInfo is the per-book state stored in .books.json.
type BookInfo struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	URL          string    `json:"url"`
	Downloaded   bool      `json:"downloaded"`
	Filename     string    `json:"filename,omitempty"`
	DownloadedAt time.Time `json:"downloadedAt,omitempty"`
	FileSize     int64     `json:"fileSize,omitempty"`
	// Extracted indicates the ZIP was extracted into a directory with the
	// same basename as the ZIP (<ArtistFolder>/<zipBasename>/) and the ZIP
	// was deleted. When true, the on-disk artifact is the extracted folder,
	// not the ZIP referenced by Filename.
	Extracted bool `json:"extracted,omitempty"`
	// DirName is the extracted directory name (same basename as the ZIP,
	// without the ".zip" extension). Only set when Extracted is true.
	DirName string `json:"dirName,omitempty"`
}

// BookStateFile is stored in each artist folder to track per-book downloads.
type BookStateFile struct {
	ArtistID   string              `json:"artistId"`
	FolderName string              `json:"folderName"`
	URL        string              `json:"url"` // artist search URL
	LastSynced time.Time           `json:"lastSynced"`
	Books      map[string]BookInfo `json:"books"` // book id -> info
}

// GlobalBookEntry tracks cross-artist deduplication state for a single book.
type GlobalBookEntry struct {
	Downloaded      bool      `json:"downloaded"`
	Filename        string    `json:"filename"`
	OwnerArtistID   string    `json:"ownerArtistID"`
	OwnerFolderName string    `json:"ownerFolderName"`
	DownloadedAt    time.Time `json:"downloadedAt"`
	FileSize        int64     `json:"fileSize"`
	// Extracted mirrors BookInfo.Extracted: the artifact is an extracted
	// folder (<OwnerFolderName>/<dirName>/), not a ZIP file.
	Extracted bool `json:"extracted,omitempty"`
	// DirName is the extracted directory name (same basename as the ZIP,
	// without the ".zip" extension). Only set when Extracted is true.
	DirName string `json:"dirName,omitempty"`
}

// GlobalBookCache is the top-level persisted global book state.
type GlobalBookCache struct {
	Version int                        `json:"version"`
	Books   map[string]GlobalBookEntry `json:"books"` // book id -> dedup info
}
