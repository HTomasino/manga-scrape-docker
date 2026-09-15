package fileutil

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	httpclient "github.com/user/comic-scraper/pkg/http"
	"github.com/user/comic-scraper/pkg/scraper"
)

// ErrImageFiltered is returned when an image is intentionally skipped
// due to failing a validation check (wrong dimensions, too small, etc.)
// rather than a download failure. Callers should not retry with fallback URLs.
type ErrImageFiltered struct {
	Reason string
}

func (e *ErrImageFiltered) Error() string {
	return e.Reason
}

// IsImageFiltered checks if an error is an ErrImageFiltered, meaning the
// image was intentionally skipped (not a download failure).
func IsImageFiltered(err error) bool {
	_, ok := err.(*ErrImageFiltered)
	return ok
}

// Operations provides file utility functions
type Operations struct {
	httpClient *httpclient.Client
}

// NewOperations creates a new file operations instance
func NewOperations(httpClient *httpclient.Client) *Operations {
	return &Operations{
		httpClient: httpClient,
	}
}

// EnsureDir creates a directory and all parent directories if they don't exist
func (o *Operations) EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

// SaveImage downloads an image from a URL and saves it to the specified path.
// Filtering strategy:
//
//	Dimensions available  → dimension-based filtering (portrait check when checkPortrait + min width/height).
//	Dimensions unavailable → fall back to size-based filtering (minSizeKB).
//
// Images that fail both dimension and portrait checks are skipped with ErrImageFiltered
// rather than treated as download failures.
//
// checkPortrait controls whether the portrait orientation check (width >= height → skip)
// is applied. Set to true for first/last images in a sequence where banners/covers are
// common; set to false for middle pages that should always be saved regardless of aspect ratio.
func (o *Operations) SaveImage(url string, destPath string, minSizeKB int, minWidth int, minHeight int, checkPortrait bool) error {
	// Ensure directory exists
	dir := filepath.Dir(destPath)
	if err := o.EnsureDir(dir); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// For data URLs or local files, handle specially
	if strings.HasPrefix(url, "data:") {
		return &ErrImageFiltered{Reason: fmt.Sprintf("data: URL image not saved (no file produced): %s", url)}
	}

	if strings.HasPrefix(url, "file://") {
		srcPath := strings.TrimPrefix(url, "file://")
		if err := o.EnsureDir(dir); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		return copyFile(srcPath, destPath)
	}

	// Download from URL
	data, err := o.httpClient.FetchBytes(url)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}

	// Try dimension-based filtering first.
	// When dimensions are available, we use portrait + min dimension checks.
	// Size-based filtering applies as a secondary guard regardless of
	// dimension availability. When dimensions cannot be determined (e.g.
	// unsupported format), size is the only filter available.
	width, height, dimErr := decodeImageDimensions(data)
	if dimErr == nil {
		// Dimensions available — use dimension-based filtering.

		// Portrait check: comic pages should be taller than they are wide.
		// Skip landscape/square images (banners, ads, icons, etc.).
		// Only applied when checkPortrait is true (first/last images in
		// sequence) — middle pages are always saved regardless of ratio.
		if checkPortrait && width >= height {
			return &ErrImageFiltered{Reason: fmt.Sprintf("image not portrait (%dx%d, width >= height): %s", width, height, url)}
		}

		// Minimum width check (if configured).
		if minWidth > 0 && width < minWidth {
			return &ErrImageFiltered{Reason: fmt.Sprintf("image too narrow (%dpx < %dpx min): %s", width, minWidth, url)}
		}

		// Minimum height check (if configured).
		if minHeight > 0 && height < minHeight {
			return &ErrImageFiltered{Reason: fmt.Sprintf("image too short (%dpx < %dpx min): %s", height, minHeight, url)}
		}
	}

	// Size-based check applies as a secondary guard for all images,
	// whether or not dimensions were available. This catches edge cases
	// like very small tracking pixels that happen to pass the portrait
	// check (e.g. a 2x3 transparent PNG).
	if minSizeKB > 0 {
		sizeKB := len(data) / 1024
		if sizeKB < minSizeKB {
			if dimErr != nil {
				return &ErrImageFiltered{Reason: fmt.Sprintf("image too small (%d KB < %d KB) and dimensions unavailable: %s", sizeKB, minSizeKB, url)}
			}
			return &ErrImageFiltered{Reason: fmt.Sprintf("image too small (%d KB < %d KB, dimensions %dx%d): %s", sizeKB, minSizeKB, width, height, url)}
		}
	}

	// Log when dimension detection fails so the user knows this image bypassed
	// the dimension filter. At this point it passed the size check (if any).
	if dimErr != nil {
		fmt.Printf("Warning: Could not verify image dimensions for %s: %v\n", url, dimErr)
	}

	// Write to a temp file and rename so a crash mid-write cannot leave a
	// truncated file at destPath (which resume logic would treat as a
	// valid, already-downloaded image).
	if err := WriteFileAtomic(destPath, data, 0644); err != nil {
		return err
	}
	return nil
}

// decodeImageDimensions extracts the width and height from image data.
// Supports JPEG, PNG, GIF via Go's image.DecodeConfig, and WebP via
// manual RIFF/WebP header parsing. Returns an error if dimensions cannot
// be determined (caller should treat this as non-fatal).
func decodeImageDimensions(data []byte) (int, int, error) {
	reader := bytes.NewReader(data)

	// Try standard image.DecodeConfig first (handles JPEG, PNG, GIF)
	cfg, _, err := image.DecodeConfig(reader)
	if err == nil {
		return cfg.Width, cfg.Height, nil
	}

	// Fall back to WebP dimension parsing from RIFF header
	reader.Seek(0, 0)
	if w, h, webpErr := decodeWebPDimensions(reader); webpErr == nil {
		return w, h, nil
	}

	return 0, 0, fmt.Errorf("could not determine image dimensions")
}

// decodeWebPDimensions parses WebP RIFF header to extract image dimensions.
// WebP format: RIFF<size>WEBP, then a chunk: <4-byte type><4-byte size><data>.
// Supported chunks: VP8 (lossy), VP8L (lossless), VP8X (extended).
func decodeWebPDimensions(r io.ReadSeeker) (int, int, error) {
	// Read and validate RIFF header (12 bytes: "RIFF" + size + "WEBP")
	header := make([]byte, 12)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, err
	}
	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WEBP" {
		return 0, 0, fmt.Errorf("not a valid WebP file")
	}

	// Read chunk type (4 bytes)
	chunkHeader := make([]byte, 4)
	if _, err := io.ReadFull(r, chunkHeader); err != nil {
		return 0, 0, err
	}
	chunkType := string(chunkHeader)

	// Skip the 4-byte chunk size field. The chunk data follows immediately
	// after, and we know the structure of each supported chunk type, so the
	// size is not needed to locate the dimension fields.
	if _, err := r.Seek(4, io.SeekCurrent); err != nil {
		return 0, 0, err
	}

	switch chunkType {
	case "VP8 ":
		// Lossy WebP: 3-byte frame tag, 3-byte key-frame signature (9D 01 2A),
		// then 2-byte width (low 14 bits) and 2-byte height (low 14 bits).
		hdr := make([]byte, 6)
		if _, err := io.ReadFull(r, hdr); err != nil {
			return 0, 0, err
		}
		if hdr[3] != 0x9D || hdr[4] != 0x01 || hdr[5] != 0x2A {
			return 0, 0, fmt.Errorf("invalid VP8 signature")
		}
		dimBytes := make([]byte, 4)
		if _, err := io.ReadFull(r, dimBytes); err != nil {
			return 0, 0, err
		}
		// Width: bits 0-13, Height: bits 16-29 (little-endian)
		val := binary.LittleEndian.Uint32(dimBytes)
		width := int(val & 0x3FFF)
		height := int((val >> 16) & 0x3FFF)
		return width, height, nil

	case "VP8L":
		// Lossless WebP: 1-byte signature (0x2F), then 4 bytes with packed
		// 14-bit width-1 and 14-bit height-1 (little-endian).
		sig := make([]byte, 1)
		if _, err := io.ReadFull(r, sig); err != nil {
			return 0, 0, err
		}
		if sig[0] != 0x2F {
			return 0, 0, fmt.Errorf("invalid VP8L signature")
		}
		dimBytes := make([]byte, 4)
		if _, err := io.ReadFull(r, dimBytes); err != nil {
			return 0, 0, err
		}
		val := binary.LittleEndian.Uint32(dimBytes)
		width := int((val & 0x3FFF) + 1)
		height := int(((val >> 14) & 0x3FFF) + 1)
		return width, height, nil

	case "VP8X":
		// Extended WebP: 4 bytes flags, then 24-bit width-1 and 24-bit height-1 (LE).
		flags := make([]byte, 4)
		if _, err := io.ReadFull(r, flags); err != nil {
			return 0, 0, err
		}
		dimBytes := make([]byte, 6)
		if _, err := io.ReadFull(r, dimBytes); err != nil {
			return 0, 0, err
		}
		width := int(dimBytes[0]) | int(dimBytes[1])<<8 | int(dimBytes[2])<<16
		height := int(dimBytes[3]) | int(dimBytes[4])<<8 | int(dimBytes[5])<<16
		return width + 1, height + 1, nil

	default:
		return 0, 0, fmt.Errorf("unknown WebP chunk type: %s", chunkType)
	}
}

// WriteFileAtomic writes data to path via a temp file and rename, so a crash
// mid-write can never leave a truncated file at path. All on-disk state
// writers (config, chapter state, images) should use this instead of
// os.WriteFile directly.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, perm); err != nil {
		return fmt.Errorf("failed to write temp file %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to move temp file into place: %w", err)
	}
	return nil
}

// ExtractZip extracts every entry of the ZIP at zipPath into destDir. Each
// entry name is validated against destDir (zip-slip guard) so malicious
// absolute paths or "../" traversal inside the archive cannot escape the
// destination directory. Directory entries are created; existing files are
// overwritten. Returns the number of files extracted (directories excluded).
func ExtractZip(zipPath, destDir string) (int, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	absDest, err := filepath.Abs(destDir)
	if err != nil {
		return 0, fmt.Errorf("resolve dest dir: %w", err)
	}
	if err := os.MkdirAll(absDest, 0755); err != nil {
		return 0, fmt.Errorf("create dest dir: %w", err)
	}

	extracted := 0
	for _, f := range r.File {
		// Zip-slip guard: the entry must resolve to a path strictly inside
		// destDir. Rejects absolute entry names, drive letters, and any
		// "../" traversal inside the archive.
		target := filepath.Join(absDest, filepath.Clean(f.Name))
		absTarget, aerr := filepath.Abs(target)
		if aerr != nil {
			return extracted, fmt.Errorf("resolve entry path: %w", aerr)
		}
		rel, rerr := filepath.Rel(absDest, absTarget)
		if rerr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return extracted, fmt.Errorf("zip entry %q escapes destination directory", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(absTarget, 0755); err != nil {
				return extracted, fmt.Errorf("create zip dir %q: %w", f.Name, err)
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(absTarget), 0755); err != nil {
			return extracted, fmt.Errorf("create parent dir for %q: %w", f.Name, err)
		}

		src, err := f.Open()
		if err != nil {
			return extracted, fmt.Errorf("open zip entry %q: %w", f.Name, err)
		}

		dst, err := os.Create(absTarget)
		if err != nil {
			src.Close()
			return extracted, fmt.Errorf("create file for %q: %w", f.Name, err)
		}

		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		src.Close()
		if copyErr != nil {
			return extracted, fmt.Errorf("extract zip entry %q: %w", f.Name, copyErr)
		}
		if closeErr != nil {
			return extracted, fmt.Errorf("close %q: %w", absTarget, closeErr)
		}
		extracted++
	}
	return extracted, nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	// Check the close error so a failed flush (e.g. disk full) is reported
	// instead of silently treated as a successful copy.
	return dstFile.Close()
}

// reservedDeviceNamePattern matches Windows reserved device names (case-insensitive),
// optionally with a file extension (e.g. "NUL", "con.txt", "COM1.zip").
var reservedDeviceNamePattern = regexp.MustCompile(`(?i)^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?$`)

// SanitizeFileName removes invalid characters from a file name
func SanitizeFileName(name string) string {
	// Remove invalid characters for Windows
	name = strings.ReplaceAll(name, ":", "-")
	name = strings.ReplaceAll(name, "*", "_")
	name = strings.ReplaceAll(name, "?", "_")
	name = strings.ReplaceAll(name, "\"", "_")
	name = strings.ReplaceAll(name, "<", "_")
	name = strings.ReplaceAll(name, ">", "_")
	name = strings.ReplaceAll(name, "|", "_")
	// Replace multiple spaces with single space
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	// Windows silently strips trailing dots and spaces, so trim them so the
	// stored name matches the on-disk name.
	name = strings.TrimRight(name, ". ")
	// Windows reserved device names would silently open devices (CON, NUL, ...)
	// instead of creating a file.
	if reservedDeviceNamePattern.MatchString(name) {
		name = "_" + name
	}
	return name
}

// SanitizeDirName sanitizes a string to be used as a directory name
func SanitizeDirName(name string) string {
	// Remove or replace invalid characters for Windows directory names
	re := regexp.MustCompile(`[<>:"/\\|?*]`)
	sanitized := re.ReplaceAllString(name, "_")
	// Replace multiple spaces/underscores with single underscore
	re2 := regexp.MustCompile(`[\s_]+`)
	sanitized = re2.ReplaceAllString(sanitized, "_")
	// Remove leading/trailing underscores
	sanitized = regexp.MustCompile(`^_+|_+$`).ReplaceAllString(sanitized, "")
	return sanitized
}

// SanitizeFolderName produces a safe single path component for use as a
// series/artist download subdirectory. It is stricter than SanitizeFileName:
// it strips path separators, the ".." traversal segment, NUL and other C0
// control characters, trailing dots and spaces (which Windows silently drops,
// causing folder-name collisions), and caps length to 100 runes. The result
// is guaranteed to be a non-empty, non-traversal single component; callers
// get "unnamed" back for empty/unsafe input. Always combine the result with
// the base download path via filepath.Join and prefer IsSafeFolderName when
// you only need to validate an existing name.
func SanitizeFolderName(name string) string {
	if name == "" {
		return "unnamed"
	}
	// Replace path separators and Windows-illegal chars with underscore.
	name = strings.ReplaceAll(name, "\x00", "")
	for _, ch := range []string{"/", "\\", "<", ">", ":", "\"", "|", "?", "*"} {
		name = strings.ReplaceAll(name, ch, "_")
	}
	// Strip remaining C0 control chars (excluding tab/newline which the
	// whitespace collapse below handles).
	name = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return -1
		}
		if r == 0x7f {
			return -1
		}
		return r
	}, name)
	// Collapse whitespace runs to a single space and trim.
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	// Windows silently strips trailing dots and spaces from path components,
	// so a user-supplied "foo." would target "foo" and collide with an existing
	// series. Strip them here so the on-disk name matches what we track.
	name = strings.TrimRight(name, ". ")
	// Reject the traversal segment and empty result.
	if name == "" || name == "." || name == ".." {
		return "unnamed"
	}
	// Cap length in runes to avoid filesystem path-length limits.
	if runes := []rune(name); len(runes) > 100 {
		name = string(runes[:100])
	}
	return name
}

// IsSafeFolderName reports whether name is a single, non-traversal path
// component safe to use as a download subdirectory. It rejects empty strings,
// ".", "..", strings containing path separators or NUL, and strings that
// differ from their SanitizeFolderName output (i.e. they contain characters
// that would be mangled). Use it to validate names read from disk or from
// user input before joining them with a base directory.
func IsSafeFolderName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return false
	}
	return name == SanitizeFolderName(name)
}

// GetFileName extracts the filename from a URL
func GetFileName(url string) string {
	parts := strings.Split(url, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return "image"
}

// GetExtension extracts the file extension from a URL
// Delegates to scraper.GetImageExtension for consistent behavior
func GetExtension(url string) string {
	return scraper.GetImageExtension(url)
}

// SaveCoverImage downloads a cover image from a URL and saves it to destPath.
// It skips the dimension/size filtering used for page images. The caller is
// responsible for validating that the image URL comes from an allowed CDN.
func (o *Operations) SaveCoverImage(imgURL, destPath string) error {
	if strings.HasPrefix(imgURL, "data:") {
		return nil
	}
	if strings.HasPrefix(imgURL, "file://") {
		return copyFile(strings.TrimPrefix(imgURL, "file://"), destPath)
	}

	data, err := o.httpClient.FetchBytes(imgURL)
	if err != nil {
		return fmt.Errorf("failed to download cover: %w", err)
	}

	ext := GetExtension(imgURL)
	if !strings.HasSuffix(destPath, ext) {
		destPath += ext
	}

	dir := filepath.Dir(destPath)
	if err := o.EnsureDir(dir); err != nil {
		return fmt.Errorf("failed to create cover directory: %w", err)
	}

	return WriteFileAtomic(destPath, data, 0644)
}

// FileExists checks if a file exists. A stat error other than not-exist
// (e.g. permission denied) is treated as "unknown", not as existing.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// GetFileSize returns the size of a file in bytes
func GetFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
