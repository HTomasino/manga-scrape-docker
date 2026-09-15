# Comic Scraper Documentation

User and developer documentation for the Comic Scraper project. The application is a Go-based manga/comic downloader with both a CLI and a local Web UI, plus a separate HentaiNexus (H-Manga) subsystem.

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [System Requirements](#system-requirements)
4. [Installation](#installation)
5. [Building from Source](#building-from-source)
6. [Application Structure](#application-structure)
7. [Running](#running)
   - [Docker Deployment](docs/DOCKER.md) (containers, Watchtower auto-updates, network drives)
8. [CLI Usage Guide](#cli-usage-guide)
9. [Web UI Usage Guide](#web-ui-usage-guide)
10. [API Reference](#api-reference)
11. [Configuration Reference](#configuration-reference)
12. [Data Layout](#data-layout)
13. [Browser Mode](#browser-mode)
14. [HentaiNexus (H-Manga) Subsystem](#hentainexus-h-manga-subsystem)
15. [Troubleshooting](#troubleshooting)
16. [Ethical Usage](#ethical-usage)

## Overview

Comic Scraper is a Windows-friendly download manager for web comics and manga. It provides:

- A **CLI** (`comic-scraper-cli`) for one-shot add / download / config operations.
- A **Web UI** (`comic-scraper-web`) that exposes a single-page application backed by a local HTTP API on `http://127.0.0.1:8080` (override with `HOST` / `PORT` env vars).
- A **HentaiNexus (H-Manga) subsystem** for tracking artists, discovering books, downloading ZIPs, and deduplicating books that are shared across artists.

All persistent state (configuration, registries, chapter state, the H-Manga global book cache) is kept under the config base directory: `%APPDATA%\comic-scraper\` on Windows, `~/.config/comic-scraper/` on Linux, or `CONFIG_DIR`/`DATA_DIR/config` when set (Docker uses `/data/config`). Playwright persistent profile data lives under `%LOCALAPPDATA%\comic-scraper-chrome-profile\` on Windows or `PROFILE_DIR`/`DATA_DIR/profile` when set (Docker uses `/data/profile`).

## Features

### Manga

- Multi-site support: AsuraScans, LuaComic, DemonicScans, ManhuaUS, ManhuaPlus, DrakeComic, ThunderScans, RavenScans.
- Per-series **auto-check interval** (`never`, `5m`, `15m`, `30m`, `1h`, `6h`, `12h`, `24h`) with a global pause/resume control.
- **Recheck All** reconciles every series against disk (missing chapter folders are re-queued).
- **Check Now** manually refreshes the chapter list for one series.
- **Sync** / **Sync All** download missing chapters (allowed even while the global scheduler is paused).
- **Force Redownload** deletes and re-downloads a chapter range.
- **Redownload Chapter** deletes and re-downloads a single chapter.
- **Scan Missing** re-downloads chapters whose recorded `imageCount` is higher than the file count on disk (without retrying images that were intentionally filtered).
- **Refresh Metadata** re-fetches `series-info.json` and the cover image.
- Per-image status tracking (`ok`, `filtered`, `failed`, `skipped`) so intentionally filtered pages are not retried.
- Image filtering by minimum size (KB), minimum width (px), and minimum height (px).

### H-Manga (HentaiNexus)

- Track artists by HentaiNexus search URL (`?q=artist:<NAME>`).
- Discover and download books as ZIPs into `<HMangaDownloadPath>\<ArtistName>\<book-id>\<book-id>.zip`.
- Cross-artist deduplication via a global book-state cache: the first artist to download a shared book owns the file; other artists that link to the same book ID mark it downloaded and skip.
- Per-artist `check-interval` and a global H-Manga pause toggle (independent of manga).
- Force redownload, scan missing, and download single book actions for individual artists.

### Cross-cutting

- **Concurrent downloads** with rate limiting (configurable millisecond delay).
- **Resume support** -- chapter state is persisted per series and reconciled against disk on startup.
- **Graceful shutdown** on Ctrl+C (or via `POST /api/shutdown`) waits for in-flight downloads, browser queues, and the H-Manga manager to finish.
- **Background browser mode** -- headed Chrome (ManhuaUS, DrakeComic) runs off-screen so it never steals focus.
- **XSS-safe HTML** rendering in the web UI.

## System Requirements

### Minimum

- OS: Windows 10 or later (64-bit). Linux/macOS should work with minor path adjustments.
- Go: 1.23 or later (only required for building from source).
- RAM: 4 GB.
- Disk: 200 MB for the binaries plus whatever your library needs.
- Display: 1280x720 (only relevant if you disable background browser mode).

### Recommended

- OS: Windows 11 (64-bit).
- RAM: 8 GB or more (for large libraries and concurrent downloads).
- Disk: SSD with enough space for the full library.
- Display: 1920x1080.

### External requirements

- **Playwright browsers** (required for AsuraScans, ManhuaUS, DrakeComic, and HentaiNexus):
  ```bash
  go run github.com/playwright-community/playwright-go/cmd/playwright@latest install
  ```
- **HentaiNexus credentials** (only required for the H-Manga subsystem): a valid `hentainexus.com` account. Set `hentaiNexusUsername` and `hentaiNexusPassword` in `config.json` or via the web UI.

## Installation

### Option 1 -- Prebuilt binaries

The repository ships with `comic-scraper-cli.exe` and `comic-scraper-web.exe` in the project root. Double-click `comic-scraper-web.exe` to start the server, then open `http://127.0.0.1:8080`.

### Option 2 -- Build from source

```bash
# Install Playwright browsers once
go run github.com/playwright-community/playwright-go/cmd/playwright@latest install

# Build both binaries
build.bat

# Or build individually
go build -o comic-scraper-cli.exe ./cmd/cli
go build -o comic-scraper-web.exe ./cmd/web
```

A successful build produces the two `.exe` files in the project root.

## Building from Source

### Build scripts

`build.bat` builds both binaries in one step:

```bash
build.bat
```

For one-off builds, use the `go build` commands above.

### Debug helper

`cmd/debug` is a standalone tool for inspecting AsuraScans HTML patterns. It loads a chapter page in headless Chrome, dumps the HTML, and reports matches for common `chapterId` regexes. Useful when the AsuraScans scraper breaks after a site change.

```bash
go run ./cmd/debug
```

## Application Structure

```text
+-- cmd/
|   +-- cli/              # CLI entry point
|   +-- web/              # Web UI server + H-Manga routes
|   +-- debug/            # AsuraScans regex debug helper
+-- pkg/
|   +-- config/           # Configuration with legacy migration
|   +-- download/         # Download manager (workers, retry, validation)
|   +-- fileutil/         # File utilities (sanitization, image save)
|   +-- hmanga/           # HentaiNexus models, manager, ZIP download
|   +-- http/             # HTTP client with rate limiting
|   +-- models/           # Series, Chapter, Download, ImageResult
|   +-- scraper/          # Site scrapers
|       +-- asura/
|       +-- browser/      # Shared persistent-context launch (off-screen)
|       +-- demonic/
|       +-- drake/
|       +-- lua/
|       +-- manhuaplus/
|       +-- manhuaus/
|       +-- thunderscans/
|       +-- ravenscans/
+-- go.mod
+-- go.sum
+-- build.bat             # Builds both CLI and web binaries
```

### Module responsibilities

- **`pkg/config`** -- JSON config load/save with legacy-path migration and validation defaults.
- **`pkg/download`** -- Worker pool, retry logic, per-image validation, status tracking.
- **`pkg/fileutil`** -- Folder sanitization, image save helper, regex helpers.
- **`pkg/hmanga`** -- HentaiNexus artist registry, per-artist `BookStateFile`, global `GlobalBookCache` (cross-artist dedup), ZIP download, browser login.
- **`pkg/http`** -- Single HTTP client with rate-limit delay between calls.
- **`pkg/models`** -- `Series`, `Chapter`, `Download`, `ImageResult`, `SeriesInfo`, `Image`.
- **`pkg/scraper`** -- Scraper interface + per-site implementations.

## Running

### CLI

```bash
comic-scraper-cli.exe <command> [flags]
```

See [CLI Usage Guide](#cli-usage-guide) for the full command list.

### Web UI

```bash
# Foreground
comic-scraper-web.exe

# Background (detaches from terminal; Windows re-execs with stdio closed)
comic-scraper-web.exe -d
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST`   | `127.0.0.1` | Bind address. Use `0.0.0.0` to listen on all interfaces (LAN-exposes the unauthenticated admin API). |
| `PORT`   | `8080` | TCP port. |

Stop the server with **Ctrl+C**. The shutdown handler waits for in-flight downloads, browser queues, and the H-Manga manager before exiting.

## CLI Usage Guide

### Commands

```text
add <url> [-name <custom_name>]   Add a series by URL
download <series_id> [-ch <num>]  Download one chapter or "all"
list                              List tracked series
config                            Show current configuration
config-set -key <key> -val <val>  Update configuration
remove <series_id>                Remove a tracked series
edit <series_id> [-name <name>] [-url <url>]
                                  Edit series name or URL
sync                              Scan download folder and import existing series
help                              Show usage
```

### Examples

```bash
# Add a series
cli add https://asurascans.com/comics/series-name
cli add https://luacomic.org/series/123 -name "My Custom Name"

# Download
cli download <series_id> -ch 5
cli download <series_id> -ch all

# Edit a series
cli edit <series_id> -name "New Name"
cli edit <series_id> -url "https://..."
cli edit <series_id> -name "New Name" -url "https://..."

# Import series that already exist on disk
cli sync

# Inspect / update configuration
cli config
cli config-set -key downloadPath      -val "C:/Comics"
cli config-set -key rateDelay         -val 500
cli config-set -key minImageSizeKB    -val 50
cli config-set -key browserBackground -val false
```

### CLI limitations

- No auto-check scheduler (the CLI is one-shot).
- No HentaiNexus support (the H-Manga subsystem is web-only).

## Web UI Usage Guide

Open `http://127.0.0.1:8080` after starting `comic-scraper-web.exe`. The web UI is a single-page app with sections for **Series**, **Settings**, and **H-Manga Artists**.

### Series tab

- **Add Series** -- paste a series URL, optionally set a custom folder name, optionally pick a check interval.
- **Sync** / **Sync All** -- download missing chapters.
- **Check Now** -- re-scrape the chapter list (paused-aware).
- **Recheck All** -- reconcile every series against disk (paused-aware).
- **Download** / **Download All** -- start chapter downloads (one or all missing).
- **Scan Missing** -- re-download chapters with fewer files than recorded.
- **Force Redownload** -- delete and re-download a chapter range.
- **Refresh Metadata** -- re-fetch `series-info.json` and cover.
- **Dark mode** toggle.

### Settings tab

- Download path, rate limit.
- Image filters: minimum size (KB), minimum width (px), minimum height (px).
- Browser background mode.
- HentaiNexus credentials.
- Verify H-Manga downloads on startup.
- **Manga scraping pause** toggle (persisted across restarts).

### H-Manga tab

- **Add Artist** -- paste a HentaiNexus search URL (`/api/hmanga/artists`).
- **Sync** / **Sync All** -- download missing books (allowed while paused).
- **Check Now** / **Check All** -- re-scrape artist pages (paused-aware).
- **Force Redownload**, **Download Book**, **Scan Missing** for individual artists.
- **H-Manga scraping pause** toggle (persisted across restarts).

### System

- **POST `/api/shutdown`** -- graceful server shutdown.
- **Background mode (`-d`)** -- launch detached from the terminal.

## API Reference

| Endpoint | Methods | Notes |
|----------|---------|-------|
| `/api/series` | GET, POST | List / add series |
| `/api/series/{id}` | DELETE, PUT, PATCH | Remove / update |
| `/api/series/{id}/chapters` | GET | List chapters |
| `/api/series/{id}/state` | GET | Chapter state (`.chapters.json`) |
| `/api/series/{id}/sync` | POST | Download missing chapters (manual; allowed when paused) |
| `/api/series/{id}/sync-all` | POST | Same as above for all series |
| `/api/series/{id}/check-now` | POST | Re-scrape chapter list (paused-aware) |
| `/api/series/{id}/check-interval` | PUT | Set auto-check interval |
| `/api/series/{id}/scan-missing` | POST | Re-download incomplete chapters |
| `/api/series/{id}/force-redownload` | POST | Range `{fromChapter, toChapter}` |
| `/api/series/{id}/redownload-chapter` | POST | Single chapter re-download |
| `/api/series/{id}/refresh-metadata` | POST | Re-fetch `series-info.json` + cover |
| `/api/series/recheck-all` | POST | Reconcile every series against disk (paused-aware) |
| `/api/download` | POST | Start a chapter download |
| `/api/downloads` | GET | List active downloads |
| `/api/downloads/clear` | POST | Clear finished downloads |
| `/api/settings` | GET, POST | Read / update configuration |
| `/api/scraping/manga` | GET, POST, PUT | Read / toggle manga scraping pause |
| `/api/scraping/hmanga` | GET, POST, PUT | Read / toggle H-Manga scraping pause |
| `/api/shutdown` | POST | Graceful server shutdown |
| `/api/hmanga/artists` | GET, POST | List / add HentaiNexus artists |
| `/api/hmanga/artists/check-all` | POST | Re-scrape every artist (paused-aware) |
| `/api/hmanga/artists/sync-all` | POST | Sync every artist (allowed when paused) |
| `/api/hmanga/artists/{id}` | DELETE | Remove artist |
| `/api/hmanga/artists/{id}/books` | GET | List books for an artist |
| `/api/hmanga/artists/{id}/state` | GET | Read `.books.json` |
| `/api/hmanga/artists/{id}/check-now` | POST | Re-scrape one artist (paused-aware) |
| `/api/hmanga/artists/{id}/check-interval` | PUT | Set interval |
| `/api/hmanga/artists/{id}/sync` | POST | Download missing books (allowed when paused) |
| `/api/hmanga/artists/{id}/scan-missing` | POST | Re-download missing books |
| `/api/hmanga/artists/{id}/force-redownload` | POST | Force re-download of books |
| `/api/hmanga/artists/{id}/download-book` | POST | Download a single book by id |

## Configuration Reference

Configuration is stored at `%APPDATA%\comic-scraper\config.json`:

```json
{
  "downloadPath":         "C:\\Users\\<user>\\AppData\\Roaming\\comic-scraper\\downloads",
  "rateDelay":            500,
  "minImageSizeKB":       0,
  "minImageWidth":        0,
  "minImageHeight":       0,
  "userAgent":            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...",
  "browserBackground":    true,

  "hmangaDownloadPath":   "C:\\Users\\<user>\\AppData\\Roaming\\comic-scraper\\hmanga-downloads",
  "hentaiNexusUsername":  "",
  "hentaiNexusPassword":  "",
  "verifyHMDownloads":    false,

  "mangaScrapingPaused":  false,
  "hmangaScrapingPaused": false
}
```

### Keys

| Key | Default | Description |
|-----|---------|-------------|
| `downloadPath` | `%APPDATA%\comic-scraper\downloads` | Where manga chapters are saved |
| `hmangaDownloadPath` | `%APPDATA%\comic-scraper\hmanga-downloads` | Where H-Manga ZIPs are saved |
| `rateDelay` | `500` | Milliseconds between HTTP requests |
| `minImageSizeKB` | `0` | Skip images smaller than this (KB) |
| `minImageWidth` | `0` | Skip images narrower than this (px) |
| `minImageHeight` | `0` | Skip images shorter than this (px) |
| `userAgent` | Chrome 120 | HTTP user agent |
| `browserBackground` | `true` | Run headed browsers off-screen |
| `hentaiNexusUsername` | `""` | HentaiNexus login (required for H-Manga) |
| `hentaiNexusPassword` | `""` | HentaiNexus password |
| `verifyHMDownloads` | `false` | Verify H-Manga ZIPs against disk on startup and manual scans |
| `mangaScrapingPaused` | `false` | Persisted manga scrape/sync pause state |
| `hmangaScrapingPaused` | `false` | Persisted H-Manga scrape/sync pause state |

### Pause semantics

Both pause flags stop the global scheduler (auto-checks, recheck-all, check-all, check-now) for their section. Manual per-series/per-artist `sync`, `force-redownload`, `download-book`, and `scan-missing` still work while paused, because they only touch already-known items. Newly added series skip their initial auto-download while paused.

### Legacy migration

`pkg/config` auto-migrates from older locations: `comic-scraper-web\config.json`, `comic-scraper-go\config.json`, and `comic-scraper\config.json`. The first successful read is written to the new unified path.

## Data Layout

### Manga

```text
<downloadPath>\
  <SeriesFolder>\
    series-info.json
    cover.<ext>
    Chapter 1\
      <page files>
    Chapter 2\
    .chapters.json
```

`series-info.json`:

```json
{
  "title":       "Series Title",
  "description": "Synopsis...",
  "author":      "Author Name",
  "status":      "Ongoing",
  "genres":      ["action", "adventure", "fantasy"],
  "coverUrl":    "https://site.cdn/cover.jpg",
  "source":      "asurascans"
}
```

- `author` is `""` when the site does not list one (e.g. LuaComic).
- `status` is the site's raw status string (e.g. `Ongoing`, `Completed`).
- `genres` are lower-cased, trimmed, deduplicated, and sorted alphabetically.
- `cover.<ext>` is downloaded next to `series-info.json`; failures are logged but do not block series creation.
- `.chapters.json` records per-chapter `downloaded`, `imageCount`, `downloadedAt`, and per-image `status`/`reason`.

### H-Manga

```text
<hmangaDownloadPath>\
  <ArtistFolder>\
    <book-id>\
      <book-id>.zip
    .books.json
```

The global cross-artist book cache lives at `%APPDATA%\comic-scraper\hmanga-global-books.json` and is rebuilt/validated from per-artist `.books.json` files at startup.

## Browser Mode

ManhuaUS and DrakeComic require a non-headless browser to pass Cloudflare Turnstile. By default, the browser window is launched with `--window-position=-32000,-32000` and `--disable-backgrounding-occluded-windows` so it stays invisible without being throttled. AsuraScans runs in headless mode and is not affected.

Toggle via the web UI Settings panel, the `browserBackground` config key, or the CLI:

```bash
cli config-set -key browserBackground -val false   # show the window
cli config-set -key browserBackground -val true    # off-screen (default)
```

## HentaiNexus (H-Manga) Subsystem

The HentaiNexus subsystem is web-only. It lives behind `/api/hmanga/*` and is exposed in the **H-Manga Artists** tab of the web UI.

### How it works

1. The server logs into HentaiNexus with the credentials from `config.json` (`hentaiNexusUsername` / `hentaiNexusPassword`) using a Playwright login flow.
2. When you add an artist, the server fetches their listing and extracts book IDs and titles.
3. Each book is downloaded as a ZIP into `<HMangaDownloadPath>\<ArtistName>\<book-id>\`.
4. A global book cache (`hmanga-global-books.json`) maps each book ID to its owning artist, so a book shared across multiple artists is downloaded once.
5. Per-artist `.books.json` files track per-book download status, filename, file size, and download time.

### Pause semantics

The H-Manga scraping pause stops the global scheduler (auto-checks, check-all, check-now). Manual `sync`, `force-redownload`, `download-book`, and `scan-missing` still work while paused.

### Disk verification

When `verifyHMDownloads` is `true`, the server verifies H-Manga ZIPs against disk on startup and manual scans, rather than trusting `.books.json` blindly. Turn this on if a book shows as downloaded but the file is missing.

## Troubleshooting

### No pages found for chapter

- The site's HTML may have changed. For AsuraScans, run `cmd/debug` to inspect regex matches.
- Increase `rateDelay` (`cli config-set -key rateDelay -val 1000`) to slow down requests.
- Try **Check Now** in the web UI to re-scrape the chapter list.

### Cover images / tiny images being downloaded

- Raise `minImageSizeKB`, `minImageWidth`, or `minImageHeight` in the web UI Settings or via `config-set`. Pages filtered by these settings are recorded with `status: "filtered"` and will not be retried.

### Cloudflare blocks ManhuaUS / DrakeComic

- Ensure `browserBackground` is enabled (default) so the browser stays off-screen without being throttled.
- Ensure Playwright is installed: `go run github.com/playwright-community/playwright-go/cmd/playwright@latest install`.

### HentaiNexus login fails

- Set `hentaiNexusUsername` and `hentaiNexusPassword` in `config.json` or via the web UI Settings.
- The persistent Playwright profile is at `%LOCALAPPDATA%\comic-scraper-chrome-profile\`. Delete it to force a fresh login.

### H-Manga "already downloaded" but no file

- Run **Recheck All** in the web UI, or set `verifyHMDownloads: true` so the next startup verifies ZIPs against disk.

### Downloads API payload

Each item returned by `GET /api/downloads` now includes optional H-Manga fields:

- `phase` — transient H-Manga sub-state: `"pending"`, `"starting"`, `"downloading"`, `"verifying"`, `"cancelled"`, or `"superseded"`. Empty for manga downloads.
- `bytesDownloaded` — live byte count for H-Manga downloads.
- `totalBytes` — expected final size when known; `0` means indeterminate.

`status` and `progress` continue to work as before; clients that ignore the new fields see no change.

### Scheduler not running

- Check the **Manga scraping** / **H-Manga scraping** pause toggles in the web UI Settings. The state is persisted across restarts via `mangaScrapingPaused` / `hmangaScrapingPaused`.

### Reset everything

- Delete `%APPDATA%\comic-scraper` (config, registries, caches) and `%LOCALAPPDATA%\comic-scraper-chrome-profile` (Playwright).
- Restart the server.

## Ethical Usage

1. Respect rate limits -- do not set `rateDelay` below 500 ms unless you understand the impact.
2. Downloaded content should be for personal use only.
3. Check each site's terms of service before scraping.
4. Do not redistribute downloaded content.
5. The HentaiNexus subsystem requires authentication -- only download books you have a right to download.

## License

MIT.
