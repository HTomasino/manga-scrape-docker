# Comic Scraper

A Go-based manga/comic downloader with both a CLI and a local Web UI. Tracks series, auto-checks for new chapters, downloads images concurrently with rate limiting, persists resume state, and supports a separate HentaiNexus (H-Manga) subsystem with cross-artist deduplication and ZIP downloads.

## Features

- **Multiple site support**: AsuraScans, LuaComic, DemonicScans, ManhuaUS, ManhuaPlus, DrakeComic, ThunderScans, RavenScans
- **Dual interfaces**:
  - `comic-scraper-cli` -- one-shot command-line client
  - `comic-scraper-web` -- local HTTP server with a single-page web UI
- **HentaiNexus (H-Manga) subsystem** -- tracked artists, book discovery, ZIP downloads, global book-state cache, per-artist `check-interval`, dedicated download path, and an independent pause toggle
- **Auto-check scheduler** -- per-series/per-artist intervals (`never`, `5m`, `15m`, `30m`, `1h`, `6h`, `12h`, `24h`)
- **Safe startup** -- stale chapter state is reconciled against disk, missing images are re-queued, and HTTP listener opens before background scans finish
- **Graceful shutdown** -- Ctrl+C / API shutdown waits for in-flight downloads, browser queues, and H-Manga manager to finish
- **Concurrent downloads** with rate limiting
- **Image filtering** by minimum size (KB) and minimum width/height (px); per-image results are recorded (`ok`, `filtered`, `failed`, `skipped`) so intentionally filtered pages are not retried
- **Resume support** -- chapter state is persisted per series; re-adding a series reuses the existing folder
- **Background browser mode** -- headed Chrome (ManhuaUS, DrakeComic) runs off-screen so it never steals focus
- **XSS-safe HTML** rendering in the web UI (escaped attributes, text, and inline `onclick` handlers)

## Supported Sites

| Site | URL pattern | Mode |
|------|-------------|------|
| AsuraScans | `asurascans.com/comics/{slug}` | Headed Playwright (off-screen) |
| LuaComic | `luacomic.org/series/{name}` | HTTP only |
| DemonicScans | `demonicscans.org/manga/{name}` | HTTP only |
| ManhuaUS | `manhuaus.com/manga/{name}` | Headed Chrome (background by default) |
| ManhuaPlus | `manhuaplus.{top,co,cc}` | HTTP + API |
| DrakeComic | `drakecomic.org/manga/{slug}` | Headed Chrome (background by default) |
| ThunderScans | `en-thunderscans.com/comics/{slug}` | HTTP only |
| HentaiNexus (H-Manga) | `hentainexus.com/?q=artist:{name}` | Playwright login + HTTP ZIP download |

### Site-specific notes

- **AsuraScans** -- Playwright fetches the chapter page, then the chapter's images are pulled from a JSON API. The browser runs headed but off-screen (same as ManhuaUS / DrakeComic).
- **LuaComic** -- prefers API calls; falls back to HTML scraping.
- **DemonicScans** -- pure HTML; images served from `mangafirst.org`.
- **ManhuaUS** -- non-headless Chrome to pass Cloudflare Turnstile; cookies and User-Agent are reused for HTTP image downloads.
- **ManhuaPlus** -- API with HTML fallback; images served via the site CDN.
- **DrakeComic** -- non-headless Chrome to pass Cloudflare Turnstile; reads image URLs from `ts_reader.run()` JS config, with `noscript` and `<img>` fallbacks.
- **ThunderScans** -- pure HTML; full series metadata (title, status, genres, cover, description) is extracted on add.
- **HentaiNexus** -- requires login (username/password stored in config); discovers book IDs from artist listing, downloads each book as a ZIP into `<HMangaDownloadPath>/<ArtistName>/`.

## Project Structure

```text
+-- cmd/
|   +-- cli/              # CLI entry point
|   +-- web/              # Web UI server + H-Manga routes
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
|       +-- ravenscans/
|       +-- thunderscans/
+-- go.mod
+-- docker/              # entrypoint.sh + CLI exec helpers
+-- docker-compose.yml   # scraper + Watchtower auto-update
+-- Dockerfile           # multi-stage build (Go + Playwright image)
```

## Building

### Prerequisites

- **Go 1.23+**
- **Playwright browsers** (required for AsuraScans, ManhuaUS, DrakeComic, and HentaiNexus):
  ```bash
  go run github.com/playwright-community/playwright-go/cmd/playwright@latest install
  ```

### Build

```bash
# Build CLI only
go build -o comic-scraper-cli.exe ./cmd/cli

# Build Web UI only
go build -o comic-scraper-web.exe ./cmd/web

# Build both (Linux/macOS)
go build -o comic-scraper-cli ./cmd/cli
go build -o comic-scraper-web ./cmd/web
```

Native Windows `.exe` builds also work: `go build -o comic-scraper-cli.exe ./cmd/cli` etc.

## Running

### Docker (recommended for servers / NAS)

```bash
docker compose up -d
```

See [Docs/DOCKER.md](Docs/DOCKER.md) for the full guide: environment variables, volumes, the Watchtower auto-update loop, and pointing downloads at SMB/NFS network drives.

### Native Windows binary

```bash
comic-scraper-web.exe
# Background (detached from terminal)
comic-scraper-web.exe -d
```

### CLI

```bash
comic-scraper-cli.exe <command> [flags]
```

Commands:

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

Examples:

```bash
# Add a series
cli add https://asurascans.com/comics/series-name
cli add https://luacomic.org/series/123 -name "My Custom Name"

# Download
cli download <series_id> -ch 5
cli download <series_id> -ch all

# Edit and sync
cli edit <series_id> -name "New Name"
cli sync

# Update settings
cli config-set -key downloadPath  -val "C:/Comics"
cli config-set -key rateDelay     -val 500
cli config-set -key browserBackground -val false
```

### Web UI

```bash
# Foreground
comic-scraper-web.exe

# Background (detaches from terminal)
comic-scraper-web.exe -d
```

Defaults: `http://127.0.0.1:8080`. Override with `HOST=0.0.0.0` and `PORT=9000` environment variables.

The single-page app is served from `/` and the embedded JavaScript/CSS lives under `/static/`. All persistence (registry, chapter state, config, H-Manga global cache) is kept under `%APPDATA%\comic-scraper\`.

#### Manga (web UI) features

- Add, edit, remove series with custom folder names
- Per-series check interval (`never`, `5m`, `15m`, `30m`, `1h`, `6h`, `12h`, `24h`)
- **Check Now** -- manually refresh chapter list for one series
- **Recheck All Series** -- reconcile every series against disk
- **Sync** / **Sync All** -- download missing chapters (allowed even while scraping is paused)
- **Download All** -- one-click download of all missing chapters
- **Scan Missing** -- re-download chapters that have fewer images than recorded
- **Force Redownload** -- delete and re-download a chapter range
- **Refresh Metadata** -- re-fetch `series-info.json` and cover from the source site
- Real-time download progress with persistent state
- **Dark mode** toggle
- **Pause/Resume** the global manga scraping scheduler (persists across restarts)
- Settings panel for download path, rate limit, image filters, browser mode

#### H-Manga (HentaiNexus) features

- Add HentaiNexus artists by search URL (`/api/hmanga/artists`)
- Per-artist `check-interval`
- **Check Now** / **Check All** -- scrape the artist page for new books
- **Sync** / **Sync All** -- download known missing books (allowed while paused)
- **Force Redownload** / **Download Book** / **Scan Missing** for individual artists
- Cross-artist deduplication: a book owned by one artist is marked downloaded for all other artists that link to it (so a shared book is downloaded once)
- Separate download path (`hmangaDownloadPath`) keeps H-Manga files isolated
- **Pause/Resume** the global H-Manga scraping scheduler (persists across restarts)
- HentaiNexus credentials are read from config (`hentaiNexusUsername` / `hentaiNexusPassword`)

#### System

- **POST `/api/shutdown`** -- graceful server shutdown (waits for in-flight work, then exits)
- **Background mode (`-d`)** -- launch detached from the terminal (Windows: re-execs with stdio closed)

### Web API quick reference

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
| `/api/series/{id}/force-redownload` | POST | Range `{fromChapter,toChapter}` |
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

## Configuration

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
  "hmangaZipNameRegex":   "\\[[^\\]]*\\]|_",
  "hmangaExtractZips":    false,

  "mangaScrapingPaused":  false,
  "hmangaScrapingPaused": false
}
```

### Configuration keys

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
| `verifyHMDownloads` | `false` | Verify H-Manga ZIPs / extracted folders against disk on startup and manual scans |
| `hmangaZipNameRegex` | `\[[^\]]*\]\|_` | Regex applied to downloaded H-Manga ZIP names; every match is removed. Set to `""` explicitly to disable renaming |
| `hmangaExtractZips` | `false` | Extract each downloaded H-Manga ZIP into a same-named folder and delete the ZIP |

### Pause semantics

Both pause flags stop the global scheduler (auto-checks, recheck-all, check-all, check-now) for their section. Manual per-series/per-artist `sync`, `force-redownload`, `download-book`, and `scan-missing` still work while paused, because they only touch already-known items. Newly added series skip their initial auto-download while paused.

### Legacy migration

The config package auto-migrates from older locations: `comic-scraper-web\config.json`, `comic-scraper-go\config.json`, and `comic-scraper\config.json`. The first successful read is written to the new unified path.

Playwright persistent profile data is kept at `%LOCALAPPDATA%\comic-scraper-chrome-profile\`.

## Data layout

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

- `author` is `""` when the site does not list one.
- `genres` are lower-cased, trimmed, deduplicated, and sorted alphabetically.
- `cover.<ext>` is downloaded next to `series-info.json`; failures are logged but do not block series creation.
- `.chapters.json` records per-chapter `downloaded`, `imageCount`, `downloadedAt`, and per-image `status`/`reason`.

### H-Manga

```text
<hmangaDownloadPath>\
  <ArtistFolder>\
    <book zip name>.zip          # e.g. "Title_12345.zip" (or the regex-cleaned name)
    .books.json
```

With `hmangaExtractZips` enabled, each ZIP is extracted and deleted after a successful extraction:

```text
<hmangaDownloadPath>\
  <ArtistFolder>\
    <book zip name>\             # extracted contents; same name as the original ZIP
    .books.json
```

Downloaded ZIP names have the `hmangaZipNameRegex` pattern applied (default strips `[tag]` groups and underscores, e.g. `[Artist] Title_12345.zip` ? `Title12345.zip`). When extraction is enabled, `.books.json` marks the book with `"extracted": true` and `"dirName": "<book zip name>"`; disk verification then checks the extracted folder instead of the ZIP. A downloaded book whose ZIP is missing but whose same-named extracted directory exists is adopted as extracted on the next verification pass (manually extracted books are not re-downloaded).

**Clean Up Zips** (button in the H-Manga section, `POST /api/hmanga/artists/cleanup-zips`) scans every artist folder for existing ZIPs, renames any whose name still matches the configured regex (skipping renames where the target already exists), and — when `hmangaExtractZips` is enabled — extracts each ZIP and deletes it. `.books.json` filenames are updated to match, and the global cache is rebuilt. The scan is single-flight (a second click while running gets a 409) and its summary counts are logged.

The global cross-artist book cache lives at `%APPDATA%\comic-scraper\hmanga-global-books.json` and is rebuilt/validated from per-artist `.books.json` files at startup.

## Browser mode

ManhuaUS, DrakeComic, and AsuraScans all run through the shared off-screen browser queue. By default, the browser window is launched with `--window-position=-32000,-32000` and `--disable-backgrounding-occluded-windows` so it stays invisible without being throttled.

Toggle via the web UI Settings, the `browserBackground` config key, or the CLI:

```bash
cli config-set -key browserBackground -val false   # show the window
cli config-set -key browserBackground -val true    # off-screen (default)
```

## Troubleshooting

- **No pages found** -- site HTML may have changed; try `cli config-set -key rateDelay -val 1000` to slow down, or re-run **Check Now**.
- **Cloudflare blocks ManhuaUS / DrakeComic** -- ensure `browserBackground` is enabled (default) and Playwright is installed.
- **HentaiNexus login fails** -- set `hentaiNexusUsername` and `hentaiNexusPassword` in `config.json` or via the web UI.
- **Duplicate covers / tiny images** -- raise `minImageSizeKB`, `minImageWidth`, or `minImageHeight`. Pages filtered by these settings are recorded with `status: "filtered"` and will not be retried.
- **H-Manga "already downloaded" but no file** -- run **Recheck All** (web) or set `verifyHMDownloads: true` so the next startup verifies ZIPs / extracted folders against disk.
- **H-Manga extraction failed** -- the ZIP is kept on disk and the download is retried on the next sync; check the log for the reason (locked file, corrupt archive).
- **Reset everything** -- delete `%APPDATA%\comic-scraper` (config, registries, caches) and `%LOCALAPPDATA%\comic-scraper-chrome-profile` (Playwright).

## Debug helper

`cmd/debug` is a small standalone tool for inspecting AsuraScans HTML patterns:

```bash
go run ./cmd/debug
```

It loads a chapter page in headless Chrome, dumps the HTML, and reports the matches it finds for common `chapterId` regexes. Useful when the AsuraScans scraper breaks after a site change.

## Dependencies

```go
require (
    github.com/PuerkitoBio/goquery v1.9.2                      // HTML parsing
    github.com/google/uuid v1.6.0                              // IDs
    github.com/playwright-community/playwright-go v0.5700.1    // Browser automation
)
```

## License

MIT
