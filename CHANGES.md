## fix: CDP attach fails forever after the Playwright driver dies (1.7.7)

- A failed `ConnectOverCDP` can leave the Playwright node driver process dead (pipe EOF: `target closed: could not read protocol padding: EOF`). The queue kept reusing the same dead `playwright` instance, so every later CDP attempt polled the dead pipe and failed identically forever (`pkg/scraper/browser/browser.go`).
- On a failed CDP connect, the queue now stops the driver (panic-guarded, since `Stop` on a dead driver can panic) and nils it, forcing a fresh driver on the next attempt; logs `[BROWSER-QUEUE] Playwright driver recycled after failed CDP connect`.

## fix: ManhuaUS browser image fetch blocked by CORS (1.7.6)

- The in-page `fetch()` approach from 1.7.5 failed with `TypeError: Failed to fetch` on every image: `img.manhuaus.com` sends no `Access-Control-Allow-Origin` headers, so a cross-origin fetch from the `manhuaus.com` origin is blocked by CORS (top-level navigation is not). Live logs showed 16/16 images failing ~1s apart with the park-to-about:blank between ops.
- `FetchImageBytesWithPlaywright` now navigates the persistent tab directly to the image URL and reads the body via Playwright's `Response.Body()` — no CORS restriction on top-level navigation, and the Cloudflare clearance from the site session still applies.

## fix: ManhuaUS image downloads 403 (Cloudflare on image CDN)

- manhuaus.com's image CDN (`img.manhuaus.com`) now sits behind a Cloudflare challenge that rejects the plain HTTP client with 403 even with the browser's cookies and User-Agent (Cloudflare fingerprints the Go TLS client). Extraction still worked, so the console showed "images extracted" followed by silence and zero files saved.
- Route ManhuaUS image bytes through the browser session: new `download.BrowserImageFetcher` interface; `download.Task.ImageFetcher` routes per-image fetches through `FetchImageBytes` when the scraper provides one. ManhuaUS implements it by fetching each image from inside the persistent tab via the JS `fetch` API (same origin/session as the challenge-passing browser) and returning the bytes. All five web task sites + CLI wired.
- `fileutil.Operations.SaveImageData` validates already-fetched bytes against the same dimension/size filters as `SaveImage` and writes them atomically.
- Removed the dead `img1.manhuaus.com` fallback: the host no longer resolves in DNS (every fallback attempt was a guaranteed `no such host` failure). CDN allowlist is now `img.manhuaus.com` + `manhuaus.com` (verified against live chapter pages and downloaded-chapter history; `image/`, `image2024`–`image2026`, and `wp-content/uploads/WP-manga` paths all live on those two hosts).
- Converted download-phase failure logs from `fmt.Printf` (stdout, invisible in the web Console tab) to `log.Printf` (`[DOWNLOAD]`/`[FILEUTIL]` tags, captured by `/api/console`), so image-phase failures are no longer silent in the UI.

## fix: downloads fail instantly after restart (1.7.4)

- Download manager's `downloadPath` was never initialized from config at startup — it was only set when settings were saved via the web UI. After a container restart, every chapter download built a CWD-relative output directory that could not be created (read-only container root), failing instantly and silently with no log output (`pkg/download/manager.go`).
- Initialize `downloadPath` from config in `NewManager`; fall back to `config.DownloadPath` when the field is empty in `DownloadChapter`.
- Log the `EnsureDir` error on the download failure path so directory-creation failures are visible in container logs.

## feat: browser/CDP and H-Manga enhancements

- Browser persistent tab management on about:blank after each op; adopt parked blank tab first
- Dedupe identical requests in the browser queue
- H-Manga refresh no longer scans the op queue without its lock
- Never close an externally attached (CDP) browser at session end
- New test files: pkg/download/manager_test.go, pkg/scraper/scraper_test.go
- Support for ManhuaPlus HTTP + API mode; DrakeComic with Cloudflare Turnstile via Chrome
- Cross-artist book deduplication in H-Manga global cache
- Pause/resume flags for manga and H-Manga scraping (persists across restarts)

### 1.7.2
