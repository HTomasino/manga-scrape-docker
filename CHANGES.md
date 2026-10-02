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
