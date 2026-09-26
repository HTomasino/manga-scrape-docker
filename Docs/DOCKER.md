# Docker Deployment Guide

Run Comic Scraper as a Docker container with automatic updates via
**Watchtower** (CI-built GHCR image) and full documentation for pointing the
scrapers at **network drives** (SMB/NFS).

- Image: `ghcr.io/htomasino/webscraper` (linux/amd64)
- Ships both binaries: the web server (`comic-scraper-web`) and the CLI
  (`comic-scraper-cli`, usable via `docker exec`)
- Headed Chrome under **Xvfb** — scraping works exactly as on a desktop,
  including Cloudflare-protected sites (ManhuaUS, DrakeComic) and the
  HentaiNexus login flow, thanks to the persistent profile volume.

---

## Table of Contents

1. [Image Architecture](#image-architecture)
2. [Quick Start](#quick-start)
3. [Environment Variables](#environment-variables)
4. [Volume Layout](#volume-layout)
5. [Auto-Update Watcher (Watchtower)](#auto-update-watcher-watchtower)
6. [Network Drives](#network-drives)
7. [CLI Usage Inside the Container](#cli-usage-inside-the-container)
8. [Migrating from Native Windows](#migrating-from-native-windows)
9. [Reverse Proxy Notes](#reverse-proxy-notes)
10. [Troubleshooting](#troubleshooting)

---

## Image Architecture

Single multi-stage image:

| Component | Purpose |
|---|---|
| `comic-scraper-web` | Web server binary (Go, statically linked) |
| `comic-scraper-cli` | CLI binary (Go, statically linked) — for `docker exec` |
| Playwright base (`mcr.microsoft.com/playwright:v1.57.0-jammy`) | Bundled Chromium matching `playwright-go` driver 1.57 |
| `xvfb` | Virtual display for headed (non-headless) Chrome |
| `fonts-liberation`, `fonts-noto-color-emoji` | Font rendering for pages |
| `gosu` | PUID/PGID privilege drop in the entrypoint |

The app launches **headed** Chrome off-screen (`Headless:false`); Xvfb provides
the display inside the container. System Google Chrome is not installed, so the
launcher falls back to Playwright's bundled Chromium automatically (this is the
normal container path).

---

## Quick Start

```bash
git clone https://github.com/HTomasino/Webscraper webscraper
cd webscraper
docker compose up -d
```

Open `http://<host>:8080`. If you access the UI from another machine, set
`CSRF_ALLOWED_HOSTS` to your host's LAN IP first (see
[Environment Variables](#environment-variables)) — otherwise every
state-changing action returns 403.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `HOST` | `127.0.0.1` | Bind address; compose sets `0.0.0.0` |
| `DATA_DIR` | unset | Container data root; subdirs `config/`, `downloads/`, `hmanga-downloads/`, `profile/` hang off it |
| `CONFIG_DIR` | `DATA_DIR/config` | Direct override for the config/registry directory |
| `DOWNLOAD_DIR` | `DATA_DIR/downloads` | Direct override for the default manga download path |
| `PROFILE_DIR` | `DATA_DIR/profile` | Chrome persistent profile directory |
| `PUID` / `PGID` | `1000` / `1000` | Runtime user/group (entrypoint creates them and chowns `DATA_DIR` subdirs) |
| `CSRF_ALLOWED_HOSTS` | *(empty)* | Comma-separated extra hosts/IPs accepted as CSRF-safe Origins (e.g. `192.168.1.50,nas.local`) |
| `SCRAPE_DOWNLOAD_PATH` | config value | Env override for `downloadPath` |
| `SCRAPE_HMANGA_DOWNLOAD_PATH` | config value | Env override for `hmangaDownloadPath` |
| `SCRAPE_RATE_DELAY` | config value | Env override for `rateDelay` (ms) |
| `SCRAPE_MIN_IMAGE_SIZE_KB` | config value | Env override for `minImageSizeKB` |
| `SCRAPE_MIN_IMAGE_WIDTH` | config value | Env override for `minImageWidth` |
| `SCRAPE_MIN_IMAGE_HEIGHT` | config value | Env override for `minImageHeight` |
| `SCRAPE_USER_AGENT` | config value | Env override for `userAgent` |
| `SCRAPE_BROWSER_BACKGROUND` | config value | Env override for `browserBackground` (`true`/`false`) |
| `SCRAPE_SHUTDOWN_TIMEOUT_SECONDS` | `600` | Env override for graceful-shutdown wait; align with `stop_grace_period` |

Path resolution precedence (in `pkg/config`): `CONFIG_DIR` env >
`DATA_DIR/config` > Windows `%APPDATA%\comic-scraper` >
`$XDG_CONFIG_HOME/comic-scraper` > `~/.config/comic-scraper`. The container
sets `DATA_DIR=/data`, so all state lands under `/data/*` on the mounted
volumes. HentaiNexus credentials stay in `config.json` (set once via the web
UI Settings page); they are intentionally not configurable via env.

---

## Volume Layout

| Compose path (host) | Container path | Contents |
|---|---|---|
| `./data/config` | `/data/config` | `config.json`, `series.json`, `hmanga-registry.json`, `hmanga-global-books.json`, per-artist H-Manga state |
| `./data/downloads` | `/data/downloads` | Manga chapters (default `downloadPath`) |
| `./data/hmanga` | `/data/hmanga-downloads` | H-Manga ZIPs / extracted books (default `hmangaDownloadPath`) |
| `./data/profile` | `/data/profile` | Chrome profile — **keeps Cloudflare session state**; must persist or you will re-solve challenges after every container replacement |

All series registries (`.state/series.json`) and chapter state files
(`.chapters.json`) live **inside the download directories**, so keeping
downloads on a persistent volume means all state survives container
replacement automatically.

---

## Auto-Update Watcher (Watchtower)

The update loop:

1. You push to `master` (or a `v*` tag).
2. GitHub Actions builds the image and pushes it to GHCR
   (`ghcr.io/htomasino/webscraper:latest` for master, semver tags for releases).
3. The `watchtower` sidecar polls GHCR every 5 minutes
   (`WATCHTOWER_POLL_INTERVAL=300`), compares the remote digest with the
   running container's, and **recreates the container** on a new digest.

Notes:

- Watchtower only watches the `scraper` service because the compose file sets
  the label `com.centurylinklabs.watchtower.enable=true` on it and
  `WATCHTOWER_LABEL_ENABLE=true` on the watcher. Remove the watchtower service
  (and the label) to disable auto-updates entirely.
- **In-flight downloads finish**: the compose file sets
  `stop_grace_period: 15m`, longer than the default shutdown timeout
  (`SCRAPE_SHUTDOWN_TIMEOUT_SECONDS=600` = 10 min). Watchtower sends SIGTERM,
  the server stops accepting new work and waits for running downloads to
  finish, then exits cleanly. Docker escalates to SIGKILL only after the grace
  period. If your downloads regularly exceed 10 minutes, raise
  `SCRAPE_SHUTDOWN_TIMEOUT_SECONDS` **and** `stop_grace_period` together, plus
  Watchtower's `WATCHTOWER_TIMEOUT` (it sends the Docker stop call itself and
  its own default of 30s would otherwise bypass the grace period).
- **Rollback**: Watchtower removes old images by default
  (`WATCHTOWER_CLEANUP=true`). To pin a specific version, set the scraper's
  `image:` to an explicit `v*` tag instead of `:latest` and disable auto-update
  for that container (remove the label), or run `docker pull` +
  `docker compose up -d` manually with the older tag.
- Test the loop manually: `docker exec comic-scraper-watchtower /watchtower --run-once`.
- The image is **public** on GHCR — no `docker login` needed on the host.

---

## Network Drives

Two supported approaches. **Approach A (host mount + bind) is recommended**;
Approach B is a Linux-engine alternative that skips the host mount.

Regardless of approach, all scraper state on the share survives container
replacement (registries and chapter state live next to the downloads), and
the app's temp+rename writes work over CIFS/NFS — just with extra latency.

### Approach A — Host mount + bind

Mount the share on the **host**, then bind-mount the mountpoint into the
container via the compose file. This keeps all share semantics (auth,
permissions, caching) in the host OS.

#### Windows Docker Desktop

1. Enable File Sharing for the drive: Docker Desktop → Settings → Resources →
   File Sharing → share the drive letter (e.g. `Z:`).
2. Bind the shared path in compose:

```yaml
    volumes:
      - /host_mnt/z/comics:/data/downloads        # drive-letter path form
      # or for UNC shares:
      - /host_mnt/nas/comics:/data/downloads      # \\nas\comics shared
```

   (Docker Desktop maps `\\nas\comics` and `Z:\comics` to `/host_mnt/...`
   paths inside the VM. You can also write the bind as
   `//nas/comics:/data/downloads` in compose.)

3. **Performance warning**: Docker Desktop's gRPC-FUSE file sharing adds
   noticeable latency to many-small-file workloads (chapter downloads are
   exactly that). Prefer a Linux host, or accept slower ZIP extraction.
4. **PUID/PGID note**: the entrypoint chowns `/data/*` subdirs to PUID/PGID
   (default 1000) — for Windows shares this is harmless (permissions are
   emulated by the sharing VM and mapped to your Windows credentials), but
   share ACLs on the NAS side are what actually restrict access; set those
   with your NAS admin tool.

#### Linux host (SMB via CIFS)

`/etc/fstab` with a credentials file:

```
//nas/comics  /mnt/nas-comics  cifs  credentials=/etc/smb-creds,uid=1000,gid=1000,file_mode=0644,dir_mode=0755  0  0
```

`/etc/smb-creds` (mode 600, root-owned):

```
username=scraper
password=secret
```

Then in compose:

```yaml
    volumes:
      - /mnt/nas-comics:/data/downloads
```

`uid=1000,gid=1000` must match the container's `PUID`/`PGID` so the Go server
(running as that UID) can write.

#### Linux host (NFS)

```
nas:/mnt/tank/comics  /mnt/nas-comics  nfs  rw,hard,intr  0  0
```

NFS mount ownership is whatever the export maps to (often `root` squashed);
align `PUID`/`PGID` with the export's anonuid/anongid or the owning UID on the
server.

#### TrueNAS / Synology example

- Create an NFS export (or SMB share) for comics with allowed CIDR of the
  Docker host, e.g. `192.168.1.0/24`, squash to a dedicated user (uid 1000).
- Mount on the host (`/etc/fstab` as above) and bind-mount the mountpoint.

### Approach B — Docker volume drivers (Linux engines only)

**Not available on Docker Desktop for Windows/macOS.** The engine itself
mounts the share, no host mountpoint needed:

```yaml
services:
  scraper:
    volumes:
      - nas-downloads:/data/downloads

volumes:
  nas-downloads:
    driver_opts:
      type: cifs
      o: addr=192.168.1.100,username=scraper,password=${NAS_PASSWORD},uid=1000,gid=1000,file_mode=0644,dir_mode=0755
      device: //nas/comics
```

NFS variant:

```yaml
volumes:
  nas-downloads:
    driver_opts:
      type: nfs
      o: addr=192.168.1.100,rw
      device: :/mnt/tank/comics
```

Constraints:

- Put credentials in a compose env file (`.env` with `NAS_PASSWORD=...`),
  **not inline** in the compose file, and keep the file out of git.
- Do **not** `chown` the share root — the entrypoint skips chown when
  `/data` is not writable by root; use existing share ownership and set
  `PUID`/`PGID` to match.
- Rename-based atomic writes (temp file + rename) work on CIFS/NFS but are
  slower than local disk; the app tolerates this.

### Scraper-specific setup rules (both approaches)

1. **Point the app at the in-container mount path.** Set `downloadPath` and
   `hmangaDownloadPath` in `config.json` (via the web UI Settings page or by
   editing `./data/config/config.json`), or override via env:

   ```yaml
   environment:
     - SCRAPE_DOWNLOAD_PATH=/data/downloads
     - SCRAPE_HMANGA_DOWNLOAD_PATH=/data/hmanga-downloads
   ```

   Never use host paths or UNC paths inside the app config — the app sees the
   container filesystem.

2. **State lives on the share.** `.state/series.json` and per-chapter
   `.chapters.json` are written next to the downloads. Atomic temp+rename
   writes work over CIFS/NFS but add latency. This is a feature: all
   series/chapter state survives image updates because Watchtower replaces
   only the container, not the volumes.

3. **Permissions.** The entrypoint chowns only `DATA_DIR` subdirs
   (`/data/config`, `/data/downloads`, `/data/hmanga-downloads`,
   `/data/profile`). For shares mounted with a read-only root or existing
   ownership, create the subdirs beforehand and match `PUID`/`PGID` to the
   share (`uid=`/`gid=` CIFS options or NFS anonuid).

4. **Disable startup verification on slow shares.** Set
   `verifyHMDownloads: false` in `config.json` — the H-Manga startup ZIP
   verification scan is slow over SMB/NFS. The app then trusts its
   `.books.json` state.

5. **`rateDelay` is unaffected.** Scraping bandwidth is the share's problem,
   not the scraper's; no per-download throttling changes are needed.

---

## CLI Usage Inside the Container

The image ships the CLI binary. Helper scripts wrap `docker exec`:

```bash
./docker/scraper-cli.sh list                 # from the repo checkout
./docker/scraper-cli.sh add https://asuracomic.net/series/...
```

```powershell
.\docker\scraper-cli.ps1 list
.\docker\scraper-cli.ps1 add https://asuracomic.com/series/...
```

Or directly (set `COMIC_SCRAPER_CONTAINER` to override the default
`comic-scraper` name):

```bash
docker exec comic-scraper comic-scraper-cli list
```

The CLI reads the same `/data/config/config.json` inside the container.

---

## Migrating from Native Windows

With the stack stopped, copy your existing state into the compose directories:

| From (Windows) | To (compose host dir) |
|---|---|
| `%APPDATA%\comic-scraper` | `./data/config` |
| `%APPDATA%\comic-scraper\downloads` | `./data/downloads` |
| `%APPDATA%\comic-scraper\hmanga-downloads` | `./data/hmanga` |
| `%LOCALAPPDATA%\comic-scraper-chrome-profile` | `./data/profile` |

Notes:

- The migrated `config.json` contains Windows paths for `downloadPath` /
  `hmangaDownloadPath`; either fix them via the Settings UI after first boot,
  edit the file to the container paths (`/data/downloads`,
  `/data/hmanga-downloads`), or set the `SCRAPE_DOWNLOAD_PATH` /
  `SCRAPE_HMANGA_DOWNLOAD_PATH` env overrides in compose.
- The startup scan imports series found on disk, so chapter folders copied
  without registry files are re-adopted automatically.
- The Chrome profile migration preserves Cloudflare/HentaiNexus session state.
- File permission note: after copying, ensure the files are readable by the
  container user (`PUID`/`PGID`, default 1000).

---

## Reverse Proxy Notes

The UI stays a LAN-trust app (no authentication) — a reverse proxy only adds
TLS/hostname convenience:

- **nginx**: `proxy_pass http://127.0.0.1:8080;` with
  `proxy_set_header Host $host;`.
- **Caddy**: `scraper.example.com { reverse_proxy 127.0.0.1:8080 }`.
- **Traefik**: standard `Service`/`Router` labels pointing at the scraper
  container's port 8080.

When the UI is reached via a non-localhost origin, set
`CSRF_ALLOWED_HOSTS=<host-or-ip>` (comma-separated list for multiple hosts) in
the scraper's environment; otherwise the CSRF guard rejects all
state-changing requests with 403 (the UI loads, but every action fails).

---

## Importing Browser Cookies (Cloudflare Escape Hatch)

Some sites enforce Cloudflare challenges that a Linux container cannot pass
(device-attestation / Private Access Token flows — the challenge page 401s on
the `/pat/` endpoint and the title stays "Just a moment..." forever, e.g.
en-thunderscans.com; some ManhuaUS deployments show the same pattern). For
those, the scraper can import cookies from your local browser and use them
directly, bypassing the challenge entirely — the same trick a browser
extension does.

### 1. Export cookies from your local browser

Open the site in your desktop browser, solve the Cloudflare challenge once,
then export its cookies with any cookie-export extension (e.g. **Get
cookies.txt LOCALLY**). Two formats are accepted:

- **JSON array** (the extension's JSON export): `[{"name": "cf_clearance",
  "value": "...", "domain": ".manhuaus.com", ...}]`
- **Netscape `cookies.txt`** (the classic tab-separated format, starts with
  `# Netscape HTTP Cookie File`)

### 2. Drop the file into the container

Mount the file as `cookies.json` in the config directory (or point
`COOKIE_FILE` at any path):

```yaml
services:
  scraper:
    environment:
      - COOKIE_FILE=/data/config/cookies.txt
    volumes:
      - ./cookies.txt:/data/config/cookies.txt:ro
```

If no `COOKIE_FILE` is set, the app also looks for
`<DATA_DIR>/config/cookies.json` automatically.

### 3. Restart

On every fresh browser context the cookies are injected
(`[COOKIES] Injected N cookies from ...` in the logs) **before** any
navigation, so the site sees your already-solved `cf_clearance` session
instead of a fresh challenge. Combined with the persistent profile volume,
the challenge is skipped entirely.

Notes:

- The cookie file is read on every context (re)creation, so you can update
  it without restarting the container — just replace the mounted file.
- Export **all** cookies for the site's domain (Cloudflare sets
  `cf_clearance` and often `__cf_bm`); filtering to one cookie name is not
  required.
- Cookies expire (typically hours to days for `cf_clearance`). When they
  expire the site falls back to the normal challenge flow; re-export fresh
  cookies from your browser.
- Cookie import does not affect the Chrome profile volume — it is additive.

---

## Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| UI loads but every action returns 403 | Browser's Origin host is not trusted. Add it to `CSRF_ALLOWED_HOSTS` (your host's LAN IP behind Docker port-mapping cannot be auto-detected inside the container). |
| Container unhealthy / `curl /healthz` fails | Check `docker logs comic-scraper`; usually a bind conflict on 8080 or `DATA_DIR` not writable. |
| Playwright/Chromium launch errors | Xvfb must be running (entrypoint starts it). Check logs for `Failed to launch browser`; ensure the image was built from this Dockerfile (bundled Chromium present). |
| Permission denied writing downloads | `PUID`/`PGID` do not match the volume/share ownership. For network shares set the CIFS `uid=`/`gid=` or NFS anonuid/anongid to match `PUID`/`PGID`; create share subdirs beforehand if the share root is read-only. |
| Profile lock errors at startup | A previous Chrome was killed while holding the profile lock; the app cleans up orphan PIDs automatically, but a hard SIGKILL can leave the lockfile. `rm ./data/profile/lockfile` (and `scraper.pid`) and restart. |
| Watchtower never updates | Label mismatch (container lacks `com.centurylinklabs.watchtower.enable=true`) or Watchtower cannot see the Docker socket. Test with `docker exec comic-scraper-watchtower /watchtower --run-once`. Image is public on GHCR — pull auth is not needed. |
| CIFS rename/permission errors in logs | Some servers reject rename-based atomic writes with specific option sets; add `noserverino`, ensure `file_mode`/`dir_mode` allow write, or switch to the host-mount approach. |
| Slow startup with H-Manga on a share | Set `verifyHMDownloads: false` in `config.json` to skip the disk verification scan. |
| Downloads killed at 10 minutes during updates | `SCRAPE_SHUTDOWN_TIMEOUT_SECONDS` exceeds `stop_grace_period`; raise both (compose default grace is 15m). |
| ThunderScans scrape fails with "Cloudflare challenge did not resolve ... device-attestation" | en-thunderscans.com enforces Cloudflare Private Access Token (device attestation) challenges that a Linux container cannot pass — the challenge page 401s on the `/pat/` endpoint and the title stays "Just a moment..." forever. Sites using plain JS challenges (AsuraScans, DrakeComic, ManhuaUS) pass fine. Scrape ThunderScans series from a native Windows install, or wait for Cloudflare/site rules to change. |
| "System Chrome not installed; using Playwright Chromium" in every container log line | Informational, logged once per process. The image now installs real Google Chrome (used when present); this line appears only if Chrome was removed at runtime. |