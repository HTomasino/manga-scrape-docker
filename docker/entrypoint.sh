#!/bin/bash
set -e

PUID=${PUID:-1000}
PGID=${PGID:-1000}
DATA_DIR=${DATA_DIR:-/data}

# Ensure data directories exist. H-Manga downloads live in a subdir of DATA_DIR
# by default (DATA_DIR/hmanga-downloads) but users may override via
# SCRAPE_HMANGA_DOWNLOAD_PATH to a mounted share; only the default is created here.
mkdir -p "$DATA_DIR/config" "$DATA_DIR/downloads" "$DATA_DIR/hmanga-downloads" "$DATA_DIR/profile"

# Only chown when running as root and the target is writable. On network-share
# mounts the entrypoint must NOT chown the share root; create subdirs beforehand
# and match PUID/PGID to the share's ownership instead (see Docs/DOCKER.md).
if [ "$(id -u)" -eq 0 ] && [ -w "$DATA_DIR" ]; then
    groupadd -g "$PGID" -o comic-scraper >/dev/null 2>&1 || true
    id -u comic-scraper >/dev/null 2>&1 || useradd -u "$PUID" -g "$PGID" -o -m comic-scraper >/dev/null 2>&1 || true
    chown -R "$PUID:$PGID" "$DATA_DIR/config" "$DATA_DIR/downloads" "$DATA_DIR/hmanga-downloads" "$DATA_DIR/profile"
    exec gosu "$PUID:$PGID" /usr/local/bin/entrypoint.sh "$@"
fi

# If the first argument is "web" (or no argument), start the web server under Xvfb
if [ "$#" -eq 0 ] || [ "$1" = "web" ]; then
    shift || true
    # Start Xvfb on a free display so the Go server becomes PID 1 and receives
    # SIGTERM properly. Container-local unix socket only, so no xauth needed.
    DISPLAY_NUM=99
    while [ -f "/tmp/.X${DISPLAY_NUM}-lock" ]; do
        DISPLAY_NUM=$((DISPLAY_NUM + 1))
    done
    Xvfb ":${DISPLAY_NUM}" -screen 0 1280x720x24 -nolisten tcp >/dev/null 2>&1 &
    export DISPLAY=":${DISPLAY_NUM}"
    exec /usr/local/bin/comic-scraper-web "$@"
fi

exec "$@"