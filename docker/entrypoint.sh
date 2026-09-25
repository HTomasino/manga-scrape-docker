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

# Start Xvfb for ALL modes (web server, CLI exec sessions). The scraper is
# headed (Headless:false) and needs a display; docker exec sessions inherit
# DISPLAY from the container ENV (set to :99 in the Dockerfile), which only
# works if Xvfb is actually running on :99 regardless of entrypoint mode.
if [ -z "$DISPLAY" ]; then
    export DISPLAY=":99"
fi
DISPLAY_NUM="${DISPLAY#:}"
if ! [ -e "/tmp/.X11-unix/X${DISPLAY_NUM}" ] && [ ! -f "/tmp/.X${DISPLAY_NUM}-lock" ]; then
    Xvfb ":${DISPLAY_NUM}" -screen 0 1280x720x24 -nolisten tcp >/dev/null 2>&1 &
    # Wait for the socket so the first browser launch cannot beat Xvfb.
    for _ in $(seq 1 50); do
        [ -e "/tmp/.X11-unix/X${DISPLAY_NUM}" ] && break
        sleep 0.1
    done
fi

# If the first argument is "web" (or no argument), start the web server
if [ "$#" -eq 0 ] || [ "$1" = "web" ]; then
    shift || true
    exec /usr/local/bin/comic-scraper-web "$@"
fi

exec "$@"