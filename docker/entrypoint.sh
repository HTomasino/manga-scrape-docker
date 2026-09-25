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
XLOCK="/tmp/.X${DISPLAY_NUM}-lock"
XSOCKET="/tmp/.X11-unix/X${DISPLAY_NUM}"

# A lock file from a previous container boot (docker restart keeps the
# filesystem) must not suppress Xvfb startup: verify the recorded PID is
# alive AND is actually Xvfb before trusting it; otherwise remove the
# stale lock and socket.
if [ -f "$XLOCK" ]; then
    XPID=$(cat "$XLOCK" 2>/dev/null | tr -dc "0-9")
    if [ -n "$XPID" ] && [ -d "/proc/$XPID" ] && grep -qx "Xvfb" "/proc/$XPID/comm" 2>/dev/null; then
        echo "[XVFB] Reusing already-running Xvfb on :${DISPLAY_NUM} (pid $XPID)"
    else
        echo "[XVFB] Removing stale display artifacts (lock pid: ${XPID:-none})"
        rm -f "$XLOCK" "$XSOCKET" 2>/dev/null || true
    fi
fi

# A socket without a lock file is stale: a live Xvfb always maintains
# its lock, so remove the orphaned socket before starting.
if [ -e "$XSOCKET" ] && [ ! -f "$XLOCK" ]; then
    echo "[XVFB] Removing orphaned socket (no lock file)"
    rm -f "$XSOCKET" 2>/dev/null || true
fi

XVFB_PID=""
if ! [ -e "$XSOCKET" ]; then
    echo "[XVFB] starting on :${DISPLAY_NUM}"
    Xvfb ":${DISPLAY_NUM}" -screen 0 1280x720x24 -nolisten tcp >/dev/null 2>&1 &
    XVFB_PID=$!
    # Wait for the socket so the first browser launch cannot beat Xvfb.
    XREADY=0
    for _ in $(seq 1 50); do
        if [ -e "$XSOCKET" ]; then XREADY=1; break; fi
        sleep 0.1
    done
    if [ "$XREADY" -eq 1 ]; then
        echo "[XVFB] ready on :${DISPLAY_NUM}"
    else
        echo "[XVFB] WARNING: socket did not appear within 5s; browser launches may fail"
    fi
fi

# If the first argument is "web" (or no argument), run the web server as a
# direct child of this shell. This shell stays as PID 1: bash reaps every
# child it waits on (including orphans reparented to PID 1, e.g. Chrome
# crashpad helpers left by the Playwright node driver), so chrome crashes do
# not pile up as <defunct> entries the way they do when a Go binary is PID 1.
# SIGTERM/SIGINT (docker stop) is forwarded to the server; the server's
# SCRAPE_SHUTDOWN_TIMEOUT_SECONDS grace applies before Docker's SIGKILL.
# The Go-side ensureXvfb() respawn is still used when Xvfb dies mid-run.
SERVER_PID=""
terminate() {
    echo "[ENTRYPOINT] Received stop signal; forwarding to children"
    if [ -n "$SERVER_PID" ]; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "$XVFB_PID" ]; then
        kill -TERM "$XVFB_PID" 2>/dev/null || true
    fi
    exit 0
}
trap terminate TERM INT

if [ "$#" -eq 0 ] || [ "$1" = "web" ]; then
    shift || true
    /usr/local/bin/comic-scraper-web "$@" &
    SERVER_PID=$!
    # Wait on the server; bash reaps reparented orphans between waits.
    wait "$SERVER_PID"
    EXIT_CODE=$?
    # Propagate the server's exit code so docker restart policies behave.
    if [ -n "$XVFB_PID" ]; then
        kill -TERM "$XVFB_PID" 2>/dev/null || true
    fi
    exit "$EXIT_CODE"
fi

# CLI/other mode: hand off completely (bash execs the command; Xvfb stays
# as a shell child and is reaped by bash when it exits).
"$@"