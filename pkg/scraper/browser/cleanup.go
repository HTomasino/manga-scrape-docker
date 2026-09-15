package browser

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// pidFilename is the name of the file written in the user-data directory to
// record the Chrome process ID we launched. It is read on the next launch so
// orphan Chrome processes from a previous (crashed or killed) scraper session
// can be cleaned up before acquiring the profile lockfile.
const pidFilename = "scraper.pid"

// chromeExe is the canonical name of the Chrome executable on Windows.
// It is reused both for the orphan check and for the kill step.
const chromeExe = "chrome.exe"

// recordChromePID writes the current process's Chrome PID (pid) to a file in
// userDataDir so a future launch can identify and clean up orphans.
// The PID file lives inside the user-data directory so it shares the same
// lifetime as the Chrome profile it tracks.
func recordChromePID(userDataDir string, pid int) error {
	if err := os.MkdirAll(userDataDir, 0755); err != nil {
		return fmt.Errorf("create user-data dir: %w", err)
	}
	pidPath := filepath.Join(userDataDir, pidFilename)
	return os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0644)
}

// ClearChromePID removes the recorded PID file. Safe to call even if the file
// does not exist. Exported so other packages (e.g. H-Manga manager) can clean
// up the bookkeeping file on their own shutdown path.
func ClearChromePID(userDataDir string) {
	pidPath := filepath.Join(userDataDir, pidFilename)
	if err := os.Remove(pidPath); err != nil && !os.IsNotExist(err) {
		log.Printf("[BROWSER-CLEANUP] Warning: failed to remove PID file %s: %v", pidPath, err)
	}
}

// readChromePID returns the previously-recorded Chrome PID and true if a
// valid PID file exists, or (0, false) if the file is missing/malformed.
func readChromePID(userDataDir string) (int, bool) {
	data, err := os.ReadFile(filepath.Join(userDataDir, pidFilename))
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	pid, err := strconv.Atoi(s)
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// readChromePIDFromLockfile returns the PID that Chrome wrote to its
// profile lockfile when it started, or 0 if the lockfile is missing,
// unreadable, or malformed.
//
// Chrome writes a small lockfile inside the user-data directory whose
// contents are the PID of its main browser process. We use this as the
// source of truth for the PID we should track for orphan cleanup, since the
// Playwright API does not expose the spawned Chrome PID directly.
//
// If Chrome is still holding the lockfile exclusively, this returns 0 and
// logs nothing — the caller will simply skip recording and the next launch
// will retry.
func readChromePIDFromLockfile(userDataDir string) int {
	lockPath := filepath.Join(userDataDir, "lockfile")
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	pid, err := strconv.Atoi(s)
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// cleanOrphanChrome inspects userDataDir for a previously-recorded Chrome PID.
// If that PID is still alive AND its command line references our user-data
// directory, the process is treated as an orphan from a prior scraper session
// and terminated so the new launch can acquire the profile lockfile cleanly.
//
// Safety: the user-data-dir match is required. If we cannot verify the match
// (e.g. WMIC unavailable, command-line truncated), the PID is NOT killed.
// This avoids terminating unrelated Chrome windows the user has open.
func cleanOrphanChrome(userDataDir string) {
	pid, ok := readChromePID(userDataDir)
	if !ok {
		return
	}
	if !pidAlive(pid) {
		// PID no longer exists; the file is stale. Clear it and move on.
		ClearChromePID(userDataDir)
		return
	}
	cmdline := processCommandLine(pid)
	if cmdline == "" {
		// Cannot verify ownership — be conservative and leave the PID alone.
		log.Printf("[BROWSER-CLEANUP] PID %d is alive but its command line could not be read; skipping cleanup to avoid touching unrelated Chrome windows", pid)
		return
	}
	if !strings.Contains(cmdline, userDataDir) {
		// PID belongs to a different Chrome instance that just happens to have
		// the same PID recycled by the OS. Do not kill.
		log.Printf("[BROWSER-CLEANUP] PID %d is alive but does not reference our user-data dir (%s); skipping", pid, userDataDir)
		return
	}
	log.Printf("[BROWSER-CLEANUP] Found orphan Chrome (PID %d) from a previous scraper session; killing before new launch", pid)
	killChromeTree(pid)
	// Give the OS a moment to release the profile lockfile after the process
	// tree has been torn down.
	waitForLockRelease(userDataDir)
	ClearChromePID(userDataDir)
}

// waitForLockRelease polls the Chrome profile lockfile until it is no longer
// exclusively held, up to a few seconds. After killChromeTree Chrome usually
// releases the lock within a few hundred milliseconds; the wait is bounded so
// we never block the launch path indefinitely.
func waitForLockRelease(userDataDir string) {
	lockPath := filepath.Join(userDataDir, "lockfile")
	const maxAttempts = 20 // ~4s at 200ms intervals
	for i := 0; i < maxAttempts; i++ {
		// Open with shared read access; if Chrome still holds it exclusively,
		// this returns a sharing-violation error.
		f, err := os.OpenFile(lockPath, os.O_RDONLY, 0)
		if err == nil {
			f.Close()
			return
		}
		// Sleep then retry.
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("[BROWSER-CLEANUP] lockfile %s still appears held after kill; launching anyway (Chrome will surface any error)", lockPath)
}
