package browser

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// getChromePIDFromSingletonLock reads the PID from Chrome's SingletonLock symlink on Linux.
// On Linux, Chrome creates a symbolic link at "SingletonLock" that points to a file named after the host and PID,
// e.g. "/tmp/.org.chromium.Chromium.ABCDEF.12345"
func getChromePIDFromSingletonLock(userDataDir string) (int, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}

	lockPath := filepath.Join(userDataDir, "SingletonLock")
	info, err := os.Lstat(lockPath)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return 0, false
	}

	target, err := os.Readlink(lockPath)
	if err != nil {
		return 0, false
	}

	// Target format: "/tmp/.org.chromium.Chromium.ABCDEF.12345"
	// Extract PID from the last part (after the final dot)
	parts := strings.Split(target, ".")
	if len(parts) < 2 {
		return 0, false
	}

	pidStr := parts[len(parts)-1]
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return 0, false
	}

	return pid, true
}

// getChromePIDFromWindowsProcessList resolves Chrome PID from process list on Windows.
// Uses PowerShell to query Win32_Process for Chrome processes with matching user-data directory.
func getChromePIDFromWindowsProcessList(userDataDir string) (int, bool) {
	if runtime.GOOS != "windows" {
		return 0, false
	}

	// Try using PowerShell command to find chrome.exe processes with the userDataDir in their command line
	cmd := fmt.Sprintf(`Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe' AND CommandLine LIKE '%s%%'" | Select-Object -First 1 ProcessId`, strings.ReplaceAll(userDataDir, `\`, `\\`))
	output, err := exec.Command("powershell", "-Command", cmd).Output()
	if err != nil {
		return 0, false
	}

	// Parse output to extract PID from the first matching process
	outputStr := string(output)
	lines := strings.Split(strings.TrimSpace(outputStr), "\n")
	for _, line := range lines[1:] { // Skip header
		line = strings.TrimSpace(line)
		if line != "" {
			pid, err := strconv.Atoi(strings.Fields(line)[0])
			if err == nil && pid > 0 {
				return pid, true
			}
		}
	}

	return 0, false
}

// getChromePID returns the Chrome PID from either SingletonLock (Linux) or process list (Windows).
// On other OSes it tries to read from scraper.pid file.
func getChromePID(userDataDir string) (int, bool) {
	if runtime.GOOS == "linux" {
		pid, ok := getChromePIDFromSingletonLock(userDataDir)
		if ok {
			return pid, true
		}
	} else if runtime.GOOS == "windows" {
		pid, ok := getChromePIDFromWindowsProcessList(userDataDir)
		if ok {
			return pid, true
		}
	}

	// Fallback to reading scraper.pid file (for compatibility with older versions)
	pid, ok := readChromePID(userDataDir)
	return pid, ok
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
	pid, ok := getChromePID(userDataDir)
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
//
// On non-Windows (Linux), Chrome uses a SingletonLock symlink instead of a
// lockfile. This function polls for the existence of the SingletonLock symlink
// rather than trying to open a lockfile, which would fail on Linux.
func waitForLockRelease(userDataDir string) {
	if runtime.GOOS != "windows" {
		// On Linux, Chrome uses SingletonLock symlink; poll for its existence
		singletonLockPath := filepath.Join(userDataDir, "SingletonLock")
		const maxAttempts = 20 // ~4s at 200ms intervals
		for i := 0; i < maxAttempts; i++ {
			if _, err := os.Lstat(singletonLockPath); err != nil {
				// Symlink no longer exists (Chrome released it)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		log.Printf("[BROWSER-CLEANUP] SingletonLock %s still appears held after kill; launching anyway (Chrome will surface any error)", singletonLockPath)
		return
	}
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
