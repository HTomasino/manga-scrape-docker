//go:build windows

package browser

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

// pidAlive reports whether a process with the given PID is currently running.
// Implemented via `tasklist /FI "PID eq <pid>"` so it works without depending
// on syscall-specific packages. We use exec.Command rather than os.FindProcess
// because the latter cannot distinguish a live process from a recycled PID on
// Windows without an additional handle check.
func pidAlive(pid int) bool {
	cmd := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	// tasklist outputs "INFO: No tasks are running..." when PID is not found,
	// otherwise the line contains the image name and "PID".
	return strings.Contains(strings.ToLower(string(out)), strings.ToLower(chromeExe))
}

// processCommandLine returns the full command line for the given PID via
// WMIC, or an empty string if it cannot be retrieved.
func processCommandLine(pid int) string {
	cmd := exec.Command("wmic", "process", "where", fmt.Sprintf("ProcessId=%d", pid), "get", "CommandLine", "/value")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	s := string(out)
	// Output format: "CommandLine=...\r\n\r\n"
	const prefix = "CommandLine="
	if idx := strings.Index(s, prefix); idx >= 0 {
		rest := s[idx+len(prefix):]
		rest = strings.TrimRight(rest, "\r\n ")
		if idx2 := strings.Index(rest, "\n"); idx2 >= 0 {
			rest = rest[:idx2]
		}
		return strings.TrimRight(rest, "\r ")
	}
	return ""
}

// killChromeTree terminates the process with the given PID and any Chrome
// child processes it spawned. We use taskkill /T /F to walk the parent->child
// tree so a single Chrome process does not leave its helpers behind.
// No-op if the PID is no longer alive.
func killChromeTree(pid int) {
	if !pidAlive(pid) {
		return
	}
	cmd := exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T", "/F")
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("[BROWSER-CLEANUP] taskkill failed for PID %d: %v (%s)", pid, err, strings.TrimSpace(string(out)))
		return
	}
	log.Printf("[BROWSER-CLEANUP] Killed orphan Chrome process tree rooted at PID %d", pid)
}