//go:build !windows

package browser

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// pidAlive reports whether a process with the given PID is currently running,
// checked via /proc/<pid> existence.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return err == nil
}

// processCommandLine returns the full command line for the given PID from
// /proc/<pid>/cmdline (NUL-separated args), or an empty string if it cannot
// be retrieved.
func processCommandLine(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil {
		return ""
	}
	return strings.ReplaceAll(string(data), "\x00", " ")
}

// killChromeTree terminates the process with the given PID and its child
// tree via pkill -f matched on the user-data dir argument. No-op unless the
// process is still alive and its command line verifiably references our
// user-data dir — the same conservative safety rule as the Windows path.
func killChromeTree(pid int) {
	// Ownership verification happens in cleanOrphanChrome before calling us;
	// here we simply kill the whole process tree rooted at pid.
	cmdline := processCommandLine(pid)
	if cmdline == "" {
		return
	}
	// pkill -P kills direct children; kill the parent last via -TERM.
	out, err := func() (string, error) {
		pkill := exec.Command("pkill", "-TERM", "-P", strconv.Itoa(pid))
		if o, err := pkill.CombinedOutput(); err != nil {
			// No children (exit 1) is fine; the parent kill below still runs.
			if !strings.Contains(err.Error(), "exit status 1") {
				return string(o), err
			}
		}
		kill := exec.Command("kill", "-TERM", strconv.Itoa(pid))
		if o, err := kill.CombinedOutput(); err != nil {
			return string(o), err
		}
		return "", nil
	}()
	if err != nil {
		log.Printf("[BROWSER-CLEANUP] kill failed for PID %d: %v (%s)", pid, err, strings.TrimSpace(out))
		return
	}
	log.Printf("[BROWSER-CLEANUP] Killed orphan Chrome process tree rooted at PID %d", pid)
}