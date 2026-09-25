//go:build linux

package browser

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// xvfbMu serializes Xvfb lifecycle management; ensureContext callers reach
// ensureXvfb before every context creation.
var xvfbMu sync.Mutex

// ensureXvfb verifies the display server named by DISPLAY is reachable and
// (re)starts Xvfb when it is not. This recovers from Xvfb dying mid-run:
// the entrypoint only guarantees the display at container start, and a
// crashed Xvfb previously left the container browser-less until restart.
// Called from ensureContext (Linux only).
func ensureXvfb() error {
	display := os.Getenv("DISPLAY")
	if display == "" {
		// No DISPLAY configured (native Windows runs never get here; this
		// file is linux-only). Nothing to verify.
		return nil
	}
	xvfbMu.Lock()
	defer xvfbMu.Unlock()

	num := strings.TrimPrefix(display, ":")
	sockPath := filepath.Join("/tmp/.X11-unix", "X"+num)
	lockPath := filepath.Join("/tmp", ".X"+num+"-lock")

	// Server reachable? Connect to the unix socket; the socket file can
	// linger after a crash, so test the connection, not existence.
	if conn, err := net.Dial("unix", sockPath); err == nil {
		conn.Close()
		return nil
	}

	log.Printf("[XVFB] Display %s unreachable (socket %s); restarting Xvfb", display, sockPath)

	// Remove stale artifacts. Verify the lock's PID is dead before deleting:
	// a live Xvfb on another display number must not be disturbed, and a
	// running Xvfb on OUR display would have a reachable socket above.
	if data, err := os.ReadFile(lockPath); err == nil {
		if pid, perr := strconv.Atoi(strings.TrimSpace(string(data))); perr == nil && pid > 0 {
			if proc, perr := os.FindProcess(pid); perr == nil {
				if err := proc.Signal(syscall.Signal(0)); err == nil {
					// Process alive but socket dead: it is a broken Xvfb,
					// not one we can reuse: kill it and clean up.
					if comm, cerr := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm"); cerr == nil && strings.TrimSpace(string(comm)) == "Xvfb" {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
			}
		}
		_ = os.Remove(lockPath)
	}
	_ = os.Remove(sockPath)

	// Run Xvfb as the current user. Chrome refuses to run against an X
	// server owned by a different user, so pass our own uid explicitly.
	uid := uint32(0)
	if u, err := user.Current(); err == nil {
		if v, perr := strconv.ParseUint(u.Uid, 10, 32); perr == nil {
			uid = uint32(v)
		}
	}
	cmd := exec.Command("Xvfb", display, "-screen", "0", "1280x720x24", "-nolisten", "tcp")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: uid},
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Xvfb on %s: %w", display, err)
	}

	// Wait for the socket to appear.
	ready := false
	for i := 0; i < 50; i++ {
		if conn, err := net.Dial("unix", sockPath); err == nil {
			conn.Close()
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		_ = cmd.Process.Kill()
		return fmt.Errorf("Xvfb started on %s but the socket never appeared", display)
	}
	log.Printf("[XVFB] Xvfb ready on %s (pid %d)", display, cmd.Process.Pid)
	return nil
}
