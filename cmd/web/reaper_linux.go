//go:build linux

package main

import (
	"syscall"
	"time"
)

// startZombieReaper reaps orphaned child processes (Chrome, crashpad,
// node driver helpers). The web server is PID 1 in the container, and Go's
// runtime only waits for children IT spawned via os/exec; grandchildren
// orphaned when the Playwright node driver exits reparent to PID 1 and
// would pile up as <defunct> entries indefinitely.
func startZombieReaper() {
	go reapZombies()
}

// reapZombies reaps any child the process has: the Playwright node
// driver's chrome/crashpad children get orphaned to PID 1 when the
// driver exits, and Go's runtime only waits for children it spawned
// itself. syscall.Wait4 exists only on Unix, hence the build tag.
func reapZombies() {
	var ws syscall.WaitStatus
	for {
		if _, err := syscall.Wait4(-1, &ws, 0, nil); err != nil {
			// EINTR is transient; anything else (ECHILD: no children)
			// just means nothing to reap right now -- sleep and retry.
			time.Sleep(5 * time.Second)
		}
	}
}
