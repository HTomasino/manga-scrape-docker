//go:build !windows

package main

import "log"

// runDetached is a no-op outside Windows: the -d flag is accepted for
// compatibility but the server always runs in the foreground (containers and
// POSIX services manage detachment themselves).
func runDetached() {
	log.Printf("-d (detached) is not supported on this platform; running in foreground")
}