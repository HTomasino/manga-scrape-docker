//go:build !linux

package main

// startZombieReaper is a no-op outside Linux: on native Windows the server
// is never PID 1 and orphaned grandchildren are handled by the OS.
func startZombieReaper() {}
