//go:build windows

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

// runDetached re-executes this binary without the -d flag so the child runs
// detached from the terminal (Windows behaviour; Linux containers run in the
// foreground and simply ignore -d via main_other.go).
func runDetached() {
	exe, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to get executable path: %v", err)
	}

	// Remove the -d flag from the child args so it doesn't fork again.
	childArgs := make([]string, 0, len(os.Args[1:]))
	for _, a := range os.Args[1:] {
		if a == "-d" || a == "--d" {
			continue
		}
		childArgs = append(childArgs, a)
	}

	cmd := exec.Command(exe, childArgs...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start background process: %v", err)
	}

	fmt.Printf("Server running in background (PID: %d)\n", cmd.Process.Pid)
	os.Exit(0)
}