//go:build !linux

package browser

// ensureXvfb is a no-op outside Linux: on native Windows there is no Xvfb;
// the real display is managed by the OS session.
func ensureXvfb() error { return nil }
