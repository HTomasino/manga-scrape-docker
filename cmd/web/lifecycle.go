package main

import (
	"sync"
	"sync/atomic"
)

// countedWaitGroup is a sync.WaitGroup that also tracks its current counter
// value so callers can log progress ("still waiting for N goroutines") without
// racing against Add/Done from other goroutines. sync.WaitGroup does not
// expose its counter directly, so we maintain a parallel atomic counter.
//
// The counter increments in Add and decrements in Done, exactly mirroring
// the WaitGroup. A caller that reads InFlight during the shutdown wait
// gets a snapshot of the current load; it may be stale by the next tick,
// which is fine -- the shutdown wait polls every 5 seconds and a few extra
// or fewer goroutines in the count does not affect correctness.
//
// safeGoTrack is the only caller path. It calls Add(1) before spawning and
// defers Done() inside the spawned goroutine, so the counter and the
// WaitGroup transition together at each spawn/exit.
type countedWaitGroup struct {
	wg  sync.WaitGroup
	cnt int64
}

// Add mirrors sync.WaitGroup.Add.
func (c *countedWaitGroup) Add(delta int) {
	c.wg.Add(delta)
	atomic.AddInt64(&c.cnt, int64(delta))
}

// Done mirrors sync.WaitGroup.Done.
func (c *countedWaitGroup) Done() {
	atomic.AddInt64(&c.cnt, -1)
	c.wg.Done()
}

// Wait mirrors sync.WaitGroup.Wait.
func (c *countedWaitGroup) Wait() {
	c.wg.Wait()
}

// InFlight returns the current goroutine count tracked by this WaitGroup.
// The result is a snapshot and may be slightly stale by the time the caller
// acts on it, but is safe to read concurrently with Add/Done.
func (c *countedWaitGroup) InFlight() int {
	return int(atomic.LoadInt64(&c.cnt))
}
