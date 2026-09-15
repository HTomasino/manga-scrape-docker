package main

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/user/comic-scraper/pkg/hmanga"
)

// hmangaOpDispatcherTimeout caps how long the dispatcher waits for an op's
// body to call markHMangaOpInProgress. The "starting" phase for a single
// download includes ensureContext + Login + triggerDownload, which can
// take 10-30 seconds on a cold start (Playwright launch, tab open, slow
// network round-trip to the home page for the login check). The previous
// 5-second timeout was far too short and caused the dispatcher to
// "advance" while the body was still warming up, producing misleading
// logs like "starting op=... / timeout... advancing" and then "didn't
// start the download" downstream. 60 seconds covers cold-start cases
// while still bounding how long a stuck op can block the queue.
const hmangaOpDispatcherTimeout = 60 * time.Second

// hmangaOpMaxIdleTime bounds how long an op may sit in Starting or InProgress
// without any activity before the reaper finalizes it as failed. The clock
// resets whenever the op reaches in-progress or a download emits a progress
// tick, so legitimately slow-but-alive multi-GB downloads are not force-failed;
// only a genuinely hung body (stuck Playwright call) exceeds it.
const hmangaOpMaxIdleTime = 30 * time.Minute

// reapSweepInterval is how often the dispatcher wakes up to run the
// max-lifetime reaper when the queue has no pending work.
const reapSweepInterval = 1 * time.Minute

type hmangaOpStatus int

const (
	hmangaOpPending hmangaOpStatus = iota
	hmangaOpStarting
	hmangaOpInProgress
	hmangaOpDone
	hmangaOpFailed
)

type hmangaOpKind int

const (
	hmangaOpRefresh hmangaOpKind = iota
	hmangaOpDownload
)

// hmangaOpItem is one queued H-Manga operation. It serializes the
// Playwright-touching "starting" phase; once an item reaches in-progress the
// dispatcher may advance to the next pending item.
type hmangaOpItem struct {
	id         string
	kind       hmangaOpKind
	artistID   string
	folderName string
	bookID     string // empty for refresh
	title      string // empty for refresh
	dlID       string // download record id (only for download)
	status     hmangaOpStatus
	createdAt  time.Time
	// activeSince is the reaper's idle clock: set at enqueue, refreshed when
	// the op reaches in-progress or a download emits a progress tick. The
	// max-idle reaper force-fails an op only when this goes stale.
	activeSince time.Time

	wg          *sync.WaitGroup // optional, released when the op completes or is dropped
	startedCh   chan struct{}   // closed when status reaches in-progress/done/failed
	runRefresh  func(opID string) error
	runDownload func(opID string) error
}

func (op *hmangaOpItem) releaseWg() {
	if op.wg != nil {
		op.wg.Done()
		op.wg = nil
	}
}

func (s *Server) startHMangaOpDispatcher() {
	s.hmangaOpQueueMu.Lock()
	if s.hmangaOpQueueCond == nil {
		s.hmangaOpQueueCond = sync.NewCond(&s.hmangaOpQueueMu)
	}
	s.hmangaOpQueueMu.Unlock()
	s.safeGoTrack("hmangaOpDispatcher", func() { s.runHMangaOpDispatcher() })
}

func (s *Server) runHMangaOpDispatcher() {
	for {
		if s.isShutdown() {
			s.hmangaOpQueueMu.Lock()
			for _, op := range s.hmangaOpQueue {
				switch op.status {
				case hmangaOpPending:
					// Body never ran; just release the wg so the caller's
					// WaitGroup isn't hung on shutdown or after a drop.
					op.releaseWg()
					if op.kind == hmangaOpDownload && op.dlID != "" {
						delete(s.hmangaOpByDlID, op.dlID)
					}
					// Refresh ops that never ran own the Scraping flag set at
					// enqueue time. Clear it here so the UI does not show
					// "Scraping..." forever after a restart or shutdown.
					if op.kind == hmangaOpRefresh && op.artistID != "" {
						s.updateHMangaRegistryEntry(op.artistID, func(a *hmanga.Artist) {
							a.Scraping = false
						})
					}
				case hmangaOpStarting:
					op.status = hmangaOpFailed
					if op.startedCh != nil {
						close(op.startedCh)
						op.startedCh = nil
					}
					// The body goroutine was already spawned and may still call
					// releaseWg via markHMangaOpFinished, but if shutdown times
					// out before it runs, release the wg here too so
					// autoDownloadMissingBooks' wg.Wait() doesn't hang. releaseWg
					// is idempotent (nils op.wg after Done).
					op.releaseWg()
				}
			}
			s.pendingReap = true
			s.reapFinishedHMangaOps()
			s.hmangaOpQueueMu.Unlock()
			return
		}

		s.hmangaOpQueueMu.Lock()
		var op *hmangaOpItem
		for _, it := range s.hmangaOpQueue {
			if it.status == hmangaOpPending {
				op = it
				break
			}
		}
		if op == nil {
			// Wait with a bounded timeout so the max-idle reaper runs even
			// when no op ever finishes (the exact stuck-op scenario it exists
			// for): a plain Wait() would sleep forever with pendingReap unset.
			s.waitHMangaOpQueue(reapSweepInterval)
			s.reapFinishedHMangaOps()
			s.pendingReap = false
			s.hmangaOpQueueMu.Unlock()
			continue
		}
		op.status = hmangaOpStarting
		// Capture the channel under the lock: markHMangaOpInProgress and the
		// shutdown path nil it out under the same lock, so reading the field
		// after unlock races them.
		startedCh := op.startedCh
		s.hmangaOpQueueMu.Unlock()

		kind := "refresh"
		if op.kind == hmangaOpDownload {
			kind = "download"
		}
		log.Printf("[HMANGA-QUEUE] starting op=%s kind=%s artist=%s book=%s", op.id, kind, op.folderName, op.bookID)

		s.safeGoTrack("hmangaOpBody", func() { s.runHMangaOpBody(op) })

		if startedCh != nil {
			select {
			case <-startedCh:
			case <-time.After(hmangaOpDispatcherTimeout):
				// The body did not signal in-progress within the timeout.
				// This branch is reachable in practice only for refresh
				// ops — download ops call markHMangaOpInProgress at the
				// very start of their runDownload closure (before
				// DownloadBook), so they always signal within
				// milliseconds. A refresh op signals in-progress only
				// after ExtractBooks returns, which involves opening a
				// tab and navigating to the artist page; a slow network
				// or a very large artist listing can push that past the
				// 60s timeout.
				//
				// In either case the body is still running in the
				// background and may still reach in-progress or finish.
				// The dispatcher just stops blocking on this op and
				// waits for the next pending op. The manager's 2-slot
				// download semaphore still serializes Playwright-touching
				// work for downloads, so a slow refresh cannot starve
				// concurrent downloads indefinitely.
				log.Printf("[HMANGA-QUEUE] op=%s (kind=%s) did not signal in-progress within %v; dispatcher continuing to wait for next pending op (body still running)", op.id, kind, hmangaOpDispatcherTimeout)
			}
		}
		s.hmangaOpQueueMu.Lock()
		s.reapFinishedHMangaOps()
		s.pendingReap = false
		s.hmangaOpQueueMu.Unlock()
	}
}

// waitHMangaOpQueue waits on the queue condition variable with a timeout,
// returning early if woken by Signal/Broadcast. Caller must hold
// hmangaOpQueueMu (and still holds it on return).
func (s *Server) waitHMangaOpQueue(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		s.hmangaOpQueueCond.Broadcast()
		close(done)
	}()
	s.hmangaOpQueueCond.Wait()
	// The Wait always returns after Broadcast; the goroutine's close is a
	// bookkeeping no-op at that point.
	<-done
}

func (s *Server) runHMangaOpBody(op *hmangaOpItem) {
	var err error
	switch op.kind {
	case hmangaOpRefresh:
		err = op.runRefresh(op.id)
	case hmangaOpDownload:
		err = op.runDownload(op.id)
	}
	if err != nil {
		log.Printf("[HMANGA-QUEUE] op=%s finished with error: %v", op.id, err)
		s.markHMangaOpFinished(op.id, hmangaOpFailed)
		return
	}
	// Mark the op terminally when the body returns. For downloads, the
	// manager's progress callback may have already marked done/failed; the
	// "don't clobber failure with success" guard prevents a late callback
	// done from overwriting a failure, and a redundant Done is harmless.
	// This ensures ops that complete without emitting a progress tick (e.g.
	// disk-verify early return, pre-emit DownloadBook failures, retry
	// markFailed paths) are still reaped.
	s.markHMangaOpFinished(op.id, hmangaOpDone)
}

// enqueueHMangaOp registers an item, returns its id, and wakes the dispatcher.
func (s *Server) enqueueHMangaOp(op *hmangaOpItem) string {
	if op.id == "" {
		op.id = uuid.New().String()
	}
	if op.createdAt.IsZero() {
		op.createdAt = time.Now()
	}
	if op.activeSince.IsZero() {
		op.activeSince = time.Now()
	}
	op.startedCh = make(chan struct{})

	s.hmangaOpQueueMu.Lock()
	if s.hmangaOpQueueCond == nil {
		s.hmangaOpQueueCond = sync.NewCond(&s.hmangaOpQueueMu)
	}
	s.hmangaOpQueue = append(s.hmangaOpQueue, op)
	s.hmangaOpByID[op.id] = op
	if op.kind == hmangaOpDownload && op.dlID != "" {
		s.hmangaOpByDlID[op.dlID] = op.id
	}
	s.hmangaOpQueueMu.Unlock()
	s.hmangaOpQueueCond.Signal()

	return op.id
}

func (s *Server) markHMangaOpInProgress(opID string) {
	s.hmangaOpQueueMu.Lock()
	defer s.hmangaOpQueueMu.Unlock()
	op, ok := s.hmangaOpByID[opID]
	if !ok || op.status != hmangaOpStarting {
		return
	}
	op.status = hmangaOpInProgress
	op.activeSince = time.Now()
	kind := "refresh"
	if op.kind == hmangaOpDownload {
		kind = "download"
	}
	log.Printf("[HMANGA-QUEUE] op=%s reached in-progress kind=%s artist=%s book=%s", op.id, kind, op.folderName, op.bookID)
	if op.startedCh != nil {
		close(op.startedCh)
		op.startedCh = nil
	}
}

// touchHMangaOpActivity refreshes the reaper's idle clock for an op so a
// slow-but-alive download (progress ticks still arriving) is never force-failed.
// Safe to call at any time; it is a no-op for unknown ops.
func (s *Server) touchHMangaOpActivity(opID string) {
	s.hmangaOpQueueMu.Lock()
	if op, ok := s.hmangaOpByID[opID]; ok {
		op.activeSince = time.Now()
	}
	s.hmangaOpQueueMu.Unlock()
}

func (s *Server) markHMangaOpFinished(opID string, status hmangaOpStatus) {
	s.hmangaOpQueueMu.Lock()
	defer s.hmangaOpQueueMu.Unlock()
	op, ok := s.hmangaOpByID[opID]
	if !ok {
		return
	}
	if status == hmangaOpDone && op.status == hmangaOpFailed {
		// don't clobber an existing failure with success
	} else {
		op.status = status
	}
	if op.startedCh != nil {
		close(op.startedCh)
		op.startedCh = nil
	}
	op.releaseWg()
	s.pendingReap = true
}

// reapFinishedHMangaOps removes done/failed ops from the queue and index maps,
// and force-finalizes ops whose body has shown no activity for
// hmangaOpMaxIdleTime. Callers must hold hmangaOpQueueMu; runs on every
// dispatcher loop pass (pending reap or timed sweep), so a lone hung op is
// reaped even when nothing else in the queue ever finishes.
func (s *Server) reapFinishedHMangaOps() {
	live := make([]*hmangaOpItem, 0, len(s.hmangaOpQueue))
	for _, op := range s.hmangaOpQueue {
		if op.status == hmangaOpDone || op.status == hmangaOpFailed {
			s.finalizeHMangaOp(op)
			continue
		}
		// Max-idle reaper: an op stuck Starting/InProgress with no activity
		// (e.g. a hung Playwright call) must not grow the queue maps
		// unboundedly. Finalize it so it is reaped like any other terminal
		// op; the underlying body goroutine, if it ever finishes, hits the
		// idempotent guards in markHMangaOpFinished.
		if op.status != hmangaOpPending && !op.activeSince.IsZero() && time.Since(op.activeSince) > hmangaOpMaxIdleTime {
			log.Printf("[HMANGA-QUEUE] op=%s (status=%d) showed no activity for %v; finalizing as failed", op.id, op.status, hmangaOpMaxIdleTime)
			op.status = hmangaOpFailed
			if op.startedCh != nil {
				close(op.startedCh)
				op.startedCh = nil
			}
			op.releaseWg()
			s.finalizeHMangaOp(op)
			continue
		}
		live = append(live, op)
	}
	s.hmangaOpQueue = live
}

// finalizeHMangaOp removes a terminal op from the queue index maps.
// Caller must hold hmangaOpQueueMu.
func (s *Server) finalizeHMangaOp(op *hmangaOpItem) {
	delete(s.hmangaOpByID, op.id)
	if op.kind == hmangaOpDownload && op.dlID != "" {
		delete(s.hmangaOpByDlID, op.dlID)
	}
}
