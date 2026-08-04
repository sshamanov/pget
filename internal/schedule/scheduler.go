// Package schedule implements the chunk scheduler and connection controller.
package schedule

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/sshamanov/pget/internal/chunk"
	"github.com/sshamanov/pget/internal/sink"
)

// Config holds scheduler configuration.
type Config struct {
	RequestedConnections int
	StreamBufferSize    int64
	SplitSize           int64
	MaxTries            int
	StreamMode          bool
}

// Scheduler manages chunk assignment, retries, and connection limits.
type Scheduler struct {
	cfg Config

	mu       sync.Mutex
	chunks   []chunk.Chunk           // all chunks in order
	workers  []*WorkerStats          // per-worker stats
	cond     *sync.Cond

	// State tracking
	nextUndispatched int
	activeCount      int
	completedCount   int
	effectiveTarget  int
	fatalErr         error

	// Stream mode state
	streamSink     sink.StreamSink
	hedgedChunkIdx int // -1 if no hedge active
	hedgeActive    bool

	// Adaptation state
	cooldownUntil       time.Time
	restoreBackoff      time.Duration
	recentChunkDurations []time.Duration
	pressureEvents      int

	// Context for cancellation.
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new scheduler. The parent context is used for cancellation;
// when it is cancelled, all blocking NextAssignment calls return.
func New(parentCtx context.Context, cfg Config, chunks []chunk.Chunk, streamSink sink.StreamSink) *Scheduler {
	ctx, cancel := context.WithCancel(parentCtx)

	workers := make([]*WorkerStats, cfg.RequestedConnections)
	for i := range workers {
		workers[i] = &WorkerStats{SlotID: i}
	}

	// Initial effective target.
	effective := cfg.RequestedConnections
	if cfg.StreamMode && streamSink != nil {
		memLimit := int(cfg.StreamBufferSize / cfg.SplitSize)
		if memLimit < effective {
			effective = memLimit
		}
		if effective < 1 {
			effective = 1
		}
	}
	if effective > len(chunks) {
		effective = len(chunks)
	}
	if effective < 1 {
		effective = 1
	}

	s := &Scheduler{
		cfg:             cfg,
		chunks:          chunks,
		workers:         workers,
		effectiveTarget: effective,
		streamSink:      streamSink,
		hedgedChunkIdx:  -1,
		restoreBackoff:  30 * time.Second,
		ctx:             ctx,
		cancel:          cancel,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Context returns the scheduler's context, cancelled on fatal error.
func (s *Scheduler) Context() context.Context {
	return s.ctx
}

// Cancel cancels the scheduler and wakes all waiting workers.
func (s *Scheduler) Cancel() {
	s.mu.Lock()
	s.cancel()
	s.mu.Unlock()
	s.cond.Broadcast()
}

// NextAssignment returns the next chunk assignment, or blocks until one is available.
// Returns (nil, error) when the job is complete or has failed.
func (s *Scheduler) NextAssignment(workerSlot int) (*chunk.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		s.cleanup()

		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		default:
		}

		if s.fatalErr != nil {
			return nil, s.fatalErr
		}

		// Check if job is complete.
		if s.completedCount >= len(s.chunks) {
			return nil, nil
		}

		// Try to assign a chunk.
		c := s.tryAssign(workerSlot)
		if c != nil {
			return c, nil
		}

		// Block until something changes.
		s.cond.Wait()
	}
}

// MarkComplete marks a chunk as successfully completed.
func (s *Scheduler) MarkComplete(idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx < 0 || idx >= len(s.chunks) {
		return
	}
	c := &s.chunks[idx]
	if c.State == chunk.StateActive {
		c.State = chunk.StateCompleted
		s.activeCount--
		s.completedCount++

		// Record duration for adaptation.
		if d := time.Since(c.StartTime); d > 0 {
			s.recordChunkDuration(d)
		}
	}
	s.cond.Broadcast()
}

// MarkFailed marks a chunk as failed, potentially retryable.
func (s *Scheduler) MarkFailed(idx int, retryable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx < 0 || idx >= len(s.chunks) {
		return
	}
	c := &s.chunks[idx]
	if c.State != chunk.StateActive {
		return
	}
	s.activeCount--

	if retryable && c.Attempts < s.cfg.MaxTries {
		c.State = chunk.StateRetryWait
	} else {
		c.State = chunk.StateFailed
		s.fatalErr = fmt.Errorf("chunk %d exhausted retries (%d attempts of max %d)", idx, c.Attempts, s.cfg.MaxTries)
		s.cancel()
	}
	s.cond.Broadcast()
}

// OnPressure handles an HTTP 429 or 503 response.
func (s *Scheduler) OnPressure(retryAfter time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newTarget := int(math.Max(1, math.Ceil(float64(s.effectiveTarget)/2)))
	if newTarget < s.effectiveTarget {
		s.effectiveTarget = newTarget
		s.pressureEvents++

		cooldown := retryAfter
		if cooldown <= 0 {
			cooldown = 30 * time.Second
		}
		s.cooldownUntil = time.Now().Add(cooldown)
	}
	s.cond.Broadcast()
}

// OnAdmissionFailure handles a worker that failed to establish a useful connection.
func (s *Scheduler) OnAdmissionFailure(workerSlot int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if workerSlot < 0 || workerSlot >= len(s.workers) {
		return
	}
	w := s.workers[workerSlot]
	w.RecordAdmissionFailure()

	if w.AdmissionFailures >= 2 {
		// Suppress this slot.
		w.SuppressedUntil = time.Now().Add(30 * time.Second)
		if s.effectiveTarget > 1 {
			s.effectiveTarget--
		}
	}
	s.cond.Broadcast()
}

// OnWorkerSuccess reports a successful transfer for adaptation tracking.
func (s *Scheduler) OnWorkerSuccess(workerSlot int, bytes int64, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if workerSlot >= 0 && workerSlot < len(s.workers) {
		s.workers[workerSlot].RecordSuccess(bytes, d)
	}
}

// EffectiveConcurrency returns the current effective connection target.
func (s *Scheduler) EffectiveConcurrency() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.effectiveTarget
}

// ActiveCount returns the number of currently active workers.
func (s *Scheduler) ActiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeCount
}

// Progress returns (completed, total) chunk counts.
func (s *Scheduler) Progress() (completed, total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completedCount, len(s.chunks)
}

// tryAssign attempts to assign a chunk to a worker. Returns nil if nothing available.
// Must be called with s.mu held.
func (s *Scheduler) tryAssign(workerSlot int) *chunk.Chunk {
	// Check if this worker is suppressed.
	if workerSlot >= 0 && workerSlot < len(s.workers) {
		if s.workers[workerSlot].IsSuppressed() {
			return nil
		}
	}

	// Can we use another worker?
	if s.activeCount >= s.effectiveTarget {
		return nil
	}

	// Priority 1: Retry a failed chunk whose backoff expired.
	if c := s.findRetryableChunk(); c != nil {
		return s.assignChunk(c, workerSlot)
	}

	// Priority 2: Assign next undispatched chunk.
	if s.nextUndispatched < len(s.chunks) {
		c := &s.chunks[s.nextUndispatched]
		s.nextUndispatched++
		return s.assignChunk(c, workerSlot)
	}

	return nil
}

func (s *Scheduler) findRetryableChunk() *chunk.Chunk {
	for i := range s.chunks {
		c := &s.chunks[i]
		if c.State == chunk.StateRetryWait {
			return c
		}
	}
	return nil
}

func (s *Scheduler) assignChunk(c *chunk.Chunk, workerSlot int) *chunk.Chunk {
	c.State = chunk.StateActive
	c.WorkerSlot = workerSlot
	c.StartTime = time.Now()
	c.Attempts++
	s.activeCount++
	return c
}

// cleanup checks for adaptation state changes.
// Must be called with s.mu held.
func (s *Scheduler) cleanup() {
	// Try to restore a connection slot.
	if s.effectiveTarget < s.cfg.RequestedConnections &&
		time.Now().After(s.cooldownUntil) &&
		s.completedCount > 0 {

		// Need at least 4 successful chunks since last reduction.
		if s.completedCount >= 4 {
			s.effectiveTarget++
			s.restoreBackoff *= 2
			if s.restoreBackoff > 5*time.Minute {
				s.restoreBackoff = 5 * time.Minute
			}
			s.cooldownUntil = time.Now().Add(s.restoreBackoff)
		}
	}
}

func (s *Scheduler) recordChunkDuration(d time.Duration) {
	s.recentChunkDurations = append(s.recentChunkDurations, d)
	if len(s.recentChunkDurations) > 32 {
		s.recentChunkDurations = s.recentChunkDurations[1:]
	}
}

// RollingMedianDuration returns the rolling median of recent successful chunk durations.
func (s *Scheduler) RollingMedianDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.recentChunkDurations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(s.recentChunkDurations))
	copy(sorted, s.recentChunkDurations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

// HedgeEligible checks whether a speculative duplicate should be started
// for the current frontier chunk. Returns the chunk index to hedge, or -1.
func (s *Scheduler) HedgeEligible() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only in stream mode.
	if !s.cfg.StreamMode || s.streamSink == nil {
		return -1
	}

	// Only one hedge at a time.
	if s.hedgeActive {
		return -1
	}

	frontier := s.streamSink.FrontierIndex()
	if frontier >= len(s.chunks) {
		return -1
	}

	fc := &s.chunks[frontier]

	// Condition 1: Original frontier request is active.
	if fc.State != chunk.StateActive {
		return -1
	}

	// Condition 3: Not already hedged.
	if fc.Hedged {
		return -1
	}

	// Condition 4: Need at least 4 recent durations.
	if len(s.recentChunkDurations) < 4 {
		return -1
	}

	median := s.rollingMedianLocked()

	// Condition 5: At least 2 later chunks completed since frontier started.
	laterDone := 0
	for i := frontier + 1; i < len(s.chunks); i++ {
		if s.chunks[i].State == chunk.StateCompleted || s.chunks[i].State == chunk.StateWritten {
			laterDone++
		}
	}
	if laterDone < 2 {
		return -1
	}

	// Conditions 6 & 7: Frontier is slow.
	elapsed := time.Since(fc.StartTime)
	if elapsed < 2*median {
		return -1
	}

	// Condition 9: Need a free worker slot.
	if s.activeCount >= s.effectiveTarget {
		return -1
	}

	// Condition 8: Buffer reservation available.
	if s.streamSink.ReservedBytes()+fc.Length > s.streamSink.BufferSize() {
		return -1
	}

	return frontier
}

// StartHedge marks a chunk as hedged and returns it for duplicate download.
func (s *Scheduler) StartHedge(chunkIdx int) (*chunk.Chunk, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if chunkIdx < 0 || chunkIdx >= len(s.chunks) {
		return nil, -1
	}

	c := &s.chunks[chunkIdx]
	c.Hedged = true
	s.hedgeActive = true
	s.hedgedChunkIdx = chunkIdx

	// Allocate a virtual worker slot for the hedge.
	// Use any slot that's within effective target but not active.
	hedgeSlot := -1
	for i := 0; i < s.effectiveTarget; i++ {
		inUse := false
		for j := range s.chunks {
			if s.chunks[j].State == chunk.StateActive && s.chunks[j].WorkerSlot == i {
				inUse = true
				break
			}
		}
		if !inUse && !s.workers[i].IsSuppressed() {
			hedgeSlot = i
			break
		}
	}

	if hedgeSlot < 0 {
		c.Hedged = false
		s.hedgeActive = false
		s.hedgedChunkIdx = -1
		return nil, -1
	}

	c.Attempts++
	s.activeCount++
	return c, hedgeSlot
}

// CancelHedge cancels the speculative duplicate (the slower copy won).
func (s *Scheduler) CancelHedge(chunkIdx int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hedgedChunkIdx == chunkIdx {
		s.hedgeActive = false
		s.hedgedChunkIdx = -1
	}
	s.activeCount--
	s.cond.Broadcast()
}

// rollingMedianLocked computes the median without locking.
func (s *Scheduler) rollingMedianLocked() time.Duration {
	if len(s.recentChunkDurations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(s.recentChunkDurations))
	copy(sorted, s.recentChunkDurations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

// RecentThroughput returns the rolling median throughput across workers.
func (s *Scheduler) RecentThroughput() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var rates []float64
	for _, w := range s.workers {
		if w.RecentThroughput > 0 {
			rates = append(rates, w.RecentThroughput)
		}
	}
	if len(rates) == 0 {
		return 0
	}
	sort.Float64s(rates)
	return rates[len(rates)/2]
}
