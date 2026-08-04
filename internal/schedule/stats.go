package schedule

import "time"

// WorkerStats tracks per-worker connection state and throughput.
type WorkerStats struct {
	SlotID             int
	Active             bool
	UsefulBytes        int64
	SuccessfulChunks   int
	AdmissionFailures  int
	RecentThroughput   float64 // bytes/sec
	RecentChunkDurations []time.Duration
	SuppressedUntil    time.Time
	LastSuccess        time.Time
}

// IsSuppressed reports whether the worker slot is currently suppressed.
func (w *WorkerStats) IsSuppressed() bool {
	return time.Now().Before(w.SuppressedUntil)
}

// RecordSuccess records a successful chunk transfer.
func (w *WorkerStats) RecordSuccess(bytes int64, d time.Duration) {
	w.SuccessfulChunks++
	w.UsefulBytes += bytes
	w.AdmissionFailures = 0
	w.LastSuccess = time.Now()

	// Rolling window of recent durations.
	w.RecentChunkDurations = append(w.RecentChunkDurations, d)
	if len(w.RecentChunkDurations) > 16 {
		w.RecentChunkDurations = w.RecentChunkDurations[1:]
	}

	if d > 0 {
		w.RecentThroughput = float64(bytes) / d.Seconds()
	}
}

// RecordAdmissionFailure records a failed connection attempt.
func (w *WorkerStats) RecordAdmissionFailure() {
	w.AdmissionFailures++
}
