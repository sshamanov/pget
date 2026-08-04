package schedule

import (
	"testing"
	"time"

	"github.com/sshamanov/pget/internal/chunk"
)

func makeChunks(n int, splitSize int64) []chunk.Chunk {
	p := chunk.NewPlanner(splitSize)
	return p.Plan(int64(n) * splitSize)
}

func TestScheduler_AssignAllChunks(t *testing.T) {
	chunks := makeChunks(10, 100)
	cfg := Config{
		RequestedConnections: 4,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	assigned := make(map[int]bool)
	for i := 0; i < len(chunks); i++ {
		c, err := s.NextAssignment(i % 4)
		if err != nil {
			t.Fatalf("NextAssignment: %v", err)
		}
		if c == nil {
			t.Fatal("unexpected nil assignment")
		}
		if assigned[c.Index] {
			t.Errorf("chunk %d assigned twice", c.Index)
		}
		assigned[c.Index] = true
		s.MarkComplete(c.Index)
	}

	// Next call should return nil (all done).
	c, err := s.NextAssignment(0)
	if err != nil {
		t.Fatalf("NextAssignment after completion: %v", err)
	}
	if c != nil {
		t.Errorf("expected nil after all chunks complete, got chunk %d", c.Index)
	}
}

func TestScheduler_ContinuousAssignment(t *testing.T) {
	// With 2 workers, chunks should be assigned continuously.
	chunks := makeChunks(6, 100)
	cfg := Config{
		RequestedConnections: 2,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	// Assign first two chunks.
	c0, _ := s.NextAssignment(0)
	c1, _ := s.NextAssignment(1)
	if c0.Index != 0 || c1.Index != 1 {
		t.Errorf("first chunks: %d, %d; want 0, 1", c0.Index, c1.Index)
	}

	// Complete chunk 0 — worker 0 should get chunk 2.
	s.MarkComplete(0)
	c2, _ := s.NextAssignment(0)
	if c2.Index != 2 {
		t.Errorf("next chunk after complete: %d, want 2", c2.Index)
	}
}

func TestScheduler_RetryExhaustion(t *testing.T) {
	chunks := makeChunks(1, 100)
	cfg := Config{
		RequestedConnections: 1,
		SplitSize:            100,
		MaxTries:             2,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	// First attempt.
	c, err := s.NextAssignment(0)
	if err != nil {
		t.Fatalf("NextAssignment: %v", err)
	}
	s.MarkFailed(c.Index, true) // retryable (attempt 1 of 2)

	// Second attempt (retry).
	c, err = s.NextAssignment(0)
	if err != nil {
		t.Fatalf("NextAssignment (retry): %v", err)
	}
	if c == nil {
		t.Fatal("expected retry chunk")
	}
	s.MarkFailed(c.Index, true) // retries exhausted (attempt 2 of 2)

	// NextAssignment should return error now.
	_, err = s.NextAssignment(0)
	if err == nil {
		t.Error("expected error after retry exhaustion")
	}
}

func TestScheduler_PressureReduction(t *testing.T) {
	chunks := makeChunks(10, 100)
	cfg := Config{
		RequestedConnections: 8,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	initial := s.EffectiveConcurrency()
	if initial != 8 {
		t.Fatalf("initial concurrency = %d, want 8", initial)
	}

	s.OnPressure(0)

	after := s.EffectiveConcurrency()
	if after >= initial {
		t.Errorf("concurrency should have decreased: %d >= %d", after, initial)
	}
}

func TestScheduler_AdmissionFailure(t *testing.T) {
	chunks := makeChunks(10, 100)
	cfg := Config{
		RequestedConnections: 3,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	initial := s.EffectiveConcurrency()

	// Two failures on the same slot should suppress it.
	s.OnAdmissionFailure(0)
	s.OnAdmissionFailure(0)

	// The effective target should have dropped.
	if s.EffectiveConcurrency() >= initial {
		t.Errorf("concurrency should have decreased after 2 admission failures")
	}
}

func TestScheduler_Progress(t *testing.T) {
	chunks := makeChunks(5, 100)
	cfg := Config{
		RequestedConnections: 2,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	completed, total := s.Progress()
	if completed != 0 || total != 5 {
		t.Errorf("initial progress: %d/%d, want 0/5", completed, total)
	}

	c, _ := s.NextAssignment(0)
	s.MarkComplete(c.Index)
	completed, total = s.Progress()
	if completed != 1 || total != 5 {
		t.Errorf("progress: %d/%d, want 1/5", completed, total)
	}
}

func TestScheduler_NonRetryableFailure(t *testing.T) {
	chunks := makeChunks(1, 100)
	cfg := Config{
		RequestedConnections: 1,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	c, _ := s.NextAssignment(0)
	s.MarkFailed(c.Index, false) // non-retryable

	_, err := s.NextAssignment(0)
	if err == nil {
		t.Error("expected error after non-retryable failure")
	}
}

func TestWorkerStats_Suppression(t *testing.T) {
	w := &WorkerStats{}
	if w.IsSuppressed() {
		t.Error("new worker should not be suppressed")
	}

	w.SuppressedUntil = time.Now().Add(time.Hour)
	if !w.IsSuppressed() {
		t.Error("worker should be suppressed")
	}
}

func TestWorkerStats_RecordSuccess(t *testing.T) {
	w := &WorkerStats{AdmissionFailures: 3}
	w.RecordSuccess(1000, time.Second)

	if w.AdmissionFailures != 0 {
		t.Error("success should reset admission failures")
	}
	if w.SuccessfulChunks != 1 {
		t.Errorf("SuccessfulChunks = %d, want 1", w.SuccessfulChunks)
	}
}

func TestRollingMedianDuration(t *testing.T) {
	chunks := makeChunks(5, 100)
	cfg := Config{
		RequestedConnections: 1,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	// Before any completions, median should be 0.
	if med := s.RollingMedianDuration(); med != 0 {
		t.Errorf("empty median = %v, want 0", med)
	}

	// Complete some chunks with known durations.
	for i := 0; i < 5; i++ {
		c, _ := s.NextAssignment(0)
		time.Sleep(time.Millisecond)
		s.MarkComplete(c.Index)
	}

	med := s.RollingMedianDuration()
	if med <= 0 {
		t.Errorf("median should be positive after completions: %v", med)
	}
}

func TestScheduler_ActiveCount(t *testing.T) {
	chunks := makeChunks(5, 100)
	cfg := Config{
		RequestedConnections: 2,
		SplitSize:            100,
		MaxTries:             3,
	}
	s := New(cfg, chunks, nil)
	defer s.Cancel()

	if s.ActiveCount() != 0 {
		t.Error("initial active count should be 0")
	}

	s.NextAssignment(0)
	s.NextAssignment(1)
	if s.ActiveCount() != 2 {
		t.Errorf("active count = %d, want 2", s.ActiveCount())
	}
}
