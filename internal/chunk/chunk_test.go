package chunk

import (
	"testing"
)

func TestPlanner_Plan(t *testing.T) {
	tests := []struct {
		name      string
		splitSize int64
		objSize   int64
		want      int
	}{
		{"exact multiple", 100, 500, 5},
		{"remainder", 100, 550, 6},
		{"one chunk", 1000, 500, 1},
		{"zero size", 100, 0, 0},
		{"negative size", 100, -1, 0},
		{"single byte", 8 << 20, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPlanner(tt.splitSize)
			chunks := p.Plan(tt.objSize)
			if len(chunks) != tt.want {
				t.Errorf("Plan(%d) = %d chunks, want %d", tt.objSize, len(chunks), tt.want)
			}
		})
	}
}

func TestPlanner_ChunkBoundaries(t *testing.T) {
	p := NewPlanner(100)
	chunks := p.Plan(250)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	// Chunk 0: 0-99
	if chunks[0].Start != 0 || chunks[0].Length != 100 {
		t.Errorf("chunk 0: start=%d length=%d, want start=0 length=100", chunks[0].Start, chunks[0].Length)
	}
	// Chunk 1: 100-199
	if chunks[1].Start != 100 || chunks[1].Length != 100 {
		t.Errorf("chunk 1: start=%d length=%d, want start=100 length=100", chunks[1].Start, chunks[1].Length)
	}
	// Chunk 2: 200-249 (50 bytes)
	if chunks[2].Start != 200 || chunks[2].Length != 50 {
		t.Errorf("chunk 2: start=%d length=%d, want start=200 length=50", chunks[2].Start, chunks[2].Length)
	}
}

func TestPlanner_InitialState(t *testing.T) {
	p := NewPlanner(100)
	chunks := p.Plan(300)

	for i, c := range chunks {
		if c.State != StatePending {
			t.Errorf("chunk %d: state=%s, want pending", i, c.State)
		}
		if c.WorkerSlot != -1 {
			t.Errorf("chunk %d: workerSlot=%d, want -1", i, c.WorkerSlot)
		}
	}
}

func TestPlanner_ChunkCount(t *testing.T) {
	p := NewPlanner(100)
	if n := p.ChunkCount(250); n != 3 {
		t.Errorf("ChunkCount(250) = %d, want 3", n)
	}
	if n := p.ChunkCount(0); n != 0 {
		t.Errorf("ChunkCount(0) = %d, want 0", n)
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		s    State
		want string
	}{
		{StatePending, "pending"},
		{StateActive, "active"},
		{StateCompleted, "completed"},
		{StateWritten, "written"},
		{StateRetryWait, "retry-wait"},
		{StateFailed, "failed"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}
