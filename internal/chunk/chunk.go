// Package chunk defines chunk types and the chunk planner.
package chunk

import "time"

// State represents the lifecycle state of a chunk.
type State int

const (
	StatePending State = iota
	StateActive
	StateCompleted
	StateWritten
	StateRetryWait
	StateFailed
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateActive:
		return "active"
	case StateCompleted:
		return "completed"
	case StateWritten:
		return "written"
	case StateRetryWait:
		return "retry-wait"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Chunk represents one byte range of a remote object.
type Chunk struct {
	Index       int
	Start       int64
	Length      int64
	State       State
	Attempts    int
	WorkerSlot   int // -1 if unassigned
	StartTime    time.Time
	RetryAfter   time.Time // when retry backoff expires (zero if immediate)
	BytesRecv    int64
	LastProgress time.Time
	Hedged       bool
}

// Planner produces chunk layouts for a given object size and split size.
type Planner struct {
	SplitSize int64
}

// NewPlanner creates a chunk planner with the given split size.
func NewPlanner(splitSize int64) *Planner {
	return &Planner{SplitSize: splitSize}
}

// Plan returns the list of chunks for an object of the given size.
// The final chunk may be smaller than SplitSize.
func (p *Planner) Plan(objectSize int64) []Chunk {
	if objectSize <= 0 {
		return nil
	}
	n := int((objectSize + p.SplitSize - 1) / p.SplitSize)
	chunks := make([]Chunk, n)
	for i := range chunks {
		start := int64(i) * p.SplitSize
		length := p.SplitSize
		if start+length > objectSize {
			length = objectSize - start
		}
		chunks[i] = Chunk{
			Index:  i,
			Start:  start,
			Length: length,
			State:  StatePending,
			WorkerSlot: -1,
		}
	}
	return chunks
}

// ChunkCount returns the number of chunks needed.
func (p *Planner) ChunkCount(objectSize int64) int {
	if objectSize <= 0 {
		return 0
	}
	return int((objectSize + p.SplitSize - 1) / p.SplitSize)
}
