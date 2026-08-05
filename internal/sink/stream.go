package sink

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/sshamanov/pget/internal/chunk"
)

// streamSink implements StreamSink with bounded in-memory reorder buffers.
type streamSink struct {
	mu             sync.Mutex
	bufferSize     int64
	reserved       int64
	nextIndex      int
	completed      map[int]*chunkData // index → completed chunk data
	active         map[int]int64      // index → reserved bytes
	writtenCount   int64
	fatalErr       error
	closed         bool
	totalExpected  int // set by Close to know when all data is out
	cond           *sync.Cond
}

type chunkData struct {
	chunk chunk.Chunk
	data  []byte
}

// NewStreamSink creates an ordered stream sink with the given buffer limit.
func NewStreamSink(bufferSize int64) StreamSink {
	s := &streamSink{
		bufferSize: bufferSize,
		completed:  make(map[int]*chunkData),
		active:     make(map[int]int64),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *streamSink) Reserve(length int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.reserved+length > s.bufferSize {
		return fmt.Errorf("buffer limit exceeded: reserved %d + %d > %d", s.reserved, length, s.bufferSize)
	}
	s.reserved += length
	return nil
}

func (s *streamSink) Submit(c chunk.Chunk, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fatalErr != nil {
		return s.fatalErr
	}

	if int64(len(data)) != c.Length {
		return fmt.Errorf("chunk %d: data length %d != expected %d", c.Index, len(data), c.Length)
	}

	s.completed[c.Index] = &chunkData{chunk: c, data: data}

	// Signal the writer that new data is available.
	s.cond.Signal()
	return nil
}

func (s *streamSink) WriteTo(ctx context.Context, w io.Writer) error {
	// Run in a goroutine so we can signal on new data.
	done := make(chan error, 1)

	go func() {
		done <- s.writeLoop(ctx, w)
	}()

	// Also wake up the loop when context is cancelled.
	go func() {
		<-ctx.Done()
		s.cond.Signal()
	}()

	return <-done
}

func (s *streamSink) writeLoop(ctx context.Context, w io.Writer) error {
	for {
		s.mu.Lock()

		// Wait for the next chunk, cancellation, or close signal.
		for s.completed[s.nextIndex] == nil && s.fatalErr == nil && !s.closed {
			select {
			case <-ctx.Done():
				s.mu.Unlock()
				return ctx.Err()
			default:
			}
			s.cond.Wait()
			select {
			case <-ctx.Done():
				s.mu.Unlock()
				return ctx.Err()
			default:
			}
		}

		if s.fatalErr != nil {
			err := s.fatalErr
			s.mu.Unlock()
			return err
		}

		// Write all contiguous completed chunks.
		for {
			cd := s.completed[s.nextIndex]
			if cd == nil {
				break
			}

			delete(s.completed, s.nextIndex)
			// Release the reservation.
			s.reserved -= cd.chunk.Length
			delete(s.active, s.nextIndex)
			s.nextIndex++
			s.mu.Unlock()

			// Perform the write outside the lock.
			if _, err := w.Write(cd.data); err != nil {
				s.mu.Lock()
				s.fatalErr = fmt.Errorf("write chunk %d: %w", cd.chunk.Index, err)
				s.cond.Signal()
				s.mu.Unlock()
				return s.fatalErr
			}
			s.mu.Lock()
			s.writtenCount++
		}

		// If closed, verify all chunks have been written.
			// Checking only s.completed[s.nextIndex] is insufficient when a gap
			// exists — later chunks would be silently dropped.
			if s.closed {
				for idx := range s.completed {
					s.fatalErr = fmt.Errorf("stream sink: closed with unflushed chunk %d (next expected %d)", idx, s.nextIndex)
					s.mu.Unlock()
					return s.fatalErr
				}
				s.mu.Unlock()
				return nil
			}

		s.mu.Unlock()
	}
}

func (s *streamSink) FrontierIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextIndex
}

func (s *streamSink) ReservedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserved
}

func (s *streamSink) BufferSize() int64 {
	return s.bufferSize
}

// Close signals that no more chunks will be submitted.
// WriteTo will return after all submitted chunks are written.
func (s *streamSink) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.Signal()
}
