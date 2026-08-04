package sink

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sshamanov/pget/internal/chunk"
)

func TestStreamSink_OrderedOutput(t *testing.T) {
	s := NewStreamSink(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.WriteTo(ctx, &buf); err != nil && err != context.Canceled {
			t.Errorf("WriteTo: %v", err)
		}
	}()

	// Submit chunks in reverse order.
	for i := 3; i >= 0; i-- {
		data := bytes.Repeat([]byte{byte('A' + i)}, 10)
		if err := s.Reserve(10); err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		c := chunk.Chunk{Index: i, Start: int64(i * 10), Length: 10}
		if err := s.Submit(c, data); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	// Give the writer time to drain.
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	expected := "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDD"
	if buf.String() != expected {
		t.Errorf("output = %q, want %q", buf.String(), expected)
	}
}

func TestStreamSink_MemoryLimit(t *testing.T) {
	s := NewStreamSink(20)
	// Reserve 15 bytes — should succeed.
	if err := s.Reserve(15); err != nil {
		t.Errorf("Reserve(15): %v", err)
	}
	// Reserve another 10 — should fail (15+10=25 > 20).
	if err := s.Reserve(10); err == nil {
		t.Error("expected error for exceeding buffer limit")
	}
}

func TestStreamSink_FrontierIndex(t *testing.T) {
	s := NewStreamSink(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	go s.WriteTo(ctx, &buf)

	// Submit chunk 1 first (out of order).
	if err := s.Reserve(10); err != nil {
		t.Fatal(err)
	}
	s.Submit(chunk.Chunk{Index: 1, Start: 10, Length: 10}, bytes.Repeat([]byte("B"), 10))
	time.Sleep(20 * time.Millisecond)

	if idx := s.FrontierIndex(); idx != 0 {
		t.Errorf("FrontierIndex = %d, want 0 (chunk 0 blocks output)", idx)
	}

	// Submit chunk 0.
	if err := s.Reserve(10); err != nil {
		t.Fatal(err)
	}
	s.Submit(chunk.Chunk{Index: 0, Start: 0, Length: 10}, bytes.Repeat([]byte("A"), 10))
	time.Sleep(20 * time.Millisecond)

	if idx := s.FrontierIndex(); idx != 2 {
		t.Errorf("FrontierIndex = %d, want 2 (chunks 0 and 1 both written)", idx)
	}

	cancel()
}

func TestStreamSink_WrongDataLength(t *testing.T) {
	s := NewStreamSink(1024)
	s.Reserve(5)
	err := s.Submit(chunk.Chunk{Index: 0, Length: 10}, []byte("short"))
	if err == nil {
		t.Error("expected error for mismatched data length")
	}
}

func TestStreamSink_ReservationTracking(t *testing.T) {
	s := NewStreamSink(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	go s.WriteTo(ctx, &buf)

	s.Reserve(10)
	s.Submit(chunk.Chunk{Index: 0, Start: 0, Length: 10}, bytes.Repeat([]byte("A"), 10))
	time.Sleep(20 * time.Millisecond)

	// After writing, reservation should be released.
	if reserved := s.ReservedBytes(); reserved != 0 {
		t.Errorf("ReservedBytes = %d, want 0 after write", reserved)
	}
}
