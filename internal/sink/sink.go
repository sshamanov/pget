// Package sink defines the sink interfaces for ordered stream and positional file output.
package sink

import (
	"context"
	"io"
	"time"

	"github.com/sshamanov/pget/internal/chunk"
)

// StreamSink receives completed chunks (possibly out of order) and writes
// them to a sequential writer in exact source order.
type StreamSink interface {
	// Reserve reserves buffer memory for a chunk before downloading.
	// Returns an error if the reservation would exceed the buffer limit.
	Reserve(length int64) error

	// Submit delivers a completed chunk buffer. The sink takes ownership
	// of the buffer and releases it after writing.
	Submit(c chunk.Chunk, data []byte) error

	// WriteTo runs the ordered write loop, emitting bytes to w.
	// It blocks until Close is called and all chunks are written,
	// the context is cancelled, or a fatal error occurs.
	WriteTo(ctx context.Context, w io.Writer) error

	// Close signals that no more chunks will be submitted.
	// WriteTo will return after all submitted chunks are written.
	Close()

	// FrontierIndex returns the index of the chunk currently blocking output.
	FrontierIndex() int

	// ReservedBytes returns the current reserved payload memory.
	ReservedBytes() int64

	// BufferSize returns the configured hard limit.
	BufferSize() int64
}

// FileSink writes completed chunks to a seekable file using positional I/O.
type FileSink interface {
	// WriteChunk writes chunk data at the correct file offset.
	WriteChunk(c chunk.Chunk, data []byte) error

	// Finalize completes the file: syncs data, sets final length and mtime,
	// and signals the sidecar manager to remove the sidecar.
	Finalize(modTime time.Time) error

	// Path returns the destination file path.
	Path() string
}
