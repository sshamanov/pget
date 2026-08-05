package sink

import (
	"fmt"
	"os"
	"time"

	"github.com/sshamanov/pget/internal/chunk"
	"github.com/sshamanov/pget/internal/sidecar"
)

// fileSink implements FileSink with positional I/O writes.
type fileSink struct {
	f        *os.File
	path     string
	sidecar  *sidecar.Manager
	baseOff  int64 // output base offset for concatenated mode
}

// NewFileSink creates a file sink that writes to the given path.
// If the file exists, it is opened for writing; otherwise it is created.
// baseOff is the byte offset within a concatenated output file.
func NewFileSink(path string, baseOff int64, sm *sidecar.Manager) (FileSink, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open destination %s: %w", path, err)
	}
	return &fileSink{
		f:       f,
		path:    path,
		sidecar: sm,
		baseOff: baseOff,
	}, nil
}

// WriteChunk writes chunk data at the correct file offset using positional I/O.
// It also updates the sidecar completion bitmap so resume works after interruption.
func (fs *fileSink) WriteChunk(c chunk.Chunk, data []byte) error {
	off := fs.baseOff + c.Start
	n, err := fs.f.WriteAt(data, off)
	if err != nil {
		return fmt.Errorf("write chunk %d at offset %d: %w", c.Index, off, err)
	}
	if int64(n) != c.Length {
		return fmt.Errorf("short write for chunk %d: wrote %d, expected %d", c.Index, n, c.Length)
	}
	// Update sidecar bitmap so this chunk is remembered across restarts.
	if fs.sidecar != nil {
		if err := fs.sidecar.MarkComplete(0, c.Index); err != nil {
			return fmt.Errorf("sidecar mark chunk %d: %w", c.Index, err)
		}
	}
	return nil
}

// Finalize completes the file: syncs data, sets length, applies mtime,
// and removes the sidecar.
func (fs *fileSink) Finalize(modTime time.Time) error {
	// Sync destination data before removing sidecar.
	if err := fs.f.Sync(); err != nil {
		return fmt.Errorf("sync destination: %w", err)
	}

	if !modTime.IsZero() {
		if err := os.Chtimes(fs.path, modTime, modTime); err != nil {
			// Non-fatal on some systems.
			_ = err
		}
	}

	if err := fs.f.Close(); err != nil {
		return fmt.Errorf("close destination: %w", err)
	}

	// Remove the sidecar to mark completion.
	if fs.sidecar != nil {
		if err := fs.sidecar.Remove(); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove sidecar: %w", err)
		}
	}

	return nil
}

func (fs *fileSink) Path() string {
	return fs.path
}
