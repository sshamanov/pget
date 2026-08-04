package sink

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sshamanov/pget/internal/chunk"
	"github.com/sshamanov/pget/internal/sidecar"
)

func TestFileSink_WriteChunk(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	fs, err := NewFileSink(destPath, 0, nil)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	c := chunk.Chunk{Index: 0, Start: 0, Length: 5}
	if err := fs.WriteChunk(c, []byte("hello")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Read back and verify.
	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("file content = %q, want %q", data, "hello")
	}
}

func TestFileSink_PositionalWrite(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	fs, err := NewFileSink(destPath, 0, nil)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	// Write chunks out of order.
	fs.WriteChunk(chunk.Chunk{Index: 1, Start: 10, Length: 5}, []byte("world"))
	fs.WriteChunk(chunk.Chunk{Index: 0, Start: 0, Length: 5}, []byte("hello"))

	data, _ := os.ReadFile(destPath)
	expected := "hello\x00\x00\x00\x00\x00world"
	if string(data) != expected {
		t.Errorf("file content = %q, want %q", data, expected)
	}
}

func TestFileSink_WithSidecar(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	sm := sidecar.NewManager(destPath)
	items := []sidecar.Item{{
		SourceURLHash: sidecar.HashURL("https://example.org/test.bin"),
		FinalURLHash:  sidecar.HashURL("https://example.org/test.bin"),
		DisplayURL:    "https://example.org/test.bin",
		Length:        10,
		SplitSize:     5,
	}}
	if err := sm.Create(destPath, sidecar.HashURLList([]string{"https://example.org/test.bin"}), items); err != nil {
		t.Fatalf("Create sidecar: %v", err)
	}

	fs, err := NewFileSink(destPath, 0, sm)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	fs.WriteChunk(chunk.Chunk{Index: 0, Start: 0, Length: 5}, []byte("hello"))
	fs.WriteChunk(chunk.Chunk{Index: 1, Start: 5, Length: 5}, []byte("world"))

	if err := fs.Finalize(time.Now()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Sidecar should be removed after successful finalization.
	if sm.Exists() {
		t.Error("sidecar should be removed after finalization")
	}

	// File should contain all bytes.
	data, _ := os.ReadFile(destPath)
	if string(data) != "helloworld" {
		t.Errorf("file = %q, want %q", data, "helloworld")
	}
}

func TestFileSink_FinalizeWithoutSidecar(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	fs, err := NewFileSink(destPath, 0, nil)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	fs.WriteChunk(chunk.Chunk{Index: 0, Start: 0, Length: 4}, []byte("data"))
	if err := fs.Finalize(time.Time{}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
}

func TestFileSink_Path(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	fs, _ := NewFileSink(destPath, 0, nil)
	if fs.Path() != destPath {
		t.Errorf("Path = %s, want %s", fs.Path(), destPath)
	}
}

func TestFileSink_BaseOffset(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	// Create file with some pre-existing content.
	if err := os.WriteFile(destPath, []byte("prefix----"), 0644); err != nil {
		t.Fatal(err)
	}

	// Base offset of 6 means chunks write after "prefix".
	fs, err := NewFileSink(destPath, 6, nil)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	fs.WriteChunk(chunk.Chunk{Index: 0, Start: 0, Length: 4}, []byte("sufx"))

	data, _ := os.ReadFile(destPath)
	if string(data) != "prefixsufx" {
		t.Errorf("file = %q, want %q", data, "prefixsufx")
	}
}
