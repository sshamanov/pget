package sidecar

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHashURL(t *testing.T) {
	h1 := HashURL("https://example.org/file.iso")
	h2 := HashURL("https://example.org/file.iso")
	h3 := HashURL("https://example.org/other.iso")

	if h1 != h2 {
		t.Errorf("same URL produced different hashes: %s != %s", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("different URLs produced same hash: %s", h1)
	}
}

func TestHashURLList(t *testing.T) {
	h1 := HashURLList([]string{"a", "b"})
	h2 := HashURLList([]string{"a", "b"})
	h3 := HashURLList([]string{"b", "a"}) // order matters

	if h1 != h2 {
		t.Errorf("same list produced different hashes")
	}
	if h1 == h3 {
		t.Errorf("different order produced same hash")
	}
}

func TestManager_CreateLoadRemove(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.iso")

	m := NewManager(destPath)
	if m.Exists() {
		t.Fatal("sidecar should not exist yet")
	}

	items := []Item{
		{
			SourceURLHash: HashURL("https://example.org/test.iso"),
			FinalURLHash:  HashURL("https://example.org/test.iso"),
			DisplayURL:    "https://example.org/test.iso",
			OutputOffset:  0,
			Length:        256,
			SplitSize:     256,
			CompletedBitmap: "",
			Complete:      false,
		},
	}

	urlListHash := HashURLList([]string{"https://example.org/test.iso"})
	if err := m.Create(destPath, urlListHash, items); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !m.Exists() {
		t.Fatal("sidecar should exist after create")
	}

	// Load and validate.
	state, err := m.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Version != Version {
		t.Errorf("version = %d, want %d", state.Version, Version)
	}
	if state.Destination != destPath {
		t.Errorf("destination = %s, want %s", state.Destination, destPath)
	}

	// Mark the single chunk complete — item should become complete.
	if err := m.MarkComplete(0, 0); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}

	// Reload and verify.
	state2, err := m.Load()
	if err != nil {
		t.Fatalf("Load after mark: %v", err)
	}
	if !state2.Items[0].Complete {
		t.Error("item should be marked complete after all chunks")
	}

	// Remove.
	if err := m.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if m.Exists() {
		t.Fatal("sidecar should not exist after remove")
	}
}

func TestBitmapRoundtrip(t *testing.T) {
	// A bitmap with every other chunk complete.
	bitmap := []bool{true, false, true, false, true, false, true}
	encoded := EncodeBitmap(bitmap)
	decoded, err := DecodeBitmap(encoded, 7)
	if err != nil {
		t.Fatalf("DecodeBitmap: %v", err)
	}
	for i, v := range bitmap {
		if decoded[i] != v {
			t.Errorf("bit %d: got %v, want %v", i, decoded[i], v)
		}
	}
}

func TestBitmapDecodeExpand(t *testing.T) {
	// Encoded bitmap is smaller than minSize — should expand.
	bitmap := []bool{true}
	encoded := EncodeBitmap(bitmap)
	decoded, err := DecodeBitmap(encoded, 10)
	if err != nil {
		t.Fatalf("DecodeBitmap: %v", err)
	}
	if len(decoded) != 10 {
		t.Errorf("decoded length = %d, want 10", len(decoded))
	}
	if !decoded[0] {
		t.Error("bit 0 should be true")
	}
	if decoded[1] {
		t.Error("bit 1 should be false (expanded)")
	}
}

func TestManager_MarkCompleteOutOfRange(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.iso")

	m := NewManager(destPath)
	items := []Item{{
		SourceURLHash: HashURL("x"),
		FinalURLHash:  HashURL("x"),
		DisplayURL:    "x",
		Length:        100,
		SplitSize:     50,
	}}
	if err := m.Create(destPath, HashURLList([]string{"x"}), items); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := m.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if err := m.MarkComplete(-1, 0); err == nil {
		t.Error("expected error for negative item index")
	}
	if err := m.MarkComplete(0, 1000); err == nil {
		t.Error("expected error for out-of-range chunk index")
	}
}

func TestLoadMissing(t *testing.T) {
	m := NewManager("/nonexistent/path")
	_, err := m.Load()
	if err == nil {
		t.Error("expected error for missing sidecar")
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "test.iso.pget")
	if err := os.WriteFile(sidecarPath, []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(filepath.Join(dir, "test.iso"))
	_, err := m.Load()
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
