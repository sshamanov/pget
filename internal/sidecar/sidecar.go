// Package sidecar manages the .pget sidecar file for resumable downloads.
package sidecar

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Version is the current sidecar format version.
const Version = 1

// Item describes one URL entry in a concatenated or single-file download.
type Item struct {
	SourceURLHash   string `json:"source_url_hash"`
	FinalURLHash    string `json:"final_url_hash"`
	DisplayURL      string `json:"display_url"`
	OutputOffset    int64  `json:"output_offset"`
	Length          int64  `json:"length"`
	SplitSize       int64  `json:"split_size"`
	ETag            string `json:"etag,omitempty"`
	LastModified    string `json:"last_modified,omitempty"`
	RemoteMtimeUnix int64  `json:"remote_mtime_unix,omitempty"`
	CompletedBitmap string `json:"completed_bitmap"` // base64-encoded bitset
	Complete        bool   `json:"complete"`
}

// State is the persisted sidecar state.
type State struct {
	Version     int       `json:"version"`
	Destination string    `json:"destination"`
	URLListHash string    `json:"url_list_hash"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Items       []Item    `json:"items"`
}

// Manager creates, validates, updates, and removes .pget sidecar files.
type Manager struct {
	mu    sync.Mutex
	path  string
	state *State
}

// NewManager creates a sidecar manager for the given destination file.
func NewManager(destPath string) *Manager {
	return &Manager{
		path: destPath + ".pget",
	}
}

// Path returns the sidecar file path.
func (m *Manager) Path() string { return m.path }

// Exists reports whether the sidecar file exists.
func (m *Manager) Exists() bool {
	_, err := os.Stat(m.path)
	return err == nil
}

// Create writes a new sidecar for the given items and loads it into memory.
func (m *Manager) Create(destPath string, urlListHash string, items []Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := &State{
		Version:     Version,
		Destination: destPath,
		URLListHash: urlListHash,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
		Items:       items,
	}
	if err := m.writeLocked(state); err != nil {
		return err
	}
	m.state = state
	return nil
}

// Load reads and validates an existing sidecar.
func (m *Manager) Load() (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if err != nil {
		return nil, fmt.Errorf("read sidecar: %w", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse sidecar: %w", err)
	}
	if state.Version != Version {
		return nil, fmt.Errorf("unsupported sidecar version: %d", state.Version)
	}
	m.state = &state
	return &state, nil
}

// MarkComplete updates the completion bitmap for a chunk.
// The caller must ensure destination data is durable before calling this.
// If no state is loaded (e.g., in tests without sidecar setup), the call is a no-op.
func (m *Manager) MarkComplete(itemIndex int, chunkIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == nil {
		return nil
	}
	if itemIndex < 0 || itemIndex >= len(m.state.Items) {
		return fmt.Errorf("item index out of range: %d", itemIndex)
	}
	item := &m.state.Items[itemIndex]

	chunkCount := int((item.Length + item.SplitSize - 1) / item.SplitSize)
	bitmap, err := DecodeBitmap(item.CompletedBitmap, chunkCount)
	if err != nil {
		return fmt.Errorf("decode bitmap: %w", err)
	}
	if chunkIndex >= len(bitmap) {
		return fmt.Errorf("chunk index out of range: %d", chunkIndex)
	}
	bitmap[chunkIndex] = true
	item.CompletedBitmap = EncodeBitmap(bitmap)

	// Check if all chunks are complete.
	allDone := true
	for i := 0; i < len(bitmap) && allDone; i++ {
		allDone = bitmap[i]
	}
	item.Complete = allDone

	return m.checkpointLocked()
}

// Remove deletes the sidecar after successful completion.
func (m *Manager) Remove() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return os.Remove(m.path)
}

// HashURL returns a sha256 hash of the URL for identity comparison.
func HashURL(s string) string {
	h := sha256.Sum256([]byte(s))
	return "sha256:" + base64.RawURLEncoding.EncodeToString(h[:])
}

// HashURLList returns a sha256 hash of the concatenated URL list.
func HashURLList(urls []string) string {
	h := sha256.New()
	for _, u := range urls {
		h.Write([]byte(u))
		h.Write([]byte{0})
	}
	return "sha256:" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// checkpointLocked atomically writes the current state. Must be called with m.mu held.
func (m *Manager) checkpointLocked() error {
	if m.state == nil {
		return fmt.Errorf("no loaded state")
	}
	m.state.UpdatedAt = time.Now().UTC()
	return m.writeLocked(m.state)
}

// writeLocked serializes and writes state to the sidecar file atomically.
// Must be called with m.mu held.
func (m *Manager) writeLocked(state *State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sidecar: %w", err)
	}

	// Atomic write via temp file + rename.
	tmpPath := m.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write sidecar tmp: %w", err)
	}
	if err := os.Rename(tmpPath, m.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename sidecar: %w", err)
	}
	return nil
}

// DecodeBitmap decodes a base64-encoded completion bitmap.
func DecodeBitmap(encoded string, minSize int) ([]bool, error) {
	if encoded == "" {
		return make([]bool, minSize), nil
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("base64 decode bitmap: %w", err)
	}
	bitmap := make([]bool, len(data)*8)
	for i := range data {
		for bit := 0; bit < 8; bit++ {
			if data[i]&(1<<bit) != 0 {
				bitmap[i*8+bit] = true
			}
		}
	}
	if len(bitmap) < minSize {
		// Expand bitmap if the file grew (shouldn't normally happen).
		expanded := make([]bool, minSize)
		copy(expanded, bitmap)
		bitmap = expanded
	}
	return bitmap, nil
}

// EncodeBitmap encodes a completion bitmap to a base64 string.
func EncodeBitmap(bitmap []bool) string {
	nBytes := (len(bitmap) + 7) / 8
	data := make([]byte, nBytes)
	for i, v := range bitmap {
		if v {
			data[i/8] |= 1 << (i % 8)
		}
	}
	return base64.RawURLEncoding.EncodeToString(data)
}
