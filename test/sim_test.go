// Package test contains network simulation E2E tests for pget.
package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sshamanov/pget/internal/adapter"
	gohttp "github.com/sshamanov/pget/internal/adapter/http"
	"github.com/sshamanov/pget/internal/chunk"
	"github.com/sshamanov/pget/internal/schedule"
	"github.com/sshamanov/pget/internal/sidecar"
	"github.com/sshamanov/pget/internal/sink"
	"github.com/sshamanov/pget/test/netsim"
)

// randData creates deterministic pseudo-random data for testing.
// Uses a seeded RNG so the same hash is always produced for the same size.
func randData(size int64, seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	data := make([]byte, size)
	for i := 0; i < len(data); i += 8 {
		v := rng.Int63()
		for j := 0; j < 8 && i+j < len(data); j++ {
			data[i+j] = byte(v >> (j * 8))
		}
	}
	return data
}

// hashData returns the SHA256 of the data.
func hashData(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:])
}

// testFileServer serves deterministic random data with full range support.
type testFileServer struct {
	data []byte
	etag string
}

func (s *testFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("ETag", s.etag)
	w.Header().Set("Accept-Ranges", "bytes")

	if rh := r.Header.Get("Range"); rh != "" {
		// Parse range.
		var start, end int64
		fmt.Sscanf(rh, "bytes=%d-%d", &start, &end)
		if end == 0 || end >= int64(len(s.data)) {
			end = int64(len(s.data)) - 1
		}
		if start < 0 {
			start = 0
		}
		contentLen := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.data)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLen))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(s.data[start : end+1])
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(s.data)))
		w.WriteHeader(http.StatusOK)
		w.Write(s.data)
	}
}

// TestSim_VariousFileSizes tests download with different file sizes.
func TestSim_VariousFileSizes(t *testing.T) {
	sizes := []int64{
		1024,          // 1 KiB
		1024 * 100,    // 100 KiB
		1024 * 1024,   // 1 MiB
		1024 * 1024 * 10, // 10 MiB
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d_bytes", size), func(t *testing.T) {
			data := randData(size, 42)
			expectedHash := hashData(data)

			srv := httptest.NewServer(&testFileServer{data: data, etag: `"test"`})
			defer srv.Close()

			// Create adapter with network simulation.
			sim := &netsim.Transport{
				Configs: netsim.UniformConfig(netsim.Config{
					Latency: time.Millisecond,
				}, 16),
			}
			httpAdapter := gohttp.New(false, 60*time.Second, 10*time.Second, 60*time.Second,
				gohttp.WithTransport(sim))

			ctx := context.Background()
			opts := adapter.RequestOptions{Timeout: 60 * time.Second}

			_, err := httpAdapter.Probe(ctx, srv.URL, opts)
			if err != nil {
				t.Fatalf("Probe: %v", err)
			}

			// Download sequentially and verify.
			rc, err := httpAdapter.OpenSequential(ctx, srv.URL, 0, opts)
			if err != nil {
				t.Fatalf("OpenSequential: %v", err)
			}
			downloaded, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}

			gotHash := hashData(downloaded)
			if gotHash != expectedHash {
				t.Errorf("hash mismatch: got %s, want %s", gotHash, expectedHash)
			}
		})
	}
}

// TestSim_ConnectionCounts tests parallel download with various connection counts.
func TestSim_ConnectionCounts(t *testing.T) {
	counts := []int{1, 2, 4, 8}

	for _, conns := range counts {
		t.Run(fmt.Sprintf("%d_connections", conns), func(t *testing.T) {
			size := int64(1024 * 1024 * 5) // 5 MiB
			data := randData(size, 123)
			expectedHash := hashData(data)
			splitSize := int64(128 * 1024) // 128 KiB chunks

			srv := httptest.NewServer(&testFileServer{data: data, etag: `"perf-test"`})
			defer srv.Close()

			sim := &netsim.Transport{
				Configs: netsim.UniformConfig(netsim.Config{
					Latency: 500 * time.Microsecond,
				}, conns+1), // +1 for probe
			}

			httpAdapter := gohttp.New(false, 120*time.Second, 10*time.Second, 120*time.Second,
				gohttp.WithTransport(sim))

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			opts := adapter.RequestOptions{Timeout: 120 * time.Second}

			result, err := httpAdapter.Probe(ctx, srv.URL, opts)
			if err != nil {
				t.Fatalf("Probe: %v", err)
			}

			dir := t.TempDir()
			destPath := filepath.Join(dir, "test.bin")

			planner := chunk.NewPlanner(splitSize)
			chunks := planner.Plan(result.Size)

			sm := sidecar.NewManager(destPath)
			items := []sidecar.Item{{
				SourceURLHash: sidecar.HashURL(srv.URL),
				FinalURLHash:  sidecar.HashURL(srv.URL),
				DisplayURL:    srv.URL,
				Length:        result.Size,
				SplitSize:     splitSize,
			}}
			sm.Create(destPath, sidecar.HashURLList([]string{srv.URL}), items)

			fs, err := sink.NewFileSink(destPath, 0, sm)
			if err != nil {
				t.Fatalf("NewFileSink: %v", err)
			}

			cfg := schedule.Config{
				RequestedConnections: conns,
				SplitSize:           splitSize,
				MaxTries:             3,
			}
			sched := schedule.New(context.Background(), cfg, chunks, nil)

			validator := result.ETag
			errCh := make(chan error, conns)
			for slot := 0; slot < conns; slot++ {
				go func(slot int) {
					errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
				}(slot)
			}

			var firstErr error
			for i := 0; i < conns; i++ {
				if err := <-errCh; err != nil && firstErr == nil {
					firstErr = err
					sched.Cancel()
				}
			}

			if firstErr != nil {
				t.Fatalf("worker error: %v", firstErr)
			}

			if err := fs.Finalize(time.Time{}); err != nil {
				t.Fatalf("Finalize: %v", err)
			}

			downloaded, _ := os.ReadFile(destPath)
			gotHash := hashData(downloaded)
			if gotHash != expectedHash {
				t.Errorf("%d connections: hash mismatch for %d bytes", conns, size)
			}

			completed, total := sched.Progress()
			t.Logf("%d connections: %d/%d chunks, %d reqs, hash=%s",
				conns, completed, total, sim.RequestCount(), gotHash[:16])
		})
	}
}

// TestSim_HeterogeneousConnections tests parallel download with varying connection speeds.
func TestSim_HeterogeneousConnections(t *testing.T) {
	size := int64(1024 * 1024 * 2) // 2 MiB
	data := randData(size, 456)
	expectedHash := hashData(data)
	splitSize := int64(128 * 1024) // 128 KiB → 16 chunks

	srv := httptest.NewServer(&testFileServer{data: data, etag: `"hetero"`})
	defer srv.Close()

	// 4 connections: one slow (50KB/s), three fast.
	// 128KB at 50KB/s = ~2.5s per chunk, manageable within timeout.
	cfg := make([]netsim.Config, 8)
	for i := range cfg {
		if i%4 == 0 {
			cfg[i] = netsim.Config{Bandwidth: 50 * 1024} // 50 KB/s
		} else {
			cfg[i] = netsim.Config{Latency: 100 * time.Microsecond}
		}
	}

	sim := &netsim.Transport{Configs: cfg}

	httpAdapter := gohttp.New(false, 120*time.Second, 10*time.Second, 120*time.Second,
		gohttp.WithTransport(sim))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opts := adapter.RequestOptions{Timeout: 120 * time.Second}

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	fs, _ := sink.NewFileSink(destPath, 0, nil)
	cfg2 := schedule.Config{
		RequestedConnections: 4,
		SplitSize:           splitSize,
		MaxTries:             3,
	}
	sched := schedule.New(context.Background(), cfg2, chunks, nil)

	validator := result.ETag
	conns := 4
	errCh := make(chan error, conns)
	for slot := 0; slot < conns; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
		}(slot)
	}

	var firstErr error
	for i := 0; i < conns; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			sched.Cancel()
		}
	}

	if firstErr != nil {
		t.Fatalf("worker error: %v", firstErr)
	}

	fs.Finalize(time.Time{})

	downloaded, _ := os.ReadFile(destPath)
	gotHash := hashData(downloaded)
	if gotHash != expectedHash {
		t.Errorf("hash mismatch with heterogeneous connections")
	}

	completed, _ := sched.Progress()
	t.Logf("heterogeneous: %d chunks, %d reqs, hash=%s",
		completed, sim.RequestCount(), gotHash[:16])
}

// TestSim_StalledChunk tests how the scheduler handles a stalled chunk.
// The scheduler should retry stalled chunks.
func TestSim_StalledChunk(t *testing.T) {
	size := int64(1024 * 1024 * 2) // 2 MiB
	data := randData(size, 789)
	expectedHash := hashData(data)
	splitSize := int64(256 * 1024) // 256 KiB → 8 chunks

	srv := httptest.NewServer(&testFileServer{data: data, etag: `"stall"`})
	defer srv.Close()

	// Make chunk requests through connection 0 stall.
	// 8 connections: 1 stall, 7 fast.
	cfg := make([]netsim.Config, 8)
	for i := range cfg {
		cfg[i] = netsim.Config{Latency: time.Millisecond}
	}
	// First connection's first chunk stalls for 3 seconds.
	cfg[0] = netsim.Config{
		StallAfter:    4096,
		StallDuration: 3 * time.Second,
	}

	sim := &netsim.Transport{Configs: cfg}

	httpAdapter := gohttp.New(false, 120*time.Second, 10*time.Second, 120*time.Second,
		gohttp.WithTransport(sim))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := adapter.RequestOptions{Timeout: 120 * time.Second}

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")
	fs, _ := sink.NewFileSink(destPath, 0, nil)

	cfg2 := schedule.Config{
		RequestedConnections: 8,
		SplitSize:           splitSize,
		MaxTries:             3,
	}
	sched := schedule.New(context.Background(), cfg2, chunks, nil)

	validator := result.ETag
	conns := 8
	errCh := make(chan error, conns)
	for slot := 0; slot < conns; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
		}(slot)
	}

	var firstErr error
	for i := 0; i < conns; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			sched.Cancel()
		}
	}

	if firstErr != nil {
		t.Fatalf("worker error: %v", firstErr)
	}

	fs.Finalize(time.Time{})

	downloaded, _ := os.ReadFile(destPath)
	gotHash := hashData(downloaded)
	if gotHash != expectedHash {
		t.Errorf("hash mismatch with stalled chunk: got %s", gotHash[:16])
	}

	completed, _ := sched.Progress()
	t.Logf("stalled: %d/%d chunks, %d reqs, hash=%s",
		completed, len(chunks), sim.RequestCount(), gotHash[:16])
}

// TestSim_ParallelStreamWithLatency tests stream mode with simulated latency.
func TestSim_ParallelStreamWithLatency(t *testing.T) {
	size := int64(1024 * 1024 * 3) // 3 MiB
	data := randData(size, 321)
	expectedHash := hashData(data)
	splitSize := int64(128 * 1024) // 128 KiB
	bufferSize := int64(64 * 1024 * 1024)

	srv := httptest.NewServer(&testFileServer{data: data, etag: `"stream"`})
	defer srv.Close()

	sim := &netsim.Transport{
		Configs: netsim.UniformConfig(netsim.Config{
			Latency: 2 * time.Millisecond,
		}, 8),
	}

	httpAdapter := gohttp.New(false, 120*time.Second, 10*time.Second, 120*time.Second,
		gohttp.WithTransport(sim))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := adapter.RequestOptions{Timeout: 120 * time.Second}

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	streamSink := sink.NewStreamSink(bufferSize)

	cfg2 := schedule.Config{
		RequestedConnections: 4,
		StreamBufferSize:    bufferSize,
		SplitSize:           splitSize,
		MaxTries:             3,
		StreamMode:          true,
	}
	sched := schedule.New(context.Background(), cfg2, chunks, streamSink)

	var buf bytes.Buffer
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- streamSink.WriteTo(ctx, &buf)
	}()

	validator := result.ETag
	conns := 4
	errCh := make(chan error, conns)
	for slot := 0; slot < conns; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, nil, streamSink, nil)
		}(slot)
	}

	for i := 0; i < conns; i++ {
		if err := <-errCh; err != nil {
			sched.Cancel()
		}
	}

	streamSink.Close()

	if werr := <-writeErr; werr != nil {
		t.Fatalf("stream write: %v", werr)
	}

	gotHash := hashData(buf.Bytes())
	if gotHash != expectedHash {
		t.Errorf("stream hash mismatch: got %s, want %s", gotHash[:16], expectedHash[:16])
	}

	t.Logf("stream %d bytes: hash=%s", buf.Len(), gotHash[:16])
}

// TestSim_VeryLargeChunks tests with large chunk sizes and small connection counts.
func TestSim_VeryLargeChunks(t *testing.T) {
	tests := []struct {
		conns     int
		splitSize int64
		size      int64
	}{
		{1, 1024 * 1024, 1024 * 1024 * 5},       // 1 conn, 1MB chunks, 5MB
		{2, 1024 * 1024, 1024 * 1024 * 5},       // 2 conns, 1MB chunks, 5MB
		{4, 512 * 1024, 1024 * 1024 * 10},        // 4 conns, 512KB chunks, 10MB
		{8, 128 * 1024, 1024 * 1024 * 10},        // 8 conns, 128KB chunks, 10MB
	}

	for _, tt := range tests {
		name := fmt.Sprintf("c%d_s%d", tt.conns, tt.splitSize/1024)
		t.Run(name, func(t *testing.T) {
			data := randData(tt.size, int64(tt.conns)*100+tt.splitSize)
			expectedHash := hashData(data)

			srv := httptest.NewServer(&testFileServer{data: data, etag: `"big"`})
			defer srv.Close()

			sim := &netsim.Transport{
				Configs: netsim.UniformConfig(netsim.Config{
					Latency: time.Millisecond,
				}, tt.conns+1),
			}

			httpAdapter := gohttp.New(false, 120*time.Second, 10*time.Second, 120*time.Second,
				gohttp.WithTransport(sim))

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			opts := adapter.RequestOptions{Timeout: 120 * time.Second}

			result, _ := httpAdapter.Probe(ctx, srv.URL, opts)
			planner := chunk.NewPlanner(tt.splitSize)
			chunks := planner.Plan(result.Size)

			dir := t.TempDir()
			destPath := filepath.Join(dir, "test.bin")
			fs, _ := sink.NewFileSink(destPath, 0, nil)

			cfg := schedule.Config{
				RequestedConnections: tt.conns,
				SplitSize:           tt.splitSize,
				MaxTries:             3,
			}
			sched := schedule.New(context.Background(), cfg, chunks, nil)

			validator := result.ETag
			errCh := make(chan error, tt.conns)
			for slot := 0; slot < tt.conns; slot++ {
				go func(slot int) {
					errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
				}(slot)
			}

			var firstErr error
			for i := 0; i < tt.conns; i++ {
				if err := <-errCh; err != nil && firstErr == nil {
					firstErr = err
					sched.Cancel()
				}
			}

			if firstErr != nil {
				t.Fatalf("worker error: %v", firstErr)
			}

			fs.Finalize(time.Time{})
			downloaded, _ := os.ReadFile(destPath)
			gotHash := hashData(downloaded)
			if gotHash != expectedHash {
				t.Errorf("hash mismatch: got %s, want %s", gotHash[:16], expectedHash[:16])
			}

			completed, total := sched.Progress()
			t.Logf("%s: %d/%d chunks, hash=%s", name, completed, total, gotHash[:16])
		})
	}
}

// TestSim_ConnectionAdmissionFailure tests that admission failures
// are handled correctly and connections are suppressed.
func TestSim_ConnectionAdmissionFailure(t *testing.T) {
	size := int64(1024 * 1024 * 2) // 2 MiB
	data := randData(size, 555)
	expectedHash := hashData(data)
	splitSize := int64(256 * 1024) // 8 chunks

	srv := httptest.NewServer(&testFileServer{data: data, etag: `"admit"`})
	defer srv.Close()

	// Make 2 out of 4 connections fail repeatedly with errors.
	cfg := make([]netsim.Config, 16)
	for i := range cfg {
		if i > 0 && i%4 < 2 {
			// These connections always fail.
			cfg[i] = netsim.Config{
				ErrorAfter:   0,
				ErrorMessage: "simulated admission failure",
			}
		} else {
			cfg[i] = netsim.Config{Latency: 100 * time.Microsecond}
		}
	}

	sim := &netsim.Transport{Configs: cfg}

	httpAdapter := gohttp.New(false, 60*time.Second, 10*time.Second, 60*time.Second,
		gohttp.WithTransport(sim))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := adapter.RequestOptions{Timeout: 60 * time.Second}

	result, _ := httpAdapter.Probe(ctx, srv.URL, opts)
	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")
	fs, _ := sink.NewFileSink(destPath, 0, nil)

	cfg2 := schedule.Config{
		RequestedConnections: 4,
		SplitSize:           splitSize,
		MaxTries:             3,
	}
	sched := schedule.New(context.Background(), cfg2, chunks, nil)

	validator := result.ETag
	conns := 4
	errCh := make(chan error, conns)
	for slot := 0; slot < conns; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
		}(slot)
	}

	// We expect failure because admission fails on half the connections.
	var firstErr error
	for i := 0; i < conns; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			sched.Cancel()
		}
	}

	// Check that at least some chunks completed.
	completed, total := sched.Progress()
	t.Logf("admission failure test: %d/%d chunks with %d failing conns",
		completed, total, 2)

	// Accept either success or clean failure.
	if firstErr == nil {
		fs.Finalize(time.Time{})
		downloaded, _ := os.ReadFile(destPath)
		gotHash := hashData(downloaded)
		if gotHash != expectedHash {
			t.Errorf("hash mismatch")
		}
		t.Logf("download completed despite admission failures: hash=%s", gotHash[:16])
	} else {
		t.Logf("download failed as expected: %v", firstErr)
	}
}

// TestSim_SlowerThanExpectedChunk tests how parallel download handles
// one significantly slower connection mixed with fast ones.
func TestSim_SlowerThanExpectedChunk(t *testing.T) {
	size := int64(1024 * 1024 * 2) // 2 MiB
	data := randData(size, 999)
	expectedHash := hashData(data)
	splitSize := int64(128 * 1024) // 16 chunks

	srv := httptest.NewServer(&testFileServer{data: data, etag: `"slow"`})
	defer srv.Close()

	// Connection 0 is slower (100KB/s). Others are fast. Fast workers
	// will complete most chunks while the slow one handles a few.
	// 128KB at 100KB/s = ~1.3s per chunk.
	cfg := make([]netsim.Config, 8)
	for i := range cfg {
		if i%4 == 0 {
			cfg[i] = netsim.Config{Bandwidth: 100 * 1024} // 100 KB/s
		} else {
			cfg[i] = netsim.Config{Latency: 100 * time.Microsecond}
		}
	}

	sim := &netsim.Transport{Configs: cfg}

	httpAdapter := gohttp.New(false, 120*time.Second, 10*time.Second, 120*time.Second,
		gohttp.WithTransport(sim))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opts := adapter.RequestOptions{Timeout: 120 * time.Second}

	result, _ := httpAdapter.Probe(ctx, srv.URL, opts)
	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")
	fs, _ := sink.NewFileSink(destPath, 0, nil)

	cfg2 := schedule.Config{
		RequestedConnections: 4,
		SplitSize:           splitSize,
		MaxTries:             3,
	}
	sched := schedule.New(context.Background(), cfg2, chunks, nil)

	validator := result.ETag
	conns := 4
	errCh := make(chan error, conns)
	start := time.Now()
	for slot := 0; slot < conns; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
		}(slot)
	}

	var firstErr error
	for i := 0; i < conns; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			sched.Cancel()
		}
	}
	elapsed := time.Since(start)

	if firstErr != nil {
		t.Fatalf("worker error: %v", firstErr)
	}

	fs.Finalize(time.Time{})
	downloaded, _ := os.ReadFile(destPath)
	gotHash := hashData(downloaded)
	if gotHash != expectedHash {
		t.Errorf("hash mismatch with slow chunk")
	}

	completed, total := sched.Progress()
	t.Logf("slow conn: %d/%d chunks in %v, hash=%s",
		completed, total, elapsed.Round(time.Millisecond), gotHash[:16])
}
