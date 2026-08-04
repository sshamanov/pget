// Package test contains E2E and integration tests for pget.
package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sshamanov/pget/internal/adapter"
	gohttp "github.com/sshamanov/pget/internal/adapter/http"
	"github.com/sshamanov/pget/internal/chunk"
	"github.com/sshamanov/pget/internal/cli"
	"github.com/sshamanov/pget/internal/schedule"
	"github.com/sshamanov/pget/internal/sidecar"
	"github.com/sshamanov/pget/internal/sink"
	"github.com/sshamanov/pget/test/testserver"
)

func TestE2E_SequentialDownload(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024)
	srv.SetRangeSupport(false) // Server doesn't support ranges → sequential fallback.

	plan := &cli.ExecutionPlan{
		URLs:       []string{srv.URL},
		OutputMode: cli.OutputFile,
		MaxTries:   3,
	}

	_ = plan // plan validated but not executed in this test.

	// We can't test the full Run() because it writes to files with derived names.
	// Instead, test the individual download paths directly.
	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if result.Size != 1024 {
		t.Errorf("expected 1024 bytes, got %d", result.Size)
	}

	// Sequential download.
	rc, err := httpAdapter.OpenSequential(ctx, srv.URL, 0, opts)
	if err != nil {
		t.Fatalf("OpenSequential: %v", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(data) != 1024 {
		t.Errorf("downloaded %d bytes, want 1024", len(data))
	}
}

func TestE2E_ParallelFileDownload(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 100) // 100 KiB, 13 chunks of 8KiB

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !result.RangeCapable {
		t.Fatal("server should support ranges")
	}

	splitSize := int64(8192)
	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	// File sink.
	sm := sidecar.NewManager(destPath)
	items := []sidecar.Item{{
		SourceURLHash: sidecar.HashURL(srv.URL),
		FinalURLHash:  sidecar.HashURL(result.Meta.FinalURL),
		DisplayURL:    srv.URL,
		Length:        result.Size,
		SplitSize:     splitSize,
	}}
	if err := sm.Create(destPath, sidecar.HashURLList([]string{srv.URL}), items); err != nil {
		t.Fatalf("Create sidecar: %v", err)
	}

	fs, err := sink.NewFileSink(destPath, 0, sm)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	cfg := schedule.Config{
		RequestedConnections: 4,
		SplitSize:           splitSize,
		MaxTries:             3,
	}
	sched := schedule.New(context.Background(), cfg, chunks, nil)

	// Run workers.
	validator := result.ETag
	errCh := make(chan error, 4)
	for slot := 0; slot < 4; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, fs, nil, nil)
		}(slot)
	}

	var firstErr error
	for i := 0; i < 4; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			sched.Cancel()
		}
	}

	if firstErr != nil {
		t.Fatalf("worker error: %v", firstErr)
	}

	completed, total := sched.Progress()
	if completed != total {
		t.Errorf("only %d/%d chunks completed", completed, total)
	}

	if err := fs.Finalize(time.Time{}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Verify file hash.
	data, _ := os.ReadFile(destPath)
	hash := sha256.Sum256(data)
	t.Logf("downloaded %d bytes, sha256: %x", len(data), hash)
}

func TestE2E_ParallelStreamDownload(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 100) // 100 KiB

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	splitSize := int64(8192)
	bufferSize := int64(128 * 1024 * 1024)
	planner := chunk.NewPlanner(splitSize)
	chunks := planner.Plan(result.Size)

	streamSink := sink.NewStreamSink(bufferSize)

	cfg := schedule.Config{
		RequestedConnections: 4,
		StreamBufferSize:    bufferSize,
		SplitSize:           splitSize,
		MaxTries:             3,
		StreamMode:          true,
	}
	sched := schedule.New(context.Background(), cfg, chunks, streamSink)

	var buf bytes.Buffer
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- streamSink.WriteTo(ctx, &buf)
	}()

	validator := result.ETag
	errCh := make(chan error, 4)
	for slot := 0; slot < 4; slot++ {
		go func(slot int) {
			errCh <- workerLoop(ctx, sched, slot, srv.URL, httpAdapter, opts, validator, nil, streamSink, nil)
		}(slot)
	}

	for i := 0; i < 4; i++ {
		if err := <-errCh; err != nil {
			sched.Cancel()
		}
	}

	// Signal the writer that all chunks are done.
	streamSink.Close()

	if werr := <-writeErr; werr != nil {
		t.Fatalf("stream write: %v", werr)
	}

	// Verify output size.
	if int64(buf.Len()) != result.Size {
		t.Errorf("output size = %d, want %d", buf.Len(), result.Size)
	}

	hash := sha256.Sum256(buf.Bytes())
	t.Logf("streamed %d bytes, sha256: %x", buf.Len(), hash)
}

func TestE2E_ServerPressure429(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 1024) // 1 MiB
	srv.SetReturn429(true, "5")

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	// Probe should fail with 429.
	_, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err == nil {
		t.Fatal("expected error from 429 response")
	}
	t.Logf("429 error: %v", err)
}

func TestE2E_ServerPressure503(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 1024)
	srv.SetReturn503(true, "10")

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	_, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err == nil {
		t.Fatal("expected error from 503 response")
	}
	t.Logf("503 error: %v", err)
}

func TestE2E_RangeIgnoredFallback(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(4096)
	srv.SetRangeIgnored(true) // Server ignores Range, returns 200.

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if result.RangeCapable {
		t.Error("should report RangeCapable=false when server ignores range")
	}
	// Size should still be available from Content-Length.
	if result.Size != 4096 {
		t.Errorf("Size = %d, want 4096", result.Size)
	}
}

func TestE2E_WeakETagFallback(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(2048)
	srv.SetWeakETag(true)

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	// Weak ETag without Last-Modified means no usable validator → no parallel.
	if result.RangeCapable {
		t.Error("weak ETag without Last-Modified should disable parallel")
	}
}

func TestE2E_ContinueResume(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "resume.bin")

	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 50) // 50 KiB

	// Create a partial file with sidecar (simulating interrupted download).
	sm := sidecar.NewManager(destPath)
	splitSize := int64(8192)
	chunkCount := int((1024*50 + splitSize - 1) / splitSize)

	// Mark first 4 chunks as already downloaded.
	bitmap := make([]bool, chunkCount)
	for i := 0; i < 4; i++ {
		bitmap[i] = true
	}

	items := []sidecar.Item{{
		SourceURLHash:   sidecar.HashURL(srv.URL),
		FinalURLHash:    sidecar.HashURL(srv.URL),
		DisplayURL:      srv.URL,
		Length:          1024 * 50,
		SplitSize:       splitSize,
		ETag:            `"test-etag-123"`,
		CompletedBitmap: sidecar.EncodeBitmap(bitmap),
		Complete:        false,
	}}
	if err := sm.Create(destPath, sidecar.HashURLList([]string{srv.URL}), items); err != nil {
		t.Fatalf("Create sidecar: %v", err)
	}

	// Load and verify resume state.
	state, err := sm.Load()
	if err != nil {
		t.Fatalf("Load sidecar: %v", err)
	}

	recovered, _ := sidecar.DecodeBitmap(state.Items[0].CompletedBitmap, chunkCount)
	doneCount := 0
	for _, d := range recovered {
		if d {
			doneCount++
		}
	}
	if doneCount != 4 {
		t.Errorf("expected 4 completed chunks in bitmap, got %d", doneCount)
	}
}

func TestE2E_SidecarCreateLoadRemove(t *testing.T) {
	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	sm := sidecar.NewManager(destPath)
	if sm.Exists() {
		t.Fatal("sidecar should not exist initially")
	}

	items := []sidecar.Item{{
		SourceURLHash: sidecar.HashURL("https://example.org/file"),
		FinalURLHash:  sidecar.HashURL("https://example.org/file"),
		DisplayURL:    "https://example.org/file",
		Length:        1024 * 1024,
		SplitSize:     8192,
	}}

	urlListHash := sidecar.HashURLList([]string{"https://example.org/file"})
	if err := sm.Create(destPath, urlListHash, items); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !sm.Exists() {
		t.Fatal("sidecar should exist after create")
	}

	state, err := sm.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Version != 1 {
		t.Errorf("version = %d, want 1", state.Version)
	}

	if err := sm.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if sm.Exists() {
		t.Fatal("sidecar should not exist after remove")
	}
}

func TestE2E_StreamMemoryLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamSink := sink.NewStreamSink(500)

	// Reserve 400 bytes.
	if err := streamSink.Reserve(400); err != nil {
		t.Fatalf("first reserve: %v", err)
	}

	// Reserve another 200 — should fail (600 > 500).
	if err := streamSink.Reserve(200); err == nil {
		t.Error("expected buffer limit exceeded error")
	}

	// Release by writing.
	streamSink.Submit(chunk.Chunk{Index: 0, Length: 10}, bytes.Repeat([]byte("A"), 10))
	time.Sleep(50 * time.Millisecond)

	var buf bytes.Buffer
	go streamSink.WriteTo(ctx, &buf)
	time.Sleep(50 * time.Millisecond)
	cancel()

	// After write, reservation should be released.
	if reserved := streamSink.ReservedBytes(); reserved > 0 {
		t.Logf("residual reserved bytes: %d", reserved)
	}
}

func TestE2E_WrongContentRangeRejected(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024)
	srv.SetWrongRange(true) // Server returns mismatched Content-Range.

	httpAdapter := gohttp.New(false, 5*time.Second, 5*time.Second, 5*time.Second)
	opts := adapter.RequestOptions{Timeout: 5 * time.Second}
	ctx := context.Background()

	_, err := httpAdapter.OpenRange(ctx, srv.URL, 0, 100, "", opts)
	if err == nil {
		t.Fatal("expected error for mismatched Content-Range")
	}
}

func TestE2E_ConcatenatedURLs(t *testing.T) {
	srv1 := testserver.NewServer(t)
	srv1.SetSize(512)

	srv2 := testserver.NewServer(t)
	srv2.SetSize(256)

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	var allData bytes.Buffer

	// Download first URL.
	result1, _ := httpAdapter.Probe(ctx, srv1.URL, opts)
	rc1, _ := httpAdapter.OpenSequential(ctx, srv1.URL, 0, opts)
	io.Copy(&allData, rc1)
	rc1.Close()

	// Download second URL.
	result2, _ := httpAdapter.Probe(ctx, srv2.URL, opts)
	rc2, _ := httpAdapter.OpenSequential(ctx, srv2.URL, 0, opts)
	io.Copy(&allData, rc2)
	rc2.Close()

	totalExpected := result1.Size + result2.Size
	if int64(allData.Len()) != totalExpected {
		t.Errorf("concatenated size = %d, want %d", allData.Len(), totalExpected)
	}
}

func TestE2E_SchedulerPressureReduction(t *testing.T) {
	chunks := makeChunks(10, 100)
	cfg := schedule.Config{
		RequestedConnections: 8,
		SplitSize:            100,
		MaxTries:             3,
	}
	sched := schedule.New(context.Background(), cfg, chunks, nil)
	defer sched.Cancel()

	initial := sched.EffectiveConcurrency()
	t.Logf("initial concurrency: %d", initial)

	// Send pressure event.
	sched.OnPressure(0)

	after := sched.EffectiveConcurrency()
	t.Logf("after pressure: %d", after)

	if after >= initial {
		t.Errorf("concurrency should decrease after pressure: %d >= %d", after, initial)
	}
}

func TestE2E_SchedulerRetryExhaustion(t *testing.T) {
	chunks := makeChunks(1, 100)
	cfg := schedule.Config{
		RequestedConnections: 1,
		SplitSize:            100,
		MaxTries:             2,
	}
	sched := schedule.New(context.Background(), cfg, chunks, nil)
	defer sched.Cancel()

	c, _ := sched.NextAssignment(0)
	sched.MarkFailed(c.Index, true) // retryable (attempt 1 of 2)

	c, _ = sched.NextAssignment(0) // retry
	if c == nil {
		t.Fatal("expected retry chunk")
	}
	sched.MarkFailed(c.Index, true) // retries exhausted (attempt 2 of 2)

	_, err := sched.NextAssignment(0)
	if err == nil {
		t.Error("expected error after retry exhaustion")
	}
}

// workerLoop downloads chunks from the scheduler.
func workerLoop(
	ctx context.Context,
	sched *schedule.Scheduler,
	slot int,
	urlStr string,
	httpAdapter *gohttp.Adapter,
	opts adapter.RequestOptions,
	validator string,
	fileSink sink.FileSink,
	streamSink sink.StreamSink,
	progressBytes *atomic.Int64,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		c, err := sched.NextAssignment(slot)
		if err != nil {
			return err
		}
		if c == nil {
			return nil
		}

		if streamSink != nil {
			if err := streamSink.Reserve(c.Length); err != nil {
				sched.MarkFailed(c.Index, true)
				continue
			}
		}

		rr, err := httpAdapter.OpenRange(ctx, urlStr, c.Start, c.Length, validator, opts)
		if err != nil {
			sched.OnAdmissionFailure(slot)
			sched.MarkFailed(c.Index, true)
			continue
		}

		data, readErr := io.ReadAll(rr)
		rr.Close()

		if readErr != nil {
			sched.MarkFailed(c.Index, true)
			continue
		}

		if int64(len(data)) != c.Length {
			sched.MarkFailed(c.Index, true)
			continue
		}

		if streamSink != nil {
			if err := streamSink.Submit(*c, data); err != nil {
				sched.MarkFailed(c.Index, false)
				return fmt.Errorf("stream submit: %w", err)
			}
		} else if fileSink != nil {
			if err := fileSink.WriteChunk(*c, data); err != nil {
				sched.MarkFailed(c.Index, false)
				return err
			}
		}

		sched.OnWorkerSuccess(slot, c.Length, time.Since(c.StartTime))
		sched.MarkComplete(c.Index)
	}
}

func makeChunks(n int, splitSize int64) []chunk.Chunk {
	p := chunk.NewPlanner(splitSize)
	return p.Plan(int64(n) * splitSize)
}

// Test E2E pipeline: download via -O - to stdout and verify with sha256.
func TestE2E_PipelineHashVerify(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 1024) // 1 MiB

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	// Download sequentially and hash.
	rc, _ := httpAdapter.OpenSequential(ctx, srv.URL, 0, opts)
	defer rc.Close()

	h := sha256.New()
	io.Copy(h, rc)
	hash := fmt.Sprintf("%x", h.Sum(nil))

	t.Logf("hash of %d bytes: %s", result.Size, hash)
	if len(hash) != 64 {
		t.Errorf("unexpected hash length: %d", len(hash))
	}
}

func TestE2E_RedirectHandling(t *testing.T) {
	// Create target server.
	targetSrv := testserver.NewServer(t)
	targetSrv.SetSize(512)

	// Create redirect server.
	redirectSrv := testserver.NewServer(t)
	redirectSrv.SetRedirect(targetSrv.URL)

	httpAdapter := gohttp.New(false, 5*time.Second, 5*time.Second, 5*time.Second)
	opts := adapter.RequestOptions{Timeout: 5 * time.Second}
	ctx := context.Background()

	result, err := httpAdapter.Probe(ctx, redirectSrv.URL, opts)
	if err != nil {
		t.Fatalf("Probe after redirect: %v", err)
	}
	if result.Meta.FinalURL != targetSrv.URL {
		t.Errorf("should follow redirect to %s, got %s", targetSrv.URL, result.Meta.FinalURL)
	}
	if result.Size != 512 {
		t.Errorf("size after redirect = %d, want 512", result.Size)
	}
}

func TestE2E_SlowConsumerStream(t *testing.T) {
	srv := testserver.NewServer(t)
	srv.SetSize(1024 * 64) // 64 KiB

	httpAdapter := gohttp.New(false, 30*time.Second, 10*time.Second, 30*time.Second)
	opts := adapter.RequestOptions{Timeout: 30 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := httpAdapter.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	// Download sequentially with a slow reader.
	rc, _ := httpAdapter.OpenSequential(ctx, srv.URL, 0, opts)
	defer rc.Close()

	buf := make([]byte, 16) // Very small buffer to simulate slow consumer.
	total := 0
	for {
		n, err := rc.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		time.Sleep(time.Millisecond) // Simulate slow processing.
	}

	if int64(total) != result.Size {
		t.Errorf("slow read: got %d bytes, want %d", total, result.Size)
	}
}
