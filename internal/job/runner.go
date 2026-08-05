package job

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sshamanov/pget/internal/adapter"
	gohttp "github.com/sshamanov/pget/internal/adapter/http"
	"github.com/sshamanov/pget/internal/chunk"
	"github.com/sshamanov/pget/internal/cli"
	"github.com/sshamanov/pget/internal/progress"
	"github.com/sshamanov/pget/internal/schedule"
	"github.com/sshamanov/pget/internal/sidecar"
	"github.com/sshamanov/pget/internal/sink"
)

// ExitCode is a Wget-compatible exit category.
type ExitCode int

const (
	ExitSuccess      ExitCode = 0
	ExitGenericError ExitCode = 1
	ExitParseError   ExitCode = 2
	ExitIOError      ExitCode = 3
	ExitNetworkFail  ExitCode = 4
	ExitTLSError     ExitCode = 5
	ExitAuthError    ExitCode = 6
	ExitProtoError   ExitCode = 7
	ExitServerError  ExitCode = 8
)

// Runner processes a sequence of DownloadJobs.
type Runner struct {
	plan *cli.ExecutionPlan
}

// NewRunner creates a job runner from an execution plan.
func NewRunner(plan *cli.ExecutionPlan) *Runner {
	return &Runner{plan: plan}
}

// Run executes all URLs in the plan. Returns the highest-priority exit code.
func (r *Runner) Run(ctx context.Context) ExitCode {
	if len(r.plan.URLs) == 0 {
		fmt.Fprintln(os.Stderr, "pget: missing URL")
		return ExitParseError
	}

	// Handle background mode.
	if r.plan.Background == cli.BackgroundChild && !IsBackgroundChild() {
		if r.plan.OutputMode == cli.OutputStdout {
			fmt.Fprintln(os.Stderr, "pget: background mode with stdout output is not supported")
			return ExitParseError
		}
		if err := DetachToBackground(); err != nil {
			fmt.Fprintf(os.Stderr, "pget: background: %v\n", err)
			return ExitGenericError
		}
		// DetachToBackground exits the parent; child continues here.
	}

	var worstExit ExitCode
	var outputFile io.WriteCloser
	var outputBaseOffset int64

	// Set up output for -O mode.
	if r.plan.OutputMode == cli.OutputSingle || r.plan.OutputMode == cli.OutputStdout {
		if r.plan.OutputMode == cli.OutputStdout {
			outputFile = os.Stdout
		} else {
			f, err := os.Create(r.plan.OutputFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "pget: %v\n", err)
				return ExitIOError
			}
			outputFile = f
			defer f.Close()
		}
	}

	// Create HTTP adapter (shared across all jobs).
	httpAdapter := gohttp.New(
		r.plan.InsecureSkipVerify,
		r.plan.Timeout,
		r.plan.ConnectTimeout,
		r.plan.ReadTimeout,
	)

	for _, urlStr := range r.plan.URLs {
		displayURL := sanitizeDisplayURL(urlStr)
		adapterOpts := adapter.RequestOptions{
			UserAgent:          r.plan.UserAgent,
			Referer:            r.plan.Referer,
			Headers:            r.plan.ExtraHeaders,
			Timeout:            r.plan.Timeout,
			ConnectTimeout:     r.plan.ConnectTimeout,
			ReadTimeout:        r.plan.ReadTimeout,
			InsecureSkipVerify: r.plan.InsecureSkipVerify,
			RetryConnRefused:   r.plan.RetryConnRefused,
			RetryOnHTTPError:   r.plan.RetryOnHTTPError,
		}

		ec := r.runOneJob(ctx, urlStr, displayURL, httpAdapter, adapterOpts, outputFile, &outputBaseOffset)

		if ec != ExitSuccess && (r.plan.OutputMode == cli.OutputSingle || r.plan.OutputMode == cli.OutputStdout) {
			return ec
		}
		if ec > worstExit {
			worstExit = ec
		}
	}

	return worstExit
}

func (r *Runner) runOneJob(
	ctx context.Context,
	urlStr, displayURL string,
	httpAdapter *gohttp.Adapter,
	adapterOpts adapter.RequestOptions,
	outputFile io.WriteCloser,
	outputBaseOffset *int64,
) ExitCode {
	// Spider mode: probe only, no download.
	if r.plan.Spider {
		result, err := httpAdapter.Probe(ctx, urlStr, adapterOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pget: %s: %v\n", displayURL, err)
			return ExitNetworkFail
		}
		if !r.plan.Quiet {
			fmt.Fprintf(os.Stderr, "%s: exists (%d bytes)\n", displayURL, result.Size)
		}
		return ExitSuccess
	}

	// Determine filename for normal file mode.
	destPath := ""
	isStreamMode := false
	switch {
	case r.plan.OutputMode == cli.OutputStdout:
		isStreamMode = true
	case r.plan.OutputMode == cli.OutputSingle:
		destPath = r.plan.OutputFile
		if destPath != "-" {
			if err := ensureDir(destPath); err != nil {
				fmt.Fprintf(os.Stderr, "pget: %v\n", err)
				return ExitIOError
			}
		}
	default:
		// Normal file mode: derive filename from URL or Content-Disposition.
		destPath = resolveFilename(nil, displayURL)
	}

	// No-clobber check.
	if r.plan.NoClobber && destPath != "" {
		if _, err := os.Stat(destPath); err == nil {
			if !r.plan.Quiet {
				fmt.Fprintf(os.Stderr, "pget: %s already exists, skipping (--no-clobber)\n", destPath)
			}
			return ExitSuccess
		}
	}

	// Timestamping check.
	if r.plan.TimestampMode == cli.TimestampCheck && destPath != "" {
		if skip, ec := r.checkTimestamp(ctx, urlStr, displayURL, destPath, httpAdapter, adapterOpts); skip {
			return ec
		}
	}

	// Probe remote metadata.
	result, err := httpAdapter.Probe(ctx, urlStr, adapterOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pget: %s: %v\n", displayURL, err)
		return ExitNetworkFail
	}

	// Update filename from probe result.
	if r.plan.OutputMode == cli.OutputFile {
		destPath = resolveFilename(result, displayURL)
	}

	// Resume from sidecar: always on when a valid sidecar exists.
	completedChunks := make(map[int]bool)
	if destPath != "" {
		if sm := sidecar.NewManager(destPath); sm.Exists() {
			state, err := sm.Load()
			if err != nil {
				fmt.Fprintf(os.Stderr, "pget: invalid sidecar, starting fresh: %v\n", err)
			} else if state.Items[0].Length != result.Size {
				fmt.Fprintf(os.Stderr, "pget: remote size changed, starting fresh\n")
			} else {
				for i, item := range state.Items {
					_ = i
					bitmap, _ := decodeBitmap(item.CompletedBitmap, int((item.Length+item.SplitSize-1)/item.SplitSize))
					for j, done := range bitmap {
						if done {
							completedChunks[j] = true
						}
					}
				}
				if len(completedChunks) > 0 && !r.plan.Quiet {
					fmt.Fprintf(os.Stderr, "%s: resuming (%d/%d chunks complete)\n",
						displayURL, len(completedChunks), int((result.Size+r.plan.SplitSize-1)/r.plan.SplitSize))
				}
			}
		} else if r.plan.ContinueMode == cli.ContinueAuto {
			// -c: Wget-style contiguous resume when no sidecar exists.
			if fi, err := os.Stat(destPath); err == nil && fi.Size() > 0 {
				return r.downloadSequential(ctx, urlStr, displayURL, httpAdapter, adapterOpts, result, destPath, nil, nil)
			}
		}
	}

	// Decide parallel vs sequential.
	useParallel := result.RangeCapable && !r.plan.NoParallel && r.plan.Connections > 1 && result.Size > 0

	if !r.plan.Quiet {
		mode := "sequential"
		if useParallel {
			mode = fmt.Sprintf("parallel (%d connections)", r.plan.Connections)
		}
		fmt.Fprintf(os.Stderr, "%s: %s mode, %d bytes\n", displayURL, mode, result.Size)
	}

	if useParallel && !isStreamMode {
		return r.downloadParallelFile(ctx, urlStr, displayURL, destPath, httpAdapter, adapterOpts, result, outputBaseOffset, completedChunks)
	}

	if useParallel && isStreamMode {
		return r.downloadParallelStream(ctx, urlStr, displayURL, httpAdapter, adapterOpts, result, outputFile)
	}

	// Sequential fallback.
	return r.downloadSequential(ctx, urlStr, displayURL, httpAdapter, adapterOpts, result, destPath, outputFile, outputBaseOffset)
}

func (r *Runner) checkTimestamp(
	ctx context.Context, urlStr, displayURL, destPath string,
	httpAdapter *gohttp.Adapter, adapterOpts adapter.RequestOptions,
) (skip bool, code ExitCode) {
	result, err := httpAdapter.Probe(ctx, urlStr, adapterOpts)
	if err != nil {
		return false, ExitSuccess // can't check, proceed with download
	}

	fi, err := os.Stat(destPath)
	if err != nil {
		return false, ExitSuccess // no local file, download
	}

	localSize := fi.Size()
	localMtime := fi.ModTime()

	// Skip if remote is not newer and sizes match.
	if !result.Meta.ModTime.IsZero() && !result.Meta.ModTime.After(localMtime) && result.Size == localSize {
		if !r.plan.Quiet {
			fmt.Fprintf(os.Stderr, "%s: not newer than local file, skipping\n", displayURL)
		}
		return true, ExitSuccess
	}

	return false, ExitSuccess
}

func (r *Runner) downloadParallelFile(
	ctx context.Context,
	urlStr, displayURL, destPath string,
	httpAdapter *gohttp.Adapter,
	adapterOpts adapter.RequestOptions,
	result *adapter.ProbeResult,
	outputBaseOffset *int64,
	completedChunks map[int]bool,
) ExitCode {
	planner := chunk.NewPlanner(r.plan.SplitSize)
	allChunks := planner.Plan(result.Size)

	// Mark already-completed chunks so the scheduler skips them.
	// We pass ALL chunks (not filtered) so chunk indices match array positions.
	pendingCount := 0
	for i := range allChunks {
		if completedChunks[allChunks[i].Index] {
			allChunks[i].State = chunk.StateCompleted
		} else {
			pendingCount++
		}
	}

	if pendingCount == 0 {
		if !r.plan.Quiet {
			fmt.Fprintf(os.Stderr, "%s: already complete\n", displayURL)
		}
		*outputBaseOffset += result.Size
		// Remove sidecar since we're done.
		sidecar.NewManager(destPath).Remove()
		return ExitSuccess
	}

	// Set up sidecar.
	sm := sidecar.NewManager(destPath)
	baseOff := *outputBaseOffset
	urlHash := sidecar.HashURL(urlStr)
	items := []sidecar.Item{{
		SourceURLHash: urlHash,
		FinalURLHash:  sidecar.HashURL(result.Meta.FinalURL),
		DisplayURL:    displayURL,
		OutputOffset:  baseOff,
		Length:        result.Size,
		SplitSize:     r.plan.SplitSize,
		ETag:          result.ETag,
		LastModified:  result.LastModified,
	}}

	// Create or update sidecar with completed bitmap.
	bitmap := make([]bool, len(allChunks))
	for idx := range completedChunks {
		if idx < len(bitmap) {
			bitmap[idx] = true
		}
	}
	items[0].CompletedBitmap = encodeBitmap(bitmap)

	if err := sm.Create(destPath, sidecar.HashURLList([]string{urlStr}), items); err != nil {
		fmt.Fprintf(os.Stderr, "pget: sidecar error: %v\n", err)
		return ExitIOError
	}

	fs, err := sink.NewFileSink(destPath, baseOff, sm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pget: %v\n", err)
		return ExitIOError
	}

	cfg := schedule.Config{
		RequestedConnections: r.plan.Connections,
		SplitSize:           r.plan.SplitSize,
		MaxTries:            r.plan.MaxTries,
		StreamMode:          false,
	}
	sched := schedule.New(ctx, cfg, allChunks, nil)

	// Set up progress reporting.
	reporter := progress.NewReporter(r.plan.Quiet, r.plan.NoVerbose, r.plan.ProgressType)
	var progressBytes atomic.Int64
	var bar *progress.ProgressBar
	progressDone := make(chan struct{})

	if result.Size > 0 {
		bar = progress.NewProgressBar(reporter, result.Size, progressLabel(displayURL), r.plan.Connections)
		go func() {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					bar.Update(progressBytes.Load())
				}
			}
		}()
	}

	ec := r.runWorkers(ctx, sched, urlStr, httpAdapter, adapterOpts, result, fs, nil, &progressBytes)
	*outputBaseOffset += result.Size

	// Finalize progress bar.
	if bar != nil {
		close(progressDone)
		bar.Done()
	}

	if ec == ExitSuccess {
		modTime := result.Meta.ModTime
		if r.plan.NoUseServerTimestamps {
			modTime = time.Time{}
		}
		if err := fs.Finalize(modTime); err != nil {
			fmt.Fprintf(os.Stderr, "pget: finalize: %v\n", err)
			return ExitIOError
		}
	}

	return ec
}

func (r *Runner) downloadParallelStream(
	ctx context.Context,
	urlStr, displayURL string,
	httpAdapter *gohttp.Adapter,
	adapterOpts adapter.RequestOptions,
	result *adapter.ProbeResult,
	w io.Writer,
) ExitCode {
	planner := chunk.NewPlanner(r.plan.SplitSize)
	chunks := planner.Plan(result.Size)

	streamSink := sink.NewStreamSink(r.plan.BufferSize)

	cfg := schedule.Config{
		RequestedConnections: r.plan.Connections,
		StreamBufferSize:    r.plan.BufferSize,
		SplitSize:           r.plan.SplitSize,
		MaxTries:            r.plan.MaxTries,
		StreamMode:          true,
	}
	sched := schedule.New(ctx, cfg, chunks, streamSink)

	writeErr := make(chan error, 1)
	go func() {
		writeErr <- streamSink.WriteTo(ctx, w)
	}()

	// Set up progress reporting.
	reporter := progress.NewReporter(r.plan.Quiet, r.plan.NoVerbose, r.plan.ProgressType)
	var progressBytes atomic.Int64
	var bar *progress.ProgressBar
	progressDone := make(chan struct{})

	if result.Size > 0 {
		bar = progress.NewProgressBar(reporter, result.Size, progressLabel(displayURL), r.plan.Connections)
		go func() {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					bar.Update(progressBytes.Load())
				}
			}
		}()
	}

	ec := r.runWorkers(ctx, sched, urlStr, httpAdapter, adapterOpts, result, nil, streamSink, &progressBytes)

	// Signal the writer that all chunks are done.
	streamSink.Close()

	if werr := <-writeErr; werr != nil && ec == ExitSuccess {
		fmt.Fprintf(os.Stderr, "pget: write error: %v\n", werr)
		return ExitIOError
	}

	// Finalize progress bar.
	if bar != nil {
		close(progressDone)
		bar.Done()
	}

	return ec
}

func (r *Runner) downloadSequential(
	ctx context.Context,
	urlStr, displayURL string,
	httpAdapter *gohttp.Adapter,
	adapterOpts adapter.RequestOptions,
	result *adapter.ProbeResult,
	destPath string,
	outputFile io.WriteCloser,
	outputBaseOffset *int64,
) ExitCode {
	var w io.WriteCloser
	var err error
	offset := int64(0)

	if outputFile != nil {
		w = outputFile
	} else if destPath != "" {
		// Check for existing file for append (continue without sidecar).
		if fi, statErr := os.Stat(destPath); statErr == nil && r.plan.ContinueMode == cli.ContinueAuto && result.Size > 0 {
			offset = fi.Size()
		}
		flag := os.O_CREATE | os.O_WRONLY
		if offset > 0 {
			flag |= os.O_APPEND
		} else {
			flag |= os.O_TRUNC
		}
		w, err = os.OpenFile(destPath, flag, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pget: %v\n", err)
			return ExitIOError
		}
		defer w.Close()
	} else {
		fmt.Fprintf(os.Stderr, "pget: no output destination\n")
		return ExitIOError
	}

	rc, err := httpAdapter.OpenSequential(ctx, urlStr, offset, adapterOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pget: %s: %v\n", displayURL, err)
		return ExitNetworkFail
	}
	defer rc.Close()

	// Set up progress reporting for sequential download.
	reporter := progress.NewReporter(r.plan.Quiet, r.plan.NoVerbose, r.plan.ProgressType)
	var progressBytes atomic.Int64
	var bar *progress.ProgressBar
	progressDone := make(chan struct{})

	if result.Size > 0 {
		bar = progress.NewProgressBar(reporter, result.Size, progressLabel(displayURL), r.plan.Connections)
		go func() {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					bar.Update(progressBytes.Load())
				}
			}
		}()
	}

	// Wrap reader to track progress.
	pr := &progressReader{rc: rc, tracker: &progressBytes}
	n, err := io.Copy(w, pr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pget: download error: %v\n", err)
		return ExitNetworkFail
	}

	// Finalize progress bar.
	if bar != nil {
		close(progressDone)
		bar.Done()
	}

	// Apply modification time.
	if destPath != "" && !r.plan.NoUseServerTimestamps && !result.Meta.ModTime.IsZero() {
		os.Chtimes(destPath, result.Meta.ModTime, result.Meta.ModTime)
	}

	if result.Size > 0 && outputBaseOffset != nil {
		*outputBaseOffset += result.Size
	}
	_ = n // bytes written

	return ExitSuccess
}

func (r *Runner) runWorkers(
	ctx context.Context,
	sched *schedule.Scheduler,
	urlStr string,
	httpAdapter *gohttp.Adapter,
	adapterOpts adapter.RequestOptions,
	result *adapter.ProbeResult,
	fileSink sink.FileSink,
	streamSink sink.StreamSink,
	progressBytes *atomic.Int64,
) ExitCode {
	validator := result.ETag
	if validator == "" {
		validator = result.LastModified
	}

	// Propagate root context cancellation to the scheduler so that
	// workers blocked in cond.Wait() are woken up immediately.
	go func() {
		<-ctx.Done()
		sched.Cancel()
	}()

	errCh := make(chan error, r.plan.Connections)
	for slot := 0; slot < r.plan.Connections; slot++ {
		go func(slot int) {
			errCh <- r.workerLoop(ctx, sched, slot, urlStr, httpAdapter, adapterOpts, validator, fileSink, streamSink, progressBytes)
		}(slot)
	}

	var firstErr error
	for i := 0; i < r.plan.Connections; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			sched.Cancel()
		}
	}

	if firstErr != nil {
		errStr := firstErr.Error()
		switch {
		case strings.Contains(errStr, "TLS") || strings.Contains(errStr, "certificate"):
			return ExitTLSError
		case strings.Contains(errStr, "401") || strings.Contains(errStr, "403"):
			return ExitAuthError
		case strings.Contains(errStr, "500") || strings.Contains(errStr, "502") || strings.Contains(errStr, "503"):
			return ExitServerError
		default:
			return ExitNetworkFail
		}
	}

	completed, total := sched.Progress()
	if completed < total {
		return ExitNetworkFail
	}
	return ExitSuccess
}

func (r *Runner) workerLoop(
	ctx context.Context,
	sched *schedule.Scheduler,
	slot int,
	urlStr string,
	httpAdapter *gohttp.Adapter,
	adapterOpts adapter.RequestOptions,
	validator string,
	fileSink sink.FileSink,
	streamSink sink.StreamSink,
	progressBytes *atomic.Int64,
) error {
	for {
		// Check for context cancellation (Ctrl+C).
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check for speculative duplicate opportunity first.
		var isHedge bool
		var c *chunk.Chunk
		var err error
		var hedgeSlot int

		if streamSink != nil {
			if hedgeIdx := sched.HedgeEligible(); hedgeIdx >= 0 {
				c, hedgeSlot = sched.StartHedge(hedgeIdx)
				if c != nil {
					isHedge = true
					slot = hedgeSlot
				}
			}
		}

		if !isHedge {
			c, err = sched.NextAssignment(slot)
			if err != nil {
				return err
			}
			if c == nil {
				return nil
			}
		}

		if streamSink != nil && !isHedge {
			if err := streamSink.Reserve(c.Length); err != nil {
				sched.MarkFailed(c.Index, true)
				continue
			}
		}

		rr, err := httpAdapter.OpenRange(ctx, urlStr, c.Start, c.Length, validator, adapterOpts)
		if err != nil {
			if isHedge {
				sched.CancelHedge(c.Index)
			} else {
				sched.OnAdmissionFailure(slot)
				sched.MarkFailed(c.Index, true)
			}
			continue
		}

		data := make([]byte, c.Length)
		_, readErr := io.ReadFull(rr, data)
		rr.Close()

		if readErr != nil {
			if isHedge {
				sched.CancelHedge(c.Index)
			} else {
				sched.MarkFailed(c.Index, true)
			}
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
				return fmt.Errorf("file write: %w", err)
			}
		}

		if isHedge {
			// Hedge completed — cancel the original.
			sched.CancelHedge(c.Index)
		}

		progressBytes.Add(int64(len(data)))
		sched.OnWorkerSuccess(slot, c.Length, time.Since(c.StartTime))
		sched.MarkComplete(c.Index)
	}
}

func resolveFilename(result *adapter.ProbeResult, displayURL string) string {
	if result != nil && result.Meta.Filename != "" && result.Meta.Filename != "index.html" {
		return result.Meta.Filename
	}
	if i := strings.LastIndex(displayURL, "/"); i >= 0 {
		name := displayURL[i+1:]
		if idx := strings.Index(name, "?"); idx >= 0 {
			name = name[:idx]
		}
		if name != "" && name != "/" {
			return name
		}
	}
	return "index.html"
}

// progressLabel extracts a short label for the progress bar from a URL.
func progressLabel(urlStr string) string {
	// Strip query string and fragment.
	u := urlStr
	if idx := strings.Index(u, "?"); idx >= 0 {
		u = u[:idx]
	}
	if idx := strings.Index(u, "#"); idx >= 0 {
		u = u[:idx]
	}
	// Extract the last path segment.
	if idx := strings.LastIndex(u, "/"); idx >= 0 {
		name := u[idx+1:]
		if name != "" {
			return name
		}
	}
	return u
}

func sanitizeDisplayURL(urlStr string) string {
	if idx := strings.Index(urlStr, "://"); idx >= 0 {
		rest := urlStr[idx+3:]
		if atIdx := strings.Index(rest, "@"); atIdx >= 0 {
			return urlStr[:idx+3] + rest[atIdx+1:]
		}
	}
	return urlStr
}

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0755)
}

// decodeBitmap is shared with sidecar for resume support.
func decodeBitmap(encoded string, minSize int) ([]bool, error) {
	return sidecar.DecodeBitmap(encoded, minSize)
}

// encodeBitmap is shared with sidecar for resume support.
func encodeBitmap(bitmap []bool) string {
	return sidecar.EncodeBitmap(bitmap)
}

// progressReader wraps an io.Reader and tracks bytes read in an atomic counter.
type progressReader struct {
	rc      io.Reader
	tracker *atomic.Int64
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	r.tracker.Add(int64(n))
	return n, err
}
