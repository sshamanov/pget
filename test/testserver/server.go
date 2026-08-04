// Package testserver provides helpers for running Docker-based test servers
// and a programmable HTTP test server for integration tests.
package testserver

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// HTTPServer is a programmable HTTP test server that can simulate
// various server behaviors needed for integration testing.
type HTTPServer struct {
	*httptest.Server

	mu            sync.Mutex
	size          int64
	etag          string
	lastModified  string
	supportRange  bool
	ignoreRange   bool
	wrongRange    bool
	weakETag     bool
	slowChunk     int   // chunk index to make slow
	slowDelay     time.Duration
	refuseAfter   int   // refuse connections after N requests
	requestCount  int
	return429     bool
	return503     bool
	retryAfter    string
	redirectTo    string
	changeMidway  bool
	changed       bool
}

// NewServer creates a new programmable HTTP test server.
func NewServer(t *testing.T) *HTTPServer {
	t.Helper()
	s := &HTTPServer{
		supportRange: true,
		etag:         `"test-etag-123"`,
		size:         1024 * 1024, // 1 MiB default
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handler))
	t.Cleanup(s.Close)
	return s
}

// SetSize sets the reported file size.
func (s *HTTPServer) SetSize(n int64) { s.mu.Lock(); s.size = n; s.mu.Unlock() }

// SetRangeSupport enables or disables range support.
func (s *HTTPServer) SetRangeSupport(v bool) { s.mu.Lock(); s.supportRange = v; s.mu.Unlock() }

// SetRangeIgnored makes the server ignore Range headers (return 200 instead of 206).
func (s *HTTPServer) SetRangeIgnored(v bool) { s.mu.Lock(); s.ignoreRange = v; s.mu.Unlock() }

// SetWrongRange makes the server return wrong Content-Range values.
func (s *HTTPServer) SetWrongRange(v bool) { s.mu.Lock(); s.wrongRange = v; s.mu.Unlock() }

// SetWeakETag makes the server use a weak ETag.
func (s *HTTPServer) SetWeakETag(v bool) { s.mu.Lock(); s.weakETag = v; s.mu.Unlock() }

// SetReturn429 makes the server return 429 Too Many Requests.
func (s *HTTPServer) SetReturn429(v bool, retryAfter string) {
	s.mu.Lock()
	s.return429 = v
	s.retryAfter = retryAfter
	s.mu.Unlock()
}

// SetReturn503 makes the server return 503 Service Unavailable.
func (s *HTTPServer) SetReturn503(v bool, retryAfter string) {
	s.mu.Lock()
	s.return503 = v
	s.retryAfter = retryAfter
	s.mu.Unlock()
}

// SetSlowChunk makes a specific chunk index respond slowly.
func (s *HTTPServer) SetSlowChunk(idx int, delay time.Duration) {
	s.mu.Lock()
	s.slowChunk = idx
	s.slowDelay = delay
	s.mu.Unlock()
}

// SetRefuseAfter makes the server refuse connections after N requests.
func (s *HTTPServer) SetRefuseAfter(n int) { s.mu.Lock(); s.refuseAfter = n; s.mu.Unlock() }

// SetRedirect makes the server redirect to another URL.
func (s *HTTPServer) SetRedirect(target string) { s.mu.Lock(); s.redirectTo = target; s.mu.Unlock() }

// SetChangeMidway makes the server change the content after the first probe.
func (s *HTTPServer) SetChangeMidway(v bool) { s.mu.Lock(); s.changeMidway = v; s.mu.Unlock() }

func (s *HTTPServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requestCount++

	// Refuse connections after threshold.
	if s.refuseAfter > 0 && s.requestCount > s.refuseAfter {
		// Drop the connection.
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			conn.Close()
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		return
	}

	// Redirect.
	if s.redirectTo != "" {
		http.Redirect(w, r, s.redirectTo, http.StatusFound)
		return
	}

	// 429 handling.
	if s.return429 {
		if s.retryAfter != "" {
			w.Header().Set("Retry-After", s.retryAfter)
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	// 503 handling.
	if s.return503 {
		if s.retryAfter != "" {
			w.Header().Set("Retry-After", s.retryAfter)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// Mid-download content change.
	size := s.size
	if s.changeMidway && s.changed {
		size = size / 2 // half the size
	}
	etag := s.etag
	if s.weakETag {
		etag = `W/` + etag
	}

	// Handle range request.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" && s.supportRange && !s.ignoreRange {
		start, end := parseRange(rangeHeader, size)

		if s.changed && s.requestCount > 1 {
			// Changed object — return 412 or different content.
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}

		var actualEnd int64
		if end < 0 || end >= size {
			actualEnd = size - 1
		} else {
			actualEnd = end
		}

		if s.wrongRange {
			actualEnd = (actualEnd + 128) % size
		}

		contentLen := actualEnd - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, actualEnd, size))
		w.Header().Set("Content-Length", strconv.FormatInt(contentLen, 10))
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusPartialContent)

		// Write bytes.
		writeBytes(w, start, actualEnd, s.slowChunk, s.slowDelay)

		if s.changeMidway {
			s.changed = true
		}
		return
	}

	// Full response (no range or range ignored).
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)

	// Write full body.
	writeBytes(w, 0, size-1, -1, 0)
}

func parseRange(h string, size int64) (start, end int64) {
	h = strings.TrimPrefix(h, "bytes=")
	parts := strings.SplitN(h, "-", 2)
	if len(parts) != 2 {
		return 0, size - 1
	}
	start, _ = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if parts[1] != "" {
		end, _ = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	} else {
		end = size - 1
	}
	return start, end
}

func writeBytes(w http.ResponseWriter, start, end int64, slowChunk int, slowDelay time.Duration) {
	length := end - start + 1
	chunkSize := int64(1024) // write in 1KB chunks
	for written := int64(0); written < length; {
		n := chunkSize
		if written+n > length {
			n = length - written
		}
		buf := make([]byte, n)
		// Fill with deterministic data: byte value = (position % 256).
		for i := range buf {
			buf[i] = byte((start + written + int64(i)) % 256)
		}
		w.Write(buf)
		written += n
	}
}

// IsPortAvailable checks if a port is available.
func IsPortAvailable(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

// RunDockerCompose starts docker compose services.
func RunDockerCompose(composeFile string, services ...string) error {
	args := append([]string{"compose", "-f", composeFile, "up", "-d", "--wait"}, services...)
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose up: %w\n%s", err, out)
	}
	return nil
}

// StopDockerCompose stops docker compose services.
func StopDockerCompose(composeFile string) error {
	cmd := exec.Command("docker", "compose", "-f", composeFile, "down", "-v")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose down: %w\n%s", err, out)
	}
	return nil
}
