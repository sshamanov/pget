// Package http implements the HTTP and HTTPS protocol adapter.
package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	gohttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sshamanov/pget/internal/adapter"
)

// Adapter implements adapter.Adapter for HTTP and HTTPS.
type Adapter struct {
	client *gohttp.Client
}

// AdapterOption is a functional option for configuring the adapter.
type AdapterOption func(a *Adapter)

// WithTransport sets a custom HTTP transport (for test injection).
func WithTransport(rt gohttp.RoundTripper) AdapterOption {
	return func(a *Adapter) {
		a.client.Transport = rt
	}
}

// New creates a new HTTP/HTTPS adapter with the given settings.
func New(insecureSkipVerify bool, timeout, connectTimeout, readTimeout time.Duration, opts ...AdapterOption) *Adapter {
	transport := &gohttp.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: insecureSkipVerify,
		},
		DisableCompression:  true,
		MaxConnsPerHost:      0, // no limit on total connections
		MaxIdleConnsPerHost:  100, // keep idle connections for all workers to avoid reconnect overhead
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second,
			Control: func(network, address string, c syscall.RawConn) error {
				// Set large socket buffers for high-BDP connections.
				// Default 256KB limits throughput to ~40Mbps at 50ms latency.
				var setErr error
				err := c.Control(func(fd uintptr) {
					setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4<<20)
					if setErr == nil {
						setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4<<20)
					}
				})
				if err != nil {
					return err
				}
				return setErr
			},
		}).DialContext,
		// Force HTTP/1.1 — HTTP/2 multiplexing is explicitly rejected (§8.2).
		ForceAttemptHTTP2: false,
	}

	client := &gohttp.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *gohttp.Request, via []*gohttp.Request) error {
			if len(via) >= 20 {
				return fmt.Errorf("too many redirects")
			}
			// Preserve Range and Accept-Encoding headers across redirects.
			// Go may strip these on cross-host redirects, breaking parallel download.
			if prev := via[len(via)-1]; prev != nil {
				if rng := prev.Header.Get("Range"); rng != "" {
					req.Header.Set("Range", rng)
				}
				if ae := prev.Header.Get("Accept-Encoding"); ae != "" {
					req.Header.Set("Accept-Encoding", ae)
				}
			}
			// Strip credentials on cross-origin redirect.
			if req.URL.User != nil {
				prev := via[len(via)-1].URL
				if prev.Host != req.URL.Host {
					req.URL.User = nil
				}
			}
			return nil
		},
	}

	a := &Adapter{client: client}
	for _, o := range opts {
		o(a)
	}

	return a
}

// Probe fetches object metadata and checks parallel capability.
func (a *Adapter) Probe(ctx context.Context, urlStr string, opts adapter.RequestOptions) (*adapter.ProbeResult, error) {
	req, err := gohttp.NewRequestWithContext(ctx, gohttp.MethodGet, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("probe request: %w", err)
	}

	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")
	a.setHeaders(req, opts)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain any response body

	// Check for HTTP error status codes.
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("probe: HTTP %d", resp.StatusCode)
	}

	result := &adapter.ProbeResult{}
	result.Meta.FinalURL = resp.Request.URL.String()
	result.Meta.DisplayURL = sanitizeURL(resp.Request.URL)
	result.Meta.Protocol = resp.Request.URL.Scheme

	// Check for range support.
	if resp.StatusCode != gohttp.StatusPartialContent {
		// Server ignored range request — no parallel capability.
		result.RangeCapable = false
		result.Meta.Size = -1
		// Try Content-Length.
		if cl := resp.ContentLength; cl > 0 {
			result.Size = cl
			result.Meta.Size = cl
		}
		result.Meta.Filename = extractFilename(resp)
		return result, nil
	}

	// Parse Content-Range: bytes 0-0/TOTAL.
	cr := resp.Header.Get("Content-Range")
	totalSize, ok := parseContentRangeTotal(cr)
	if !ok || totalSize <= 0 {
		result.RangeCapable = false
		result.Meta.Size = -1
		result.Meta.Filename = extractFilename(resp)
		return result, nil
	}

	result.Size = totalSize
	result.Meta.Size = totalSize

	// Check for usable validator.
	etag := resp.Header.Get("ETag")
	lastMod := resp.Header.Get("Last-Modified")

	if isStrongETag(etag) {
		result.ETag = etag
		result.Meta.ETag = etag
		result.RangeCapable = true
		result.Meta.RangeCapable = true
	} else if lastMod != "" && totalSize > 0 {
		result.LastModified = lastMod
		result.Meta.LastModified = lastMod
		result.RangeCapable = true
		result.Meta.RangeCapable = true
	} else {
		result.RangeCapable = false
		result.Meta.RangeCapable = false
	}

	if t, err := parseHTTPTime(lastMod); err == nil {
		result.Meta.ModTime = t
	}

	result.Meta.Filename = extractFilename(resp)
	return result, nil
}

// OpenRange starts a ranged request for the given byte range.
func (a *Adapter) OpenRange(ctx context.Context, urlStr string, start, length int64, validator string, opts adapter.RequestOptions) (adapter.RangeReader, error) {
	end := start + length - 1
	req, err := gohttp.NewRequestWithContext(ctx, gohttp.MethodGet, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("range request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("Accept-Encoding", "identity")

	if validator != "" {
		if isStrongETag(validator) {
			req.Header.Set("If-Range", validator)
		} else {
			req.Header.Set("If-Range", validator)
		}
	}

	a.setHeaders(req, opts)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("range request: %w", err)
	}

	if resp.StatusCode != gohttp.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("range request: unexpected status %d", resp.StatusCode)
	}

	// Validate Content-Range.
	cr := resp.Header.Get("Content-Range")
	if !validateContentRange(cr, start, end) {
		resp.Body.Close()
		return nil, fmt.Errorf("range request: invalid Content-Range: %s", cr)
	}

	// Validate content length matches expected.
	if resp.ContentLength != length {
		resp.Body.Close()
		return nil, fmt.Errorf("range request: Content-Length %d != expected %d", resp.ContentLength, length)
	}

	return &rangeReader{
		ReadCloser: resp.Body,
		length:     length,
	}, nil
}

// OpenSequential starts a sequential download from the given offset.
func (a *Adapter) OpenSequential(ctx context.Context, urlStr string, offset int64, opts adapter.RequestOptions) (io.ReadCloser, error) {
	req, err := gohttp.NewRequestWithContext(ctx, gohttp.MethodGet, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("sequential request: %w", err)
	}

	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	req.Header.Set("Accept-Encoding", "identity")
	a.setHeaders(req, opts)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sequential request: %w", err)
	}

	if resp.StatusCode != gohttp.StatusOK && resp.StatusCode != gohttp.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("sequential request: unexpected status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

func (a *Adapter) setHeaders(req *gohttp.Request, opts adapter.RequestOptions) {
	if opts.UserAgent != "" {
		req.Header.Set("User-Agent", opts.UserAgent)
	}
	if opts.Referer != "" {
		req.Header.Set("Referer", opts.Referer)
	}
	for k, v := range opts.Headers {
		req.Header.Set(k, v)
	}
}

// rangeReader wraps an io.ReadCloser with a known content length.
type rangeReader struct {
	io.ReadCloser
	length int64
}

func (r *rangeReader) ContentLength() int64 { return r.length }

// parseContentRangeTotal parses "bytes 0-0/12345" and returns 12345.
func parseContentRangeTotal(cr string) (int64, bool) {
	if cr == "" {
		return 0, false
	}
	parts := strings.Split(cr, "/")
	if len(parts) != 2 {
		return 0, false
	}
	total, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || total <= 0 {
		return 0, false
	}
	return total, true
}

// validateContentRange checks that the Content-Range matches the requested range.
func validateContentRange(cr string, start, end int64) bool {
	if cr == "" {
		return false
	}
	// Format: "bytes START-END/TOTAL"
	cr = strings.TrimPrefix(cr, "bytes ")
	parts := strings.SplitN(cr, "/", 2)
	if len(parts) != 2 {
		return false
	}
	rangeParts := strings.SplitN(parts[0], "-", 2)
	if len(rangeParts) != 2 {
		return false
	}
	gotStart, err := strconv.ParseInt(strings.TrimSpace(rangeParts[0]), 10, 64)
	if err != nil {
		return false
	}
	gotEnd, err := strconv.ParseInt(strings.TrimSpace(rangeParts[1]), 10, 64)
	if err != nil {
		return false
	}
	return gotStart == start && gotEnd == end
}

// isStrongETag returns true if the ETag is strong (not weak, i.e., no W/ prefix).
func isStrongETag(etag string) bool {
	return etag != "" && !strings.HasPrefix(etag, "W/") && !strings.HasPrefix(etag, "w/")
}

// extractFilename extracts a filename from Content-Disposition or URL path.
func extractFilename(resp *gohttp.Response) string {
	// Try Content-Disposition first.
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		const prefix = "filename="
		if i := strings.Index(cd, prefix); i >= 0 {
			name := cd[i+len(prefix):]
			name = strings.Trim(name, `"'`)
			if name != "" {
				return sanitizeFilename(name)
			}
		}
	}
	// Fall back to URL path.
	path := resp.Request.URL.Path
	if path == "" || path == "/" {
		return "index.html"
	}
	// Get last path segment.
	if i := strings.LastIndex(path, "/"); i >= 0 {
		name := path[i+1:]
		if name != "" {
			return sanitizeFilename(name)
		}
	}
	return "index.html"
}

// sanitizeFilename removes directory separators and rejects dangerous names.
func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, "\x00", "")
	if name == "." || name == ".." || name == "" {
		return "index.html"
	}
	return name
}

// sanitizeURL removes credentials from a URL for display.
func sanitizeURL(u *url.URL) string {
	u2 := *u
	u2.User = nil
	return u2.String()
}

// parseHTTPTime parses an HTTP-date timestamp.
func parseHTTPTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC1123,
		time.RFC1123Z,
		time.RFC850,
		time.RFC3339,
		"Mon, 02 Jan 2006 15:04:05 MST",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse HTTP time: %s", s)
}
