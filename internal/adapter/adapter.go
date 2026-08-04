// Package adapter defines the protocol adapter interface and common types
// shared across HTTP, HTTPS, FTP, and FTPS implementations.
package adapter

import (
	"context"
	"io"
	"time"
)

// ObjectMetadata describes a remote object discovered during probing.
type ObjectMetadata struct {
	OriginalURL    string
	FinalURL       string
	DisplayURL     string // sanitized, no credentials
	Protocol       string // "http", "https", "ftp", "ftps"
	Size           int64  // -1 if unknown
	ModTime        time.Time
	ETag           string // strong ETag if available
	LastModified   string // raw Last-Modified header value
	RangeCapable   bool
	RestartCapable bool
	Filename       string // suggested filename from URL or Content-Disposition
}

// RequestOptions carries per-request settings from the execution plan.
type RequestOptions struct {
	UserAgent           string
	Referer             string
	Headers             map[string]string
	Timeout             time.Duration
	ConnectTimeout      time.Duration
	ReadTimeout         time.Duration
	InsecureSkipVerify  bool
	RetryConnRefused    bool
	RetryOnHTTPError    []int
}

// RangeReader delivers bytes for a specific byte range.
// The caller must read exactly length bytes and then close.
type RangeReader interface {
	io.ReadCloser
	// ContentLength returns the expected number of bytes in this range.
	ContentLength() int64
}

// ProbeResult contains metadata about a probed object.
type ProbeResult struct {
	Meta         ObjectMetadata
	ETag         string
	LastModified string
	Size         int64
	RangeCapable bool
}

// Adapter is the common protocol adapter interface.
type Adapter interface {
	// Probe fetches metadata and checks parallel capability.
	Probe(ctx context.Context, urlStr string, opts RequestOptions) (*ProbeResult, error)

	// OpenRange starts a ranged request for the given byte range.
	// The validator (ETag or If-Range value) must be sent to detect
	// representation changes.
	OpenRange(ctx context.Context, urlStr string, start, length int64, validator string, opts RequestOptions) (RangeReader, error)

	// OpenSequential starts a sequential download from the given offset.
	OpenSequential(ctx context.Context, urlStr string, offset int64, opts RequestOptions) (io.ReadCloser, error)
}
