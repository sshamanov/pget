// Package cli defines the execution plan, CLI parsing, and shared types.
package cli

import (
	"time"
)

// OutputMode describes where downloaded bytes are written.
type OutputMode int

const (
	// OutputFile writes each URL to its own file.
	OutputFile OutputMode = iota
	// OutputSingle writes all URLs concatenated to one named file.
	OutputSingle
	// OutputStdout writes all URLs concatenated to stdout.
	OutputStdout
)

// ContinueMode describes resume behavior.
type ContinueMode int

const (
	ContinueNone ContinueMode = iota
	ContinueAuto            // -c: resume from sidecar or contiguous prefix
)

// TimestampMode describes timestamp checking behavior.
type TimestampMode int

const (
	TimestampNone TimestampMode = iota
	TimestampCheck            // -N: check remote vs local timestamps
)

// LogMode describes log output behavior.
type LogMode int

const (
	LogStderr LogMode = iota
	LogFile
	LogAppend
)

// BackgroundMode describes background execution.
type BackgroundMode int

const (
	BackgroundNone BackgroundMode = iota
	BackgroundChild
)

// ExecutionPlan is an immutable plan produced by the CLI parser.
// It captures all validated options and URLs before any network or file I/O.
type ExecutionPlan struct {
	URLs        []string
	OutputFile  string // "" means derive from URL; "-" means stdout
	OutputMode  OutputMode

	// Logging
	Quiet       bool
	Verbose     bool
	NoVerbose   bool
	ServerResp  bool
	LogMode     LogMode
	LogFile     string

	// Download behavior
	ContinueMode    ContinueMode
	NoClobber       bool
	TimestampMode   TimestampMode
	ContentDisposition bool
	NoUseServerTimestamps bool
	Spider          bool
	InputFile       string

	// Parallel settings
	Connections int
	SplitSize   int64
	BufferSize  int64
	NoParallel  bool

	// Retry and timeout
	MaxTries         int
	Timeout          time.Duration
	ConnectTimeout   time.Duration
	ReadTimeout      time.Duration
	RetryConnRefused bool
	RetryOnHTTPError []int

	// Request metadata
	UserAgent          string
	Referer            string
	ExtraHeaders       map[string]string
	InsecureSkipVerify bool
	FTPSImplicit       bool

	// Progress
	ProgressType string

	// Background
	Background BackgroundMode
}
