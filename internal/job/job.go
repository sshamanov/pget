// Package job defines the DownloadJob and the job runner.
package job

import (
	"time"

	"github.com/sshamanov/pget/internal/cli"
)

// OutputMode describes where downloaded bytes are written.
type OutputMode = cli.OutputMode

// ContinueMode describes resume behavior.
type ContinueMode = cli.ContinueMode

// TimestampMode describes timestamp checking behavior.
type TimestampMode = cli.TimestampMode

// Re-export constants for convenience.
const (
	OutputFile   = cli.OutputFile
	OutputSingle = cli.OutputSingle
	OutputStdout = cli.OutputStdout

	ContinueNone = cli.ContinueNone
	ContinueAuto = cli.ContinueAuto

	TimestampNone  = cli.TimestampNone
	TimestampCheck = cli.TimestampCheck
)

// DownloadJob describes one URL to be downloaded, including all resolved policy.
type DownloadJob struct {
	SourceURL       string
	DisplayURL      string // sanitized
	DestPath        string // empty for stdout
	OutputMode      OutputMode
	OutputOffset    int64 // byte offset within concatenated output
	Connections     int
	SplitSize       int64
	BufferSize      int64
	ContinueMode    ContinueMode
	TimestampMode   TimestampMode
	NoClobber       bool
	ContentDisposition bool
	NoUseServerTimestamps bool
	Spider          bool
	MaxTries        int
	Timeout         time.Duration
	ConnectTimeout  time.Duration
	ReadTimeout     time.Duration
	RetryConnRefused bool
	RetryOnHTTPError []int
	UserAgent       string
	Referer         string
	ExtraHeaders    map[string]string
	InsecureSkipVerify bool
	FTPSImplicit    bool
	ProgressType    string // "bar", "dot", or ""
}
