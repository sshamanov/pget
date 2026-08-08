// Package progress provides terminal and non-terminal progress display.
package progress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

// termWidth returns the terminal width in columns, defaulting to 80.
func termWidth() int {
	if s := os.Getenv("COLUMNS"); s != "" {
		var w int
		if _, err := fmt.Sscanf(s, "%d", &w); err == nil && w > 0 {
			return w
		}
	}
	var ws struct {
		row, col, xpixel, ypixel uint16
	}
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, os.Stderr.Fd(), syscall.TIOCGWINSZ, uintptr(unsafe.Pointer(&ws))); err == 0 {
		if ws.col > 0 {
			return int(ws.col)
		}
	}
	return 80
}

// shortLabel truncates a label to maxLen characters, adding "…" when truncated.
func shortLabel(label string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if len(label) <= maxLen {
		return label
	}
	if maxLen <= 1 {
		return "…"
	}
	return label[:maxLen-1] + "…"
}

// spaces returns a string of n spaces.
func spaces(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat(" ", n)
}

// isatty checks if the given file descriptor is a terminal.
func isatty(fd uintptr) bool {
	var termios syscall.Termios
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, fd, syscall.TCGETS, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return err == 0
}

// Reporter formats and emits progress information.
type Reporter struct {
	quiet        bool
	noVerbose    bool
	progressType string
	isTTY        bool
	out          io.Writer
}

// NewReporter creates a progress reporter.
func NewReporter(quiet, noVerbose bool, progressType string) *Reporter {
	isTTY := isatty(os.Stderr.Fd())
	return &Reporter{
		quiet:        quiet,
		noVerbose:    noVerbose,
		progressType: progressType,
		isTTY:        isTTY,
		out:          os.Stderr,
	}
}

// Info prints an informational message (unless quiet or no-verbose).
func (r *Reporter) Info(format string, args ...interface{}) {
	if r.quiet || r.noVerbose {
		return
	}
	fmt.Fprintf(r.out, format+"\n", args...)
}

// Verbose prints a verbose diagnostic message.
func (r *Reporter) Verbose(format string, args ...interface{}) {
	if r.quiet {
		return
	}
	fmt.Fprintf(r.out, format+"\n", args...)
}

// Error prints an error message.
func (r *Reporter) Error(format string, args ...interface{}) {
	fmt.Fprintf(r.out, format+"\n", args...)
}

// ProgressBar renders a simple progress bar.
type ProgressBar struct {
	reporter        *Reporter
	total           int64
	current         int64
	startTime       time.Time
	lastPrint       time.Time
	lastNonTTYPrint time.Time
	label           string
	connections     int
	connFn          func() int // if non-nil, returns live active connection count
}

// NewProgressBar creates a progress bar for the given total size.
func NewProgressBar(reporter *Reporter, total int64, label string, connections int) *ProgressBar {
	return &ProgressBar{
		reporter:    reporter,
		total:       total,
		startTime:   time.Now(),
		label:       label,
		connections: connections,
	}
}

// SetConnFn sets a function that returns the live active connection count.
// When set, the live count is displayed instead of the static connections value.
func (p *ProgressBar) SetConnFn(fn func() int) {
	p.connFn = fn
}

// Update sets the current progress.
func (p *ProgressBar) Update(current int64) {
	p.current = current
	now := time.Now()
	if now.Sub(p.lastPrint) < 200*time.Millisecond && current < p.total {
		return
	}
	p.lastPrint = now
	p.render()
}

// Done marks the progress as complete and prints the final summary.
func (p *ProgressBar) Done() {
	if p.reporter.quiet {
		return
	}
	elapsed := time.Since(p.startTime)
	speed := float64(p.current) / elapsed.Seconds()
	status := p.completionStatus()
	if p.reporter.isTTY {
		p.render()
		fmt.Fprintf(p.reporter.out, "\n%s %s/%s in %s (%s)\n",
			status, formatSize(p.current), formatSize(p.total),
			formatDuration(elapsed), formatSpeed(speed))
	} else {
		fmt.Fprintf(p.reporter.out, "%s: %s %s/%s in %s (%s)\n",
			p.label, status, formatSize(p.current), formatSize(p.total),
			formatDuration(elapsed), formatSpeed(speed))
	}
}

func (p *ProgressBar) completionStatus() string {
	if p.current >= p.total {
		return "complete"
	}
	return "interrupted"
}

func (p *ProgressBar) render() {
	if p.reporter.quiet {
		return
	}

	if !p.reporter.isTTY {
		if p.current >= p.total {
			return
		}
		now := time.Now()
		if now.Sub(p.lastNonTTYPrint) < 3*time.Second {
			return
		}
		p.lastNonTTYPrint = now

		elapsed := time.Since(p.startTime)
		speed := float64(p.current) / elapsed.Seconds()
		ratio := float64(p.current) / float64(p.total)
		eta := "---"
		if speed > 0 {
			remaining := float64(p.total-p.current) / speed
			eta = formatDuration(time.Duration(remaining) * time.Second)
		}
		cn := p.connections
		if p.connFn != nil {
			cn = p.connFn()
		}
		fmt.Fprintf(p.reporter.out, "%s: %d%% %s/%s CN:%d %s ETA %s\n",
			p.label, int(ratio*100), formatSize(p.current), formatSize(p.total),
			cn, formatSpeed(speed), eta)
		return
	}

	// TTY: animated progress bar, sized to fit terminal width.
	ratio := float64(p.current) / float64(p.total)
	if p.total == 0 {
		ratio = 0
	}

	elapsed := time.Since(p.startTime)
	speed := float64(p.current) / elapsed.Seconds()

	pctStr := fmt.Sprintf("%d%%", int(ratio*100))
	sizeStr := fmt.Sprintf("%s/%s", formatSize(p.current), formatSize(p.total))
	speedStr := formatSpeed(speed)
	eta := "---"
	if speed > 0 {
		remaining := float64(p.total-p.current) / speed
		eta = formatDuration(time.Duration(remaining) * time.Second)
	}

	// Line layout: "\r" + label + " CN:" + conns + " [" + bar + "]" + stats
	// Stats part after bar: pct size speed ETA eta
	cn := p.connections
	if p.connFn != nil {
		cn = p.connFn()
	}
	connStr := fmt.Sprintf(" CN:%d", cn)
	statsPart := fmt.Sprintf(" %s %s %s ETA %s  ", pctStr, sizeStr, speedStr, eta)

	tw := termWidth()
	// Overhead: \r(1) + label + connStr + " ["(2) + "]"(1) + statsPart
	overhead := 1 + len(connStr) + 3 + len(statsPart)

	available := tw - overhead // space for label + bar

	barWidth := available
	if barWidth < 4 {
		barWidth = 4
	}
	if barWidth > 30 {
		barWidth = 30
	}

	maxLabel := available - barWidth
	label := shortLabel(p.label, maxLabel)

	filled := int(ratio * float64(barWidth))
	bar := make([]byte, barWidth)
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar[i] = '='
		} else if i == filled {
			bar[i] = '>'
		} else {
			bar[i] = ' '
		}
	}

	line := fmt.Sprintf("\r%s%s [%s]%s", label, connStr, string(bar), statsPart)
	if len(line) < tw {
		line += spaces(tw - len(line))
	}
	fmt.Fprint(p.reporter.out, line)
}

func formatSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fK", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func formatSpeed(bytesPerSec float64) string {
	switch {
	case bytesPerSec >= 1<<30:
		return fmt.Sprintf("%.1fGB/s", bytesPerSec/(1<<30))
	case bytesPerSec >= 1<<20:
		return fmt.Sprintf("%.1fMB/s", bytesPerSec/(1<<20))
	case bytesPerSec >= 1<<10:
		return fmt.Sprintf("%.1fKB/s", bytesPerSec/(1<<10))
	default:
		return fmt.Sprintf("%.0fB/s", bytesPerSec)
	}
}
