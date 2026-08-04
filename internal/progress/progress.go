// Package progress provides terminal and non-terminal progress display.
package progress

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// isatty checks if the given file descriptor is a terminal.
func isatty(fd uintptr) bool {
	var termios syscall.Termios
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, fd, syscall.TCGETS, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return err == 0
}

// Reporter formats and emits progress information.
type Reporter struct {
	quiet    bool
	noVerbose bool
	progressType string
	isTTY    bool
	out      io.Writer
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
	reporter  *Reporter
	total     int64
	current   int64
	startTime time.Time
	lastPrint time.Time
	label     string
}

// NewProgressBar creates a progress bar for the given total size.
func NewProgressBar(reporter *Reporter, total int64, label string) *ProgressBar {
	return &ProgressBar{
		reporter:  reporter,
		total:     total,
		startTime: time.Now(),
		label:     label,
	}
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

// Done marks the progress as complete.
func (p *ProgressBar) Done() {
	p.current = p.total
	p.render()
	if p.reporter.isTTY {
		fmt.Fprintln(p.reporter.out)
	}
}

func (p *ProgressBar) render() {
	if p.reporter.quiet {
		return
	}

	if !p.reporter.isTTY {
		// Non-TTY: periodic line output.
		if p.current == p.total {
			fmt.Fprintf(p.reporter.out, "%s: complete (%d bytes)\n", p.label, p.total)
		}
		return
	}

	// TTY: progress bar.
	ratio := float64(p.current) / float64(p.total)
	if p.total == 0 {
		ratio = 0
	}
	width := 30
	filled := int(ratio * float64(width))

	elapsed := time.Since(p.startTime)
	speed := float64(p.current) / elapsed.Seconds()

	bar := ""
	for i := 0; i < width; i++ {
		if i < filled {
			bar += "="
		} else if i == filled {
			bar += ">"
		} else {
			bar += " "
		}
	}

	eta := "---"
	if speed > 0 {
		remaining := float64(p.total-p.current) / speed
		eta = formatDuration(time.Duration(remaining) * time.Second)
	}

	fmt.Fprintf(p.reporter.out, "\r%s [%s] %d%% %s %s  ",
		p.label, bar, int(ratio*100), formatSize(p.current), eta)
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
