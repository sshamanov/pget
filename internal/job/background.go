package job

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

const childEnvMarker = "PGET_BACKGROUND_CHILD=1"

// IsBackgroundChild reports whether this process is the background child.
func IsBackgroundChild() bool {
	return os.Getenv("PGET_BACKGROUND_CHILD") == "1"
}

// DetachToBackground spawns a detached child process and exits the parent.
// The child runs the same executable with the same arguments plus the child marker.
// This function does not return in the parent process.
func DetachToBackground() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find executable: %w", err)
	}

	// Build child args: same args, skip background flag.
	var args []string
	for _, a := range os.Args[1:] {
		if a != "-b" && a != "--background" {
			args = append(args, a)
		}
	}

	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), childEnvMarker)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	// Detach: new session, no controlling terminal.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start background child: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Continuing in background, pid %d.\n", cmd.Process.Pid)
	fmt.Fprintf(os.Stderr, "Output will be written to 'wget-log'.\n")
	os.Exit(0)
	return nil // unreachable
}
