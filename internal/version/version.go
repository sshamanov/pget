// Package version provides build-time version information.
package version

import (
	"fmt"
	"runtime/debug"
	"strings"
)

var (
	Version = "dev"
	Commit  = "unknown"
)

func init() {
	// When built via "go install" (no ldflags), read embedded build info.
	if Version != "dev" {
		return
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		if rev, ok := vcsRevision(info); ok {
			Commit = rev
		}
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			Version = info.Main.Version
		}
	}
}

// String returns a human-readable version string.
func String() string {
	// CI build with ldflags — show version and commit.
	if Version != "dev" && !strings.HasPrefix(Version, "v0.0.0-") {
		if Commit != "unknown" {
			return fmt.Sprintf("pget %s (%s)", Version, Commit)
		}
		return fmt.Sprintf("pget %s", Version)
	}
	// Local build with VCS info — show commit only.
	if Commit != "unknown" {
		return fmt.Sprintf("pget %s", Commit)
	}
	return "pget dev (unknown)"
}

func vcsRevision(info *debug.BuildInfo) (string, bool) {
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			rev := s.Value
			if len(rev) > 7 {
				rev = rev[:7]
			}
			return rev, true
		}
	}
	return "", false
}
