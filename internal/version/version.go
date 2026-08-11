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
		// VCS revision from local git checkout.
		if rev, ok := vcsRevision(info); ok {
			Commit = rev
		} else if rev := commitFromPseudoVersion(info.Main.Version); rev != "" {
			// go install via proxy: no .git, but pseudo-version embeds the hash.
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
	// Local/git or go-install build — show commit only.
	if Commit != "unknown" {
		return fmt.Sprintf("pget %s", Commit)
	}
	return "pget dev (unknown)"
}

// vcsRevision extracts the short git revision from Go's embedded build settings.
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

// commitFromPseudoVersion extracts the commit hash from a Go pseudo-version.
// Pseudo-versions have the form "v0.0.0-yyyymmddhhmmss-abcdefabcdef".
func commitFromPseudoVersion(v string) string {
	if !strings.HasPrefix(v, "v0.0.0-") {
		return ""
	}
	// Last segment is the 12-char commit hash.
	if idx := strings.LastIndex(v, "-"); idx >= 0 {
		hash := v[idx+1:]
		if len(hash) >= 7 {
			return hash[:7]
		}
	}
	return ""
}
