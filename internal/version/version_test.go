package version
import "testing"
func TestCommitFromPseudoVersion(t *testing.T) {
    tests := []struct{ in, want string }{
        {"v0.0.0-20260811145336-292a0e204ab6", "292a0e2"},
        {"v0.0.0-20210101000000-abcdefabcdef", "abcdefa"},
        {"v1.2.3", ""},
        {"(devel)", ""},
        {"", ""},
    }
    for _, tt := range tests {
        got := commitFromPseudoVersion(tt.in)
        if got != tt.want {
            t.Errorf("commitFromPseudoVersion(%q) = %q, want %q", tt.in, got, tt.want)
        }
    }
}
