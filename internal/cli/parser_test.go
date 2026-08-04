package cli

import (
	"reflect"
	"testing"
)

func TestParse_EmptyArgs(t *testing.T) {
	plan, err := Parse([]string{})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(plan.URLs) != 0 {
		t.Errorf("expected 0 URLs, got %d", len(plan.URLs))
	}
}

func TestParse_SimpleURL(t *testing.T) {
	plan, err := Parse([]string{"https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(plan.URLs) != 1 {
		t.Fatalf("expected 1 URL, got %d", len(plan.URLs))
	}
	if plan.URLs[0] != "https://example.org/file.iso" {
		t.Errorf("URL = %s", plan.URLs[0])
	}
}

func TestParse_MultipleURLs(t *testing.T) {
	plan, err := Parse([]string{"https://a", "https://b", "https://c"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(plan.URLs) != 3 {
		t.Fatalf("expected 3 URLs, got %d", len(plan.URLs))
	}
}

func TestParse_OptionsAfterURLs(t *testing.T) {
	plan, err := Parse([]string{"https://a", "-q", "-c"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !plan.Quiet {
		t.Error("expected quiet mode")
	}
	if plan.ContinueMode != ContinueAuto {
		t.Error("expected continue mode")
	}
	if len(plan.URLs) != 1 {
		t.Errorf("expected 1 URL, got %d", len(plan.URLs))
	}
}

func TestParse_OutputDocument(t *testing.T) {
	plan, err := Parse([]string{"-O", "image.iso", "https://example.org/image.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.OutputFile != "image.iso" {
		t.Errorf("OutputFile = %s, want image.iso", plan.OutputFile)
	}
	if plan.OutputMode != OutputSingle {
		t.Errorf("OutputMode = %v, want OutputSingle", plan.OutputMode)
	}
}

func TestParse_StdoutOutput(t *testing.T) {
	plan, err := Parse([]string{"-O", "-", "https://example.org/image.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.OutputMode != OutputStdout {
		t.Errorf("OutputMode = %v, want OutputStdout", plan.OutputMode)
	}
}

func TestParse_Continue(t *testing.T) {
	plan, err := Parse([]string{"-c", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.ContinueMode != ContinueAuto {
		t.Error("expected continue mode")
	}
}

func TestParse_NoClobber(t *testing.T) {
	plan, err := Parse([]string{"-nc", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !plan.NoClobber {
		t.Error("expected no-clobber mode")
	}
}

func TestParse_Timestamping(t *testing.T) {
	plan, err := Parse([]string{"-N", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.TimestampMode != TimestampCheck {
		t.Errorf("TimestampMode = %v, want TimestampCheck", plan.TimestampMode)
	}
}

func TestParse_ParallelSettings(t *testing.T) {
	plan, err := Parse([]string{
		"--connections=4",
		"--split-size=16M",
		"--buffer-size=256M",
		"https://example.org/file.iso",
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.Connections != 4 {
		t.Errorf("Connections = %d, want 4", plan.Connections)
	}
	if plan.SplitSize != 16<<20 {
		t.Errorf("SplitSize = %d, want %d", plan.SplitSize, 16<<20)
	}
	if plan.BufferSize != 256<<20 {
		t.Errorf("BufferSize = %d, want %d", plan.BufferSize, 256<<20)
	}
}

func TestParse_NoParallel(t *testing.T) {
	plan, err := Parse([]string{"--no-parallel", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.Connections != 1 {
		t.Errorf("Connections = %d, want 1 (no-parallel forces 1)", plan.Connections)
	}
}

func TestParse_Tries(t *testing.T) {
	plan, err := Parse([]string{"-t", "5", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.MaxTries != 5 {
		t.Errorf("MaxTries = %d, want 5", plan.MaxTries)
	}
}

func TestParse_Timeouts(t *testing.T) {
	plan, err := Parse([]string{
		"-T", "30",
		"--connect-timeout=10",
		"--read-timeout=60",
		"https://example.org/file.iso",
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.Timeout.Seconds() != 30 {
		t.Errorf("Timeout = %v, want 30s", plan.Timeout)
	}
}

func TestParse_Headers(t *testing.T) {
	plan, err := Parse([]string{
		"--header", "Accept: application/json",
		"--header", "X-Custom: value",
		"https://example.org/file.iso",
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.ExtraHeaders["Accept"] != "application/json" {
		t.Errorf("Accept header = %s", plan.ExtraHeaders["Accept"])
	}
	if plan.ExtraHeaders["X-Custom"] != "value" {
		t.Errorf("X-Custom header = %s", plan.ExtraHeaders["X-Custom"])
	}
}

func TestParse_CombinedShortFlags(t *testing.T) {
	plan, err := Parse([]string{"-qcv", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !plan.Quiet {
		t.Error("expected quiet mode")
	}
	if plan.ContinueMode != ContinueAuto {
		t.Error("expected continue mode")
	}
	if !plan.Verbose {
		t.Error("expected verbose mode")
	}
}

func TestParse_LongEqualsSyntax(t *testing.T) {
	plan, err := Parse([]string{"--connections=16", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.Connections != 16 {
		t.Errorf("Connections = %d, want 16", plan.Connections)
	}
}

func TestParse_DoubleDashEndsOptions(t *testing.T) {
	plan, err := Parse([]string{"-q", "--", "-not-a-flag"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !plan.Quiet {
		t.Error("expected quiet mode")
	}
	if len(plan.URLs) != 1 {
		t.Fatalf("expected 1 positional arg, got %d", len(plan.URLs))
	}
	if plan.URLs[0] != "-not-a-flag" {
		t.Errorf("URL = %s", plan.URLs[0])
	}
}

func TestParse_Defaults(t *testing.T) {
	plan, err := Parse([]string{"https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.Connections != defaultConnections {
		t.Errorf("default connections = %d, want %d", plan.Connections, defaultConnections)
	}
	if plan.SplitSize != defaultSplitSize {
		t.Errorf("default split size = %d, want %d", plan.SplitSize, defaultSplitSize)
	}
	if plan.BufferSize != defaultBufferSize {
		t.Errorf("default buffer size = %d, want %d", plan.BufferSize, defaultBufferSize)
	}
	if plan.MaxTries != defaultMaxTries {
		t.Errorf("default tries = %d, want %d", plan.MaxTries, defaultMaxTries)
	}
}

func TestParse_BackgroundStdoutRejected(t *testing.T) {
	_, err := Parse([]string{"-b", "-O", "-", "https://example.org/file.iso"})
	if err == nil {
		t.Fatal("expected error for -b -O - combination")
	}
}

func TestParse_ContinueStdoutRejected(t *testing.T) {
	_, err := Parse([]string{"-c", "-O", "-", "https://example.org/file.iso"})
	if err == nil {
		t.Fatal("expected error for -c -O - combination")
	}
}

func TestParse_BufferSmallerThanSplitRejected(t *testing.T) {
	_, err := Parse([]string{"--split-size=100M", "--buffer-size=10M", "https://example.org/file.iso"})
	if err == nil {
		t.Fatal("expected error for buffer < split")
	}
}

func TestParse_InvalidOption(t *testing.T) {
	_, err := Parse([]string{"--nonexistent", "https://example.org/file.iso"})
	if err == nil {
		t.Fatal("expected error for invalid option")
	}
}

func TestParse_ShortOptionWithArgumentFromNextArg(t *testing.T) {
	plan, err := Parse([]string{"-o", "log.txt", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.LogFile != "log.txt" {
		t.Errorf("LogFile = %s, want log.txt", plan.LogFile)
	}
}

func TestParse_ShortOptionWithArgumentAppended(t *testing.T) {
	plan, err := Parse([]string{"-olog.txt", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plan.LogFile != "log.txt" {
		t.Errorf("LogFile = %s, want log.txt", plan.LogFile)
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"100", 100},
		{"1K", 1024},
		{"1KB", 1024},
		{"1M", 1024 * 1024},
		{"1MB", 1024 * 1024},
		{"8M", 8 * 1024 * 1024},
		{"1G", 1024 * 1024 * 1024},
		{"1GiB", 1024 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSize(tt.input)
			if err != nil {
				t.Fatalf("parseSize(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("parseSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParse_RetryOnHTTPError(t *testing.T) {
	plan, err := Parse([]string{"--retry-on-http-error=500,502,503", "https://example.org/file.iso"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := []int{500, 502, 503}
	if !reflect.DeepEqual(plan.RetryOnHTTPError, want) {
		t.Errorf("RetryOnHTTPError = %v, want %v", plan.RetryOnHTTPError, want)
	}
}
