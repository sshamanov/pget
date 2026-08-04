package http

import (
	"context"
	"io"
	gohttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sshamanov/pget/internal/adapter"
)

func TestProbe_RangeSupported(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Content-Range", "bytes 0-0/1024")
			w.Header().Set("ETag", `"abc123"`)
			w.WriteHeader(gohttp.StatusPartialContent)
			w.Write([]byte{0})
		} else {
			w.WriteHeader(gohttp.StatusOK)
		}
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	result, err := a.Probe(ctx, srv.URL, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !result.RangeCapable {
		t.Error("expected RangeCapable=true")
	}
	if result.Size != 1024 {
		t.Errorf("Size = %d, want 1024", result.Size)
	}
	if result.ETag != `"abc123"` {
		t.Errorf("ETag = %s, want \"abc123\"", result.ETag)
	}
}

func TestProbe_RangeIgnored(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		// Server ignores Range header, returns 200 with full body.
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(gohttp.StatusOK)
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	result, err := a.Probe(ctx, srv.URL, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if result.RangeCapable {
		t.Error("expected RangeCapable=false when server ignores range")
	}
	if result.Size != 1024 {
		t.Errorf("Size = %d, want 1024", result.Size)
	}
}

func TestProbe_WeakETag(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/1024")
		w.Header().Set("ETag", `W/"abc123"`)
		w.WriteHeader(gohttp.StatusPartialContent)
		w.Write([]byte{0})
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	result, err := a.Probe(ctx, srv.URL, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	// Weak ETag alone is NOT sufficient — but with Last-Modified, it is.
	// Here there's no Last-Modified, so range should be disabled.
	if result.RangeCapable {
		t.Error("expected RangeCapable=false with only weak ETag")
	}
}

func TestProbe_LastModifiedValidator(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/2048")
		w.Header().Set("Last-Modified", "Mon, 03 Aug 2026 12:00:00 GMT")
		w.WriteHeader(gohttp.StatusPartialContent)
		w.Write([]byte{0})
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	result, err := a.Probe(ctx, srv.URL, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !result.RangeCapable {
		t.Error("expected RangeCapable=true with Last-Modified")
	}
	if result.LastModified != "Mon, 03 Aug 2026 12:00:00 GMT" {
		t.Errorf("LastModified = %s", result.LastModified)
	}
}

func TestProbe_NoValidator(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/1024")
		// No ETag, no Last-Modified — no usable validator.
		w.WriteHeader(gohttp.StatusPartialContent)
		w.Write([]byte{0})
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	result, err := a.Probe(ctx, srv.URL, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if result.RangeCapable {
		t.Error("expected RangeCapable=false without validator")
	}
}

func TestOpenRange_Success(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		w.Header().Set("Content-Range", "bytes 100-199/1024")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(gohttp.StatusPartialContent)
		data := make([]byte, 100)
		for i := range data {
			data[i] = byte(i % 256)
		}
		w.Write(data)
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	rr, err := a.OpenRange(ctx, srv.URL, 100, 100, "", adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("OpenRange: %v", err)
	}
	defer rr.Close()

	if rr.ContentLength() != 100 {
		t.Errorf("ContentLength = %d, want 100", rr.ContentLength())
	}

	data, err := io.ReadAll(rr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(data) != 100 {
		t.Errorf("read %d bytes, want 100", len(data))
	}
}

func TestOpenRange_InvalidContentRange(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		// Server sends 206 but with wrong range.
		w.Header().Set("Content-Range", "bytes 0-99/1024") // requested 100-199
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(gohttp.StatusPartialContent)
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	_, err := a.OpenRange(ctx, srv.URL, 100, 100, "", adapter.RequestOptions{})
	if err == nil {
		t.Fatal("expected error for mismatched Content-Range")
	}
}

func TestOpenSequential(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		w.WriteHeader(gohttp.StatusOK)
		w.Write([]byte("hello world"))
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	rc, err := a.OpenSequential(ctx, srv.URL, 0, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("OpenSequential: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q, want %q", data, "hello world")
	}
}

func TestSanitizeURL(t *testing.T) {
	req, err := gohttp.NewRequest(gohttp.MethodGet, "https://user:pass@example.org/path", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := sanitizeURL(req.URL)
	if strings.Contains(result, "user") || strings.Contains(result, "pass") {
		t.Errorf("sanitized URL should not contain credentials: %s", result)
	}
}

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"normal.txt", "normal.txt"},
		{"path/to/file.txt", "path_to_file.txt"},
		{".", "index.html"},
		{"..", "index.html"},
		{"", "index.html"},
		{"file\x00name.txt", "filename.txt"},
	}

	for _, tt := range tests {
		got := sanitizeFilename(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseContentRangeTotal(t *testing.T) {
	tests := []struct {
		input string
		want  int64
		ok    bool
	}{
		{"bytes 0-0/1024", 1024, true},
		{"bytes 0-0/0", 0, false},
		{"", 0, false},
		{"invalid", 0, false},
	}

	for _, tt := range tests {
		got, ok := parseContentRangeTotal(tt.input)
		if ok != tt.ok || got != tt.want {
			t.Errorf("parseContentRangeTotal(%q) = (%d, %v), want (%d, %v)",
				tt.input, got, ok, tt.want, tt.ok)
		}
	}
}

func TestValidateContentRange(t *testing.T) {
	if !validateContentRange("bytes 100-199/1024", 100, 199) {
		t.Error("expected valid match")
	}
	if validateContentRange("bytes 0-99/1024", 100, 199) {
		t.Error("expected mismatch")
	}
	if validateContentRange("", 0, 0) {
		t.Error("expected invalid for empty")
	}
}

func TestIsStrongETag(t *testing.T) {
	if !isStrongETag(`"abc"`) {
		t.Error(`"abc" should be strong`)
	}
	if isStrongETag(`W/"abc"`) {
		t.Error(`W/"abc" should be weak`)
	}
	if isStrongETag("") {
		t.Error("empty should not be strong")
	}
}

func TestParseHTTPTime(t *testing.T) {
	_, err := parseHTTPTime("Mon, 03 Aug 2026 12:00:00 GMT")
	if err != nil {
		t.Fatalf("parseHTTPTime: %v", err)
	}

	_, err = parseHTTPTime("not a date")
	if err == nil {
		t.Error("expected error for invalid date")
	}
}

func TestProbe_Redirect(t *testing.T) {
	// Create a server that redirects to another server.
	targetSrv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Content-Range", "bytes 0-0/512")
			w.Header().Set("ETag", `"def456"`)
			w.WriteHeader(gohttp.StatusPartialContent)
			w.Write([]byte{0})
		} else {
			w.WriteHeader(gohttp.StatusOK)
		}
	}))
	defer targetSrv.Close()

	redirectSrv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		gohttp.Redirect(w, r, targetSrv.URL, gohttp.StatusFound)
	}))
	defer redirectSrv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	result, err := a.Probe(ctx, redirectSrv.URL, adapter.RequestOptions{})
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !result.RangeCapable {
		t.Error("expected RangeCapable after redirect")
	}
	if result.Meta.FinalURL != targetSrv.URL {
		t.Errorf("FinalURL = %s, want %s", result.Meta.FinalURL, targetSrv.URL)
	}
}

func TestProbe_CustomHeaders(t *testing.T) {
	var receivedUA string
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		receivedUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Range", "bytes 0-0/100")
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(gohttp.StatusPartialContent)
		w.Write([]byte{0})
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx := context.Background()
	opts := adapter.RequestOptions{
		UserAgent: "pget/1.0",
		Referer:   "https://example.org",
		Headers:   map[string]string{"X-Custom": "value"},
	}
	_, err := a.Probe(ctx, srv.URL, opts)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if receivedUA != "pget/1.0" {
		t.Errorf("User-Agent = %s, want pget/1.0", receivedUA)
	}
}

func TestContextCancellation(t *testing.T) {
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	a := New(false, 0, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := a.Probe(ctx, srv.URL, adapter.RequestOptions{})
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}
