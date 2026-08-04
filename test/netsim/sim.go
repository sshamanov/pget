// Package netsim provides network simulation for pget integration tests.
// It implements http.RoundTripper wrappers that can inject latency,
// bandwidth limits, stalls, and packet loss at the HTTP transport level.
package netsim

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Config configures network simulation for a single connection.
type Config struct {
	// Latency is the fixed delay added before reading the first byte.
	Latency time.Duration

	// Bandwidth limits read speed. Bytes are read in chunks with a delay
	// between chunks. Zero means unlimited.
	Bandwidth int64 // bytes per second

	// StallAfter stalls the connection after reading this many bytes.
	// A stalled connection blocks forever (or until context cancellation).
	StallAfter int64

	// StallDuration is how long to stall before resuming.
	// If zero, the stall is permanent until context cancellation.
	StallDuration time.Duration

	// ErrorAfter causes a read error after this many bytes.
	ErrorAfter int64

	// ErrorMessage is the error returned when ErrorAfter is exceeded.
	ErrorMessage string
}

// Transport is an http.RoundTripper that applies network simulation.
type Transport struct {
	// Base is the underlying transport (defaults to http.DefaultTransport).
	Base http.RoundTripper

	// Configs is a list of per-connection configs. Config[0] applies to
	// the first request, Config[1] to the second, etc. If there are more
	// requests than configs, the last config is reused.
	Configs []Config

	mu          sync.Mutex
	requestNum  int
	activeConns int
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	idx := t.requestNum
	if idx < len(t.Configs) {
		// use idx
	} else if len(t.Configs) > 0 {
		idx = len(t.Configs) - 1
	} else {
		idx = 0
	}
	t.requestNum++
	t.activeConns++
	t.mu.Unlock()

	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}

	resp, err := base.RoundTrip(req)
	if err != nil {
		t.mu.Lock()
		t.activeConns--
		t.mu.Unlock()
		return nil, err
	}

	var cfg Config
	t.mu.Lock()
	if idx < len(t.Configs) {
		cfg = t.Configs[idx]
	}
	t.mu.Unlock()

	// Wrap the response body with simulation.
	resp.Body = &simReader{
		rc:      resp.Body,
		cfg:     cfg,
		start:   time.Now(),
		connIdx: idx,
		onClose: func() {
			t.mu.Lock()
			t.activeConns--
			t.mu.Unlock()
		},
	}

	return resp, nil
}

// ActiveConns returns the current number of active connections.
func (t *Transport) ActiveConns() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.activeConns
}

// RequestCount returns the total number of requests made.
func (t *Transport) RequestCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requestNum
}

// simReader wraps a ReadCloser with simulated network conditions.
type simReader struct {
	rc      io.ReadCloser
	cfg     Config
	start   time.Time
	read    int64
	connIdx int
	onClose func()
}

func (r *simReader) Read(p []byte) (int, error) {
	// Apply initial latency on first read.
	if r.read == 0 && r.cfg.Latency > 0 {
		select {
		case <-time.After(r.cfg.Latency):
		}
	}

	// Check for stall.
	if r.cfg.StallAfter > 0 && r.read >= r.cfg.StallAfter {
		if r.cfg.StallDuration > 0 {
			time.Sleep(r.cfg.StallDuration)
			r.cfg.StallAfter += r.cfg.StallAfter // don't stall again
		} else {
			// Permanent stall — block forever.
			select {}
		}
	}

	// Check for error injection.
	if r.cfg.ErrorAfter > 0 && r.read >= r.cfg.ErrorAfter {
		msg := r.cfg.ErrorMessage
		if msg == "" {
			msg = "simulated network error"
		}
		return 0, fmt.Errorf("%s after %d bytes", msg, r.read)
	}

	// Apply bandwidth limiting.
	limit := 4096 // read at most 4KB at a time for simulation granularity
	if r.cfg.Bandwidth > 0 {
		if limit > len(p) {
			limit = len(p)
		}
	} else {
		limit = len(p)
	}

	n, err := r.rc.Read(p[:limit])
	r.read += int64(n)

	// Bandwidth throttle: sleep proportional to bytes read.
	if r.cfg.Bandwidth > 0 && n > 0 {
		expectedTime := time.Duration(float64(r.read) / float64(r.cfg.Bandwidth) * float64(time.Second))
		elapsed := time.Since(r.start)
		if expectedTime > elapsed {
			time.Sleep(expectedTime - elapsed)
		}
	}

	return n, err
}

func (r *simReader) Close() error {
	if r.onClose != nil {
		r.onClose()
	}
	return r.rc.Close()
}

// UniformConfig returns a slice with n identical configs.
func UniformConfig(cfg Config, n int) []Config {
	configs := make([]Config, n)
	for i := range configs {
		configs[i] = cfg
	}
	return configs
}

// JitterConfig returns configs with progressively increasing latency.
// Useful for testing how the scheduler handles heterogeneous connection speeds.
func JitterConfig(n int, baseLatency time.Duration) []Config {
	configs := make([]Config, n)
	rng := rand.New(rand.NewSource(42))
	for i := range configs {
		jitter := time.Duration(rng.Int63n(int64(baseLatency)))
		configs[i] = Config{
			Latency:   baseLatency + jitter,
			Bandwidth: 1024*1024 + rng.Int63n(10*1024*1024), // 1-11 MB/s
		}
	}
	return configs
}

// StallConfig creates configs where one specific connection stalls.
func StallConfig(n int, stallConn int) []Config {
	configs := make([]Config, n)
	for i := range configs {
		if i == stallConn {
			configs[i] = Config{StallAfter: 0} // permanent stall
		}
	}
	return configs
}

// ChunkStallConfig creates configs where the chunk at stallConn stalls.
func ChunkStallConfig(n int, stallConn int, stallDuration time.Duration) []Config {
	configs := make([]Config, n)
	for i := range configs {
		if i == stallConn {
			configs[i] = Config{
				StallAfter:    4096, // stall partway through
				StallDuration: stallDuration,
			}
		}
	}
	return configs
}

// SlowConnConfig makes one connection substantially slower than others.
func SlowConnConfig(n int, slowConn int, bandwidth int64) []Config {
	configs := make([]Config, n)
	for i := range configs {
		if i == slowConn {
			configs[i] = Config{Bandwidth: bandwidth}
		}
	}
	return configs
}
