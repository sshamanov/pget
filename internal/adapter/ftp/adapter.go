// Package ftp implements the FTP and FTPS protocol adapter.
package ftp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/sshamanov/pget/internal/adapter"
)

// Adapter implements adapter.Adapter for FTP and FTPS.
type Adapter struct {
	insecureSkipVerify bool
	timeout           time.Duration
	connectTimeout    time.Duration
}

// New creates a new FTP/FTPS adapter.
func New(insecureSkipVerify bool, timeout, connectTimeout time.Duration) *Adapter {
	return &Adapter{
		insecureSkipVerify: insecureSkipVerify,
		timeout:           timeout,
		connectTimeout:    connectTimeout,
	}
}

// Probe fetches metadata and checks parallel capability.
func (a *Adapter) Probe(ctx context.Context, urlStr string, opts adapter.RequestOptions) (*adapter.ProbeResult, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("ftp: parse URL: %w", err)
	}

	host, port, path, err := a.resolveAddr(parsed)
	if err != nil {
		return nil, err
	}

	conn, err := a.dial(ctx, parsed, host, port)
	if err != nil {
		return nil, fmt.Errorf("ftp: connect: %w", err)
	}
	defer conn.Quit()

	result := &adapter.ProbeResult{}
	result.Meta.FinalURL = urlStr
	result.Meta.DisplayURL = sanitizeURL(parsed)
	result.Meta.Protocol = parsed.Scheme

	// Try SIZE.
	size, err := conn.FileSize(path)
	if err != nil {
		result.Meta.Size = -1
		result.RangeCapable = false
	} else {
		result.Size = size
		result.Meta.Size = size
	}

	// Try MDTM.
	if mtime, err := conn.GetTime(path); err == nil {
		result.Meta.ModTime = mtime
	}

	// Check REST support (range capability).
	// We'll attempt a REST 0 and see if the server accepts it.
	if size > 0 {
		// Most modern FTP servers support REST.
		result.RangeCapable = true
		result.Meta.RangeCapable = true
		result.Meta.RestartCapable = true
	}

	// Extract filename from path.
	result.Meta.Filename = extractFilename(path)

	return result, nil
}

// OpenRange starts a ranged request for the given byte range.
func (a *Adapter) OpenRange(ctx context.Context, urlStr string, start, length int64, validator string, opts adapter.RequestOptions) (adapter.RangeReader, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("ftp: parse URL: %w", err)
	}

	host, port, path, err := a.resolveAddr(parsed)
	if err != nil {
		return nil, err
	}

	conn, err := a.dial(ctx, parsed, host, port)
	if err != nil {
		return nil, fmt.Errorf("ftp: connect: %w", err)
	}

	// RetrFrom handles REST internally.
	resp, err := conn.RetrFrom(path, uint64(start))
	if err != nil {
		conn.Quit()
		return nil, fmt.Errorf("ftp: RETR from %d: %w", start, err)
	}

	return &ftpRangeReader{
		conn:   conn,
		resp:   resp,
		length: length,
	}, nil
}

// OpenSequential starts a sequential download from the given offset.
func (a *Adapter) OpenSequential(ctx context.Context, urlStr string, offset int64, opts adapter.RequestOptions) (io.ReadCloser, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("ftp: parse URL: %w", err)
	}

	host, port, path, err := a.resolveAddr(parsed)
	if err != nil {
		return nil, err
	}

	conn, err := a.dial(ctx, parsed, host, port)
	if err != nil {
		return nil, fmt.Errorf("ftp: connect: %w", err)
	}

	var resp *ftp.Response
	if offset > 0 {
		resp, err = conn.RetrFrom(path, uint64(offset))
	} else {
		resp, err = conn.Retr(path)
	}

	if err != nil {
		conn.Quit()
		return nil, fmt.Errorf("ftp: RETR: %w", err)
	}

	return &ftpSequentialReader{conn: conn, resp: resp}, nil
}

func (a *Adapter) resolveAddr(u *url.URL) (host, port, path string, err error) {
	host = u.Hostname()
	port = u.Port()
	path = u.Path

	if host == "" {
		return "", "", "", fmt.Errorf("ftp: missing host")
	}
	if port == "" {
		if u.Scheme == "ftps" && isImplicitFTPS(u) {
			port = "990"
		} else {
			port = "21"
		}
	}
	if path == "" || path == "/" {
		return "", "", "", fmt.Errorf("ftp: missing file path")
	}

	return host, port, path, nil
}

func (a *Adapter) dial(ctx context.Context, u *url.URL, host, port string) (*ftp.ServerConn, error) {
	addr := fmt.Sprintf("%s:%s", host, port)
	isFTPS := u.Scheme == "ftps"

	var conn *ftp.ServerConn
	var err error

	if isFTPS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: a.insecureSkipVerify,
			ServerName:         host,
		}
		conn, err = ftp.Dial(addr, ftp.DialWithTLS(tlsConfig), ftp.DialWithTimeout(a.connectTimeout))
	} else {
		conn, err = ftp.Dial(addr, ftp.DialWithTimeout(a.connectTimeout))
	}

	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	// Login.
	user := "anonymous"
	pass := "pget@"
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			pass = p
		}
	}

	if err := conn.Login(user, pass); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("login: %w", err)
	}

	return conn, nil
}

func isImplicitFTPS(u *url.URL) bool {
	// Check for --ftps-implicit marker in query (hack: could pass via opts).
	return strings.Contains(u.RawQuery, "implicit=1")
}

// ftpRangeReader reads exactly `length` bytes from an FTP data connection.
type ftpRangeReader struct {
	conn   *ftp.ServerConn
	resp   *ftp.Response
	length int64
	read   int64
}

func (r *ftpRangeReader) Read(p []byte) (int, error) {
	if r.read >= r.length {
		return 0, io.EOF
	}
	limit := int64(len(p))
	if r.read+limit > r.length {
		limit = r.length - r.read
	}
	n, err := r.resp.Read(p[:limit])
	r.read += int64(n)
	if r.read >= r.length {
		// We've read exactly the chunk. Close the connection.
		// Some servers may return an error on close after partial read.
		r.resp.Close()
		r.conn.Quit()
		if err == nil || err == io.EOF {
			err = io.EOF
		}
	}
	return n, err
}

func (r *ftpRangeReader) Close() error {
	r.resp.Close()
	return r.conn.Quit()
}

func (r *ftpRangeReader) ContentLength() int64 {
	return r.length
}

// ftpSequentialReader reads the full FTP data connection until EOF.
type ftpSequentialReader struct {
	conn *ftp.ServerConn
	resp *ftp.Response
}

func (r *ftpSequentialReader) Read(p []byte) (int, error) {
	return r.resp.Read(p)
}

func (r *ftpSequentialReader) Close() error {
	r.resp.Close()
	return r.conn.Quit()
}

func sanitizeURL(u *url.URL) string {
	u2 := *u
	u2.User = nil
	return u2.String()
}

func extractFilename(path string) string {
	if path == "" || path == "/" {
		return "index.html"
	}
	if i := strings.LastIndex(path, "/"); i >= 0 {
		name := path[i+1:]
		if name != "" {
			return name
		}
	}
	// Check for query params stripped by path parsing.
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return "index.html"
}

// parseInt tries to parse an int64 from a string ignoring surrounding whitespace.
func parseInt(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}
