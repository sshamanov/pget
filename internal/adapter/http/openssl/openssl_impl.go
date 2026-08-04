//go:build openssl

// Package openssl provides an OpenSSL-backed TLS connection via cgo.
package openssl

/*
#cgo LDFLAGS: -lssl -lcrypto
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <errno.h>
#include <string.h>

static SSL_CTX* new_ssl_ctx() {
	SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
	if (!ctx) return NULL;
	SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION);
	SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
	return ctx;
}

static SSL* new_ssl(SSL_CTX *ctx, int fd) {
	SSL *ssl = SSL_new(ctx);
	if (!ssl) return NULL;
	SSL_set_fd(ssl, fd);
	SSL_set_tlsext_host_name(ssl, "");
	return ssl;
}

static int set_sni(SSL *ssl, const char *name) {
	return SSL_set_tlsext_host_name(ssl, name);
}

static int get_errno() {
	return errno;
}

#include <fcntl.h>
static int set_blocking(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1) return -1;
	return fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}
*/
import "C"

import (
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// IsAvailable reports whether the OpenSSL backend is compiled in.
const IsAvailable = true

func init() {
	// Verify we can load OpenSSL
	if C.new_ssl_ctx() == nil {
		panic("openssl: failed to create SSL context")
	}
}

// Conn wraps an OpenSSL TLS connection.
type Conn struct {
	conn net.Conn
	ssl  *C.SSL

	// Guards SSL_read and SSL_write (OpenSSL requires serialization
	// on the same SSL object, though concurrent read/write is OK).
	readMu  sync.Mutex
	writeMu sync.Mutex
}

// Dial connects to addr using OpenSSL TLS, verifying the certificate if verify is true.
func Dial(network, addr, serverName string, insecure bool) (*Conn, error) {
	raw, err := net.Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("openssl: dial %s: %w", addr, err)
	}

	// Duplicate the fd so OpenSSL owns its own copy and doesn't conflict
	// with Go's runtime network poller.
	dupFD, err := dupFD(raw)
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("openssl: dup fd: %w", err)
	}

	// Go sets sockets to non-blocking. OpenSSL needs blocking mode.
	if C.set_blocking(C.int(dupFD)) != 0 {
		syscall.Close(dupFD)
		raw.Close()
		return nil, fmt.Errorf("openssl: fcntl set_blocking failed")
	}

	ctx := C.new_ssl_ctx()
	if ctx == nil {
		syscall.Close(dupFD)
		raw.Close()
		return nil, fmt.Errorf("openssl: SSL_CTX_new failed")
	}

	ssl := C.new_ssl(ctx, C.int(dupFD))
	if ssl == nil {
		C.SSL_CTX_free(ctx)
		syscall.Close(dupFD)
		raw.Close()
		return nil, fmt.Errorf("openssl: SSL_new failed")
	}

	// Set SNI.
	if serverName != "" {
		cName := C.CString(serverName)
		C.set_sni(ssl, cName)
		C.free(unsafe.Pointer(cName))
	}

	if insecure {
		C.SSL_set_verify(ssl, C.SSL_VERIFY_NONE, nil)
	}

retry:
	ret := C.SSL_connect(ssl)
	if ret != 1 {
		errCode := C.SSL_get_error(ssl, ret)
		switch errCode {
		case C.SSL_ERROR_SYSCALL:
			if ret == 0 {
				err = fmt.Errorf("openssl: SSL_connect: EOF")
			} else {
				err = fmt.Errorf("openssl: SSL_connect: syscall error")
			}
		case C.SSL_ERROR_SSL:
			err = fmt.Errorf("openssl: SSL_connect: %s", sslError())
		default:
			goto retry
		}
		C.SSL_free(ssl)
		C.SSL_CTX_free(ctx)
		syscall.Close(dupFD)
		raw.Close()
		return nil, err
	}

	// Close Go's original connection — OpenSSL owns the dup'd fd now.
	raw.Close()

	return &Conn{
		conn: &fdConn{fd: dupFD, addr: raw.RemoteAddr()},
		ssl:  ssl,
	}, nil
}

// Read reads data from the TLS connection.
func (c *Conn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if c.ssl == nil {
		return 0, io.EOF
	}

retry:
	n := C.SSL_read(c.ssl, unsafe.Pointer(&b[0]), C.int(len(b)))
	if n > 0 {
		return int(n), nil
	}

	errCode := C.SSL_get_error(c.ssl, n)
	switch errCode {
	case C.SSL_ERROR_ZERO_RETURN:
		return 0, io.EOF
	case C.SSL_ERROR_WANT_READ, C.SSL_ERROR_WANT_WRITE:
		// Non-blocking retry; in blocking mode this shouldn't happen,
		// but handle it gracefully.
		goto retry
	case C.SSL_ERROR_SYSCALL:
		if n == 0 {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("openssl: read: syscall error (errno: %d)", sslErrno())
	case C.SSL_ERROR_SSL:
		return 0, fmt.Errorf("openssl: read: %s", sslError())
	default:
		goto retry
	}
}

// Write writes data to the TLS connection.
func (c *Conn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.ssl == nil {
		return 0, fmt.Errorf("openssl: write: connection closed")
	}

retry:
	n := C.SSL_write(c.ssl, unsafe.Pointer(&b[0]), C.int(len(b)))
	if n > 0 {
		return int(n), nil
	}

	errCode := C.SSL_get_error(c.ssl, n)
	switch errCode {
	case C.SSL_ERROR_WANT_READ, C.SSL_ERROR_WANT_WRITE:
		goto retry
	case C.SSL_ERROR_SYSCALL:
		return 0, fmt.Errorf("openssl: write: syscall error")
	case C.SSL_ERROR_SSL:
		return 0, fmt.Errorf("openssl: write: %s", sslError())
	case C.SSL_ERROR_ZERO_RETURN:
		return 0, io.EOF
	default:
		goto retry
	}
}

// Close shuts down the TLS connection.
func (c *Conn) Close() error {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if c.ssl == nil {
		return nil
	}

	C.SSL_shutdown(c.ssl)
	C.SSL_free(c.ssl)
	c.ssl = nil
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// socketFD extracts the file descriptor from a net.Conn.
func socketFD(conn net.Conn) int {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		panic(fmt.Sprintf("openssl: cannot get raw fd from %T", conn))
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		panic(fmt.Sprintf("openssl: SyscallConn: %v", err))
	}
	var fd int
	err = rc.Control(func(f uintptr) {
		fd = int(f)
	})
	if err != nil {
		panic(fmt.Sprintf("openssl: Control: %v", err))
	}
	return fd
}

// dupFD duplicates a file descriptor from a net.Conn.
func dupFD(conn net.Conn) (int, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1, fmt.Errorf("openssl: cannot get raw fd from %T", conn)
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("openssl: SyscallConn: %w", err)
	}
	var dupFD int
	var dupErr error
	err = rc.Control(func(f uintptr) {
		dupFD, dupErr = syscall.Dup(int(f))
	})
	if err != nil {
		return -1, err
	}
	return dupFD, dupErr
}

// fdConn wraps a raw file descriptor as a net.Conn-compatible object.
type fdConn struct {
	fd   int
	addr net.Addr
}

func (c *fdConn) Read(b []byte) (int, error)       { return syscall.Read(c.fd, b) }
func (c *fdConn) Write(b []byte) (int, error)      { return syscall.Write(c.fd, b) }
func (c *fdConn) Close() error                     { return syscall.Close(c.fd) }
func (c *fdConn) LocalAddr() net.Addr              { return c.addr }
func (c *fdConn) RemoteAddr() net.Addr             { return c.addr }
func (c *fdConn) SetDeadline(t time.Time) error    { return nil } // handled by Go's transport
func (c *fdConn) SetReadDeadline(t time.Time) error { return nil }
func (c *fdConn) SetWriteDeadline(t time.Time) error { return nil }

func sslError() string {
	// Get the most recent OpenSSL error
	err := C.ERR_get_error()
	if err == 0 {
		return "unknown error"
	}
	buf := make([]byte, 256)
	C.ERR_error_string_n(err, (*C.char)(unsafe.Pointer(&buf[0])), 256)
	// Find null terminator
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i])
		}
	}
	return string(buf)
}

func sslErrno() int {
	return int(C.get_errno())
}
