//go:build !openssl

// Package openssl provides an OpenSSL-backed TLS connection.
// It is used as an alternative to crypto/tls for HTTPS connections,
// activated via the "openssl" build tag.
//
// Build with: go build -tags openssl ./cmd/pget/
//
// Requires: libssl-dev (build), libssl3 (runtime).
package openssl

import (
	"fmt"
	"net"
)

// IsAvailable reports whether the OpenSSL backend is compiled in.
const IsAvailable = false

// Dial is a stub that returns an error when OpenSSL is not compiled in.
func Dial(network, addr, serverName string, insecure bool) (net.Conn, error) {
	return nil, fmt.Errorf("openssl: not compiled in (build with -tags openssl)")
}
