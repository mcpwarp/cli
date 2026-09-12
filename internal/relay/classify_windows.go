//go:build windows

package relay

import (
	"errors"
	"syscall"
)

// Winsock error codes not exposed as constants by the standard syscall
// package on windows (only WSAECONNRESET is) — values per Winsock2.h.
const (
	wsaeConnRefused syscall.Errno = 10061 // WSAECONNREFUSED
	wsaeHostUnreach syscall.Errno = 10065 // WSAEHOSTUNREACH
)

// isConnRefused/isConnReset/isHostUnreachable classify a forward error by
// its underlying Winsock errno — Go's syscall.Errno.Is does not map
// WSAECONNREFUSED/WSAECONNRESET/WSAEHOSTUNREACH onto the POSIX
// ECONNREFUSED/ECONNRESET/EHOSTUNREACH constants used in classify_other.go,
// so Windows needs its own matches against the WSA* codes.
func isConnRefused(err error) bool {
	return errors.Is(err, wsaeConnRefused)
}

func isConnReset(err error) bool {
	return errors.Is(err, syscall.WSAECONNRESET)
}

func isHostUnreachable(err error) bool {
	return errors.Is(err, wsaeHostUnreach)
}
