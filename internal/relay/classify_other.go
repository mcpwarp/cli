//go:build !windows

package relay

import (
	"errors"
	"syscall"
)

// isConnRefused/isConnReset/isHostUnreachable classify a forward error by
// its underlying errno on POSIX systems — see classify_windows.go for why
// this needs a platform split: Go's syscall.Errno.Is on Windows does not
// map the WSA* codes onto these POSIX constants.
func isConnRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func isConnReset(err error) bool {
	return errors.Is(err, syscall.ECONNRESET)
}

func isHostUnreachable(err error) bool {
	return errors.Is(err, syscall.EHOSTUNREACH)
}
