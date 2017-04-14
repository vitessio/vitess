package consultopo

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// Errors specific to this package.
var (
	// ErrBadResponse is returned from this package if the
	// response from the consul server does not contain the data
	// that the API promises. The consul client unmarshals JSON
	// from the server into a Response struct that uses pointers,
	// so we need to check for nil pointers, or else a misbehaving
	// consul could cause us to panic.
	ErrBadResponse = errors.New("consul request returned success, but response is missing required data")
)

// convertError converts a context error into a topo error. All errors
// are either application-level errors, or context errors.
func convertError(err error) error {
	switch err {
	case context.Canceled:
		return topo.ErrInterrupted
	case context.DeadlineExceeded:
		return topo.ErrTimeout
	}
	return err
}
