package consultopo

import (
	"errors"
	"net/url"

	"context"

	"vitess.io/vitess/go/vt/topo"
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
func convertError(err error, nodePath string) error {
	// Unwrap errors from the Go HTTP client.
	if urlErr, ok := err.(*url.Error); ok {
		err = urlErr.Err
	}

	// Convert specific sentinel values.
	switch err {
	case context.Canceled:
		return topo.NewError(topo.Interrupted, nodePath)
	case context.DeadlineExceeded:
		return topo.NewError(topo.Timeout, nodePath)
	}

	return err
}
