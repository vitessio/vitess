package vterrors

import (
	"fmt"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

type vtError struct {
	code vtrpcpb.Code
	err  string
}

// New creates a new error using the code and input string.
func New(code vtrpcpb.Code, in string) error {
	return &vtError{
		code: code,
		err:  in,
	}
}

// Errorf returns a new error built using Printf style arguments.
func Errorf(code vtrpcpb.Code, format string, args ...interface{}) error {
	return &vtError{
		code: code,
		err:  fmt.Sprintf(format, args...),
	}
}

func (e *vtError) Error() string {
	return e.err
}

// Code returns the error code if it's a vtError.
// If err is nil, it returns ok. Otherwise, it returns unknown.
func Code(err error) vtrpcpb.Code {
	if err == nil {
		return vtrpcpb.Code_OK
	}
	if err, ok := err.(*vtError); ok {
		return err.code
	}
	return vtrpcpb.Code_UNKNOWN
}
