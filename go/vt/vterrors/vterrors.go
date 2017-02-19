package vterrors

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// Code returns the error code if it's a VitessError.
// Otherwise, it returns unknown.
func Code(err error) vtrpcpb.Code {
	if err, ok := err.(*VitessError); ok {
		return err.Code
	}
	if err, ok := err.(VtError); ok {
		return err.VtErrorCode()
	}
	return vtrpcpb.Code_UNKNOWN
}

// ConcatenateErrors aggregates an array of errors into a single error by string concatenation.
func ConcatenateErrors(errs []error) error {
	errStrs := make([]string, 0, len(errs))
	for _, e := range errs {
		errStrs = append(errStrs, fmt.Sprintf("%v", e))
	}
	// sort the error strings so we always have deterministic ordering
	sort.Strings(errStrs)
	return errors.New(strings.Join(errStrs, "\n"))
}

// VtError is implemented by any type that exposes a vtrpcpb.ErrorCode.
type VtError interface {
	VtErrorCode() vtrpcpb.Code
}

// VitessError is the error type that we use internally for passing structured errors.
type VitessError struct {
	// Error code of the Vitess error.
	Code vtrpcpb.Code
	// Error message that should be returned. This allows us to change an error message
	// without losing the underlying error. For example, if you have an error like
	// context.DeadlikeExceeded, you don't want to modify it - otherwise you would lose
	// the ability to programatically check for that error. However, you might want to
	// add some context to the error, giving you a message like "command failed: deadline exceeded".
	// To do that, you can create a NewVitessError to wrap the original error, but redefine
	// the error message.
	Message string
	err     error
}

// New creates a new error using the code and input string.
func New(code vtrpcpb.Code, in string) error {
	return &VitessError{
		Code: code,
		err:  errors.New(in),
	}
}

// Error implements the error interface. It will return the redefined error message, if there
// is one. If there isn't, it will return the original error message.
func (e *VitessError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("%v", e.err)
	}
	return e.Message
}

// VtErrorCode returns the underlying Vitess error code.
func (e *VitessError) VtErrorCode() vtrpcpb.Code {
	return e.Code
}

// AsString returns a VitessError as a string, with more detailed information than Error().
func (e *VitessError) AsString() string {
	if e.Message != "" {
		return fmt.Sprintf("Code: %v, Message: %v, err: %v", e.Code, e.Message, e.err)
	}
	return fmt.Sprintf("Code: %v, err: %v", e.Code, e.err)
}

// FromError returns a VitessError with the supplied error code by wrapping an
// existing error.
// Use this method also when you want to create a VitessError without a custom
// message. For example:
//	 err := vterrors.FromError(vtrpcpb.Code_INTERNAL,
//     errors.New("no valid endpoint"))
func FromError(code vtrpcpb.Code, err error) error {
	return &VitessError{
		Code: code,
		err:  err,
	}
}

// NewVitessError returns a VitessError backed error with the given arguments.
// Useful for preserving an underlying error while creating a new error message.
func NewVitessError(code vtrpcpb.Code, err error, format string, args ...interface{}) error {
	return &VitessError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		err:     err,
	}
}

// WithPrefix allows a string to be prefixed to an error.
// If the original error implements the VtError interface it returns a VitessError wrapping the
// original error (with one exception: if the original error is an instance of VitessError it
// doesn't wrap it in a new VitessError instance, but only changes the 'Message' field).
// Otherwise, it returns a string prefixed with the given prefix.
func WithPrefix(prefix string, in error) error {
	return New(Code(in), fmt.Sprintf("%s%v", prefix, in))
}

// WithSuffix allows a string to be suffixed to an error.
// If the original error implements the VtError interface it returns a VitessError wrapping the
// original error (with one exception: if the original error is an instance of VitessError
// it doesn't wrap it in a new VitessError instance, but only changes the 'Message' field).
// Otherwise, it returns a string suffixed with the given suffix.
func WithSuffix(in error, suffix string) error {
	if vitessError, ok := in.(*VitessError); ok {
		return &VitessError{
			Code:    vitessError.Code,
			err:     vitessError.err,
			Message: fmt.Sprintf("%s%s", in.Error(), suffix),
		}
	}
	if vtError, ok := in.(VtError); ok {
		return &VitessError{
			Code:    vtError.VtErrorCode(),
			err:     in,
			Message: fmt.Sprintf("%s%s", in.Error(), suffix),
		}
	}
	return fmt.Errorf("%s%s", in, suffix)
}
