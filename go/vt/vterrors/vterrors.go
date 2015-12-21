// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vterrors provides helpers for propagating internal errors through the Vitess
// system (including across RPC boundaries) in a structured way.
package vterrors

import (
	"fmt"
	"sort"
	"strings"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// ConcatenateErrors aggregates an array of errors into a single error by string concatenation
func ConcatenateErrors(errors []error) error {
	errStrs := make([]string, 0, len(errors))
	for _, e := range errors {
		errStrs = append(errStrs, fmt.Sprintf("%v", e))
	}
	// sort the error strings so we always have deterministic ordering
	sort.Strings(errStrs)
	return fmt.Errorf("%v", strings.Join(errStrs, "\n"))
}

// VtError is implemented by any type that exposes a vtrpcpb.ErrorCode
type VtError interface {
	VtErrorCode() vtrpcpb.ErrorCode
}

// RecoverVtErrorCode attempts to recover a vtrpcpb.ErrorCode from an error
func RecoverVtErrorCode(err error) vtrpcpb.ErrorCode {
	if vtErr, ok := err.(VtError); ok {
		return vtErr.VtErrorCode()
	}
	return vtrpcpb.ErrorCode_UNKNOWN_ERROR
}

// VitessError is the error type that we use internally for passing structured errors
type VitessError struct {
	// Error code of the Vitess error
	Code vtrpcpb.ErrorCode
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

// Error implements the error interface. It will return the redefined error message, if there
// is one. If there isn't, it will return the original error message.
func (e *VitessError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("%v", e.err)
	}
	return e.Message
}

// VtErrorCode returns the underlying Vitess error code
func (e *VitessError) VtErrorCode() vtrpcpb.ErrorCode {
	return e.Code
}

// AsString returns a VitessError as a string, with more detailed information than Error().
func (e *VitessError) AsString() string {
	if e.Message != "" {
		return fmt.Sprintf("Code: %v, Message: %v, err: %v", e.Code, e.Message, e.err)
	}
	return fmt.Sprintf("Code: %v, err: %v", e.Code, e.err)
}

// NewVitessError returns a VitessError backed error with the given arguments.
// Useful for preserving an underlying error while creating a new error message.
func NewVitessError(code vtrpcpb.ErrorCode, err error, format string, args ...interface{}) error {
	return &VitessError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		err:     err,
	}
}

// FromError returns a VitessError with the supplied error code by wrapping an
// existing error.
func FromError(code vtrpcpb.ErrorCode, err error) error {
	return &VitessError{
		Code: code,
		err:  err,
	}
}

// WithPrefix allows a string to be prefixed to an error, without chaining a new VitessError.
func WithPrefix(prefix string, in error) error {
	vtErr, ok := in.(*VitessError)
	if !ok {
		return fmt.Errorf("%s%s", prefix, in)
	}

	return &VitessError{
		Code:    vtErr.Code,
		err:     vtErr.err,
		Message: fmt.Sprintf("%s%s", prefix, vtErr.Error()),
	}
}

// WithSuffix allows a string to be suffixed to an error, without chaining a new VitessError.
func WithSuffix(in error, suffix string) error {
	vtErr, ok := in.(*VitessError)
	if !ok {
		return fmt.Errorf("%s%s", in, suffix)
	}

	return &VitessError{
		Code:    vtErr.Code,
		err:     vtErr.err,
		Message: fmt.Sprintf("%s%s", vtErr.Error(), suffix),
	}
}
