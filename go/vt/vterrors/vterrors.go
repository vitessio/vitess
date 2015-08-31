// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vterrors provides helpers for propagating internal errors through the Vitess
// system (including across RPC boundaries) in a structured way.
package vterrors

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// VitessError is the error type that we use internally for passing structured errors
type VitessError struct {
	// Error code of the Vitess error
	Code vtrpc.ErrorCode
	// Additional context string, distinct from the error message. For example, if
	// you wanted an error like "foo error: original error", the Message string
	// should be "foo error: "
	Message string
	err     error
}

// Error implements the error interface. For now, it should exactly recreate the original error string.
// It intentionally (for now) does not expose all the information that VitessError has. This
// is so that it can be used in the mixed state where parts of the stack are trying to parse
// error strings.
func (e *VitessError) Error() string {
	return fmt.Sprintf("%v", e.err)
}

// AsString returns a VitessError as a string, with more detailed information than Error().
func (e *VitessError) AsString() string {
	if e.Message != "" {
		return fmt.Sprintf("Code: %v, Message: %v, err: %v", e.Code, e.Message, e.err)
	}
	return fmt.Sprintf("Code: %v, err: %v", e.Code, e.err)
}

// FromError returns a VitessError with the supplied error code and wrapped error.
func FromError(code vtrpc.ErrorCode, err error) error {
	return &VitessError{
		Code: code,
		err:  err,
	}
}

// FromRPCError recovers a VitessError from a *RPCError (which is how VitessErrors
// are transmitted across RPC boundaries).
func FromRPCError(rpcErr *mproto.RPCError) error {
	if rpcErr == nil {
		return nil
	}
	return &VitessError{
		Code: vtrpc.ErrorCode(rpcErr.Code),
		err:  fmt.Errorf("%v", rpcErr.Message),
	}
}

// FromVtRPCError recovers a VitessError from a *vtrpc.RPCError (which is how VitessErrors
// are transmitted across proto3 RPC boundaries).
func FromVtRPCError(rpcErr *vtrpc.RPCError) *VitessError {
	if rpcErr == nil {
		return nil
	}
	return &VitessError{
		Code: rpcErr.Code,
		err:  fmt.Errorf("%v", rpcErr.Message),
	}
}

// WithPrefix allows a string to be prefixed to an error, without nesting a new VitessError.
func WithPrefix(prefix string, in error) error {
	vtErr, ok := in.(*VitessError)
	if !ok {
		return fmt.Errorf("%s: %s", prefix, in)
	}

	return &VitessError{
		Code:    vtErr.Code,
		err:     vtErr.err,
		Message: fmt.Sprintf("%s: %s", prefix, vtErr.Message),
	}
}

// ErrorCodeToGRPCCode maps a vtrpc.ErrorCode to a gRPC codes.Code
func ErrorCodeToGRPCCode(code vtrpc.ErrorCode) codes.Code {
	switch code {
	case vtrpc.ErrorCode_SUCCESS:
		return codes.OK
	case vtrpc.ErrorCode_CANCELLED:
		return codes.Canceled
	case vtrpc.ErrorCode_UNKNOWN_ERROR:
		return codes.Unknown
	case vtrpc.ErrorCode_BAD_INPUT:
		return codes.InvalidArgument
	case vtrpc.ErrorCode_DEADLINE_EXCEEDED:
		return codes.DeadlineExceeded
	case vtrpc.ErrorCode_INTEGRITY_ERROR:
		return codes.AlreadyExists
	case vtrpc.ErrorCode_PERMISSION_DENIED:
		return codes.PermissionDenied
	case vtrpc.ErrorCode_RESOURCE_EXHAUSTED:
		return codes.ResourceExhausted
	case vtrpc.ErrorCode_QUERY_NOT_SERVED:
		return codes.FailedPrecondition
	case vtrpc.ErrorCode_NOT_IN_TX:
		return codes.Aborted
	case vtrpc.ErrorCode_INTERNAL_ERROR:
		return codes.Internal
	case vtrpc.ErrorCode_TRANSIENT_ERROR:
		return codes.Unavailable
	case vtrpc.ErrorCode_UNAUTHENTICATED:
		return codes.Unauthenticated
	default:
		return codes.Unknown
	}
}

// toGRPCCode will attempt to determine the best gRPC code for a particular error.
func toGRPCCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if vtErr, ok := err.(*VitessError); ok {
		return ErrorCodeToGRPCCode(vtErr.Code)
	}
	// Returns the underlying grpc Code, or codes.Unknown if one doesn't exist
	return grpc.Code(err)
}

// ToGRPCError returns an error as a grpc error, with the appropriate error code
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return grpc.Errorf(toGRPCCode(err), "%v", err)
}
