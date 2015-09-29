// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vterrors provides helpers for propagating internal errors through the Vitess
// system (including across RPC boundaries) in a structured way.
package vterrors

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
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

// VtError is implemented by any type that exposes a vtrpc.ErrorCode
type VtError interface {
	VtErrorCode() vtrpc.ErrorCode
}

// RecoverVtErrorCode attempts to recover a vtrpc.ErrorCode from an error
func RecoverVtErrorCode(err error) vtrpc.ErrorCode {
	if vtErr, ok := err.(VtError); ok {
		return vtErr.VtErrorCode()
	}
	return vtrpc.ErrorCode_UNKNOWN_ERROR
}

// VitessError is the error type that we use internally for passing structured errors
type VitessError struct {
	// Error code of the Vitess error
	Code vtrpc.ErrorCode
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
func (e *VitessError) VtErrorCode() vtrpc.ErrorCode {
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
func NewVitessError(code vtrpc.ErrorCode, err error, format string, args ...interface{}) error {
	return &VitessError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		err:     err,
	}
}

// FromError returns a VitessError with the supplied error code by wrapping an
// existing error.
func FromError(code vtrpc.ErrorCode, err error) error {
	return &VitessError{
		Code: code,
		err:  err,
	}
}

// FromRPCError recovers a VitessError from a *mproto.RPCError (which is how VitessErrors
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

// RPCErrFromVtError convert from a VtError to an *mproto.RPCError
func RPCErrFromVtError(err error) *mproto.RPCError {
	if err == nil {
		return nil
	}
	return &mproto.RPCError{
		Code:    int64(RecoverVtErrorCode(err)),
		Message: err.Error(),
	}
}

// FromVtRPCError recovers a VitessError from a *vtrpc.RPCError (which is how VitessErrors
// are transmitted across proto3 RPC boundaries).
func FromVtRPCError(rpcErr *vtrpc.RPCError) error {
	if rpcErr == nil {
		return nil
	}
	return &VitessError{
		Code: rpcErr.Code,
		err:  fmt.Errorf("%v", rpcErr.Message),
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

// This is the string that we prefix gRPC server errors with. This is necessary
// because there is currently no good way, in gRPC, to differentiate between an
// error from a server vs the client.
// See: https://github.com/grpc/grpc-go/issues/319
const GRPCServerErrPrefix = "gRPCServerError:"

// GRPCCodeToErrorCode maps a gRPC codes.Code to a vtrpc.ErrorCode
func GRPCCodeToErrorCode(code codes.Code) vtrpc.ErrorCode {
	switch code {
	case codes.OK:
		return vtrpc.ErrorCode_SUCCESS
	case codes.Canceled:
		return vtrpc.ErrorCode_CANCELLED
	case codes.Unknown:
		return vtrpc.ErrorCode_UNKNOWN_ERROR
	case codes.InvalidArgument:
		return vtrpc.ErrorCode_BAD_INPUT
	case codes.DeadlineExceeded:
		return vtrpc.ErrorCode_DEADLINE_EXCEEDED
	case codes.AlreadyExists:
		return vtrpc.ErrorCode_INTEGRITY_ERROR
	case codes.PermissionDenied:
		return vtrpc.ErrorCode_PERMISSION_DENIED
	case codes.ResourceExhausted:
		return vtrpc.ErrorCode_RESOURCE_EXHAUSTED
	case codes.FailedPrecondition:
		return vtrpc.ErrorCode_QUERY_NOT_SERVED
	case codes.Aborted:
		return vtrpc.ErrorCode_NOT_IN_TX
	case codes.Internal:
		return vtrpc.ErrorCode_INTERNAL_ERROR
	case codes.Unavailable:
		return vtrpc.ErrorCode_TRANSIENT_ERROR
	case codes.Unauthenticated:
		return vtrpc.ErrorCode_UNAUTHENTICATED
	default:
		return vtrpc.ErrorCode_UNKNOWN_ERROR
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
	if vtErr, ok := err.(VtError); ok {
		return ErrorCodeToGRPCCode(vtErr.VtErrorCode())
	}
	// Returns the underlying grpc Code, or codes.Unknown if one doesn't exist
	return grpc.Code(err)
}

// ToGRPCError returns an error as a grpc error, with the appropriate error code
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return grpc.Errorf(toGRPCCode(err), "%v %v", GRPCServerErrPrefix, err)
}

// FromGRPCError return a grpc error as a VitessError, translating between error codes
func FromGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return &VitessError{
		Code: GRPCCodeToErrorCode(grpc.Code(err)),
		err:  err,
	}
}

// ToJSONError return an error as a JSON-encoded error string.
// Specifically, it creates a vtrpc.RPCError and marshalls that into JSON.
func ToJSONError(err error) error {
	if err == nil {
		return nil
	}

	// We'll need to marshal a known type into JSON, so that we can unmarshal easily
	// on the other side without worrying about matching field names. vtrpc.RPCError
	// seems like a reasonable choice of type.
	vtrpcErr := vtrpc.RPCError{
		Code:    vtrpc.ErrorCode_UNKNOWN_ERROR,
		Message: err.Error(),
	}
	if vtErr, ok := err.(VtError); ok {
		vtrpcErr.Code = vtErr.VtErrorCode()
	}
	b, err := json.Marshal(vtrpcErr)
	if err != nil {
		// If we can't marshal the JSON, return the error directly. On the receiving side, this
		// should be returned as an error with an Unknown code.
		// We can't return an error with an explicit error code, because it's possible that
		// the error here also can't be marshaled into JSON...
		return err
	}
	return fmt.Errorf(string(b))
}

// FromJSONError return a JSON-encoded error string as a VitessError.
func FromJSONError(err error) error {
	if err == nil {
		return nil
	}

	// JSON with expected fields will overwrite this
	vtrpcErr := &vtrpc.RPCError{
		Code:    vtrpc.ErrorCode_INTERNAL_ERROR,
		Message: fmt.Sprintf("unexpected fields in JSONError: %v", err),
	}

	b := []byte(err.Error())
	jerr := json.Unmarshal(b, vtrpcErr)
	if jerr != nil {
		return WithPrefix(
			"can't unmarshal JSON: ",
			FromError(vtrpc.ErrorCode_UNKNOWN_ERROR, err),
		)
	}
	return FromVtRPCError(vtrpcErr)
}
