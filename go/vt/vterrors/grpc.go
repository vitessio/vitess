// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file contains functions to convert errors to and from gRPC codes.
// Use these methods to return an error through gRPC and still
// retain its code.

// GRPCServerErrPrefix is the string we prefix gRPC server errors with. This is
// necessary because there is currently no good way, in gRPC, to differentiate
// between an error from a server vs the client.
// See: https://github.com/grpc/grpc-go/issues/319
const GRPCServerErrPrefix = "gRPCServerError:"

// GRPCCodeToErrorCode maps a gRPC codes.Code to a vtrpcpb.ErrorCode.
func GRPCCodeToErrorCode(code codes.Code) vtrpcpb.ErrorCode {
	switch code {
	case codes.OK:
		return vtrpcpb.ErrorCode_SUCCESS
	case codes.Canceled:
		return vtrpcpb.ErrorCode_CANCELLED_LEGACY
	case codes.Unknown:
		return vtrpcpb.ErrorCode_UNKNOWN_ERROR
	case codes.InvalidArgument:
		return vtrpcpb.ErrorCode_BAD_INPUT
	case codes.DeadlineExceeded:
		return vtrpcpb.ErrorCode_DEADLINE_EXCEEDED_LEGACY
	case codes.AlreadyExists:
		return vtrpcpb.ErrorCode_INTEGRITY_ERROR
	case codes.PermissionDenied:
		return vtrpcpb.ErrorCode_PERMISSION_DENIED_LEGACY
	case codes.ResourceExhausted:
		return vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY
	case codes.FailedPrecondition:
		return vtrpcpb.ErrorCode_QUERY_NOT_SERVED
	case codes.Aborted:
		return vtrpcpb.ErrorCode_NOT_IN_TX
	case codes.Internal:
		return vtrpcpb.ErrorCode_INTERNAL_ERROR
	case codes.Unavailable:
		return vtrpcpb.ErrorCode_TRANSIENT_ERROR
	case codes.Unauthenticated:
		return vtrpcpb.ErrorCode_UNAUTHENTICATED_LEGACY
	default:
		return vtrpcpb.ErrorCode_UNKNOWN_ERROR
	}
}

// ErrorCodeToGRPCCode maps a vtrpcpb.ErrorCode to a gRPC codes.Code.
func ErrorCodeToGRPCCode(code vtrpcpb.ErrorCode) codes.Code {
	switch code {
	case vtrpcpb.ErrorCode_SUCCESS:
		return codes.OK
	case vtrpcpb.ErrorCode_CANCELLED_LEGACY:
		return codes.Canceled
	case vtrpcpb.ErrorCode_UNKNOWN_ERROR:
		return codes.Unknown
	case vtrpcpb.ErrorCode_BAD_INPUT:
		return codes.InvalidArgument
	case vtrpcpb.ErrorCode_DEADLINE_EXCEEDED_LEGACY:
		return codes.DeadlineExceeded
	case vtrpcpb.ErrorCode_INTEGRITY_ERROR:
		return codes.AlreadyExists
	case vtrpcpb.ErrorCode_PERMISSION_DENIED_LEGACY:
		return codes.PermissionDenied
	case vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY:
		return codes.ResourceExhausted
	case vtrpcpb.ErrorCode_QUERY_NOT_SERVED:
		return codes.FailedPrecondition
	case vtrpcpb.ErrorCode_NOT_IN_TX:
		return codes.Aborted
	case vtrpcpb.ErrorCode_INTERNAL_ERROR:
		return codes.Internal
	case vtrpcpb.ErrorCode_TRANSIENT_ERROR:
		return codes.Unavailable
	case vtrpcpb.ErrorCode_UNAUTHENTICATED_LEGACY:
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
	// Returns the underlying gRPC Code, or codes.Unknown if one doesn't exist.
	return grpc.Code(err)
}

// truncateError shortens errors because gRPC has a size restriction on them.
func truncateError(err error) error {
	// For more details see: https://github.com/grpc/grpc-go/issues/443
	// The gRPC spec says "Clients may limit the size of Response-Headers,
	// Trailers, and Trailers-Only, with a default of 8 KiB each suggested."
	// Therefore, we assume 8 KiB minus some headroom.
	GRPCErrorLimit := 8*1024 - 512
	if len(err.Error()) <= GRPCErrorLimit {
		return err
	}
	truncateInfo := "[...] [remainder of the error is truncated because gRPC has a size limit on errors.]"
	truncatedErr := err.Error()[:GRPCErrorLimit]
	return fmt.Errorf("%v %v", truncatedErr, truncateInfo)
}

// ToGRPCError returns an error as a gRPC error, with the appropriate error code.
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return grpc.Errorf(toGRPCCode(err), "%v %v", GRPCServerErrPrefix, truncateError(err))
}

// FromGRPCError returns a gRPC error as a VitessError, translating between error codes.
// However, there are a few errors which are not translated and passed as they
// are. For example, io.EOF since our code base checks for this error to find
// out that a stream has finished.
func FromGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		// Do not wrap io.EOF because we compare against it for finished streams.
		return err
	}
	return &VitessError{
		Code: GRPCCodeToErrorCode(grpc.Code(err)),
		err:  err,
	}
}
