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

// CodeToLegacyErrorCode maps a vtrpcpb.Code to a vtrpcpb.ErrorCode.
func CodeToLegacyErrorCode(code vtrpcpb.Code) vtrpcpb.ErrorCode {
	switch code {
	case vtrpcpb.Code_OK:
		return vtrpcpb.ErrorCode_SUCCESS
	case vtrpcpb.Code_CANCELED:
		return vtrpcpb.ErrorCode_CANCELLED_LEGACY
	case vtrpcpb.Code_UNKNOWN:
		return vtrpcpb.ErrorCode_UNKNOWN_ERROR
	case vtrpcpb.Code_INVALID_ARGUMENT:
		return vtrpcpb.ErrorCode_BAD_INPUT
	case vtrpcpb.Code_DEADLINE_EXCEEDED:
		return vtrpcpb.ErrorCode_DEADLINE_EXCEEDED_LEGACY
	case vtrpcpb.Code_ALREADY_EXISTS:
		return vtrpcpb.ErrorCode_INTEGRITY_ERROR
	case vtrpcpb.Code_PERMISSION_DENIED:
		return vtrpcpb.ErrorCode_PERMISSION_DENIED_LEGACY
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		return vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY
	case vtrpcpb.Code_FAILED_PRECONDITION:
		return vtrpcpb.ErrorCode_QUERY_NOT_SERVED
	case vtrpcpb.Code_ABORTED:
		return vtrpcpb.ErrorCode_NOT_IN_TX
	case vtrpcpb.Code_INTERNAL:
		return vtrpcpb.ErrorCode_INTERNAL_ERROR
	case vtrpcpb.Code_UNAVAILABLE:
		return vtrpcpb.ErrorCode_TRANSIENT_ERROR
	case vtrpcpb.Code_UNAUTHENTICATED:
		return vtrpcpb.ErrorCode_UNAUTHENTICATED_LEGACY
	default:
		return vtrpcpb.ErrorCode_UNKNOWN_ERROR
	}
}

// LegacyErrorCodeToCode maps a vtrpcpb.ErrorCode to a gRPC vtrpcpb.Code.
func LegacyErrorCodeToCode(code vtrpcpb.ErrorCode) vtrpcpb.Code {
	switch code {
	case vtrpcpb.ErrorCode_SUCCESS:
		return vtrpcpb.Code_OK
	case vtrpcpb.ErrorCode_CANCELLED_LEGACY:
		return vtrpcpb.Code_CANCELED
	case vtrpcpb.ErrorCode_UNKNOWN_ERROR:
		return vtrpcpb.Code_UNKNOWN
	case vtrpcpb.ErrorCode_BAD_INPUT:
		return vtrpcpb.Code_INVALID_ARGUMENT
	case vtrpcpb.ErrorCode_DEADLINE_EXCEEDED_LEGACY:
		return vtrpcpb.Code_DEADLINE_EXCEEDED
	case vtrpcpb.ErrorCode_INTEGRITY_ERROR:
		return vtrpcpb.Code_ALREADY_EXISTS
	case vtrpcpb.ErrorCode_PERMISSION_DENIED_LEGACY:
		return vtrpcpb.Code_PERMISSION_DENIED
	case vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY:
		return vtrpcpb.Code_RESOURCE_EXHAUSTED
	case vtrpcpb.ErrorCode_QUERY_NOT_SERVED:
		return vtrpcpb.Code_FAILED_PRECONDITION
	case vtrpcpb.ErrorCode_NOT_IN_TX:
		return vtrpcpb.Code_ABORTED
	case vtrpcpb.ErrorCode_INTERNAL_ERROR:
		return vtrpcpb.Code_INTERNAL
	case vtrpcpb.ErrorCode_TRANSIENT_ERROR:
		return vtrpcpb.Code_UNAVAILABLE
	case vtrpcpb.ErrorCode_UNAUTHENTICATED_LEGACY:
		return vtrpcpb.Code_UNAUTHENTICATED
	default:
		return vtrpcpb.Code_UNKNOWN
	}
}

// CodeToGRPC maps a vtrpcpb.Code to a grpc Code.
func CodeToGRPC(code vtrpcpb.Code) codes.Code {
	return codes.Code(code)
}

// GRPCToCode maps a grpc Code to a vtrpcpb.Code
func GRPCToCode(code codes.Code) vtrpcpb.Code {
	return vtrpcpb.Code(code)
}

// toGRPCCode will attempt to determine the best gRPC code for a particular error.
func toGRPCCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if vtErr, ok := err.(VtError); ok {
		return CodeToGRPC(vtErr.VtErrorCode())
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
		Code: GRPCToCode(grpc.Code(err)),
		err:  err,
	}
}
