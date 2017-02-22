// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// A list of all vtrpcpb.Code, ordered by priority. These priorities are
// used when aggregating multiple errors in VtGate.
// Higher priority error codes are more urgent for users to see. They are
// prioritized based on the following question: assuming a scatter query produced multiple
// errors, which of the errors is the most likely to give the user useful information
// about why the query failed and how they should proceed?
const (
	// Informational errors.
	PriorityOK = iota
	PriorityCanceled
	PriorityAlreadyExists
	PriorityOutOfRange
	// Potentially retryable errors.
	PriorityUnavailable
	PriorityFailedPrecondition
	PriorityResourceExhausted
	PriorityDeadlineExceeded
	PriorityAborted
	// Permanent errors.
	PriorityUnknown
	PriorityUnauthenticated
	PriorityPermissionDenied
	PriorityInvalidArgument
	PriorityNotFound
	PriorityUnimplemented
	// Serious errors.
	PriorityInternal
	PriorityDataLoss
)

var errorPriorities = map[vtrpcpb.Code]int{
	vtrpcpb.Code_OK:                  PriorityOK,
	vtrpcpb.Code_CANCELED:            PriorityCanceled,
	vtrpcpb.Code_UNKNOWN:             PriorityUnknown,
	vtrpcpb.Code_INVALID_ARGUMENT:    PriorityInvalidArgument,
	vtrpcpb.Code_DEADLINE_EXCEEDED:   PriorityDeadlineExceeded,
	vtrpcpb.Code_NOT_FOUND:           PriorityNotFound,
	vtrpcpb.Code_ALREADY_EXISTS:      PriorityAlreadyExists,
	vtrpcpb.Code_PERMISSION_DENIED:   PriorityPermissionDenied,
	vtrpcpb.Code_UNAUTHENTICATED:     PriorityUnauthenticated,
	vtrpcpb.Code_RESOURCE_EXHAUSTED:  PriorityResourceExhausted,
	vtrpcpb.Code_FAILED_PRECONDITION: PriorityFailedPrecondition,
	vtrpcpb.Code_ABORTED:             PriorityAborted,
	vtrpcpb.Code_OUT_OF_RANGE:        PriorityOutOfRange,
	vtrpcpb.Code_UNIMPLEMENTED:       PriorityUnimplemented,
	vtrpcpb.Code_INTERNAL:            PriorityInternal,
	vtrpcpb.Code_UNAVAILABLE:         PriorityUnavailable,
	vtrpcpb.Code_DATA_LOSS:           PriorityDataLoss,
}

// AggregateVtGateErrorCodes aggregates a list of errors into a single
// error code.  It does so by finding the highest priority error code
// in the list.
func AggregateVtGateErrorCodes(errors []error) vtrpcpb.Code {
	highCode := vtrpcpb.Code_OK
	for _, e := range errors {
		code := RecoverVtErrorCode(e)
		if errorPriorities[code] > errorPriorities[highCode] {
			highCode = code
		}
	}
	return highCode
}

// AggregateVtGateErrors aggregates several errors into a single one.
func AggregateVtGateErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	return FromError(
		AggregateVtGateErrorCodes(errors),
		ConcatenateErrors(errors),
	)
}
