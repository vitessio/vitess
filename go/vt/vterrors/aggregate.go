// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// A list of all vtrpcpb.ErrorCodes, ordered by priority. These priorities are
// used when aggregating multiple errors in VtGate.
// Higher priority error codes are more urgent for users to see. They are
// prioritized based on the following question: assuming a scatter query produced multiple
// errors, which of the errors is the most likely to give the user useful information
// about why the query failed and how they should proceed?
const (
	PrioritySuccess = iota
	PriorityTransientError
	PriorityQueryNotServed
	PriorityDeadlineExceeded
	PriorityCancelled
	PriorityIntegrityError
	PriorityNotInTx
	PriorityUnknownError
	PriorityInternalError
	PriorityResourceExhausted
	PriorityUnauthenticated
	PriorityPermissionDenied
	PriorityBadInput
)

var errorPriorities = map[vtrpcpb.ErrorCode]int{
	vtrpcpb.ErrorCode_SUCCESS:                   PrioritySuccess,
	vtrpcpb.ErrorCode_CANCELLED_LEGACY:          PriorityCancelled,
	vtrpcpb.ErrorCode_UNKNOWN_ERROR:             PriorityUnknownError,
	vtrpcpb.ErrorCode_BAD_INPUT:                 PriorityBadInput,
	vtrpcpb.ErrorCode_DEADLINE_EXCEEDED_LEGACY:  PriorityDeadlineExceeded,
	vtrpcpb.ErrorCode_INTEGRITY_ERROR:           PriorityIntegrityError,
	vtrpcpb.ErrorCode_PERMISSION_DENIED_LEGACY:  PriorityPermissionDenied,
	vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY: PriorityResourceExhausted,
	vtrpcpb.ErrorCode_QUERY_NOT_SERVED:          PriorityQueryNotServed,
	vtrpcpb.ErrorCode_NOT_IN_TX:                 PriorityNotInTx,
	vtrpcpb.ErrorCode_INTERNAL_ERROR:            PriorityInternalError,
	vtrpcpb.ErrorCode_TRANSIENT_ERROR:           PriorityTransientError,
	vtrpcpb.ErrorCode_UNAUTHENTICATED_LEGACY:    PriorityUnauthenticated,
}

// AggregateVtGateErrorCodes aggregates a list of errors into a single
// error code.  It does so by finding the highest priority error code
// in the list.
func AggregateVtGateErrorCodes(errors []error) vtrpcpb.ErrorCode {
	highCode := vtrpcpb.ErrorCode_SUCCESS
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
