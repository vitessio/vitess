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

var errorPriorities = map[vtrpcpb.Code]int{
	vtrpcpb.Code_OK:                  PrioritySuccess,
	vtrpcpb.Code_CANCELED:            PriorityCancelled,
	vtrpcpb.Code_UNKNOWN:             PriorityUnknownError,
	vtrpcpb.Code_INVALID_ARGUMENT:    PriorityBadInput,
	vtrpcpb.Code_DEADLINE_EXCEEDED:   PriorityDeadlineExceeded,
	vtrpcpb.Code_ALREADY_EXISTS:      PriorityIntegrityError,
	vtrpcpb.Code_PERMISSION_DENIED:   PriorityPermissionDenied,
	vtrpcpb.Code_RESOURCE_EXHAUSTED:  PriorityResourceExhausted,
	vtrpcpb.Code_FAILED_PRECONDITION: PriorityQueryNotServed,
	vtrpcpb.Code_ABORTED:             PriorityNotInTx,
	vtrpcpb.Code_INTERNAL:            PriorityInternalError,
	vtrpcpb.Code_UNAVAILABLE:         PriorityTransientError,
	vtrpcpb.Code_UNAUTHENTICATED:     PriorityUnauthenticated,
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
