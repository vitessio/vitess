// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// A list of all vtrpc.ErrorCodes, ordered by priority. These priorities are
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

var errorPriorities = map[vtrpc.ErrorCode]int{
	vtrpc.ErrorCode_SUCCESS:            PrioritySuccess,
	vtrpc.ErrorCode_CANCELLED:          PriorityCancelled,
	vtrpc.ErrorCode_UNKNOWN_ERROR:      PriorityUnknownError,
	vtrpc.ErrorCode_BAD_INPUT:          PriorityBadInput,
	vtrpc.ErrorCode_DEADLINE_EXCEEDED:  PriorityDeadlineExceeded,
	vtrpc.ErrorCode_INTEGRITY_ERROR:    PriorityIntegrityError,
	vtrpc.ErrorCode_PERMISSION_DENIED:  PriorityPermissionDenied,
	vtrpc.ErrorCode_RESOURCE_EXHAUSTED: PriorityResourceExhausted,
	vtrpc.ErrorCode_QUERY_NOT_SERVED:   PriorityQueryNotServed,
	vtrpc.ErrorCode_NOT_IN_TX:          PriorityNotInTx,
	vtrpc.ErrorCode_INTERNAL_ERROR:     PriorityInternalError,
	vtrpc.ErrorCode_TRANSIENT_ERROR:    PriorityTransientError,
	vtrpc.ErrorCode_UNAUTHENTICATED:    PriorityUnauthenticated,
}

// rpcErrFromTabletError translate an error from VTGate to an *mproto.RPCError
func rpcErrFromVtGateError(err error) *mproto.RPCError {
	if err == nil {
		return nil
	}
	return &mproto.RPCError{
		Code:    int64(vterrors.RecoverVtErrorCode(err)),
		Message: err.Error(),
	}
}

// aggregateVtGateErrorCodes aggregates a list of errors into a single error code.
// It does so by finding the highest priority error code in the list.
func aggregateVtGateErrorCodes(errors []error) vtrpc.ErrorCode {
	highCode := vtrpc.ErrorCode_SUCCESS
	for _, e := range errors {
		code := vterrors.RecoverVtErrorCode(e)
		if errorPriorities[code] > errorPriorities[highCode] {
			highCode = code
		}
	}
	return highCode
}

func aggregateVtGateErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	return vterrors.FromError(
		aggregateVtGateErrorCodes(errors),
		vterrors.ConcatenateErrors(errors),
	)
}

// AddVtGateErrorToQueryResult will mutate a QueryResult struct to fill in the Err
// field with details from the VTGate error.
func AddVtGateErrorToQueryResult(err error, reply *proto.QueryResult) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromVtGateError(err)
}

// AddVtGateErrorToQueryResultList will mutate a QueryResultList struct to fill in the Err
// field with details from the VTGate error.
func AddVtGateErrorToQueryResultList(err error, reply *proto.QueryResultList) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromVtGateError(err)
}

// AddVtGateErrorToSplitQueryResult will mutate a SplitQueryResult struct to fill in the Err
// field with details from the VTGate error.
func AddVtGateErrorToSplitQueryResult(err error, reply *proto.SplitQueryResult) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromVtGateError(err)
}

// AddVtGateErrorToBeginResponse will mutate a BeginResponse struct to fill in the Err
// field with details from the VTGate error.
func AddVtGateErrorToBeginResponse(err error, reply *proto.BeginResponse) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromVtGateError(err)
}

// AddVtGateErrorToCommitResponse will mutate a CommitResponse struct to fill in the Err
// field with details from the VTGate error.
func AddVtGateErrorToCommitResponse(err error, reply *proto.CommitResponse) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromVtGateError(err)
}

// AddVtGateErrorToRollbackResponse will mutate a RollbackResponse struct to fill in the Err
// field with details from the VTGate error.
func AddVtGateErrorToRollbackResponse(err error, reply *proto.RollbackResponse) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromVtGateError(err)
}

// RPCErrorToVtRPCError converts a VTGate error into a vtrpc error.
func RPCErrorToVtRPCError(rpcErr *mproto.RPCError) *vtrpc.RPCError {
	if rpcErr == nil {
		return nil
	}
	return &vtrpc.RPCError{
		Code:    vtrpc.ErrorCode(rpcErr.Code),
		Message: rpcErr.Message,
	}
}

// VtGateErrorToVtRPCError converts a vtgate error into a vtrpc error.
// TODO(aaijazi): rename this guy, and correct the usage of it everywhere. As it's currently used,
// it will almost never return the correct error code, as it's only getting executeErr and reply.Error.
// It should actually just use reply.Err.
func VtGateErrorToVtRPCError(err error, errString string) *vtrpc.RPCError {
	if err == nil && errString == "" {
		return nil
	}
	message := ""
	if err != nil {
		message = err.Error()
	} else {
		message = errString
	}
	return &vtrpc.RPCError{
		Code:    vterrors.RecoverVtErrorCode(err),
		Message: message,
	}
}
