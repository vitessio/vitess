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

// rpcErrFromTabletError translate an error from VTGate to an *mproto.RPCError
func rpcErrFromVtGateError(err error) *mproto.RPCError {
	if err == nil {
		return nil
	}
	// TODO(aaijazi): for now, we don't have any differentiation of VtGate errors.
	// However, we should have them soon, so that clients don't have to parse the
	// returned error string.
	return &mproto.RPCError{
		Code:    vterrors.UnknownVtgateError,
		Message: err.Error(),
	}
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

// VtGateErrorToVtRPCError converts a vtgate error into a vtrpc error.
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
		Code:    vtrpc.ErrorCodeDeprecated_UnknownVtgateError,
		Message: message,
	}
}
