package vterrors

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file contains the necessary methods to send and receive errors
// as payloads of proto3 structures. It converts vtError to and from
// *vtrpcpb.RPCError. Use these methods when a RPC call can return both
// data and an error.

// FromVTRPC recovers a vtError from a *vtrpcpb.RPCError (which is how vtError
// is transmitted across proto3 RPC boundaries).
func FromVTRPC(rpcErr *vtrpcpb.RPCError) error {
	if rpcErr == nil {
		return nil
	}
	return New(rpcErr.Code, rpcErr.Message)
}

// ToVTRPC converts from vtError to a vtrpcpb.RPCError.
func ToVTRPC(err error) *vtrpcpb.RPCError {
	if err == nil {
		return nil
	}
	return &vtrpcpb.RPCError{
		Code:    Code(err),
		Message: err.Error(),
	}
}
