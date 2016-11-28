// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	"errors"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file contains the necessary methods to send and receive errors
// as payloads of proto3 structures. It converts VitessError to and from
// vtrpcpb.Error. Use these methods when a RPC call can return both
// data and an error.

// FromVtRPCError recovers a VitessError from a *vtrpcpb.RPCError (which is how VitessErrors
// are transmitted across proto3 RPC boundaries).
func FromVtRPCError(rpcErr *vtrpcpb.RPCError) error {
	if rpcErr == nil {
		return nil
	}
	return &VitessError{
		Code: rpcErr.Code,
		err:  errors.New(rpcErr.Message),
	}
}

// FromVtRPCErrors recovers VitessErrors from an array of *vtrpcpb.RPCError.
func FromVtRPCErrors(rpcErr []*vtrpcpb.RPCError) []error {
	if rpcErr == nil {
		return nil
	}
	vitessErrors := make([]error, 0, len(rpcErr))
	for _, err := range rpcErr {
		vitessErrors = append(vitessErrors, &VitessError{
			Code: err.Code,
			err:  errors.New(err.Message),
		})
	}
	return vitessErrors
}

// VtRPCErrorFromVtError converts from a VtError to a vtrpcpb.RPCError.
func VtRPCErrorFromVtError(err error) *vtrpcpb.RPCError {
	if err == nil {
		return nil
	}
	return &vtrpcpb.RPCError{
		Code:    RecoverVtErrorCode(err),
		Message: err.Error(),
	}
}

// VtRPCErrorFromVtErrors converts from an array of VtError to an array of vtrpcpb.RPCError.
func VtRPCErrorFromVtErrors(errs []error) []*vtrpcpb.RPCError {
	if errs == nil {
		return nil
	}
	rpcErrors := make([]*vtrpcpb.RPCError, len(errs))
	for errNum, err := range errs {
		rpcErrors[errNum] = &vtrpcpb.RPCError{
			Code:    RecoverVtErrorCode(err),
			Message: err.Error(),
		}
	}
	return rpcErrors
}
