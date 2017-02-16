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
	code := rpcErr.Code
	if code == vtrpcpb.Code_OK {
		code = LegacyErrorCodeToCode(rpcErr.LegacyCode)
	}
	return &VitessError{
		Code: code,
		err:  errors.New(rpcErr.Message),
	}
}

// VtRPCErrorFromVtError converts from a VtError to a vtrpcpb.RPCError.
func VtRPCErrorFromVtError(err error) *vtrpcpb.RPCError {
	if err == nil {
		return nil
	}
	code := RecoverVtErrorCode(err)
	return &vtrpcpb.RPCError{
		LegacyCode: CodeToLegacyErrorCode(code),
		Code:       code,
		Message:    err.Error(),
	}
}
