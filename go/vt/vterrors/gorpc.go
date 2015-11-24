// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file contains methods to pack errors in and out of mproto.RPCError
// and are only used by bson gorpc.

// FromRPCError recovers a VitessError from a *mproto.RPCError (which is how VitessErrors
// are transmitted across RPC boundaries).
func FromRPCError(rpcErr *mproto.RPCError) error {
	if rpcErr == nil {
		return nil
	}
	return &VitessError{
		Code: vtrpcpb.ErrorCode(rpcErr.Code),
		err:  fmt.Errorf("%v", rpcErr.Message),
	}
}

// RPCErrFromVtError convert from a VtError to an *mproto.RPCError
func RPCErrFromVtError(err error) *mproto.RPCError {
	if err == nil {
		return nil
	}
	return &mproto.RPCError{
		Code:    int64(RecoverVtErrorCode(err)),
		Message: err.Error(),
	}
}
