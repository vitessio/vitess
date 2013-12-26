// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpc

// RPC structs shared between many components

// UnusedRequest is the type to be used in a bson RPC server call that
// doesn't take any parameter, for instance:
//    func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
type UnusedRequest string

// UnusedResponse is the type to be used in a bson RPC server call that
// doesn't return any parameter, for instance:
//   func (tm *TabletManager) StopSlave(context *rpcproto.Context, args *rpc.UnusedRequest, reply *rpc.UnusedResponse) error {
type UnusedResponse string

// When calling an RPC service that doesn't take any parameter, just pass in ""
// When calling an RPC service that doesn't return anything, declare
// an UnusedResponse and pass that as a pointer:
//   var noOutput rpc.UnusedResponse
//   client.rpcCall("StopSlave", "", &noOutput, waitTime)
