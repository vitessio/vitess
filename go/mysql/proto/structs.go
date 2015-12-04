// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// RPCError is the structure that is returned by each RPC call, which contains
// the error information for that call.
type RPCError struct {
	Code    int64
	Message string
}

//go:generate bsongen -file $GOFILE -type RPCError -o rpcerror_bson.go
