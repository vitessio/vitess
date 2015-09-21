// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"

	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// This file contains the data structures used by bson rpc for vtgate service.

// GetSrvKeyspaceRequest is the payload to GetSrvRequest
type GetSrvKeyspaceRequest struct {
	Keyspace string
}

// SplitQueryResult is the response from SplitQueryRequest
type SplitQueryResult struct {
	Splits []*pbg.SplitQueryResponse_Part
	Err    *mproto.RPCError
}
