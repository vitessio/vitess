// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the data structures used by bson rpc for update stream.

// UpdateStreamRequest is used to make a request for ServeUpdateStream.
type UpdateStreamRequest struct {
	Position string
}

// KeyRangeRequest is used to make a request for StreamKeyRange.
type KeyRangeRequest struct {
	Position       string
	KeyspaceIdType pbt.KeyspaceIdType
	KeyRange       *pbt.KeyRange
	Charset        *pb.Charset
}

// TablesRequest is used to make a request for StreamTables.
type TablesRequest struct {
	Position string
	Tables   []string
	Charset  *pb.Charset
}
