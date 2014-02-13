// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"github.com/youtube/vitess/go/vt/key"
)

// UpdateStreamRequest is used to make a request for ServeUpdateStream.
type UpdateStreamRequest struct {
	GroupId int64
}

// KeyRangeRequest is used to make a request for StreamKeyRange.
type KeyRangeRequest struct {
	GroupId        int64
	KeyspaceIdType key.KeyspaceIdType
	KeyRange       key.KeyRange
}

// TablesRequest is used to make a request for StreamTables.
type TablesRequest struct {
	GroupId int64
	Tables  []string
}
