// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcbinlogcommon contains data structures shared by
// gorpcbinlogplayer and gorpcbinlogstreamer packages.
package gorpcbinlogcommon

import (
	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// UpdateStreamRequest is used to make a request for ServeUpdateStream.
type UpdateStreamRequest struct {
	Position string
}

// KeyRangeRequest is used to make a request for StreamKeyRange.
type KeyRangeRequest struct {
	Position       string
	KeyspaceIdType topodatapb.KeyspaceIdType
	KeyRange       *topodatapb.KeyRange
	Charset        *binlogdatapb.Charset
}

// TablesRequest is used to make a request for StreamTables.
type TablesRequest struct {
	Position string
	Tables   []string
	Charset  *binlogdatapb.Charset
}
