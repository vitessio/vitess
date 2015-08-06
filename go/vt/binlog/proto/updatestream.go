// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// UpdateStream is the interface for the server
type UpdateStream interface {
	// ServeUpdateStream serves the query and streams the result
	// for the full update stream
	ServeUpdateStream(position string, sendReply func(reply *StreamEvent) error) error

	// StreamKeyRange streams events related to a KeyRange only
	StreamKeyRange(position string, keyspaceIdType key.KeyspaceIdType, keyRange *pb.KeyRange, charset *mproto.Charset, sendReply func(reply *BinlogTransaction) error) error

	// StreamTables streams events related to a set of Tables only
	StreamTables(position string, tables []string, charset *mproto.Charset, sendReply func(reply *BinlogTransaction) error) error

	// HandlePanic should be called in a defer,
	// first thing in the RPC implementation.
	HandlePanic(*error)
}
