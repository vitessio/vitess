// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogstreamer

import (
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/servenv"
)

// UpdateStream is the go rpc UpdateStream server
type UpdateStream struct {
	updateStream proto.UpdateStream
}

// ServeUpdateStream is part of the gorpc UpdateStream service
func (server *UpdateStream) ServeUpdateStream(req *proto.UpdateStreamRequest, sendReply func(reply interface{}) error) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.ServeUpdateStream(req.Position, func(reply *proto.StreamEvent) error {
		return sendReply(reply)
	})
}

// StreamKeyRange is part of the gorpc UpdateStream service
func (server *UpdateStream) StreamKeyRange(req *proto.KeyRangeRequest, sendReply func(reply interface{}) error) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamKeyRange(req.Position, req.KeyspaceIdType, key.KeyRangeToProto(req.KeyRange), req.Charset, func(reply *proto.BinlogTransaction) error {
		return sendReply(reply)
	})
}

// StreamTables is part of the gorpc UpdateStream service
func (server *UpdateStream) StreamTables(req *proto.TablesRequest, sendReply func(reply interface{}) error) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamTables(req.Position, req.Tables, req.Charset, func(reply *proto.BinlogTransaction) error {
		return sendReply(reply)
	})
}

// New returns a new go rpc server implementation stub for UpdateStream
func New(updateStream proto.UpdateStream) *UpdateStream {
	return &UpdateStream{updateStream}
}

// registration mechanism

func init() {
	binlog.RegisterUpdateStreamServices = append(binlog.RegisterUpdateStreamServices, func(updateStream proto.UpdateStream) {
		servenv.Register("updatestream", New(updateStream))
	})
}
