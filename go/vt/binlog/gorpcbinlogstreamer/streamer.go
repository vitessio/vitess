// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogstreamer

import (
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/binlog/binlogproto"
	"github.com/youtube/vitess/go/vt/servenv"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// UpdateStream is the go rpc UpdateStream server
type UpdateStream struct {
	updateStream binlogproto.UpdateStream
}

// ServeUpdateStream is part of the gorpc UpdateStream service
func (server *UpdateStream) ServeUpdateStream(req *binlogproto.UpdateStreamRequest, sendReply func(reply interface{}) error) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.ServeUpdateStream(req.Position, func(reply *pb.StreamEvent) error {
		return sendReply(reply)
	})
}

// StreamKeyRange is part of the gorpc UpdateStream service
func (server *UpdateStream) StreamKeyRange(req *binlogproto.KeyRangeRequest, sendReply func(reply interface{}) error) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamKeyRange(req.Position, req.KeyspaceIdType, req.KeyRange, req.Charset, func(reply *pb.BinlogTransaction) error {
		return sendReply(reply)
	})
}

// StreamTables is part of the gorpc UpdateStream service
func (server *UpdateStream) StreamTables(req *binlogproto.TablesRequest, sendReply func(reply interface{}) error) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamTables(req.Position, req.Tables, req.Charset, func(reply *pb.BinlogTransaction) error {
		return sendReply(reply)
	})
}

// New returns a new go rpc server implementation stub for UpdateStream
func New(updateStream binlogproto.UpdateStream) *UpdateStream {
	return &UpdateStream{updateStream}
}

// registration mechanism

func init() {
	binlog.RegisterUpdateStreamServices = append(binlog.RegisterUpdateStreamServices, func(updateStream binlogproto.UpdateStream) {
		servenv.Register("updatestream", New(updateStream))
	})
}
