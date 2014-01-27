// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogstreamer

import (
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/binlog/proto"
)

type UpdateStream struct {
	updateStream *binlog.UpdateStream
}

func (server *UpdateStream) ServeUpdateStream(req *proto.UpdateStreamRequest, sendReply func(reply interface{}) error) (err error) {
	return server.updateStream.ServeUpdateStream(req, func(reply *proto.StreamEvent) error {
		return sendReply(reply)
	})
}

func (server *UpdateStream) StreamKeyRange(req *proto.KeyRangeRequest, sendReply func(reply interface{}) error) (err error) {
	return server.updateStream.StreamKeyRange(req, func(reply *proto.BinlogTransaction) error {
		return sendReply(reply)
	})
}

func (server *UpdateStream) StreamTables(req *proto.TablesRequest, sendReply func(reply interface{}) error) (err error) {
	return server.updateStream.StreamTables(req, func(reply *proto.BinlogTransaction) error {
		return sendReply(reply)
	})
}

// registration mechanism

var server *UpdateStream

func init() {
	binlog.RegisterUpdateStreamServices = append(binlog.RegisterUpdateStreamServices, func(updateStream *binlog.UpdateStream) {
		server = &UpdateStream{updateStream}
		rpcwrap.RegisterAuthenticated(server)
	})
}
