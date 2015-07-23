// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcbinlogstreamer contains the gRPC implementation of the binlog
// streamer server component.
package grpcbinlogstreamer

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/servenv"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbs "github.com/youtube/vitess/go/vt/proto/binlogservice"
)

// UpdateStream is the gRPC UpdateStream server
type UpdateStream struct {
	updateStream proto.UpdateStream
}

// New returns a new go rpc server implementation stub for UpdateStream
func New(updateStream proto.UpdateStream) *UpdateStream {
	return &UpdateStream{updateStream}
}

// StreamUpdate is part of the pbs.UpdateStreamServer interface
func (server *UpdateStream) StreamUpdate(req *pb.StreamUpdateRequest, stream pbs.UpdateStream_StreamUpdateServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	pos, err := myproto.DecodeReplicationPosition(req.Position)
	if err != nil {
		return err
	}
	return server.updateStream.ServeUpdateStream(&proto.UpdateStreamRequest{
		Position: pos,
	}, func(reply *proto.StreamEvent) error {
		return stream.Send(&pb.StreamUpdateResponse{
			StreamEvent: proto.StreamEventToProto(reply),
		})
	})
}

// StreamKeyRange is part of the pbs.UpdateStreamServer interface
func (server *UpdateStream) StreamKeyRange(req *pb.StreamKeyRangeRequest, stream pbs.UpdateStream_StreamKeyRangeServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	pos, err := myproto.DecodeReplicationPosition(req.Position)
	if err != nil {
		return err
	}
	return server.updateStream.StreamKeyRange(&proto.KeyRangeRequest{
		Position:       pos,
		KeyspaceIdType: key.ProtoToKeyspaceIdType(req.KeyspaceIdType),
		KeyRange:       key.ProtoToKeyRange(req.KeyRange),
		Charset:        mproto.ProtoToCharset(req.Charset),
	}, func(reply *proto.BinlogTransaction) error {
		return stream.Send(&pb.StreamKeyRangeResponse{
			BinlogTransaction: proto.BinlogTransactionToProto(reply),
		})
	})
}

// StreamTables is part of the pbs.UpdateStreamServer interface
func (server *UpdateStream) StreamTables(req *pb.StreamTablesRequest, stream pbs.UpdateStream_StreamTablesServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	pos, err := myproto.DecodeReplicationPosition(req.Position)
	if err != nil {
		return err
	}
	return server.updateStream.StreamTables(&proto.TablesRequest{
		Position: pos,
		Tables:   req.Tables,
		Charset:  mproto.ProtoToCharset(req.Charset),
	}, func(reply *proto.BinlogTransaction) error {
		return stream.Send(&pb.StreamTablesResponse{
			BinlogTransaction: proto.BinlogTransactionToProto(reply),
		})
	})
}

// registration mechanism

func init() {
	binlog.RegisterUpdateStreamServices = append(binlog.RegisterUpdateStreamServices, func(updateStream proto.UpdateStream) {
		if servenv.GRPCCheckServiceMap("updatestream") {
			pbs.RegisterUpdateStreamServer(servenv.GRPCServer, New(updateStream))
		}
	})
}
