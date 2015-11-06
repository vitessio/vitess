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
	return server.updateStream.ServeUpdateStream(req.Position, func(reply *proto.StreamEvent) error {
		return stream.Send(&pb.StreamUpdateResponse{
			StreamEvent: proto.StreamEventToProto(reply),
		})
	})
}

// StreamKeyRange is part of the pbs.UpdateStreamServer interface
func (server *UpdateStream) StreamKeyRange(req *pb.StreamKeyRangeRequest, stream pbs.UpdateStream_StreamKeyRangeServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamKeyRange(req.Position, key.ProtoToKeyspaceIdType(req.KeyspaceIdType), req.KeyRange, mproto.ProtoToCharset(req.Charset), func(reply *proto.BinlogTransaction) error {
		return stream.Send(&pb.StreamKeyRangeResponse{
			BinlogTransaction: proto.BinlogTransactionToProto(reply),
		})
	})
}

// StreamTables is part of the pbs.UpdateStreamServer interface
func (server *UpdateStream) StreamTables(req *pb.StreamTablesRequest, stream pbs.UpdateStream_StreamTablesServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamTables(req.Position, req.Tables, mproto.ProtoToCharset(req.Charset), func(reply *proto.BinlogTransaction) error {
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
