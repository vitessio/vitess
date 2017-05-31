/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package grpcbinlogstreamer contains the gRPC implementation of the binlog
// streamer server component.
package grpcbinlogstreamer

import (
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/servenv"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	binlogservicepb "github.com/youtube/vitess/go/vt/proto/binlogservice"
)

// UpdateStream is the gRPC UpdateStream server
type UpdateStream struct {
	updateStream binlog.UpdateStream
}

// New returns a new go rpc server implementation stub for UpdateStream
func New(updateStream binlog.UpdateStream) *UpdateStream {
	return &UpdateStream{updateStream}
}

// StreamKeyRange is part of the binlogservicepb.UpdateStreamServer interface
func (server *UpdateStream) StreamKeyRange(req *binlogdatapb.StreamKeyRangeRequest, stream binlogservicepb.UpdateStream_StreamKeyRangeServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamKeyRange(stream.Context(), req.Position, req.KeyRange, req.Charset, func(reply *binlogdatapb.BinlogTransaction) error {
		return stream.Send(&binlogdatapb.StreamKeyRangeResponse{
			BinlogTransaction: reply,
		})
	})
}

// StreamTables is part of the binlogservicepb.UpdateStreamServer interface
func (server *UpdateStream) StreamTables(req *binlogdatapb.StreamTablesRequest, stream binlogservicepb.UpdateStream_StreamTablesServer) (err error) {
	defer server.updateStream.HandlePanic(&err)
	return server.updateStream.StreamTables(stream.Context(), req.Position, req.Tables, req.Charset, func(reply *binlogdatapb.BinlogTransaction) error {
		return stream.Send(&binlogdatapb.StreamTablesResponse{
			BinlogTransaction: reply,
		})
	})
}

// registration mechanism

func init() {
	binlog.RegisterUpdateStreamServices = append(binlog.RegisterUpdateStreamServices, func(updateStream binlog.UpdateStream) {
		if servenv.GRPCCheckServiceMap("updatestream") {
			binlogservicepb.RegisterUpdateStreamServer(servenv.GRPCServer, New(updateStream))
		}
	})
}
