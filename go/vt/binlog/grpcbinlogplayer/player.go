// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcbinlogplayer

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbs "github.com/youtube/vitess/go/vt/proto/binlogservice"
)

// response is the type returned by the Client for streaming
type response struct {
	err error
}

func (response *response) Error() error {
	return response.err
}

// client implements a BinlogPlayerClient over go rpc
type client struct {
	cc  *grpc.ClientConn
	c   pbs.UpdateStreamClient
	ctx context.Context
}

func (client *client) Dial(addr string, connTimeout time.Duration) error {
	var err error
	client.cc, err = grpc.Dial(addr)
	if err != nil {
		return err
	}
	client.c = pbs.NewUpdateStreamClient(client.cc)
	client.ctx = context.Background()
	return nil
}

func (client *client) Close() {
	client.cc.Close()
}

func (client *client) ServeUpdateStream(req *proto.UpdateStreamRequest, responseChan chan *proto.StreamEvent) binlogplayer.BinlogPlayerResponse {
	return nil
}

func (client *client) StreamKeyRange(req *proto.KeyRangeRequest, responseChan chan *proto.BinlogTransaction) binlogplayer.BinlogPlayerResponse {
	query := &pb.StreamKeyRangeRequest{
		Position:       myproto.ReplicationPositionToProto(req.Position),
		KeyspaceIdType: key.KeyspaceIdTypeToProto(req.KeyspaceIdType),
		KeyRange:       key.KeyRangeToProto(req.KeyRange),
		Charset:        proto.CharsetToProto(req.Charset),
	}

	response := &response{}
	stream, err := client.c.StreamKeyRange(client.ctx, query)
	if err != nil {
		response.err = err
		close(responseChan)
		return response
	}
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					response.err = err
				}
				close(responseChan)
				return
			}
			responseChan <- proto.ProtoToBinlogTransaction(r.BinlogTransaction)
		}
	}()
	return response
}

func (client *client) StreamTables(req *proto.TablesRequest, responseChan chan *proto.BinlogTransaction) binlogplayer.BinlogPlayerResponse {
	query := &pb.StreamTablesRequest{
		Position: myproto.ReplicationPositionToProto(req.Position),
		Tables:   req.Tables,
		Charset:  proto.CharsetToProto(req.Charset),
	}

	response := &response{}
	stream, err := client.c.StreamTables(client.ctx, query)
	if err != nil {
		response.err = err
		close(responseChan)
		return response
	}
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					response.err = err
				}
				close(responseChan)
				return
			}
			responseChan <- proto.ProtoToBinlogTransaction(r.BinlogTransaction)
		}
	}()
	return response
}

// Registration as a factory
func init() {
	binlogplayer.RegisterBinlogPlayerClientFactory("grpc", func() binlogplayer.BinlogPlayerClient {
		return &client{}
	})
}
