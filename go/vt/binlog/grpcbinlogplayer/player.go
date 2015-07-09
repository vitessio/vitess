// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcbinlogplayer

import (
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbs "github.com/youtube/vitess/go/vt/proto/binlogservice"
)

// client implements a Client over go rpc
type client struct {
	cc *grpc.ClientConn
	c  pbs.UpdateStreamClient
}

func (client *client) Dial(endPoint topo.EndPoint, connTimeout time.Duration) error {
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.NamedPortMap["grpc"])
	var err error
	client.cc, err = grpc.Dial(addr, grpc.WithBlock(), grpc.WithTimeout(connTimeout))
	if err != nil {
		return err
	}
	client.c = pbs.NewUpdateStreamClient(client.cc)
	return nil
}

func (client *client) Close() {
	client.cc.Close()
}

func (client *client) ServeUpdateStream(ctx context.Context, req *proto.UpdateStreamRequest) (chan *proto.StreamEvent, binlogplayer.ErrFunc, error) {
	return nil, nil, fmt.Errorf("NYI")
}

func (client *client) StreamKeyRange(ctx context.Context, req *proto.KeyRangeRequest) (chan *proto.BinlogTransaction, binlogplayer.ErrFunc, error) {
	response := make(chan *proto.BinlogTransaction, 10)
	query := &pb.StreamKeyRangeRequest{
		Position:       myproto.ReplicationPositionToProto(req.Position),
		KeyspaceIdType: key.KeyspaceIdTypeToProto(req.KeyspaceIdType),
		KeyRange:       key.KeyRangeToProto(req.KeyRange),
		Charset:        mproto.CharsetToProto(req.Charset),
	}

	stream, err := client.c.StreamKeyRange(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	var finalErr error
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalErr = err
				}
				close(response)
				return
			}
			response <- proto.ProtoToBinlogTransaction(r.BinlogTransaction)
		}
	}()
	return response, func() error {
		return finalErr
	}, nil
}

func (client *client) StreamTables(ctx context.Context, req *proto.TablesRequest) (chan *proto.BinlogTransaction, binlogplayer.ErrFunc, error) {
	response := make(chan *proto.BinlogTransaction, 10)
	query := &pb.StreamTablesRequest{
		Position: myproto.ReplicationPositionToProto(req.Position),
		Tables:   req.Tables,
		Charset:  mproto.CharsetToProto(req.Charset),
	}

	stream, err := client.c.StreamTables(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	var finalErr error
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalErr = err
				}
				close(response)
				return
			}
			response <- proto.ProtoToBinlogTransaction(r.BinlogTransaction)
		}
	}()
	return response, func() error {
		return finalErr
	}, nil
}

// Registration as a factory
func init() {
	binlogplayer.RegisterClientFactory("grpc", func() binlogplayer.Client {
		return &client{}
	})
}
