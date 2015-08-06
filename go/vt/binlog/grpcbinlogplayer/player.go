// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcbinlogplayer

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	pbs "github.com/youtube/vitess/go/vt/proto/binlogservice"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

// client implements a Client over go rpc
type client struct {
	cc *grpc.ClientConn
	c  pbs.UpdateStreamClient
}

func (client *client) Dial(endPoint *pbt.EndPoint, connTimeout time.Duration) error {
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.PortMap["grpc"])
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

func (client *client) ServeUpdateStream(ctx context.Context, position string) (chan *proto.StreamEvent, binlogplayer.ErrFunc, error) {
	response := make(chan *proto.StreamEvent, 10)
	query := &pb.StreamUpdateRequest{
		Position: position,
	}

	stream, err := client.c.StreamUpdate(ctx, query)
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
			response <- proto.ProtoToStreamEvent(r.StreamEvent)
		}
	}()
	return response, func() error {
		return finalErr
	}, nil
}

func (client *client) StreamKeyRange(ctx context.Context, position string, keyspaceIdType key.KeyspaceIdType, keyRange *pbt.KeyRange, charset *mproto.Charset) (chan *proto.BinlogTransaction, binlogplayer.ErrFunc, error) {
	response := make(chan *proto.BinlogTransaction, 10)
	query := &pb.StreamKeyRangeRequest{
		Position:       position,
		KeyspaceIdType: key.KeyspaceIdTypeToProto(keyspaceIdType),
		KeyRange:       keyRange,
		Charset:        mproto.CharsetToProto(charset),
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

func (client *client) StreamTables(ctx context.Context, position string, tables []string, charset *mproto.Charset) (chan *proto.BinlogTransaction, binlogplayer.ErrFunc, error) {
	response := make(chan *proto.BinlogTransaction, 10)
	query := &pb.StreamTablesRequest{
		Position: position,
		Tables:   tables,
		Charset:  mproto.CharsetToProto(charset),
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
