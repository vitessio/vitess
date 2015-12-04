// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcbinlogplayer

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	binlogservicepb "github.com/youtube/vitess/go/vt/proto/binlogservice"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// client implements a Client over go rpc
type client struct {
	cc *grpc.ClientConn
	c  binlogservicepb.UpdateStreamClient
}

func (client *client) Dial(endPoint *topodatapb.EndPoint, connTimeout time.Duration) error {
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.PortMap["grpc"])
	var err error
	client.cc, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(connTimeout))
	if err != nil {
		return err
	}
	client.c = binlogservicepb.NewUpdateStreamClient(client.cc)
	return nil
}

func (client *client) Close() {
	client.cc.Close()
}

func (client *client) ServeUpdateStream(ctx context.Context, position string) (chan *binlogdatapb.StreamEvent, binlogplayer.ErrFunc, error) {
	response := make(chan *binlogdatapb.StreamEvent, 10)
	query := &binlogdatapb.StreamUpdateRequest{
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
			response <- r.StreamEvent
		}
	}()
	return response, func() error {
		return finalErr
	}, nil
}

func (client *client) StreamKeyRange(ctx context.Context, position string, keyspaceIdType topodatapb.KeyspaceIdType, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (chan *binlogdatapb.BinlogTransaction, binlogplayer.ErrFunc, error) {
	response := make(chan *binlogdatapb.BinlogTransaction, 10)
	query := &binlogdatapb.StreamKeyRangeRequest{
		Position:       position,
		KeyspaceIdType: keyspaceIdType,
		KeyRange:       keyRange,
		Charset:        charset,
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
			response <- r.BinlogTransaction
		}
	}()
	return response, func() error {
		return finalErr
	}, nil
}

func (client *client) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (chan *binlogdatapb.BinlogTransaction, binlogplayer.ErrFunc, error) {
	response := make(chan *binlogdatapb.BinlogTransaction, 10)
	query := &binlogdatapb.StreamTablesRequest{
		Position: position,
		Tables:   tables,
		Charset:  charset,
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
			response <- r.BinlogTransaction
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
