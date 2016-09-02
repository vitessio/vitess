// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcbinlogplayer

import (
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

func (client *client) Dial(tablet *topodatapb.Tablet, connTimeout time.Duration) error {
	addr := netutil.JoinHostPort(tablet.Hostname, tablet.PortMap["grpc"])
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

type serveStreamKeyRangeAdapter struct {
	stream binlogservicepb.UpdateStream_StreamKeyRangeClient
}

func (s *serveStreamKeyRangeAdapter) Recv() (*binlogdatapb.BinlogTransaction, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return r.BinlogTransaction, nil
}

func (client *client) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	query := &binlogdatapb.StreamKeyRangeRequest{
		Position: position,
		KeyRange: keyRange,
		Charset:  charset,
	}
	stream, err := client.c.StreamKeyRange(ctx, query)
	if err != nil {
		return nil, err
	}
	return &serveStreamKeyRangeAdapter{stream}, nil
}

type serveStreamTablesAdapter struct {
	stream binlogservicepb.UpdateStream_StreamTablesClient
}

func (s *serveStreamTablesAdapter) Recv() (*binlogdatapb.BinlogTransaction, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return r.BinlogTransaction, nil
}

func (client *client) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	query := &binlogdatapb.StreamTablesRequest{
		Position: position,
		Tables:   tables,
		Charset:  charset,
	}
	stream, err := client.c.StreamTables(ctx, query)
	if err != nil {
		return nil, err
	}
	return &serveStreamTablesAdapter{stream}, nil
}

// Registration as a factory
func init() {
	binlogplayer.RegisterClientFactory("grpc", func() binlogplayer.Client {
		return &client{}
	})
}
