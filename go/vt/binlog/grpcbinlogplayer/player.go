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
