// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// successClient implements vtgateservice.VTGateService
// and returns specific values. It is meant to test all possible success cases,
// and make sure all clients handle any corner case correctly.
type successClient struct {
	fallbackClient
}

func newSuccessClient(fallback vtgateservice.VTGateService) *successClient {
	return &successClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

func (c *successClient) Begin(ctx context.Context, outSession *pbg.Session) error {
	outSession.InTransaction = true
	return nil
}

func (c *successClient) Commit(ctx context.Context, inSession *pbg.Session) error {
	if inSession != nil && inSession.InTransaction {
		return nil
	}
	return c.fallback.Commit(ctx, inSession)
}

func (c *successClient) Rollback(ctx context.Context, inSession *pbg.Session) error {
	if inSession != nil && inSession.InTransaction {
		return nil
	}
	return c.fallback.Rollback(ctx, inSession)
}

func (c *successClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
	if keyspace == "big" {
		return &pb.SrvKeyspace{
			Partitions: []*pb.SrvKeyspace_KeyspacePartition{
				&pb.SrvKeyspace_KeyspacePartition{
					ServedType: pb.TabletType_REPLICA,
					ShardReferences: []*pb.ShardReference{
						&pb.ShardReference{
							Name: "shard0",
							KeyRange: &pb.KeyRange{
								Start: []byte{0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
								End:   []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
							},
						},
					},
				},
			},
			ShardingColumnName: "sharding_column_name",
			ShardingColumnType: pb.KeyspaceIdType_UINT64,
			ServedFrom: []*pb.SrvKeyspace_ServedFrom{
				&pb.SrvKeyspace_ServedFrom{
					TabletType: pb.TabletType_MASTER,
					Keyspace:   "other_keyspace",
				},
			},
			SplitShardCount: 128,
		}, nil
	}
	if keyspace == "small" {
		return &pb.SrvKeyspace{}, nil
	}
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}
