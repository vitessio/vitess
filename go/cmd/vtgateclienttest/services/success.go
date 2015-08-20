// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"
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

func (c *successClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	if keyspace == "big" {
		return &topo.SrvKeyspace{
			Partitions: map[topo.TabletType]*topo.KeyspacePartition{
				topo.TYPE_REPLICA: &topo.KeyspacePartition{
					ShardReferences: []topo.ShardReference{
						topo.ShardReference{
							Name: "shard0",
							KeyRange: key.KeyRange{
								Start: key.Uint64Key(0x4000000000000000).KeyspaceId(),
								End:   key.Uint64Key(0x8000000000000000).KeyspaceId(),
							},
						},
					},
				},
			},
			ShardingColumnName: "sharding_column_name",
			ShardingColumnType: key.KIT_UINT64,
			ServedFrom: map[topo.TabletType]string{
				topo.TYPE_MASTER: "other_keyspace",
			},
			SplitShardCount: 128,
		}, nil
	}
	if keyspace == "small" {
		return &topo.SrvKeyspace{}, nil
	}
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}
