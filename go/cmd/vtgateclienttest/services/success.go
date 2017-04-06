// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
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

func (c *successClient) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	if singledb {
		return nil, errors.New("single db")
	}
	return &vtgatepb.Session{
		InTransaction: true,
	}, nil
}

func (c *successClient) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	if session != nil && session.InTransaction {
		return nil
	}
	if twopc {
		return errors.New("twopc")
	}
	return c.fallback.Commit(ctx, twopc, session)
}

func (c *successClient) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	if session != nil && session.InTransaction {
		return nil
	}
	return c.fallback.Rollback(ctx, session)
}

func (c *successClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	if keyspace == "big" {
		return &topodatapb.SrvKeyspace{
			Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
				{
					ServedType: topodatapb.TabletType_REPLICA,
					ShardReferences: []*topodatapb.ShardReference{
						{
							Name: "shard0",
							KeyRange: &topodatapb.KeyRange{
								Start: []byte{0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
								End:   []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
							},
						},
					},
				},
			},
			ShardingColumnName: "sharding_column_name",
			ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
			ServedFrom: []*topodatapb.SrvKeyspace_ServedFrom{
				{
					TabletType: topodatapb.TabletType_MASTER,
					Keyspace:   "other_keyspace",
				},
			},
		}, nil
	}
	if keyspace == "small" {
		return &topodatapb.SrvKeyspace{}, nil
	}
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}
