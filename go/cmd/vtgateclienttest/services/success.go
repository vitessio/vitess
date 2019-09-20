/*
Copyright 2019 The Vitess Authors.

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

package services

import (
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
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
	return &vtgatepb.Session{
		InTransaction: true,
	}, nil
}

func (c *successClient) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	if session != nil && session.InTransaction {
		return nil
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
