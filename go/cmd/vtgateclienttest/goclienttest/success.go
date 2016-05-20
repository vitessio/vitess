// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goclienttest

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// testSuccess exercises the test cases provided by the "success" service.
func testSuccess(t *testing.T, conn *vtgateconn.VTGateConn) {
	testGetSrvKeyspace(t, conn)
}

func testGetSrvKeyspace(t *testing.T, conn *vtgateconn.VTGateConn) {
	want := &topodatapb.SrvKeyspace{
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
	}
	got, err := conn.GetSrvKeyspace(context.Background(), "big")
	if err != nil {
		t.Fatalf("GetSrvKeyspace error: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetSrvKeyspace() = %v, want %v", proto.MarshalTextString(got), proto.MarshalTextString(want))
	}
}
