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

package goclienttest

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
