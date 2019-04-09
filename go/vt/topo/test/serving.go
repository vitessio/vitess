/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// checkSrvKeyspace tests the SrvKeyspace methods (other than watch).
func checkSrvKeyspace(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	// Test GetSrvKeyspaceNames returns an empty list correctly.
	if names, err := ts.GetSrvKeyspaceNames(ctx, LocalCellName); err != nil || len(names) != 0 {
		t.Errorf("GetSrvKeyspace(not there): %v %v", names, err)
	}

	// test cell/keyspace entries (SrvKeyspace)
	srvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "-80",
						KeyRange: &topodatapb.KeyRange{
							End: []byte{0x80},
						},
					},
				},
			},
		},
		ShardingColumnName: "video_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		ServedFrom: []*topodatapb.SrvKeyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_REPLICA,
				Keyspace:   "other_keyspace",
			},
		},
	}
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "test_keyspace", srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(1): %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, LocalCellName, "test_keyspace666"); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetSrvKeyspace(invalid): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, LocalCellName, "test_keyspace"); err != nil || !proto.Equal(srvKeyspace, k) {
		t.Errorf("GetSrvKeyspace(valid): %v %v", err, k)
	}
	if k, err := ts.GetSrvKeyspaceNames(ctx, LocalCellName); err != nil || len(k) != 1 || k[0] != "test_keyspace" {
		t.Errorf("GetSrvKeyspaceNames(): %v", err)
	}

	// check that updating a SrvKeyspace out of the blue works
	if err := ts.UpdateSrvKeyspace(ctx, LocalCellName, "unknown_keyspace_so_far", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(2): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, LocalCellName, "unknown_keyspace_so_far"); err != nil || !proto.Equal(srvKeyspace, k) {
		t.Errorf("GetSrvKeyspace(out of the blue): %v %v", err, *k)
	}

	// Delete the SrvKeyspace.
	if err := ts.DeleteSrvKeyspace(ctx, LocalCellName, "unknown_keyspace_so_far"); err != nil {
		t.Fatalf("DeleteSrvKeyspace: %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, LocalCellName, "unknown_keyspace_so_far"); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetSrvKeyspace(deleted) got %v, want ErrNoNode", err)
	}
}

// checkSrvVSchema tests the SrvVSchema methods (other than watch).
func checkSrvVSchema(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	// check GetSrvVSchema returns topo.ErrNoNode if no SrvVSchema
	if _, err := ts.GetSrvVSchema(ctx, LocalCellName); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetSrvVSchema(not set): %v", err)
	}

	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"test_keyspace": {
				Sharded: true,
			},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, LocalCellName, srvVSchema); err != nil {
		t.Errorf("UpdateSrvVSchema(1): %v", err)
	}
	if v, err := ts.GetSrvVSchema(ctx, LocalCellName); err != nil || !proto.Equal(srvVSchema, v) {
		t.Errorf("GetSrvVSchema(valid): %v %v", err, v)
	}
}
