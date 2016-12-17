package test

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// checkSrvKeyspace tests the SrvKeyspace methods (other than watch).
func checkSrvKeyspace(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)

	// Test GetSrvKeyspaceNames returns an empty list correctly.
	if names, err := ts.GetSrvKeyspaceNames(ctx, cell); err != nil || len(names) != 0 {
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
	if err := ts.UpdateSrvKeyspace(ctx, cell, "test_keyspace", srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(1): %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, cell, "test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(invalid): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, cell, "test_keyspace"); err != nil || !proto.Equal(srvKeyspace, k) {
		t.Errorf("GetSrvKeyspace(valid): %v %v", err, k)
	}
	if k, err := ts.GetSrvKeyspaceNames(ctx, cell); err != nil || len(k) != 1 || k[0] != "test_keyspace" {
		t.Errorf("GetSrvKeyspaceNames(): %v", err)
	}

	// check that updating a SrvKeyspace out of the blue works
	if err := ts.UpdateSrvKeyspace(ctx, cell, "unknown_keyspace_so_far", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(2): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != nil || !proto.Equal(srvKeyspace, k) {
		t.Errorf("GetSrvKeyspace(out of the blue): %v %v", err, *k)
	}

	// Delete the SrvKeyspace.
	if err := ts.DeleteSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != nil {
		t.Fatalf("DeleteSrvKeyspace: %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(deleted) got %v, want ErrNoNode", err)
	}
}

// checkSrvVSchema tests the SrvVSchema methods (other than watch).
func checkSrvVSchema(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)

	// check GetSrvVSchema returns topo.ErrNoNode if no SrvVSchema
	if _, err := ts.GetSrvVSchema(ctx, cell); err != topo.ErrNoNode {
		t.Errorf("GetSrvVSchema(not set): %v", err)
	}

	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"test_keyspace": {
				Sharded: true,
			},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
		t.Errorf("UpdateSrvVSchema(1): %v", err)
	}
	if v, err := ts.GetSrvVSchema(ctx, cell); err != nil || !proto.Equal(srvVSchema, v) {
		t.Errorf("GetSrvVSchema(valid): %v %v", err, v)
	}
}
