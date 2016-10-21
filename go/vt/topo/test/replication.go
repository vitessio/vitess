package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// checkShardReplication tests ShardReplication objects
func checkShardReplication(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)
	if _, err := ts.GetShardReplication(ctx, cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetShardReplication(not there): %v", err)
	}

	sr := &topodatapb.ShardReplication{
		Nodes: []*topodatapb.ShardReplication_Node{
			{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "c1",
					Uid:  1,
				},
			},
		},
	}
	if err := ts.UpdateShardReplicationFields(ctx, cell, "test_keyspace", "-10", func(oldSr *topodatapb.ShardReplication) error {
		return topo.ErrNoUpdateNeeded
	}); err != nil {
		t.Fatalf("UpdateShardReplicationFields() failed: %v", err)
	}
	if err := ts.UpdateShardReplicationFields(ctx, cell, "test_keyspace", "-10", func(oldSr *topodatapb.ShardReplication) error {
		*oldSr = *sr
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardReplicationFields() failed: %v", err)
	}

	if sri, err := ts.GetShardReplication(ctx, cell, "test_keyspace", "-10"); err != nil {
		t.Errorf("GetShardReplication(new guy) failed: %v", err)
	} else {
		if len(sri.Nodes) != 1 ||
			sri.Nodes[0].TabletAlias.Cell != "c1" ||
			sri.Nodes[0].TabletAlias.Uid != 1 {
			t.Errorf("GetShardReplication(new guy) returned wrong value: %v", *sri)
		}
	}

	if err := ts.UpdateShardReplicationFields(ctx, cell, "test_keyspace", "-10", func(sr *topodatapb.ShardReplication) error {
		sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{
			TabletAlias: &topodatapb.TabletAlias{
				Cell: "c3",
				Uid:  3,
			},
		})
		return nil
	}); err != nil {
		t.Errorf("UpdateShardReplicationFields() failed: %v", err)
	}

	if sri, err := ts.GetShardReplication(ctx, cell, "test_keyspace", "-10"); err != nil {
		t.Errorf("GetShardReplication(after append) failed: %v", err)
	} else {
		if len(sri.Nodes) != 2 ||
			sri.Nodes[0].TabletAlias.Cell != "c1" ||
			sri.Nodes[0].TabletAlias.Uid != 1 ||
			sri.Nodes[1].TabletAlias.Cell != "c3" ||
			sri.Nodes[1].TabletAlias.Uid != 3 {
			t.Errorf("GetShardReplication(new guy) returned wrong value: %v", *sri)
		}
	}

	if err := ts.DeleteShardReplication(ctx, cell, "test_keyspace", "-10"); err != nil {
		t.Errorf("DeleteShardReplication(existing) failed: %v", err)
	}
	if err := ts.DeleteShardReplication(ctx, cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("DeleteShardReplication(again) returned: %v", err)
	}

	if err := ts.DeleteKeyspaceReplication(ctx, cell, "test_keyspace"); err != nil {
		t.Errorf("DeleteKeyspaceReplication(existing) failed: %v", err)
	}
	if err := ts.DeleteKeyspaceReplication(ctx, cell, "test_keyspace"); err != topo.ErrNoNode {
		t.Errorf("DeleteKeyspaceReplication(again) returned: %v", err)
	}
}
