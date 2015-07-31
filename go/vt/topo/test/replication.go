// Package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CheckShardReplication tests ShardReplication objects
func CheckShardReplication(ctx context.Context, t *testing.T, ts topo.Server) {
	cell := getLocalCell(ctx, t, ts)
	if _, err := ts.GetShardReplication(ctx, cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetShardReplication(not there): %v", err)
	}

	sr := &pb.ShardReplication{
		Nodes: []*pb.ShardReplication_Node{
			&pb.ShardReplication_Node{
				TabletAlias: &pb.TabletAlias{
					Cell: "c1",
					Uid:  1,
				},
			},
		},
	}
	if err := ts.UpdateShardReplicationFields(ctx, cell, "test_keyspace", "-10", func(oldSr *pb.ShardReplication) error {
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

	if err := ts.UpdateShardReplicationFields(ctx, cell, "test_keyspace", "-10", func(sr *pb.ShardReplication) error {
		sr.Nodes = append(sr.Nodes, &pb.ShardReplication_Node{
			TabletAlias: &pb.TabletAlias{
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
