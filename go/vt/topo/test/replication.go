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

	// Some implementations may already remove the directory if not data is in there, so we ignore topo.ErrNoNode.
	if err := ts.DeleteKeyspaceReplication(ctx, cell, "test_keyspace"); err != nil && err != topo.ErrNoNode {
		t.Errorf("DeleteKeyspaceReplication(existing) failed: %v", err)
	}
	// The second time though, it should be gone.
	if err := ts.DeleteKeyspaceReplication(ctx, cell, "test_keyspace"); err != topo.ErrNoNode {
		t.Errorf("DeleteKeyspaceReplication(again) returned: %v", err)
	}
}
