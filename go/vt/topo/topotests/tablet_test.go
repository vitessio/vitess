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

package topotests

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains tests for the tablet.go file.

// TestCreateTablet tests all the logic in the topo.CreateTablet method.
func TestCreateTablet(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	shard := "shard1"
	ctx := context.Background()
	ts := topo.Server{Impl: memorytopo.New(cell)}

	// Create a tablet.
	alias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  1,
	}
	tablet := &topodatapb.Tablet{
		Keyspace: keyspace,
		Shard:    shard,
		Alias:    alias,
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	// Get the tablet, make sure it's good. Also check ShardReplication.
	ti, err := ts.GetTablet(ctx, alias)
	if err != nil || !proto.Equal(ti.Tablet, tablet) {
		t.Fatalf("Created Tablet doesn't match: %v %v", ti, err)
	}
	sri, err := ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil || len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, alias) {
		t.Fatalf("Created ShardReplication doesn't match: %v %v", sri, err)
	}

	// Create the same tablet again, make sure it fails with ErrNodeExists.
	if err := ts.CreateTablet(ctx, tablet); err != topo.ErrNodeExists {
		t.Fatalf("CreateTablet(again) returned: %v", err)
	}

	// Remove the ShardReplication record, try to create the
	// tablets again, make sure it's fixed.
	if err := topo.RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, alias); err != nil {
		t.Fatalf("RemoveShardReplicationRecord failed: %v", err)
	}
	sri, err = ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil || len(sri.Nodes) != 0 {
		t.Fatalf("Modifed ShardReplication doesn't match: %v %v", sri, err)
	}
	if err := ts.CreateTablet(ctx, tablet); err != topo.ErrNodeExists {
		t.Fatalf("CreateTablet(again and again) returned: %v", err)
	}
	sri, err = ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil || len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, alias) {
		t.Fatalf("Created ShardReplication doesn't match: %v %v", sri, err)
	}
}
