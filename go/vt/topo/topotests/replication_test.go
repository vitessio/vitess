package topotests

import (
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains tests for the replication.go file.  Note we use a
// zktestserver, because memorytopo doesn't support all topo server
// methods yet.

func TestFixShardReplication(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	shard := "shard1"
	ctx := context.Background()
	ts := zktestserver.New(t, []string{cell})

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

	// Make sure it's in the ShardReplication.
	sri, err := ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil {
		t.Fatalf("GetShardReplication failed: %v", err)
	}
	if len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, alias) {
		t.Errorf("Missing or wrong alias in ShardReplication: %v", sri)
	}

	// Run FixShardReplication, should do nothing.
	logger := logutil.NewMemoryLogger()
	if err := topo.FixShardReplication(ctx, ts, logger, cell, keyspace, shard); err != nil {
		t.Errorf("FixShardReplication failed: %v", err)
	}
	sri, err = ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil {
		t.Fatalf("GetShardReplication failed: %v", err)
	}
	if len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, alias) {
		t.Errorf("Missing or wrong alias in ShardReplication: %v", sri)
	}
	if !strings.Contains(logger.String(), "All entries in replication graph are valid") {
		t.Errorf("Wrong log: %v", logger.String())
	}

	// Add a bogus entries: a non-existing tablet.
	if err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(sr *topodatapb.ShardReplication) error {
		sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{
			TabletAlias: &topodatapb.TabletAlias{
				Cell: cell,
				Uid:  2,
			},
		})
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardReplicationFields failed: %v", err)
	}
	logger.Clear()
	if err := topo.FixShardReplication(ctx, ts, logger, cell, keyspace, shard); err != nil {
		t.Errorf("FixShardReplication failed: %v", err)
	}
	sri, err = ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil {
		t.Fatalf("GetShardReplication failed: %v", err)
	}
	if len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, alias) {
		t.Errorf("Missing or wrong alias in ShardReplication: %v", sri)
	}
	if !strings.Contains(logger.String(), "but does not exist, removing it") {
		t.Errorf("Wrong log: %v", logger.String())
	}

	// Add a bogus entries: a tablet with wrong keyspace.
	if err := ts.CreateTablet(ctx, &topodatapb.Tablet{
		Keyspace: "other" + keyspace,
		Shard:    shard,
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  3,
		},
	}); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	if err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(sr *topodatapb.ShardReplication) error {
		sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{
			TabletAlias: &topodatapb.TabletAlias{
				Cell: cell,
				Uid:  3,
			},
		})
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardReplicationFields failed: %v", err)
	}
	logger.Clear()
	if err := topo.FixShardReplication(ctx, ts, logger, cell, keyspace, shard); err != nil {
		t.Errorf("FixShardReplication failed: %v", err)
	}
	sri, err = ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil {
		t.Fatalf("GetShardReplication failed: %v", err)
	}
	if len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, alias) {
		t.Errorf("Missing or wrong alias in ShardReplication: %v", sri)
	}
	if !strings.Contains(logger.String(), "but has wrong keyspace/shard/cell, removing it") {
		t.Errorf("Wrong log: %v", logger.String())
	}
}
