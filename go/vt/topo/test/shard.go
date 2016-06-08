package test

import (
	"encoding/json"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func shardEqual(left, right *topodatapb.Shard) (bool, error) {
	lj, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rj, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return string(lj) == string(rj), nil
}

// checkShard verifies the Shard operations work correctly
func checkShard(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	tts := topo.Server{Impl: ts}

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	shard := &topodatapb.Shard{
		KeyRange: newKeyRange("b0-c0"),
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0", shard); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0", shard); err != topo.ErrNodeExists {
		t.Errorf("CreateShard called second time, got: %v", err)
	}

	// Delete shard and see if we can re-create it.
	if err := ts.DeleteShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0", shard); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	// Delete ALL shards.
	if err := ts.DeleteKeyspaceShards(ctx, "test_keyspace"); err != nil {
		t.Fatalf("DeleteKeyspaceShards: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0", shard); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	if _, _, err := ts.GetShard(ctx, "test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetShard(666): %v", err)
	}

	shard, version, err := ts.GetShard(ctx, "test_keyspace", "b0-c0")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if want := newKeyRange("b0-c0"); !key.KeyRangeEqual(shard.KeyRange, want) {
		t.Errorf("shard.KeyRange: want %v, got %v", want, shard.KeyRange)
	}
	master := &topodatapb.TabletAlias{Cell: "ny", Uid: 1}
	shard.MasterAlias = master
	shard.KeyRange = newKeyRange("b0-c0")
	shard.ServedTypes = []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_MASTER,
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Cells:      []string{"c1"},
		},
		{
			TabletType: topodatapb.TabletType_RDONLY,
		},
	}
	shard.SourceShards = []*topodatapb.Shard_SourceShard{
		{
			Uid:      1,
			Keyspace: "source_ks",
			Shard:    "b8-c0",
			KeyRange: newKeyRange("b8-c0"),
			Tables:   []string{"table1", "table2"},
		},
	}
	shard.TabletControls = []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_MASTER,
			Cells:             []string{"c1", "c2"},
			BlacklistedTables: []string{"black1", "black2"},
		},
		{
			TabletType:          topodatapb.TabletType_REPLICA,
			DisableQueryService: true,
		},
	}
	if _, err := ts.UpdateShard(ctx, "test_keyspace", "b0-c0", shard, version); err != nil {
		t.Errorf("UpdateShard: %v", err)
	}

	other := &topodatapb.TabletAlias{Cell: "ny", Uid: 82873}
	_, err = tts.UpdateShardFields(ctx, "test_keyspace", "b0-c0", func(si *topo.ShardInfo) error {
		si.MasterAlias = other
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields error: %v", err)
	}

	s, _, err := ts.GetShard(ctx, "test_keyspace", "b0-c0")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if *s.MasterAlias != *other {
		t.Fatalf("shard.MasterAlias = %v, want %v", s.MasterAlias, other)
	}

	// unconditional shard update
	_, err = ts.UpdateShard(ctx, "test_keyspace", "b0-c0", shard, -1)
	if err != nil {
		t.Fatalf("UpdateShard(-1) error: %v", err)
	}

	updatedShard, _, err := ts.GetShard(ctx, "test_keyspace", "b0-c0")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}

	if eq, err := shardEqual(shard, updatedShard); err != nil {
		t.Errorf("cannot compare shards: %v", err)
	} else if !eq {
		t.Errorf("put and got shards are not identical:\n%#v\n%#v", shard, updatedShard)
	}

	// test GetShardNames
	shards, err := ts.GetShardNames(ctx, "test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 || shards[0] != "b0-c0" {
		t.Errorf(`GetShardNames: want [ "b0-c0" ], got %v`, shards)
	}

	if _, err := ts.GetShardNames(ctx, "test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetShardNames(666): %v", err)
	}

	// test ValidateShard
	if err := ts.ValidateShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Errorf("ValidateShard(test_keyspace, b0-c0) failed: %v", err)
	}
}
