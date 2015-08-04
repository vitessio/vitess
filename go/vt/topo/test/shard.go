// Package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"encoding/json"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func shardEqual(left, right *pb.Shard) (bool, error) {
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

// CheckShard verifies the Shard operations work correctly
func CheckShard(ctx context.Context, t *testing.T, ts topo.Server) {
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &pb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	if err := topo.CreateShard(ctx, ts, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if err := topo.CreateShard(ctx, ts, "test_keyspace", "b0-c0"); err != topo.ErrNodeExists {
		t.Errorf("CreateShard called second time, got: %v", err)
	}

	// Delete shard and see if we can re-create it.
	if err := ts.DeleteShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}
	if err := topo.CreateShard(ctx, ts, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	// Delete ALL shards.
	if err := ts.DeleteKeyspaceShards(ctx, "test_keyspace"); err != nil {
		t.Fatalf("DeleteKeyspaceShards: %v", err)
	}
	if err := topo.CreateShard(ctx, ts, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	if _, err := topo.GetShard(ctx, ts, "test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetShard(666): %v", err)
	}

	shardInfo, err := topo.GetShard(ctx, ts, "test_keyspace", "b0-c0")
	if err != nil {
		t.Errorf("GetShard: %v", err)
	}
	if want := newKeyRange3("b0-c0"); !key.KeyRangeEqual(shardInfo.KeyRange, want) {
		t.Errorf("shardInfo.KeyRange: want %v, got %v", want, shardInfo.KeyRange)
	}
	master := &pb.TabletAlias{Cell: "ny", Uid: 1}
	shardInfo.MasterAlias = master
	shardInfo.KeyRange = newKeyRange3("b0-c0")
	shardInfo.ServedTypes = []*pb.Shard_ServedType{
		&pb.Shard_ServedType{
			TabletType: pb.TabletType_MASTER,
		},
		&pb.Shard_ServedType{
			TabletType: pb.TabletType_REPLICA,
			Cells:      []string{"c1"},
		},
		&pb.Shard_ServedType{
			TabletType: pb.TabletType_RDONLY,
		},
	}
	shardInfo.SourceShards = []*pb.Shard_SourceShard{
		&pb.Shard_SourceShard{
			Uid:      1,
			Keyspace: "source_ks",
			Shard:    "b8-c0",
			KeyRange: newKeyRange3("b8-c0"),
			Tables:   []string{"table1", "table2"},
		},
	}
	shardInfo.TabletControls = []*pb.Shard_TabletControl{
		&pb.Shard_TabletControl{
			TabletType:        pb.TabletType_MASTER,
			Cells:             []string{"c1", "c2"},
			BlacklistedTables: []string{"black1", "black2"},
		},
		&pb.Shard_TabletControl{
			TabletType:          pb.TabletType_REPLICA,
			DisableQueryService: true,
		},
	}
	if err := topo.UpdateShard(ctx, ts, shardInfo); err != nil {
		t.Errorf("UpdateShard: %v", err)
	}

	other := &pb.TabletAlias{Cell: "ny", Uid: 82873}
	_, err = topo.UpdateShardFields(ctx, ts, "test_keyspace", "b0-c0", func(shard *pb.Shard) error {
		shard.MasterAlias = other
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields error: %v", err)
	}
	si, err := topo.GetShard(ctx, ts, "test_keyspace", "b0-c0")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if *si.MasterAlias != *other {
		t.Fatalf("shard.MasterAlias = %v, want %v", si.MasterAlias, other)
	}
	_, err = topo.UpdateShardFields(ctx, ts, "test_keyspace", "b0-c0", func(shard *pb.Shard) error {
		shard.MasterAlias = master
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields error: %v", err)
	}

	updatedShardInfo, err := topo.GetShard(ctx, ts, "test_keyspace", "b0-c0")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}

	if eq, err := shardEqual(shardInfo.Shard, updatedShardInfo.Shard); err != nil {
		t.Errorf("cannot compare shards: %v", err)
	} else if !eq {
		t.Errorf("put and got shards are not identical:\n%#v\n%#v", shardInfo.Shard, updatedShardInfo.Shard)
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
