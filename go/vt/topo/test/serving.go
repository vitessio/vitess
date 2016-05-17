// Package test contains utilities to test topo.Impl
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CheckServingGraph makes sure the serving graph functions work properly.
func CheckServingGraph(ctx context.Context, t *testing.T, ts topo.Impl) {
	cell := getLocalCell(ctx, t, ts)

	// test cell/keyspace/shard entries (SrvShard)
	srvShard := &topodatapb.SrvShard{
		Name:       "-10",
		KeyRange:   newKeyRange("-10"),
		MasterCell: "test",
	}
	if err := ts.UpdateSrvShard(ctx, cell, "test_keyspace", "-10", srvShard); err != nil {
		t.Fatalf("UpdateSrvShard(1): %v", err)
	}
	if _, err := ts.GetSrvShard(ctx, cell, "test_keyspace", "666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvShard(invalid): %v", err)
	}
	if s, err := ts.GetSrvShard(ctx, cell, "test_keyspace", "-10"); err != nil ||
		s.Name != "-10" ||
		!key.KeyRangeEqual(s.KeyRange, newKeyRange("-10")) ||
		s.MasterCell != "test" {
		t.Errorf("GetSrvShard(valid): %v", err)
	}

	// test cell/keyspace entries (SrvKeyspace)
	srvKeyspace := topodatapb.SrvKeyspace{
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
	if err := ts.UpdateSrvKeyspace(ctx, cell, "test_keyspace", &srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(1): %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, cell, "test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(invalid): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, cell, "test_keyspace"); err != nil ||
		len(k.Partitions) != 1 ||
		k.Partitions[0].ServedType != topodatapb.TabletType_MASTER ||
		len(k.Partitions[0].ShardReferences) != 1 ||
		k.Partitions[0].ShardReferences[0].Name != "-80" ||
		key.KeyRangeString(k.Partitions[0].ShardReferences[0].KeyRange) != "-80" ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != topodatapb.KeyspaceIdType_UINT64 ||
		len(k.ServedFrom) != 1 ||
		k.ServedFrom[0].TabletType != topodatapb.TabletType_REPLICA ||
		k.ServedFrom[0].Keyspace != "other_keyspace" {
		t.Errorf("GetSrvKeyspace(valid): %v %v", err, k)
	}
	if k, err := ts.GetSrvKeyspaceNames(ctx, cell); err != nil || len(k) != 1 || k[0] != "test_keyspace" {
		t.Errorf("GetSrvKeyspaceNames(): %v", err)
	}

	// check that updating a SrvKeyspace out of the blue works
	if err := ts.UpdateSrvKeyspace(ctx, cell, "unknown_keyspace_so_far", &srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(2): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != nil ||
		len(k.Partitions) != 1 ||
		k.Partitions[0].ServedType != topodatapb.TabletType_MASTER ||
		len(k.Partitions[0].ShardReferences) != 1 ||
		k.Partitions[0].ShardReferences[0].Name != "-80" ||
		key.KeyRangeString(k.Partitions[0].ShardReferences[0].KeyRange) != "-80" ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != topodatapb.KeyspaceIdType_UINT64 ||
		len(k.ServedFrom) != 1 ||
		k.ServedFrom[0].TabletType != topodatapb.TabletType_REPLICA ||
		k.ServedFrom[0].Keyspace != "other_keyspace" {
		t.Errorf("GetSrvKeyspace(out of the blue): %v %v", err, *k)
	}

	// Delete the SrvKeyspace.
	if err := ts.DeleteSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != nil {
		t.Fatalf("DeleteSrvShard: %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(deleted) got %v, want ErrNoNode", err)
	}
}

// CheckWatchSrvKeyspace makes sure WatchSrvKeyspace works as expected
func CheckWatchSrvKeyspace(ctx context.Context, t *testing.T, ts topo.Impl) {
	cell := getLocalCell(ctx, t, ts)
	keyspace := "test_keyspace"

	// start watching, should get nil first
	ctx, cancel := context.WithCancel(ctx)
	notifications, err := ts.WatchSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		t.Fatalf("WatchSrvKeyspace failed: %v", err)
	}
	sk, ok := <-notifications
	if !ok || sk != nil {
		t.Fatalf("first value is wrong: %v %v", sk, ok)
	}

	// update the SrvKeyspace, should get a notification
	srvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "test_column",
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "0",
					},
				},
			},
		},
		ServedFrom: []*topodatapb.SrvKeyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   "other_keyspace",
			},
		},
	}
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace failed: %v", err)
	}
	for {
		sk, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if sk == nil {
			// duplicate notification of the first value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(sk, srvKeyspace) {
			t.Fatalf("first value is wrong: got %v expected %v", sk, srvKeyspace)
		}
		break
	}

	// delete the SrvKeyspace, should get a notification
	if err := ts.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil {
		t.Fatalf("DeleteSrvKeyspace failed: %v", err)
	}
	for {
		sk, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if sk == nil {
			break
		}

		// duplicate notification of the first value, that's OK,
		// but value better be good.
		if !reflect.DeepEqual(srvKeyspace, sk) {
			t.Fatalf("duplicate notification value is bad: %v", sk)
		}
	}

	// re-create the value, a bit different, should get a notification
	srvKeyspace.SplitShardCount = 2
	if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace failed: %v", err)
	}
	for {
		sk, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if sk == nil {
			// duplicate notification of the closed value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(srvKeyspace, sk) {
			t.Fatalf("value after delete / re-create is wrong: %v %v", sk, ok)
		}
		break
	}

	// close the context, should eventually get a closed
	// notifications channel too
	cancel()
	for {
		sk, ok := <-notifications
		if !ok {
			break
		}
		if !reflect.DeepEqual(srvKeyspace, sk) {
			t.Fatalf("duplicate notification value is bad: %v", sk)
		}
	}
}
