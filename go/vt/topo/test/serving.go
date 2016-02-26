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

	// test individual cell/keyspace/shard/type entries
	if _, err := ts.GetSrvTabletTypesPerShard(ctx, cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetSrvTabletTypesPerShard(invalid): %v", err)
	}
	if _, _, err := ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER); err != topo.ErrNoNode {
		t.Errorf("GetEndPoints(invalid): %v", err)
	}

	endPoints := &topodatapb.EndPoints{
		Entries: []*topodatapb.EndPoint{
			{
				Uid:  1,
				Host: "host1",
				PortMap: map[string]int32{
					"vt":    1234,
					"mysql": 1235,
					"grpc":  1236,
				},
			},
		},
	}

	if err := ts.CreateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints); err != nil {
		t.Fatalf("CreateEndPoints(master): %v", err)
	}
	// Try to create again.
	if err := ts.CreateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints); err != topo.ErrNodeExists {
		t.Fatalf("CreateEndPoints(master): err = %v, want topo.ErrNodeExists", err)
	}

	// Get version.
	_, version, err := ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints(master): %v", err)
	}
	// Make a change.
	tmp := endPoints.Entries[0].Uid
	endPoints.Entries[0].Uid = tmp + 1
	if err := ts.UpdateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}
	endPoints.Entries[0].Uid = tmp
	// Try to delete with the wrong version.
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, version); err != topo.ErrBadVersion {
		t.Fatalf("DeleteEndPoints: err = %v, want topo.ErrBadVersion", err)
	}
	// Delete with the correct version.
	_, version, err = ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints(master): %v", err)
	}
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, version); err != nil {
		t.Fatalf("DeleteEndPoints: %v", err)
	}
	// Recreate it with an unconditional update.
	if err := ts.UpdateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}

	if types, err := ts.GetSrvTabletTypesPerShard(ctx, cell, "test_keyspace", "-10"); err != nil || len(types) != 1 || types[0] != topodatapb.TabletType_MASTER {
		t.Errorf("GetSrvTabletTypesPerShard(1): %v %v", err, types)
	}

	// Delete it unconditionally.
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, -1); err != nil {
		t.Fatalf("DeleteEndPoints: %v", err)
	}

	// Delete the SrvShard.
	if err := ts.DeleteSrvShard(ctx, cell, "test_keyspace", "-10"); err != nil {
		t.Fatalf("DeleteSrvShard: %v", err)
	}
	if _, err := ts.GetSrvShard(ctx, cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetSrvShard(deleted) got %v, want ErrNoNode", err)
	}

	// Re-add endpoints.
	if err := ts.UpdateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}

	addrs, version, err := ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER)
	if err != nil {
		t.Errorf("GetEndPoints: %v", err)
	}
	if len(addrs.Entries) != 1 || addrs.Entries[0].Uid != 1 {
		t.Errorf("GetEndPoints(1): %v", addrs)
	}
	if pm := addrs.Entries[0].PortMap; pm["vt"] != 1234 || pm["mysql"] != 1235 || pm["grpc"] != 1236 {
		t.Errorf("GetSrcTabletType(1).PortMap: want %v, got %v", endPoints.Entries[0].PortMap, pm)
	}

	// Update with the wrong version.
	if err := ts.UpdateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints, version+1); err != topo.ErrBadVersion {
		t.Fatalf("UpdateEndPoints(master): err = %v, want topo.ErrBadVersion", err)
	}
	// Update with the right version.
	if err := ts.UpdateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints, version); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}
	// Update existing EndPoints unconditionally.
	if err := ts.UpdateEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}

	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_REPLICA, -1); err != topo.ErrNoNode {
		t.Errorf("DeleteEndPoints(unknown): %v", err)
	}
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topodatapb.TabletType_MASTER, -1); err != nil {
		t.Errorf("DeleteEndPoints(master): %v", err)
	}

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
