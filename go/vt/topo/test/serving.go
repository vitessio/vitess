// Package test contains utilities to test topo.Server
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

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CheckServingGraph makes sure the serving graph functions work properly.
func CheckServingGraph(ctx context.Context, t *testing.T, ts topo.Server) {
	cell := getLocalCell(ctx, t, ts)

	// test individual cell/keyspace/shard/type entries
	if _, err := ts.GetSrvTabletTypesPerShard(ctx, cell, "test_keyspace", "-10"); err != topo.ErrNoNode {
		t.Errorf("GetSrvTabletTypesPerShard(invalid): %v", err)
	}
	if _, _, err := ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER); err != topo.ErrNoNode {
		t.Errorf("GetEndPoints(invalid): %v", err)
	}

	endPoints := &pb.EndPoints{
		Entries: []*pb.EndPoint{
			&pb.EndPoint{
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

	if err := ts.CreateEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints); err != nil {
		t.Fatalf("CreateEndPoints(master): %v", err)
	}
	// Try to create again.
	if err := ts.CreateEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints); err != topo.ErrNodeExists {
		t.Fatalf("CreateEndPoints(master): err = %v, want topo.ErrNodeExists", err)
	}

	// Get version.
	_, version, err := ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints(master): %v", err)
	}
	// Make a change.
	tmp := endPoints.Entries[0].Uid
	endPoints.Entries[0].Uid = tmp + 1
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}
	endPoints.Entries[0].Uid = tmp
	// Try to delete with the wrong version.
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER, version); err != topo.ErrBadVersion {
		t.Fatalf("DeleteEndPoints: err = %v, want topo.ErrBadVersion", err)
	}
	// Delete with the correct version.
	_, version, err = ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER)
	if err != nil {
		t.Fatalf("GetEndPoints(master): %v", err)
	}
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER, version); err != nil {
		t.Fatalf("DeleteEndPoints: %v", err)
	}
	// Recreate it with an unconditional update.
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}

	if types, err := ts.GetSrvTabletTypesPerShard(ctx, cell, "test_keyspace", "-10"); err != nil || len(types) != 1 || types[0] != topo.TYPE_MASTER {
		t.Errorf("GetSrvTabletTypesPerShard(1): %v %v", err, types)
	}

	// Delete it unconditionally.
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER, -1); err != nil {
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
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}

	addrs, version, err := ts.GetEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER)
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
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints, version+1); err != topo.ErrBadVersion {
		t.Fatalf("UpdateEndPoints(master): err = %v, want topo.ErrBadVersion", err)
	}
	// Update with the right version.
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints, version); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}
	// Update existing EndPoints unconditionally.
	if err := topo.UpdateEndPoints(ctx, ts, cell, "test_keyspace", "-10", topo.TYPE_MASTER, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints(master): %v", err)
	}

	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_REPLICA, -1); err != topo.ErrNoNode {
		t.Errorf("DeleteEndPoints(unknown): %v", err)
	}
	if err := ts.DeleteEndPoints(ctx, cell, "test_keyspace", "-10", topo.TYPE_MASTER, -1); err != nil {
		t.Errorf("DeleteEndPoints(master): %v", err)
	}

	// test cell/keyspace/shard entries (SrvShard)
	srvShard := &pb.SrvShard{
		Name:       "-10",
		KeyRange:   newKeyRange3("-10"),
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
		!key.KeyRangeEqual(s.KeyRange, newKeyRange3("-10")) ||
		s.MasterCell != "test" {
		t.Errorf("GetSrvShard(valid): %v", err)
	}

	// test cell/keyspace entries (SrvKeyspace)
	srvKeyspace := topo.SrvKeyspace{
		Partitions: map[topo.TabletType]*topo.KeyspacePartition{
			topo.TYPE_MASTER: &topo.KeyspacePartition{
				ShardReferences: []topo.ShardReference{
					topo.ShardReference{
						Name:     "-80",
						KeyRange: newKeyRange("-80"),
					},
				},
			},
		},
		ShardingColumnName: "video_id",
		ShardingColumnType: key.KIT_UINT64,
		ServedFrom: map[topo.TabletType]string{
			topo.TYPE_REPLICA: "other_keyspace",
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
		len(k.Partitions[topo.TYPE_MASTER].ShardReferences) != 1 ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].Name != "-80" ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].KeyRange != newKeyRange("-80") ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != key.KIT_UINT64 ||
		k.ServedFrom[topo.TYPE_REPLICA] != "other_keyspace" {
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
		len(k.Partitions[topo.TYPE_MASTER].ShardReferences) != 1 ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].Name != "-80" ||
		k.Partitions[topo.TYPE_MASTER].ShardReferences[0].KeyRange != newKeyRange("-80") ||
		k.ShardingColumnName != "video_id" ||
		k.ShardingColumnType != key.KIT_UINT64 ||
		k.ServedFrom[topo.TYPE_REPLICA] != "other_keyspace" {
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

// CheckWatchEndPoints makes sure WatchEndPoints works as expected
func CheckWatchEndPoints(ctx context.Context, t *testing.T, ts topo.Server) {
	cell := getLocalCell(ctx, t, ts)
	keyspace := "test_keyspace"
	shard := "-10"
	tabletType := topo.TYPE_MASTER

	// start watching, should get nil first
	notifications, stopWatching, err := ts.WatchEndPoints(ctx, cell, keyspace, shard, tabletType)
	if err != nil {
		t.Fatalf("WatchEndPoints failed: %v", err)
	}
	ep, ok := <-notifications
	if !ok || ep != nil {
		t.Fatalf("first value is wrong: %v %v", ep, ok)
	}

	// update the endpoints, should get a notification
	endPoints := &pb.EndPoints{
		Entries: []*pb.EndPoint{
			&pb.EndPoint{
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
	if err := topo.UpdateEndPoints(ctx, ts, cell, keyspace, shard, tabletType, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints failed: %v", err)
	}
	for {
		ep, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if ep == nil {
			// duplicate notification of the first value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(endPoints, ep) {
			t.Fatalf("first value is wrong: %v %v", ep, ok)
		}
		break
	}

	// delete the endpoints, should get a notification
	if err := ts.DeleteEndPoints(ctx, cell, keyspace, shard, tabletType, -1); err != nil {
		t.Fatalf("DeleteEndPoints failed: %v", err)
	}
	for {
		ep, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if ep == nil {
			break
		}

		// duplicate notification of the first value, that's OK,
		// but value better be good.
		if !reflect.DeepEqual(endPoints, ep) {
			t.Fatalf("duplicate notification value is bad: %v", ep)
		}
	}

	// re-create the value, a bit different, should get a notification
	endPoints.Entries[0].Uid = 2
	if err := topo.UpdateEndPoints(ctx, ts, cell, keyspace, shard, tabletType, endPoints, -1); err != nil {
		t.Fatalf("UpdateEndPoints failed: %v", err)
	}
	for {
		ep, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if ep == nil {
			// duplicate notification of the closed value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(endPoints, ep) {
			t.Fatalf("value after delete / re-create is wrong: %v %v", ep, ok)
		}
		break
	}

	// close the stopWatching channel, should eventually get a closed
	// notifications channel too
	close(stopWatching)
	for {
		ep, ok := <-notifications
		if !ok {
			break
		}
		if !reflect.DeepEqual(endPoints, ep) {
			t.Fatalf("duplicate notification value is bad: %v", ep)
		}
	}
}
