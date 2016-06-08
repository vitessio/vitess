package test

import (
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// checkSrvKeyspace tests the SrvKeyspace methods (other than watch).
func checkSrvKeyspace(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)

	// test cell/keyspace entries (SrvKeyspace)
	srvKeyspace := &topodatapb.SrvKeyspace{
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
	if err := ts.UpdateSrvKeyspace(ctx, cell, "test_keyspace", srvKeyspace); err != nil {
		t.Errorf("UpdateSrvKeyspace(1): %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, cell, "test_keyspace666"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(invalid): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, cell, "test_keyspace"); err != nil || !proto.Equal(srvKeyspace, k) {
		t.Errorf("GetSrvKeyspace(valid): %v %v", err, k)
	}
	if k, err := ts.GetSrvKeyspaceNames(ctx, cell); err != nil || len(k) != 1 || k[0] != "test_keyspace" {
		t.Errorf("GetSrvKeyspaceNames(): %v", err)
	}

	// check that updating a SrvKeyspace out of the blue works
	if err := ts.UpdateSrvKeyspace(ctx, cell, "unknown_keyspace_so_far", srvKeyspace); err != nil {
		t.Fatalf("UpdateSrvKeyspace(2): %v", err)
	}
	if k, err := ts.GetSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != nil || !proto.Equal(srvKeyspace, k) {
		t.Errorf("GetSrvKeyspace(out of the blue): %v %v", err, *k)
	}

	// Delete the SrvKeyspace.
	if err := ts.DeleteSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != nil {
		t.Fatalf("DeleteSrvKeyspace: %v", err)
	}
	if _, err := ts.GetSrvKeyspace(ctx, cell, "unknown_keyspace_so_far"); err != topo.ErrNoNode {
		t.Errorf("GetSrvKeyspace(deleted) got %v, want ErrNoNode", err)
	}
}

// checkWatchSrvKeyspace makes sure WatchSrvKeyspace works as expected
func checkWatchSrvKeyspace(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
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
	srvKeyspace.ShardingColumnName = "test_column2"
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

// checkSrvVSchema tests the SrvVSchema methods (other than watch).
func checkSrvVSchema(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)

	// check GetSrvVSchema returns topo.ErrNoNode if no SrvVSchema
	if _, err := ts.GetSrvVSchema(ctx, cell); err != topo.ErrNoNode {
		t.Errorf("GetSrvVSchema(not set): %v", err)
	}

	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"test_keyspace": {
				Sharded: true,
			},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
		t.Errorf("UpdateSrvVSchema(1): %v", err)
	}
	if v, err := ts.GetSrvVSchema(ctx, cell); err != nil || !proto.Equal(srvVSchema, v) {
		t.Errorf("GetSrvVSchema(valid): %v %v", err, v)
	}
}

func checkWatchSrvVSchema(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	cell := getLocalCell(ctx, t, ts)
	emptySrvVSchema := &vschemapb.SrvVSchema{}

	// start watching, should get nil first
	ctx, cancel := context.WithCancel(ctx)
	notifications, err := ts.WatchSrvVSchema(ctx, cell)
	if err != nil {
		t.Fatalf("WatchSrvVSchema failed: %v", err)
	}
	v, ok := <-notifications
	if !ok || v != nil {
		t.Fatalf("first value is wrong: %v %v", v, ok)
	}

	// update the SrvVSchema, should get a notification
	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"test_keyspace": {
				Sharded: true,
			},
		},
	}
	if err := ts.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
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
		if !reflect.DeepEqual(sk, srvVSchema) {
			t.Fatalf("first value is wrong: got %v expected %v", sk, srvVSchema)
		}
		break
	}

	// update with an empty value, should get a notification
	if err := ts.UpdateSrvVSchema(ctx, cell, emptySrvVSchema); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
	}
	for {
		v, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if v == nil || proto.Equal(v, emptySrvVSchema) {
			break
		}
		// duplicate notification of the first value, that's OK,
		// but value better be good.
		if !reflect.DeepEqual(v, srvVSchema) {
			t.Fatalf("duplicate notification value is bad: %v", v)
		}
	}

	// re-create the value, a bit different, should get a notification
	srvVSchema.Keyspaces["test_keyspace"].Sharded = false
	if err := ts.UpdateSrvVSchema(ctx, cell, srvVSchema); err != nil {
		t.Fatalf("UpdateSrvVSchema failed: %v", err)
	}
	for {
		v, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if v == nil || proto.Equal(v, emptySrvVSchema) {
			// duplicate notification of the closed value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !reflect.DeepEqual(v, srvVSchema) {
			t.Fatalf("value after delete / re-create is wrong: %v %v", v, ok)
		}
		break
	}

	// close the context, should eventually get a closed
	// notifications channel too
	cancel()
	for {
		v, ok := <-notifications
		if !ok {
			break
		}
		if !reflect.DeepEqual(v, srvVSchema) {
			t.Fatalf("duplicate notification value is bad: %v", v)
		}
	}
}
