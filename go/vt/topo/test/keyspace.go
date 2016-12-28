package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// checkKeyspace tests the keyspace part of the API
func checkKeyspace(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		t.Errorf("GetKeyspaces(empty): %v", err)
	}
	if len(keyspaces) != 0 {
		t.Errorf("len(GetKeyspaces()) != 0: %v", keyspaces)
	}

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != topo.ErrNodeExists {
		t.Errorf("CreateKeyspace(again) is not ErrNodeExists: %v", err)
	}

	// Delete and re-create.
	if err := ts.DeleteKeyspace(ctx, "test_keyspace"); err != nil {
		t.Errorf("DeleteKeyspace: %v", err)
	}
	if err := ts.DeleteKeyspace(ctx, "test_keyspace"); err != topo.ErrNoNode {
		t.Errorf("DeleteKeyspace(again): %v", err)
	}
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}

	keyspaces, err = ts.GetKeyspaces(ctx)
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 1 || keyspaces[0] != "test_keyspace" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace"}, keyspaces)
	}

	k := &topodatapb.Keyspace{
		ShardingColumnName: "user_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		ServedFroms: []*topodatapb.Keyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_REPLICA,
				Cells:      []string{"c1", "c2"},
				Keyspace:   "test_keyspace3",
			},
			{
				TabletType: topodatapb.TabletType_MASTER,
				Cells:      nil,
				Keyspace:   "test_keyspace3",
			},
		},
	}
	if err := ts.CreateKeyspace(ctx, "test_keyspace2", k); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	keyspaces, err = ts.GetKeyspaces(ctx)
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 2 ||
		keyspaces[0] != "test_keyspace" ||
		keyspaces[1] != "test_keyspace2" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace", "test_keyspace2"}, keyspaces)
	}

	// re-read and update.
	storedK, storedVersion, err := ts.GetKeyspace(ctx, "test_keyspace2")
	if err != nil {
		t.Fatalf("GetKeyspace: %v", err)
	}
	storedK.ShardingColumnName = "other_id"
	var newServedFroms []*topodatapb.Keyspace_ServedFrom
	for _, ksf := range storedK.ServedFroms {
		if ksf.TabletType == topodatapb.TabletType_MASTER {
			continue
		}
		if ksf.TabletType == topodatapb.TabletType_REPLICA {
			ksf.Keyspace = "test_keyspace4"
		}
		newServedFroms = append(newServedFroms, ksf)
	}
	storedK.ServedFroms = newServedFroms
	_, err = ts.UpdateKeyspace(ctx, "test_keyspace2", storedK, storedVersion)
	if err != nil {
		t.Fatalf("UpdateKeyspace: %v", err)
	}

	// unconditional update
	storedK.ShardingColumnType = topodatapb.KeyspaceIdType_BYTES
	_, err = ts.UpdateKeyspace(ctx, "test_keyspace2", storedK, -1)
	if err != nil {
		t.Fatalf("UpdateKeyspace(-1): %v", err)
	}

	storedK, storedVersion, err = ts.GetKeyspace(ctx, "test_keyspace2")
	if err != nil {
		t.Fatalf("GetKeyspace: %v", err)
	}
	if storedK.ShardingColumnName != "other_id" ||
		storedK.ShardingColumnType != topodatapb.KeyspaceIdType_BYTES ||
		len(storedK.ServedFroms) != 1 ||
		storedK.ServedFroms[0].TabletType != topodatapb.TabletType_REPLICA ||
		storedK.ServedFroms[0].Keyspace != "test_keyspace4" {
		t.Errorf("GetKeyspace: unexpected keyspace, got %v", *storedK)
	}
}
