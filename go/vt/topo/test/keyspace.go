// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"reflect"
	"testing"

	"github.com/henryanand/vitess/go/vt/key"
	"github.com/henryanand/vitess/go/vt/topo"
)

func CheckKeyspace(t *testing.T, ts topo.Server) {
	keyspaces, err := ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces(empty): %v", err)
	}
	if len(keyspaces) != 0 {
		t.Errorf("len(GetKeyspaces()) != 0: %v", keyspaces)
	}

	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	if err := ts.CreateKeyspace("test_keyspace", &topo.Keyspace{}); err != topo.ErrNodeExists {
		t.Errorf("CreateKeyspace(again) is not ErrNodeExists: %v", err)
	}

	keyspaces, err = ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 1 || keyspaces[0] != "test_keyspace" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace"}, keyspaces)
	}

	k := &topo.Keyspace{
		ShardingColumnName: "user_id",
		ShardingColumnType: key.KIT_UINT64,
		ServedFromMap: map[topo.TabletType]*topo.KeyspaceServedFrom{
			topo.TYPE_REPLICA: &topo.KeyspaceServedFrom{
				Cells:    []string{"c1", "c2"},
				Keyspace: "test_keyspace3",
			},
			topo.TYPE_MASTER: &topo.KeyspaceServedFrom{
				Cells:    nil,
				Keyspace: "test_keyspace3",
			},
		},
		SplitShardCount: 64,
	}
	if err := ts.CreateKeyspace("test_keyspace2", k); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	keyspaces, err = ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 2 ||
		keyspaces[0] != "test_keyspace" ||
		keyspaces[1] != "test_keyspace2" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace", "test_keyspace2"}, keyspaces)
	}

	ki, err := ts.GetKeyspace("test_keyspace2")
	if err != nil {
		t.Fatalf("GetKeyspace: %v", err)
	}
	if !reflect.DeepEqual(ki.Keyspace, k) {
		t.Fatalf("returned keyspace doesn't match: got %v expected %v", ki.Keyspace, k)
	}

	ki.ShardingColumnName = "other_id"
	ki.ShardingColumnType = key.KIT_BYTES
	delete(ki.ServedFromMap, topo.TYPE_MASTER)
	ki.ServedFromMap[topo.TYPE_REPLICA].Keyspace = "test_keyspace4"
	err = topo.UpdateKeyspace(ts, ki)
	if err != nil {
		t.Fatalf("UpdateKeyspace: %v", err)
	}
	ki, err = ts.GetKeyspace("test_keyspace2")
	if err != nil {
		t.Fatalf("GetKeyspace: %v", err)
	}
	if ki.ShardingColumnName != "other_id" ||
		ki.ShardingColumnType != key.KIT_BYTES ||
		ki.ServedFromMap[topo.TYPE_REPLICA].Keyspace != "test_keyspace4" {
		t.Errorf("GetKeyspace: unexpected keyspace, got %v", *ki)
	}
}
