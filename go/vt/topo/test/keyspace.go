/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"testing"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// checkKeyspace tests the keyspace part of the API
func checkKeyspace(t *testing.T, ts *topo.Server) {
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
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); !topo.IsErrType(err, topo.NodeExists) {
		t.Errorf("CreateKeyspace(again) is not ErrNodeExists: %v", err)
	}

	// Delete and re-create.
	if err := ts.DeleteKeyspace(ctx, "test_keyspace"); err != nil {
		t.Errorf("DeleteKeyspace: %v", err)
	}
	if err := ts.DeleteKeyspace(ctx, "test_keyspace"); !topo.IsErrType(err, topo.NoNode) {
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

	// Re-read and update. Have to lock it, because UpdateKeyspace
	// checks for the keyspace lock.
	storedKI, err := ts.GetKeyspace(ctx, "test_keyspace2")
	if err != nil {
		t.Fatalf("GetKeyspace: %v", err)
	}
	storedKI.Keyspace.ShardingColumnName = "other_id"
	lockCtx, unlock, err := ts.LockKeyspace(ctx, "test_keyspace2", "fake-action")
	if err != nil {
		t.Fatalf("LockKeyspace: %v", err)
	}
	if err := ts.UpdateKeyspace(lockCtx, storedKI); err != nil {
		t.Fatalf("UpdateKeyspace: %v", err)
	}
	unlock(&err)
	if err != nil {
		t.Fatalf("unlock(test_keyspace2): %v", err)
	}

	// And read again to make sure it's good.
	storedKI, err = ts.GetKeyspace(ctx, "test_keyspace2")
	if err != nil {
		t.Fatalf("GetKeyspace: %v", err)
	}
	if storedKI.Keyspace.ShardingColumnName != "other_id" {
		t.Errorf("UpdateKeyspace failed: got %v, want 'other_id'", storedKI.Keyspace.ShardingColumnName)
	}
}
