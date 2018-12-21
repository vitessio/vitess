/*
Copyright 2017 Google Inc.

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

package topo

import (
	"reflect"
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file tests the keyspace related object functionnalities.

func TestUpdateServedFromMap(t *testing.T) {
	ki := &KeyspaceInfo{
		keyspace: "ks",
		version:  nil,
		Keyspace: &topodatapb.Keyspace{
			ServedFroms: []*topodatapb.Keyspace_ServedFrom{
				{
					TabletType: topodatapb.TabletType_RDONLY,
					Keyspace:   "source",
				},
				{
					TabletType: topodatapb.TabletType_MASTER,
					Keyspace:   "source",
				},
			},
		},
	}

	// couple error cases
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, "othersource", true); err == nil || (err.Error() != "Inconsistent keypace specified in migration: othersource != source for type MASTER" && err.Error() != "Inconsistent keypace specified in migration: othersource != source for type RDONLY") {
		t.Fatalf("different keyspace should fail: %v", err)
	}
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, "source", true); err == nil || err.Error() != "Cannot migrate master into ks until everything else is migrated" {
		t.Fatalf("migrate the master early should have failed: %v", err)
	}

	// now remove rdonly type from served map
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, "source", true); err != nil || !reflect.DeepEqual(ki.ServedFroms, []*topodatapb.Keyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_MASTER,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("remove all cells failed: %v", ki)
	}
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, "source", true); err == nil || err.Error() != "Supplied type cannot be migrated" {
		t.Fatalf("migrate rdonly again should have failed: %v", err)
	}

	// finally migrate the master
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, "source", true); err != nil || ki.ServedFroms != nil {
		t.Fatalf("migrate the master failed: %v", ki)
	}

	// error case again
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, "source", true); err == nil || err.Error() != "Supplied type cannot be migrated" {
		t.Fatalf("migrate the master again should have failed: %v", err)
	}
}

func TestComputeCellServedFrom(t *testing.T) {
	ki := &KeyspaceInfo{
		keyspace: "ks",
		version:  nil,
		Keyspace: &topodatapb.Keyspace{
			ServedFroms: []*topodatapb.Keyspace_ServedFrom{
				{
					TabletType: topodatapb.TabletType_MASTER,
					Keyspace:   "source",
				},
				{
					TabletType: topodatapb.TabletType_REPLICA,
					Keyspace:   "source",
				},
			},
		},
	}

	// Tablets are served from any cell
	m := ki.ComputeCellServedFrom("c3")
	if !reflect.DeepEqual(m, []*topodatapb.SrvKeyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_MASTER,
			Keyspace:   "source",
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("c3 failed: %v", m)
	}

	m = ki.ComputeCellServedFrom("c2")
	if !reflect.DeepEqual(m, []*topodatapb.SrvKeyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_MASTER,
			Keyspace:   "source",
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("c2 failed: %v", m)
	}
}
