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
					Cells:      nil,
					Keyspace:   "source",
				},
				{
					TabletType: topodatapb.TabletType_MASTER,
					Cells:      nil,
					Keyspace:   "source",
				},
			},
		},
	}
	allCells := []string{"first", "second", "third"}

	// migrate one cell
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, []string{"first"}, "source", true, allCells); err != nil || !reflect.DeepEqual(ki.ServedFroms, []*topodatapb.Keyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      []string{"second", "third"},
			Keyspace:   "source",
		},
		{
			TabletType: topodatapb.TabletType_MASTER,
			Cells:      nil,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("one cell add failed: %v", ki)
	}

	// re-add that cell, going back
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, []string{"first"}, "source", false, nil); err != nil || !reflect.DeepEqual(ki.ServedFroms, []*topodatapb.Keyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      []string{"second", "third", "first"},
			Keyspace:   "source",
		},
		{
			TabletType: topodatapb.TabletType_MASTER,
			Cells:      nil,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("going back should have remove the record: %#v", ki.Keyspace.ServedFroms)
	}

	// now remove the cell again
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, []string{"first"}, "source", true, allCells); err != nil || !reflect.DeepEqual(ki.ServedFroms, []*topodatapb.Keyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      []string{"second", "third"},
			Keyspace:   "source",
		},
		{
			TabletType: topodatapb.TabletType_MASTER,
			Cells:      nil,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("one cell add failed: %v", ki)
	}

	// couple error cases
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, []string{"second"}, "othersource", true, allCells); err == nil || (err.Error() != "inconsistent keypace specified in migration: othersource != source for type MASTER" && err.Error() != "inconsistent keypace specified in migration: othersource != source for type RDONLY") {
		t.Fatalf("different keyspace should fail: %v", err)
	}
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, nil, "source", true, allCells); err == nil || err.Error() != "cannot migrate master into ks until everything else is migrated" {
		t.Fatalf("migrate the master early should have failed: %v", err)
	}

	// now remove all cells
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, []string{"second", "third"}, "source", true, allCells); err != nil || !reflect.DeepEqual(ki.ServedFroms, []*topodatapb.Keyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_MASTER,
			Cells:      nil,
			Keyspace:   "source",
		},
	}) {
		t.Fatalf("remove all cells failed: %v", ki)
	}
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_RDONLY, nil, "source", true, allCells); err == nil || err.Error() != "supplied type cannot be migrated" {
		t.Fatalf("migrate rdonly again should have failed: %v", err)
	}

	// finally migrate the master
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, []string{"second"}, "source", true, allCells); err == nil || err.Error() != "cannot migrate only some cells for master removal in keyspace ks" {
		t.Fatalf("migrate master with cells should have failed: %v", err)
	}
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, nil, "source", true, allCells); err != nil || ki.ServedFroms != nil {
		t.Fatalf("migrate the master failed: %v", ki)
	}

	// error case again
	if err := ki.UpdateServedFromMap(topodatapb.TabletType_MASTER, nil, "source", true, allCells); err == nil || err.Error() != "supplied type cannot be migrated" {
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
					Cells:      nil,
					Keyspace:   "source",
				},
				{
					TabletType: topodatapb.TabletType_REPLICA,
					Cells:      []string{"c1", "c2"},
					Keyspace:   "source",
				},
			},
		},
	}

	m := ki.ComputeCellServedFrom("c3")
	if !reflect.DeepEqual(m, []*topodatapb.SrvKeyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_MASTER,
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
