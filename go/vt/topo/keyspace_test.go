// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"reflect"
	"testing"
)

// This file tests the keyspace related object functionnalities.

func TestUpdateServedFromMap(t *testing.T) {
	ki := NewKeyspaceInfo("ks", &Keyspace{
		ServedFromMap: map[TabletType]*KeyspaceServedFrom{
			TYPE_RDONLY: &KeyspaceServedFrom{
				Cells:    nil,
				Keyspace: "source",
			},
			TYPE_MASTER: &KeyspaceServedFrom{
				Cells:    nil,
				Keyspace: "source",
			},
		},
	}, 1)
	allCells := []string{"first", "second", "third"}

	// migrate one cell
	if err := ki.UpdateServedFromMap(TYPE_RDONLY, []string{"first"}, "source", true, allCells); err != nil || !reflect.DeepEqual(ki.ServedFromMap, map[TabletType]*KeyspaceServedFrom{
		TYPE_RDONLY: &KeyspaceServedFrom{
			Cells:    []string{"second", "third"},
			Keyspace: "source",
		},
		TYPE_MASTER: &KeyspaceServedFrom{
			Cells:    nil,
			Keyspace: "source",
		},
	}) {
		t.Fatalf("one cell add failed: %v", ki)
	}

	// re-add that cell, going back
	if err := ki.UpdateServedFromMap(TYPE_RDONLY, []string{"first"}, "source", false, nil); err != nil || !reflect.DeepEqual(ki.ServedFromMap, map[TabletType]*KeyspaceServedFrom{
		TYPE_RDONLY: &KeyspaceServedFrom{
			Cells:    []string{"second", "third", "first"},
			Keyspace: "source",
		},
		TYPE_MASTER: &KeyspaceServedFrom{
			Cells:    nil,
			Keyspace: "source",
		},
	}) {
		t.Fatalf("going back should have remove the record: %#v", ki.Keyspace.ServedFromMap[TYPE_RDONLY])
	}

	// now remove the cell again
	if err := ki.UpdateServedFromMap(TYPE_RDONLY, []string{"first"}, "source", true, allCells); err != nil || !reflect.DeepEqual(ki.ServedFromMap, map[TabletType]*KeyspaceServedFrom{
		TYPE_RDONLY: &KeyspaceServedFrom{
			Cells:    []string{"second", "third"},
			Keyspace: "source",
		},
		TYPE_MASTER: &KeyspaceServedFrom{
			Cells:    nil,
			Keyspace: "source",
		},
	}) {
		t.Fatalf("one cell add failed: %v", ki)
	}

	// couple error cases
	if err := ki.UpdateServedFromMap(TYPE_RDONLY, []string{"second"}, "othersource", true, allCells); err == nil || (err.Error() != "Inconsistent keypace specified in migration: othersource != source for type master" && err.Error() != "Inconsistent keypace specified in migration: othersource != source for type rdonly") {
		t.Fatalf("different keyspace should fail: %v", err)
	}
	if err := ki.UpdateServedFromMap(TYPE_MASTER, nil, "source", true, allCells); err == nil || err.Error() != "Cannot migrate master into ks until everything else is migrated" {
		t.Fatalf("migrate the master early should have failed: %v", err)
	}

	// now remove all cells
	if err := ki.UpdateServedFromMap(TYPE_RDONLY, []string{"second", "third"}, "source", true, allCells); err != nil || !reflect.DeepEqual(ki.ServedFromMap, map[TabletType]*KeyspaceServedFrom{
		TYPE_MASTER: &KeyspaceServedFrom{
			Cells:    nil,
			Keyspace: "source",
		},
	}) {
		t.Fatalf("remove all cells failed: %v", ki)
	}
	if err := ki.UpdateServedFromMap(TYPE_RDONLY, nil, "source", true, allCells); err == nil || err.Error() != "Supplied type cannot be migrated" {
		t.Fatalf("migrate rdonly again should have failed: %v", err)
	}

	// finally migrate the master
	if err := ki.UpdateServedFromMap(TYPE_MASTER, []string{"second"}, "source", true, allCells); err == nil || err.Error() != "Cannot migrate only some cells for master removal in keyspace ks" {
		t.Fatalf("migrate master with cells should have failed: %v", err)
	}
	if err := ki.UpdateServedFromMap(TYPE_MASTER, nil, "source", true, allCells); err != nil || ki.ServedFromMap != nil {
		t.Fatalf("migrate the master failed: %v", ki)
	}

	// error case again
	if err := ki.UpdateServedFromMap(TYPE_MASTER, nil, "source", true, allCells); err == nil || err.Error() != "Supplied type cannot be migrated" {
		t.Fatalf("migrate the master again should have failed: %v", err)
	}
}

func TestComputeCellServedFrom(t *testing.T) {
	ki := NewKeyspaceInfo("ks", &Keyspace{
		ServedFromMap: map[TabletType]*KeyspaceServedFrom{
			TYPE_MASTER: &KeyspaceServedFrom{
				Cells:    nil,
				Keyspace: "source",
			},
			TYPE_REPLICA: &KeyspaceServedFrom{
				Cells:    []string{"c1", "c2"},
				Keyspace: "source",
			},
		},
	}, 1)

	m := ki.ComputeCellServedFrom("c3")
	if !reflect.DeepEqual(m, map[TabletType]string{
		TYPE_MASTER: "source",
	}) {
		t.Fatalf("c3 failed: %v", m)
	}

	m = ki.ComputeCellServedFrom("c2")
	if !reflect.DeepEqual(m, map[TabletType]string{
		TYPE_MASTER:  "source",
		TYPE_REPLICA: "source",
	}) {
		t.Fatalf("c2 failed: %v", m)
	}
}
