// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"reflect"
	"testing"
)

// This file tests the shard related object functionnalities.

func TestAddCells(t *testing.T) {
	var cells []string

	// no restriction + no restriction -> no restrictions
	cells = addCells(cells, nil)
	if cells != nil {
		t.Fatalf("addCells(no restriction)+no restriction should be no restriction")
	}

	// no restriction + cells -> no restrictions
	cells = addCells(cells, []string{"c1", "c2"})
	if cells != nil {
		t.Fatalf("addCells(no restriction)+restriction should be no restriction")
	}

	// cells + no restriction -> no restrictions
	cells = []string{"c1", "c2"}
	cells = addCells(cells, nil)
	if cells != nil {
		t.Fatalf("addCells(restriction)+no restriction should be no restriction")
	}

	// cells + cells -> union
	cells = []string{"c1", "c2"}
	cells = addCells(cells, []string{"c2", "c3"})
	if !reflect.DeepEqual(cells, []string{"c1", "c2", "c3"}) {
		t.Fatalf("addCells(restriction)+restriction failed: got %v", cells)
	}
}

func TestRemoveCells(t *testing.T) {
	var cells []string
	allCells := []string{"first", "second", "third"}

	// remove from empty list should return allCells - what we remove
	var emptyResult bool
	cells, emptyResult = removeCells(cells, []string{"second"}, allCells)
	if emptyResult || !reflect.DeepEqual(cells, []string{"first", "third"}) {
		t.Fatalf("removeCells(full)-second failed: got %v", cells)
	}

	// removethe next two cells, should return empty list
	cells, emptyResult = removeCells(cells, []string{"first", "third"}, allCells)
	if !emptyResult {
		t.Fatalf("removeCells(full)-first-third is not empty: %v", cells)
	}
}

func TestParseKeyspaceShardString(t *testing.T) {
	zkPath := "/zk/tablet"
	keyspace := "key01"
	shard := "shard0"
	tabletAlias := keyspace + "/" + shard

	if _, _, err := ParseKeyspaceShardString(zkPath); err == nil {
		t.Fatalf("zk path: %s should cause error.", zkPath)
	}
	k, s, err := ParseKeyspaceShardString(tabletAlias)
	if err != nil {
		t.Fatalf("Failed to parse valid tablet alias: %s", tabletAlias)
	}
	if keyspace != k {
		t.Fatalf("keyspace parsed from tablet alias %s is %s, but expect %s",
			tabletAlias, k, keyspace)
	}
	if shard != s {
		t.Fatalf("shard parsed from tablet alias %s is %s, but expect %s",
			tabletAlias, s, shard)
	}
}

func TestUpdateSourceBlacklistedTables(t *testing.T) {
	si := NewShardInfo("ks", "sh", &Shard{
		Cells: []string{"first", "second", "third"},
	}, 1)

	// add one cell
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"first"}, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:             []string{"first"},
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("one cell add failed: %v", si)
	}

	// remove that cell, going back
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"first"}, true, nil); err != nil || si.TabletControlMap != nil {
		t.Fatalf("going back should have remove the record: %v", si)
	}

	// re-add a cell, then another with different table list to
	// make sure it fails
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"first"}, false, []string{"t1", "t2"}); err != nil {
		t.Fatalf("one cell add failed: %v", si)
	}
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"second"}, false, []string{"t2", "t3"}); err == nil || err.Error() != "trying to use two different sets of blacklisted tables for shard ks/sh: [t1 t2] and [t2 t3]" {
		t.Fatalf("different table list should fail: %v", err)
	}
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, []string{"first"}, true); err == nil || err.Error() != "cannot safely alter DisableQueryService as BlacklistedTables is set" {
		t.Fatalf("UpdateDisableQueryService should fail: %v", err)
	}

	// add another cell, see the list grow
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"second"}, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:             []string{"first", "second"},
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("second cell add failed: %v", si)
	}

	// add all cells, see the list grow to all
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, nil, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:             nil,
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("all cells add failed: %v", si)
	}

	// remove one cell from the full list
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"second"}, true, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:             []string{"first", "third"},
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("one cell removal from all failed: %v", si)
	}
}

func TestUpdateDisableQueryService(t *testing.T) {
	si := NewShardInfo("ks", "sh", &Shard{
		Cells: []string{"first", "second", "third"},
	}, 1)

	// add one cell
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, []string{"first"}, true); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:               []string{"first"},
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("one cell add failed: %v", si)
	}

	// remove that cell, going back
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, []string{"first"}, false); err != nil || si.TabletControlMap != nil {
		t.Fatalf("going back should have remove the record: %v %v", err, si)
	}

	// re-add a cell, then another with a table list to
	// make sure it fails
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, []string{"first"}, true); err != nil {
		t.Fatalf("one cell add failed: %v", si)
	}
	if err := si.UpdateSourceBlacklistedTables(TYPE_RDONLY, []string{"second"}, false, []string{"t1", "t1"}); err == nil || err.Error() != "cannot safely alter BlacklistedTables as DisableQueryService is set for shard ks/sh" {
		t.Fatalf("UpdateSourceBlacklistedTables should fail: %v", err)
	}

	// add another cell, see the list grow
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, []string{"second"}, true); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:               []string{"first", "second"},
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("second cell add failed: %v", si)
	}

	// add all cells, see the list grow to all
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, nil, true); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:               nil,
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("all cells add failed: %v", si)
	}

	// remove one cell from the full list
	if err := si.UpdateDisableQueryService(TYPE_RDONLY, []string{"second"}, false); err != nil || !reflect.DeepEqual(si.TabletControlMap, map[TabletType]*TabletControl{
		TYPE_RDONLY: &TabletControl{
			Cells:               []string{"first", "third"},
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("one cell removal from all failed: %v", si)
	}
}

func TestUpdateServedTypesMap(t *testing.T) {
	si := NewShardInfo("ks", "sh", &Shard{
		Cells: []string{"first", "second", "third"},
	}, 1)

	// add all cells for rdonly
	if err := si.UpdateServedTypesMap(TYPE_RDONLY, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: nil,
		},
	}) {
		t.Fatalf("rdonly all cells add failed: %v", err)
	}

	// add some cells for replica
	if err := si.UpdateServedTypesMap(TYPE_REPLICA, []string{"second"}, false); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: nil,
		},
		TYPE_REPLICA: &ShardServedType{
			Cells: []string{"second"},
		},
	}) {
		t.Fatalf("replica some cells add failed: %v", err)
	}

	// remove some cells for rdonly
	if err := si.UpdateServedTypesMap(TYPE_RDONLY, []string{"second"}, true); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: []string{"first", "third"},
		},
		TYPE_REPLICA: &ShardServedType{
			Cells: []string{"second"},
		},
	}) {
		t.Fatalf("remove some cells for rdonly failed: %v", err)
	}

	// remove last cell for replica
	if err := si.UpdateServedTypesMap(TYPE_REPLICA, []string{"second"}, true); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: []string{"first", "third"},
		},
	}) {
		t.Fatalf("remove last cell for replica failed: %v", err)
	}

	// migrate all
	if err := si.UpdateServedTypesMap(TYPE_REPLICA, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: []string{"first", "third"},
		},
		TYPE_REPLICA: &ShardServedType{
			Cells: nil,
		},
	}) {
		t.Fatalf("migrate replica failed: %v", err)
	}
	if err := si.UpdateServedTypesMap(TYPE_RDONLY, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: nil,
		},
		TYPE_REPLICA: &ShardServedType{
			Cells: nil,
		},
	}) {
		t.Fatalf("migrate rdonly failed: %v", err)
	}
	if err := si.UpdateServedTypesMap(TYPE_MASTER, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypesMap, map[TabletType]*ShardServedType{
		TYPE_RDONLY: &ShardServedType{
			Cells: nil,
		},
		TYPE_REPLICA: &ShardServedType{
			Cells: nil,
		},
		TYPE_MASTER: &ShardServedType{
			Cells: nil,
		},
	}) {
		t.Fatalf("migrate master failed: %v", err)
	}

	// try to migrate master away, see it fail
	if err := si.UpdateServedTypesMap(TYPE_MASTER, nil, true); err == nil || err.Error() != "cannot migrate master away from ks/sh until everything else is migrated" {
		t.Fatalf("migrate master away unexpected error: %v", err)
	}

	// remove all, see the map get emptied
	if err := si.UpdateServedTypesMap(TYPE_RDONLY, nil, true); err != nil {
		t.Fatalf("remove master failed: %v", err)
	}
	if err := si.UpdateServedTypesMap(TYPE_REPLICA, nil, true); err != nil {
		t.Fatalf("remove master failed: %v", err)
	}
	if err := si.UpdateServedTypesMap(TYPE_MASTER, nil, true); err != nil {
		t.Fatalf("remove master failed: %v", err)
	}
	if si.ServedTypesMap != nil {
		t.Fatalf("expected empty map after removing all")
	}
}
