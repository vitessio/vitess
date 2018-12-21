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

	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

func lockedKeyspaceContext(keyspace string) context.Context {
	ctx := context.Background()
	return context.WithValue(ctx, locksKey, &locksInfo{
		info: map[string]*lockInfo{
			// An empty entry is good enough for this.
			keyspace: {},
		},
	})
}

func TestUpdateSourceBlacklistedTables(t *testing.T) {
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{
		Cells: []string{"first", "second", "third"},
	}, nil)

	// check we enforce the keyspace lock
	ctx := context.Background()
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, nil, false, nil); err == nil || err.Error() != "keyspace ks is not locked (no locksInfo)" {
		t.Fatalf("unlocked keyspace produced wrong error: %v", err)
	}
	ctx = lockedKeyspaceContext("ks")

	// add one cell
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			Cells:             []string{"first"},
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("one cell add failed: %v", si)
	}

	// remove that cell, going back
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, true, nil); err != nil || len(si.TabletControls) != 0 {
		t.Fatalf("going back should have remove the record: %v", si)
	}

	// re-add a cell, then another with different table list to
	// make sure it fails
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, false, []string{"t1", "t2"}); err != nil {
		t.Fatalf("one cell add failed: %v", si)
	}
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"second"}, false, []string{"t2", "t3"}); err == nil || err.Error() != "trying to use two different sets of blacklisted tables for shard ks/sh: [t1 t2] and [t2 t3]" {
		t.Fatalf("different table list should fail: %v", err)
	}
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, true); err == nil || err.Error() != "cannot safely alter DisableQueryService as BlacklistedTables is set" {
		t.Fatalf("UpdateDisableQueryService should fail: %v", err)
	}

	// add another cell, see the list grow
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"second"}, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			Cells:             []string{"first", "second"},
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("second cell add failed: %v", si)
	}

	// add all cells, see the list grow to all
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, nil, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			Cells:             nil,
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("all cells add failed: %v", si)
	}

	// remove one cell from the full list
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"second"}, true, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			Cells:             []string{"first", "third"},
			BlacklistedTables: []string{"t1", "t2"},
		},
	}) {
		t.Fatalf("one cell removal from all failed: %v", si)
	}
}

func TestUpdateDisableQueryService(t *testing.T) {
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{
		Cells: []string{"first", "second", "third"},
	}, nil)

	// check we enforce the keyspace lock
	ctx := context.Background()
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, nil, true); err == nil || err.Error() != "keyspace ks is not locked (no locksInfo)" {
		t.Fatalf("unlocked keyspace produced wrong error: %v", err)
	}
	ctx = lockedKeyspaceContext("ks")

	// add one cell
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, true); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:          topodatapb.TabletType_RDONLY,
			Cells:               []string{"first"},
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("one cell add failed: %v", si)
	}

	// remove that cell, going back
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, false); err != nil || len(si.TabletControls) != 0 {
		t.Fatalf("going back should have remove the record: %v %v", err, si)
	}

	// re-add a cell, then another with a table list to
	// make sure it fails
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, []string{"first"}, true); err != nil {
		t.Fatalf("one cell add failed: %v", si)
	}
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"second"}, false, []string{"t1", "t1"}); err == nil || err.Error() != "cannot safely alter BlacklistedTables as DisableQueryService is set for shard ks/sh" {
		t.Fatalf("UpdateSourceBlacklistedTables should fail: %v", err)
	}

	// add another cell, see the list grow
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, []string{"second"}, true); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:          topodatapb.TabletType_RDONLY,
			Cells:               []string{"first", "second"},
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("second cell add failed: %v", si)
	}

	// add all cells, see the list grow to all
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, nil, true); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:          topodatapb.TabletType_RDONLY,
			Cells:               nil,
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("all cells add failed: %v", si)
	}

	// remove one cell from the full list
	if err := si.UpdateDisableQueryService(ctx, topodatapb.TabletType_RDONLY, []string{"second"}, false); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:          topodatapb.TabletType_RDONLY,
			Cells:               []string{"first", "third"},
			DisableQueryService: true,
		},
	}) {
		t.Fatalf("one cell removal from all failed: %v", si)
	}
}

func TestUpdateServedTypesMap(t *testing.T) {
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{
		Cells: []string{"first", "second", "third"},
	}, nil)

	// add all cells for rdonly
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_RDONLY, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      nil,
		},
	}) {
		t.Fatalf("rdonly all cells add failed: %v", err)
	}

	// add some cells for replica
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_REPLICA, []string{"second"}, false); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      nil,
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Cells:      []string{"second"},
		},
	}) {
		t.Fatalf("replica some cells add failed: %v", err)
	}

	// remove some cells for rdonly
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_RDONLY, []string{"second"}, true); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      []string{"first", "third"},
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Cells:      []string{"second"},
		},
	}) {
		t.Fatalf("remove some cells for rdonly failed: %v", err)
	}

	// remove last cell for replica
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_REPLICA, []string{"second"}, true); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      []string{"first", "third"},
		},
	}) {
		t.Fatalf("remove last cell for replica failed: %v", err)
	}

	// Migrate each serving type (add it to this shard).
	// REPLICA
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_REPLICA, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      []string{"first", "third"},
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Cells:      nil,
		},
	}) {
		t.Fatalf("migrate replica failed: %v", err)
	}
	// RDONLY
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_RDONLY, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      nil,
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Cells:      nil,
		},
	}) {
		t.Fatalf("migrate rdonly failed: %v", err)
	}
	// MASTER
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_MASTER, nil, false); err != nil || !reflect.DeepEqual(si.ServedTypes, []*topodatapb.Shard_ServedType{
		{
			TabletType: topodatapb.TabletType_RDONLY,
			Cells:      nil,
		},
		{
			TabletType: topodatapb.TabletType_REPLICA,
			Cells:      nil,
		},
		{
			TabletType: topodatapb.TabletType_MASTER,
			Cells:      nil,
		},
	}) {
		t.Fatalf("migrate master failed: %v", err)
	}

	// Migrate each serving type away from this shard.
	// RDONLY
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_RDONLY, nil, true); err != nil {
		t.Fatalf("remove master failed: %v", err)
	}
	// REPLICA
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_REPLICA, nil, true); err != nil {
		t.Fatalf("remove master failed: %v", err)
	}
	// MASTER
	if err := si.UpdateServedTypesMap(topodatapb.TabletType_MASTER, nil, true); err != nil {
		t.Fatalf("remove master failed: %v", err)
	}
	if len(si.ServedTypes) != 0 {
		t.Fatalf("expected empty map after removing all")
	}
}

func TestGetServedType(t *testing.T) {
	si := NewShardInfo(
		"ks",
		"sh",
		&topodatapb.Shard{
			ServedTypes: []*topodatapb.Shard_ServedType{
				&topodatapb.Shard_ServedType{
					TabletType: topodatapb.TabletType_RDONLY,
				},
			},
		},
		nil)

	servedType := si.GetServedType(topodatapb.TabletType_BACKUP)
	if servedType != nil {
		t.Fatalf("expected servedType to be nil, got: %v", servedType)
	}

	servedType = si.GetServedType(topodatapb.TabletType_REPLICA)
	if servedType == nil {
		t.Fatalf("expected servedType to be TabletType_REPLICA, got: nil")
	}
}
