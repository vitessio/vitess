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

package topo

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"context"

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

func TestRemoveCellsFromList(t *testing.T) {
	var cells []string
	allCells := []string{"first", "second", "third"}

	// remove from empty list should return allCells - what we remove
	cells = removeCellsFromList([]string{"second"}, allCells)
	if !reflect.DeepEqual(cells, []string{"first", "third"}) {
		t.Fatalf("removeCells(full)-second failed: got %v", allCells)
	}

	// removethe next two cells, should return empty list
	cells = removeCellsFromList(cells, []string{"first", "third"})
	if len(cells) != 0 {
		t.Fatalf("removeCells(full)-first-third is not empty: %v", cells)
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

func addToBlacklist(ctx context.Context, si *ShardInfo, tabletType topodatapb.TabletType, cells, tables []string) error {
	if err := si.UpdateSourceBlacklistedTables(ctx, tabletType, cells, false, tables); err != nil {
		return err
	}
	return nil
}

func removeFromBlacklist(ctx context.Context, si *ShardInfo, tabletType topodatapb.TabletType, cells, tables []string) error {
	if err := si.UpdateSourceBlacklistedTables(ctx, tabletType, cells, true, tables); err != nil {
		return err
	}
	return nil
}

func validateBlacklist(t *testing.T, si *ShardInfo, tabletType topodatapb.TabletType, cells, tables []string) {
	tc := si.GetTabletControl(tabletType)
	require.ElementsMatch(t, tc.Cells, cells)
	require.ElementsMatch(t, tc.BlacklistedTables, tables)
}

func TestUpdateSourceMasterBlacklistedTables(t *testing.T) {
	master := topodatapb.TabletType_MASTER
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{}, nil)
	ctx := lockedKeyspaceContext("ks")
	t1, t2, t3, t4 := "t1", "t2", "t3", "t4"
	tables1 := []string{t1, t2}
	tables2 := []string{t3, t4}

	require.NoError(t, addToBlacklist(ctx, si, master, nil, tables1))
	validateBlacklist(t, si, master, nil, tables1)

	require.NoError(t, addToBlacklist(ctx, si, master, nil, tables2))
	validateBlacklist(t, si, master, nil, append(tables1, tables2...))

	require.Error(t, addToBlacklist(ctx, si, master, nil, tables2), blTablesAlreadyPresent)
	require.Error(t, addToBlacklist(ctx, si, master, nil, []string{t1}), blTablesAlreadyPresent)

	require.NoError(t, removeFromBlacklist(ctx, si, master, nil, tables2))
	validateBlacklist(t, si, master, nil, tables1)

	require.Error(t, removeFromBlacklist(ctx, si, master, nil, tables2), blTablesNotPresent)
	require.Error(t, removeFromBlacklist(ctx, si, master, nil, []string{t3}), blTablesNotPresent)
	validateBlacklist(t, si, master, nil, tables1)

	require.NoError(t, removeFromBlacklist(ctx, si, master, nil, []string{t1}))
	require.NoError(t, removeFromBlacklist(ctx, si, master, nil, []string{t2}))
	require.Nil(t, si.GetTabletControl(master))

	require.Error(t, addToBlacklist(ctx, si, master, []string{"cell"}, tables1), blNoCellsForMaster)
}

func TestUpdateSourceBlacklistedTables(t *testing.T) {
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{}, nil)

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
	if err := si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_RDONLY, []string{"first", "second", "third"}, false, []string{"t1", "t2"}); err != nil || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			Cells:             []string{"first", "second", "third"},
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
