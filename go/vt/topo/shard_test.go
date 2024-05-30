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
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
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

func lockedKeyspaceContext(keyspace string) context.Context {
	ctx := context.Background()
	return context.WithValue(ctx, locksKey, &locksInfo{
		info: map[string]*lockInfo{
			// An empty entry is good enough for this.
			keyspace: {},
		},
	})
}

func addToDenyList(ctx context.Context, si *ShardInfo, tabletType topodatapb.TabletType, cells, tables []string) error {
	if err := si.UpdateDeniedTables(ctx, tabletType, cells, false, tables); err != nil {
		return err
	}
	return nil
}

func removeFromDenyList(ctx context.Context, si *ShardInfo, tabletType topodatapb.TabletType, cells, tables []string) error {
	if err := si.UpdateDeniedTables(ctx, tabletType, cells, true, tables); err != nil {
		return err
	}
	return nil
}

func validateDenyList(t *testing.T, si *ShardInfo, tabletType topodatapb.TabletType, cells, tables []string) {
	tc := si.GetTabletControl(tabletType)
	require.ElementsMatch(t, tc.Cells, cells)
	require.ElementsMatch(t, tc.DeniedTables, tables)
}

func TestUpdateSourcePrimaryDeniedTables(t *testing.T) {
	primary := topodatapb.TabletType_PRIMARY
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{}, nil)
	ctx := lockedKeyspaceContext("ks")
	t1, t2, t3, t4 := "t1", "t2", "t3", "t4"
	tables1 := []string{t1, t2}
	tables2 := []string{t3, t4}

	require.NoError(t, addToDenyList(ctx, si, primary, nil, tables1))
	validateDenyList(t, si, primary, nil, tables1)

	require.NoError(t, addToDenyList(ctx, si, primary, nil, tables2))
	validateDenyList(t, si, primary, nil, append(tables1, tables2...))

	require.Error(t, addToDenyList(ctx, si, primary, nil, tables2), dlTablesAlreadyPresent)
	require.Error(t, addToDenyList(ctx, si, primary, nil, []string{t1}), dlTablesAlreadyPresent)

	require.NoError(t, removeFromDenyList(ctx, si, primary, nil, tables2))
	validateDenyList(t, si, primary, nil, tables1)

	require.Error(t, removeFromDenyList(ctx, si, primary, nil, tables2), dlTablesNotPresent)
	require.Error(t, removeFromDenyList(ctx, si, primary, nil, []string{t3}), dlTablesNotPresent)
	validateDenyList(t, si, primary, nil, tables1)

	require.NoError(t, removeFromDenyList(ctx, si, primary, nil, []string{t1}))
	require.NoError(t, removeFromDenyList(ctx, si, primary, nil, []string{t2}))
	require.Nil(t, si.GetTabletControl(primary))

	require.Error(t, addToDenyList(ctx, si, primary, []string{"cell"}, tables1), dlNoCellsForPrimary)
}

func TestUpdateSourceDeniedTables(t *testing.T) {
	si := NewShardInfo("ks", "sh", &topodatapb.Shard{}, nil)
	ctx := context.Background()
	ctxWithLock := lockedKeyspaceContext("ks")

	type testCase struct {
		name       string
		ctx        context.Context
		tabletType topodatapb.TabletType
		cells      []string
		remove     bool
		tables     []string

		wantError         string
		wantTabletControl *topodatapb.Shard_TabletControl
	}

	// These tests update the state of the shard tablet controls, so subsequent tests
	// depend on the cumulative state from the previous tests.
	testCases := []testCase{
		{
			name:       "enforce keyspace lock",
			ctx:        ctx,
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first"},

			wantError: "keyspace ks is not locked (no locksInfo)",
		},
		{
			name:       "add one cell",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first"},
			tables:     []string{"t1", "t2"},
			wantTabletControl: &topodatapb.Shard_TabletControl{
				TabletType:   topodatapb.TabletType_RDONLY,
				Cells:        []string{"first"},
				DeniedTables: []string{"t1", "t2"},
			},
		},
		{
			name:       "remove the only cell",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first"},
			remove:     true,
		},
		{
			name:       "re-add cell",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first"},
			tables:     []string{"t1", "t2"},
			wantTabletControl: &topodatapb.Shard_TabletControl{
				TabletType:   topodatapb.TabletType_RDONLY,
				Cells:        []string{"first"},
				DeniedTables: []string{"t1", "t2"},
			},
		},
		{
			name:       "re-add existing cell, different tables, should fail",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first"},
			tables:     []string{"t3"},
			wantError:  "trying to use two different sets of denied tables for shard",
		},
		{
			name:       "add all cells, see cell list grow to all",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first", "second", "third"},
			tables:     []string{"t1", "t2"},
			wantTabletControl: &topodatapb.Shard_TabletControl{
				TabletType:   topodatapb.TabletType_RDONLY,
				Cells:        []string{"first", "second", "third"},
				DeniedTables: []string{"t1", "t2"},
			},
		},
		{
			name:       "remove one cell",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"second"},
			remove:     true,
			tables:     []string{"t1", "t2"},
			wantTabletControl: &topodatapb.Shard_TabletControl{
				TabletType:   topodatapb.TabletType_RDONLY,
				Cells:        []string{"first", "third"},
				DeniedTables: []string{"t1", "t2"},
			},
		},
		{
			name:       "add replica tablet type",
			tabletType: topodatapb.TabletType_REPLICA,
			cells:      []string{"first"},
			tables:     []string{"t1", "t2"},
			wantTabletControl: &topodatapb.Shard_TabletControl{
				TabletType:   topodatapb.TabletType_REPLICA,
				Cells:        []string{"first"},
				DeniedTables: []string{"t1", "t2"},
			},
		},
		{
			name:       "confirm rdonly still stays the same, after replica was added",
			tabletType: topodatapb.TabletType_RDONLY,
			wantTabletControl: &topodatapb.Shard_TabletControl{
				TabletType:   topodatapb.TabletType_RDONLY,
				Cells:        []string{"first", "third"},
				DeniedTables: []string{"t1", "t2"},
			},
		},
		{
			name:       "remove rdonly entry",
			tabletType: topodatapb.TabletType_RDONLY,
			cells:      []string{"first", "third"},
			remove:     true,
			tables:     []string{"t1", "t2"},
		},
		{
			name:       "remove replica entry",
			tabletType: topodatapb.TabletType_REPLICA,
			cells:      []string{"first", "third"},
			remove:     true,
			tables:     []string{"t1", "t2"},
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			if tcase.ctx == nil {
				tcase.ctx = ctxWithLock
			}
			var err error
			if tcase.tables != nil || tcase.cells != nil {
				err = si.UpdateDeniedTables(tcase.ctx, tcase.tabletType, tcase.cells, tcase.remove, tcase.tables)
			}
			if tcase.wantError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tcase.wantError)
				return
			}
			require.NoError(t, err)
			if tcase.wantTabletControl == nil {
				require.Nil(t, si.GetTabletControl(tcase.tabletType))
			} else {
				require.EqualValuesf(t, tcase.wantTabletControl, si.GetTabletControl(tcase.tabletType),
					"want: %v, got: %v", tcase.wantTabletControl, si.GetTabletControl(tcase.tabletType))
			}
		})
	}
}

func TestValidateShardName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		expectedRange *topodatapb.KeyRange
		valid         bool
	}{
		{
			name:  "0",
			valid: true,
		},
		{
			name: "-80",
			expectedRange: &topodatapb.KeyRange{
				Start: nil,
				End:   []byte{0x80},
			},
			valid: true,
		},
		{
			name: "40-80",
			expectedRange: &topodatapb.KeyRange{
				Start: []byte{0x40},
				End:   []byte{0x80},
			},
			valid: true,
		},
		{
			name:  "foo-bar",
			valid: false,
		},
		{
			name:  "a/b",
			valid: false,
		},
	}

	for _, tcase := range cases {
		tcase := tcase
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			_, kr, err := ValidateShardName(tcase.name)
			if !tcase.valid {
				assert.Error(t, err, "expected %q to be an invalid shard name", tcase.name)
				return
			}

			require.NoError(t, err, "expected %q to be a valid shard name, got error: %v", tcase.name, err)
			utils.MustMatch(t, tcase.expectedRange, kr)
		})
	}
}
