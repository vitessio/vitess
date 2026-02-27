/*
Copyright 2026 The Vitess Authors.

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

package vreplication

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestBuildTxnWritesetSinglePK(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	change := &binlogdatapb.RowChange{After: row}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	require.Equal(t, []string{"t1:INT64(1)"}, keys)
}

func TestBuildTxnWritesetUsesBeforeAndAfter(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	beforeRow := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	afterRow := &querypb.Row{Values: []byte("2"), Lengths: []int64{1}}
	change := &binlogdatapb.RowChange{Before: beforeRow, After: afterRow}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	sort.Strings(keys) // buildTxnWriteset no longer sorts; sort for deterministic assertion
	require.Equal(t, []string{"t1:INT64(1)", "t1:INT64(2)"}, keys)
}

func TestBuildTxnWritesetNoPK(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{false},
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	change := &binlogdatapb.RowChange{After: row}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	require.Nil(t, keys)
}

func TestWritesetKeysForChangeMissingPlan(t *testing.T) {
	keySet := map[string]struct{}{}
	var buf strings.Builder
	err := writesetKeysForChange(nil, "t1", nil, nil, keySet, &buf)
	require.NoError(t, err)
	require.Empty(t, keySet)
}

func TestWritesetKeysForChangeMultiplePK(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "name", Type: querypb.Type_VARCHAR},
		},
		PKIndices: []bool{true, true},
	}
	row := &querypb.Row{Values: []byte("1foo"), Lengths: []int64{1, 3}}
	afterVals := sqltypes.MakeRowTrusted(plan.Fields, row)
	keySet := map[string]struct{}{}
	var buf strings.Builder
	err := writesetKeysForChange(plan, "t1", nil, afterVals, keySet, &buf)
	require.NoError(t, err)
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	require.Equal(t, []string{"t1:INT64(1),VARCHAR(\"foo\")"}, keys)
}

func TestWritesetKeysForChangeUsesMakeRowTrusted(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	afterVals := sqltypes.MakeRowTrusted(plan.Fields, row)
	keySet := map[string]struct{}{}
	var buf strings.Builder
	err := writesetKeysForChange(plan, "t1", nil, afterVals, keySet, &buf)
	require.NoError(t, err)
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	require.Equal(t, []string{"t1:" + sqltypes.MakeRowTrusted(plan.Fields, row)[0].String()}, keys)
}

func TestWritesetKeysForFKRef(t *testing.T) {
	// Child table has columns: id (PK), parent_id (FK -> parent.id)
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_id", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{true, false},
	}
	ref := &fkConstraintRef{
		ParentTable:      "parent",
		ChildColumnNames: []string{"parent_id"},
	}
	// child row: id=5, parent_id=42
	row := &querypb.Row{Values: []byte("542"), Lengths: []int64{1, 2}}
	afterVals := sqltypes.MakeRowTrusted(childPlan.Fields, row)
	// Build fieldIdx once per table, as buildTxnWriteset now does.
	fieldIdx := make(map[string]int, len(childPlan.Fields))
	for i, f := range childPlan.Fields {
		fieldIdx[f.Name] = i
	}
	keySet := map[string]struct{}{}
	var buf strings.Builder
	writesetKeysForFKRef(ref, fieldIdx, nil, afterVals, keySet, &buf)
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	require.Equal(t, []string{"parent:INT64(42)"}, keys)
}

func TestBuildTxnWritesetWithFKRefs(t *testing.T) {
	// Parent table: parent(id PK)
	parentPlan := &TablePlan{
		TargetName: "parent",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	// Child table: child(id PK, parent_id FK -> parent.id)
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_id", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{true, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {
			{ParentTable: "parent", ChildColumnNames: []string{"parent_id"}},
		},
	}
	tablePlans := map[string]*TablePlan{
		"parent": parentPlan,
		"child":  childPlan,
	}

	// Parent insert: id=42
	parentRow := &querypb.Row{Values: []byte("42"), Lengths: []int64{2}}
	parentChange := &binlogdatapb.RowChange{After: parentRow}
	parentEvent := &binlogdatapb.VEvent{
		Type:     binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "parent", RowChanges: []*binlogdatapb.RowChange{parentChange}},
	}

	// Child insert: id=5, parent_id=42
	childRow := &querypb.Row{Values: []byte("542"), Lengths: []int64{1, 2}}
	childChange := &binlogdatapb.RowChange{After: childRow}
	childEvent := &binlogdatapb.VEvent{
		Type:     binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{childChange}},
	}

	// Build writeset for parent txn — should have "parent:INT64(42)"
	parentKeys, err := buildTxnWriteset(tablePlans, fkRefs, []*binlogdatapb.VEvent{parentEvent})
	require.NoError(t, err)
	require.Equal(t, []string{"parent:INT64(42)"}, parentKeys)

	// Build writeset for child txn — should have both "child:INT64(5)" (PK) and "parent:INT64(42)" (FK ref)
	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)
	sort.Strings(childKeys) // buildTxnWriteset no longer sorts; sort for deterministic assertion
	require.Equal(t, []string{"child:INT64(5)", "parent:INT64(42)"}, childKeys)

	// The key "parent:INT64(42)" appears in both writesets — this creates a conflict
	// that forces serialization, preventing FK constraint violations.
	conflict := false
	parentKeySet := map[string]struct{}{}
	for _, k := range parentKeys {
		parentKeySet[k] = struct{}{}
	}
	for _, k := range childKeys {
		if _, ok := parentKeySet[k]; ok {
			conflict = true
			break
		}
	}
	require.True(t, conflict, "parent and child writesets should conflict on parent:INT64(42)")
}
