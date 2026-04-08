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
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

// testWritesetHash mirrors production hash logic for test assertions.
func testWritesetHash(tableName string, vals ...sqltypes.Value) uint64 {
	var d xxhash.Digest
	writesetDigestInit(&d, tableName)
	for i, v := range vals {
		if i > 0 {
			d.Write([]byte{','})
		}
		writesetDigestAddValue(&d, v)
	}
	return d.Sum64()
}

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

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	expected := testWritesetHash("t1", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")))
	require.Equal(t, []uint64{expected}, keys)
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

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	require.Len(t, keys, 2)
	h1 := testWritesetHash("t1", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")))
	h2 := testWritesetHash("t1", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("2")))
	assert.ElementsMatch(t, []uint64{h1, h2}, keys)
}

func TestBuildTxnWritesetRejectsPartialRowImageWithoutFKRefs(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "a", Type: querypb.Type_INT64},
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "b", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{false, true, false},
	}
	change := &binlogdatapb.RowChange{
		After: &querypb.Row{Values: []byte("23"), Lengths: []int64{1, 1}},
		DataColumns: &binlogdatapb.RowChange_Bitmap{
			Count: 3,
			Cols:  []byte{0x06},
		},
	}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.Error(t, err)
	require.Contains(t, err.Error(), "partial row image")
	require.Nil(t, keys)
	assert.NotEqual(t, []uint64{testWritesetHash("t1", sqltypes.NewInt64(3))}, keys)
}

func TestBuildTxnWritesetAllowsBeforeImageWithNullValue(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "nullable_col", Type: querypb.Type_VARCHAR},
		},
		PKIndices: []bool{true, false},
	}
	change := &binlogdatapb.RowChange{
		Before: &querypb.Row{Values: []byte("1"), Lengths: []int64{1, -1}},
	}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	expected := testWritesetHash("t1", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")))
	require.Equal(t, []uint64{expected}, keys)
}

func TestBuildTxnWritesetRejectsSparseBeforeImageOnRelevantFKColumn(t *testing.T) {
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_id", Type: querypb.Type_INT64},
			{Name: "val", Type: querypb.Type_VARCHAR},
		},
		PKIndices: []bool{true, false, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {{ParentTable: "parent", ChildColumnNames: []string{"parent_id"}, ReferencedColumnNames: []string{"id"}}},
	}
	change := &binlogdatapb.RowChange{
		Before: &querypb.Row{
			Lengths: []int64{1, -1, 3},
			Values:  []byte("5aaa"),
		},
	}
	rowEvent := &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(
		map[string]*TablePlan{"child": childPlan},
		fkRefs,
		buildParentFKRefs(fkRefs),
		[]*binlogdatapb.VEvent{vevent},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "partial row image")
	require.Nil(t, keys)
}

func TestBuildTxnWritesetAllowsFullRowImageWithNullValue(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "nullable_col", Type: querypb.Type_VARCHAR},
		},
		PKIndices: []bool{true, false},
	}
	change := &binlogdatapb.RowChange{
		After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1, -1}},
	}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	expected := testWritesetHash("t1", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")))
	require.Equal(t, []uint64{expected}, keys)
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

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	require.Nil(t, keys)
}

func TestBuildTxnWritesetFailsClosedWithoutUsableIdentity(t *testing.T) {
	plan := &TablePlan{
		TargetName:      "t1",
		Fields:          []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		IdentityColumns: []string{"id"},
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	change := &binlogdatapb.RowChange{After: row}
	rowEvent := &binlogdatapb.RowEvent{TableName: "t1", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{"t1": plan}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no usable writeset identity")
	require.Nil(t, keys)
}

func TestWritesetKeysForChangeMissingPlan(t *testing.T) {
	keySet := map[uint64]struct{}{}
	err := writesetKeysForChange(nil, "t1", nil, nil, keySet)
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
	keySet := map[uint64]struct{}{}
	err := writesetKeysForChange(plan, "t1", nil, afterVals, keySet)
	require.NoError(t, err)
	require.Len(t, keySet, 1)
	expected := testWritesetHash("t1",
		sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("foo")),
	)
	_, ok := keySet[expected]
	require.True(t, ok)
}

func TestWritesetKeysForChangeUsesMakeRowTrusted(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	afterVals := sqltypes.MakeRowTrusted(plan.Fields, row)
	keySet := map[uint64]struct{}{}
	err := writesetKeysForChange(plan, "t1", nil, afterVals, keySet)
	require.NoError(t, err)
	require.Len(t, keySet, 1)
	expected := testWritesetHash("t1", sqltypes.MakeRowTrusted(plan.Fields, row)[0])
	_, ok := keySet[expected]
	require.True(t, ok)
}

type stubDBClient struct {
	result *sqltypes.Result
	err    error
}

func (s *stubDBClient) DBName() string  { return "db" }
func (s *stubDBClient) Connect() error  { return nil }
func (s *stubDBClient) Begin() error    { return nil }
func (s *stubDBClient) Commit() error   { return nil }
func (s *stubDBClient) Rollback() error { return nil }
func (s *stubDBClient) Close()          {}
func (s *stubDBClient) IsClosed() bool  { return false }
func (s *stubDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

func (s *stubDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	if s.err != nil {
		return nil, s.err
	}
	return []*sqltypes.Result{s.result}, nil
}

func (s *stubDBClient) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return false, nil
}

func TestWritesetKeysForChangePKOutOfRange(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "other", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true, true},
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	afterVals := sqltypes.MakeRowTrusted(plan.Fields[:1], row)
	keySet := map[uint64]struct{}{}
	err := writesetKeysForChange(plan, "t1", nil, afterVals, keySet)
	require.Error(t, err)
}

func TestQueryFKRefs(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	qr := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"TABLE_NAME|CONSTRAINT_NAME|COLUMN_NAME|REFERENCED_TABLE_NAME|REFERENCED_COLUMN_NAME",
			"varchar|varchar|varchar|varchar|varchar",
		),
		"child|fk_child_parent|parent_id|parent|id",
		"child|fk_child_parent|parent_id2|parent|id2",
		"other|fk_other_parent|parent_id|parent|id",
	)
	client := newVDBClient(&stubDBClient{result: qr}, stats, 100)
	refs, err := queryFKRefs(client, "db")
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Len(t, refs["child"], 1)
	require.Equal(t, "parent", refs["child"][0].ParentTable)
	require.Equal(t, []string{"parent_id", "parent_id2"}, refs["child"][0].ChildColumnNames)
	require.Equal(t, []string{"id", "id2"}, refs["child"][0].ReferencedColumnNames)
}

func TestQueryFKRefsError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	client := newVDBClient(&stubDBClient{err: assert.AnError}, stats, 100)
	refs, err := queryFKRefs(client, "db")
	require.Error(t, err)
	require.Nil(t, refs)
}

func TestBuildTxnWritesetMissingTablePlan(t *testing.T) {
	rowEvent := &binlogdatapb.RowEvent{
		TableName: "missing",
		RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}},
		}},
	}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(map[string]*TablePlan{}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.Error(t, err)
	require.Nil(t, keys)
}

func TestBuildTxnWritesetNoRows(t *testing.T) {
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_BEGIN}
	keys, err := buildTxnWriteset(map[string]*TablePlan{}, nil, nil, []*binlogdatapb.VEvent{vevent})
	require.NoError(t, err)
	require.Nil(t, keys)
}

func TestWritesetKeysForFKRefMissingColumn(t *testing.T) {
	ref := &fkConstraintRef{ParentTable: "parent", ChildColumnNames: []string{"missing"}, ReferencedColumnNames: []string{"id"}}
	fieldIdx := map[string]int{"id": 0}
	vals := []sqltypes.Value{sqltypes.NewInt64(1)}
	keySet := map[uint64]struct{}{}
	// When an FK column is missing from the streamed fields, the function
	// should return an error (fail closed) instead of silently dropping the edge.
	err := writesetKeysForFKRef(ref, fieldIdx, nil, vals, keySet)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in streamed fields")
	require.Empty(t, keySet)
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
	keySet := map[uint64]struct{}{}
	writesetKeysForFKRef(ref, fieldIdx, nil, afterVals, keySet)
	require.Len(t, keySet, 1)
	expected := testWritesetHash("parent", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("42")))
	_, ok := keySet[expected]
	require.True(t, ok)
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
			{ParentTable: "parent", ChildColumnNames: []string{"parent_id"}, ReferencedColumnNames: []string{"id"}},
		},
	}
	parentRefs := buildParentFKRefs(fkRefs)
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

	// Build writeset for parent txn
	parentKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{parentEvent})
	require.NoError(t, err)
	parentHash := testWritesetHash("parent", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("42")))
	require.Equal(t, []uint64{parentHash}, parentKeys)

	// Build writeset for child txn — should have both child PK hash and parent FK ref hash
	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)
	require.Len(t, childKeys, 2)
	childPKHash := testWritesetHash("child", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("5")))
	assert.ElementsMatch(t, []uint64{childPKHash, parentHash}, childKeys)

	// The parent hash appears in both writesets — this creates a conflict
	// that forces serialization, preventing FK constraint violations.
	parentKeySet := map[uint64]struct{}{}
	for _, k := range parentKeys {
		parentKeySet[k] = struct{}{}
	}
	conflict := false
	for _, k := range childKeys {
		if _, ok := parentKeySet[k]; ok {
			conflict = true
			break
		}
	}
	require.True(t, conflict, "parent and child writesets should conflict on parent hash")
}

func TestBuildTxnWritesetWithRenamedTableFKRefsUsesTargetTableNames(t *testing.T) {
	parentPlan := &TablePlan{
		TargetName: "parent",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
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
			{ParentTable: "parent", ChildColumnNames: []string{"parent_id"}, ReferencedColumnNames: []string{"id"}},
		},
	}
	parentRefs := buildParentFKRefs(fkRefs)
	tablePlans := map[string]*TablePlan{
		"parent_src": parentPlan,
		"child_src":  childPlan,
	}

	parentRow := &querypb.Row{Values: []byte("42"), Lengths: []int64{2}}
	parentEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "parent_src",
			RowChanges: []*binlogdatapb.RowChange{{After: parentRow}},
		},
	}
	childRow := &querypb.Row{Values: []byte("542"), Lengths: []int64{1, 2}}
	childEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "child_src",
			RowChanges: []*binlogdatapb.RowChange{{After: childRow}},
		},
	}

	parentKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{parentEvent})
	require.NoError(t, err)
	parentHash := testWritesetHash("parent", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("42")))
	require.Equal(t, []uint64{parentHash}, parentKeys)

	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)
	require.Len(t, childKeys, 2)
	childPKHash := testWritesetHash("child", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("5")))
	assert.ElementsMatch(t, []uint64{childPKHash, parentHash}, childKeys)

	parentKeySet := map[uint64]struct{}{}
	for _, k := range parentKeys {
		parentKeySet[k] = struct{}{}
	}
	conflict := false
	for _, k := range childKeys {
		if _, ok := parentKeySet[k]; ok {
			conflict = true
			break
		}
	}
	require.True(t, conflict, "renamed parent and child writesets should still conflict on target parent hash")
}

func TestBuildTxnWritesetExpressionPlanIsMarkedUnsupported(t *testing.T) {
	vttablet.InitVReplicationConfigDefaults()
	vr := &vreplicator{workflowConfig: vttablet.DefaultVReplicationConfig}
	plan, err := vr.buildReplicatorPlan(
		getSource(&binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select a + b as c1, c as c2 from t1",
		}}}),
		map[string][]*ColumnInfo{"t1": {{Name: "c1", IsPK: true}, {Name: "c2"}}},
		nil,
		binlogplayer.NewStats(),
		collations.MySQL8(),
		sqlparser.NewTestParser(),
	)
	require.NoError(t, err)

	tplan, err := plan.buildExecutionPlan(&binlogdatapb.FieldEvent{
		TableName: "t1",
		Fields: []*querypb.Field{
			{Name: "a", Type: querypb.Type_INT64},
			{Name: "b", Type: querypb.Type_INT64},
			{Name: "c", Type: querypb.Type_INT64},
		},
	})
	require.NoError(t, err)
	assert.True(t, tplan.HasUnsupportedWritesetMapping)
}

func TestBuildTxnWritesetAliasedFKColumnPlanIsMarkedUnsupported(t *testing.T) {
	vttablet.InitVReplicationConfigDefaults()
	vr := &vreplicator{workflowConfig: vttablet.DefaultVReplicationConfig}
	plan, err := vr.buildReplicatorPlan(
		getSource(&binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
			Match:  "child",
			Filter: "select id, parent_id as pid from child",
		}}}),
		map[string][]*ColumnInfo{"child": {{Name: "id", IsPK: true}, {Name: "pid"}}},
		nil,
		binlogplayer.NewStats(),
		collations.MySQL8(),
		sqlparser.NewTestParser(),
	)
	require.NoError(t, err)

	tplan, err := plan.buildExecutionPlan(&binlogdatapb.FieldEvent{
		TableName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_id", Type: querypb.Type_INT64},
		},
	})
	require.NoError(t, err)
	assert.True(t, tplan.HasUnsupportedWritesetMapping)
}

func TestBuildTxnWritesetMatchingAliasExpressionPlanIsMarkedUnsupported(t *testing.T) {
	vttablet.InitVReplicationConfigDefaults()
	vr := &vreplicator{workflowConfig: vttablet.DefaultVReplicationConfig}
	plan, err := vr.buildReplicatorPlan(
		getSource(&binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select lower(email) as email from t1",
		}}}),
		map[string][]*ColumnInfo{"t1": {{Name: "email", IsPK: true}}},
		nil,
		binlogplayer.NewStats(),
		collations.MySQL8(),
		sqlparser.NewTestParser(),
	)
	require.NoError(t, err)

	tplan, err := plan.buildExecutionPlan(&binlogdatapb.FieldEvent{
		TableName: "t1",
		Fields: []*querypb.Field{
			{Name: "email", Type: querypb.Type_VARCHAR},
		},
	})
	require.NoError(t, err)
	assert.True(t, tplan.HasUnsupportedWritesetMapping)
}
