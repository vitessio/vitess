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
	"fmt"
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
	for _, v := range vals {
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

func BenchmarkBuildTxnWriteset_NoFKRefsAvoidsPlanWideCanonicalization(b *testing.B) {
	const tableCount = 256
	tablePlans := make(map[string]*TablePlan, tableCount)
	for i := range tableCount {
		name := fmt.Sprintf("t%d", i)
		tablePlans[name] = &TablePlan{
			TargetName: name,
			Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
			PKIndices:  []bool{true},
		}
	}
	row := &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}
	events := []*binlogdatapb.VEvent{{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t0",
			RowChanges: []*binlogdatapb.RowChange{{After: row}},
		},
	}}

	b.ReportAllocs()
	for b.Loop() {
		keys, err := buildTxnWriteset(tablePlans, nil, nil, events)
		if err != nil {
			b.Fatal(err)
		}
		if len(keys) != 1 {
			b.Fatalf("unexpected key count: %d", len(keys))
		}
	}
}

func BenchmarkWritesetDigestAddFieldValue_TextAllocations(b *testing.B) {
	collationID := uint32(collations.MySQL8().LookupByName("utf8mb4_general_ci"))
	field := &querypb.Field{Name: "email", Type: querypb.Type_VARCHAR, Charset: collationID}
	value := sqltypes.NewVarChar("user@example.com   ")

	b.ReportAllocs()
	for b.Loop() {
		var d xxhash.Digest
		writesetDigestInit(&d, "emails")
		if err := writesetDigestAddFieldValue(&d, field, value); err != nil {
			b.Fatal(err)
		}
		_ = d.Sum64()
	}
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

func TestBuildTxnWritesetAllowsCaseOnlyFKColumnNameMismatch(t *testing.T) {
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "ID", Type: querypb.Type_INT64},
			{Name: "PARENT_ID", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{true, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {{ParentTable: "parent", ChildColumnNames: []string{"parent_id"}, ReferencedColumnNames: []string{"id"}}},
	}
	change := &binlogdatapb.RowChange{
		After: &querypb.Row{Values: []byte("12"), Lengths: []int64{1, 1}},
	}
	rowEvent := &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(
		map[string]*TablePlan{"child": childPlan},
		fkRefs,
		buildParentFKRefs(fkRefs),
		[]*binlogdatapb.VEvent{vevent},
	)
	require.NoError(t, err)
	require.Len(t, keys, 2)
}

func TestBuildTxnWritesetAllowsMixedCaseFKColumnNameMismatch(t *testing.T) {
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "ID", Type: querypb.Type_INT64},
			{Name: "PARENT_ID", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{true, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {{ParentTable: "parent", ChildColumnNames: []string{"Parent_ID"}, ReferencedColumnNames: []string{"ID"}}},
	}
	change := &binlogdatapb.RowChange{
		After: &querypb.Row{Values: []byte("12"), Lengths: []int64{1, 1}},
	}
	rowEvent := &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{change}}
	vevent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: rowEvent}

	keys, err := buildTxnWriteset(
		map[string]*TablePlan{"child": childPlan},
		fkRefs,
		buildParentFKRefs(fkRefs),
		[]*binlogdatapb.VEvent{vevent},
	)
	require.NoError(t, err)
	require.Len(t, keys, 2)
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

func TestWritesetKeysForChangeCompositeBinaryPKValuesDoNotAlias(t *testing.T) {
	plan := &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "id1", Type: querypb.Type_VARBINARY},
			{Name: "id2", Type: querypb.Type_VARBINARY},
		},
		PKIndices: []bool{true, true},
	}
	valueType := querypb.Type_VARBINARY
	typeByte := byte(valueType)
	firstTuple := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte{'a'}),
		sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte{'x', ',', typeByte, 'y'}),
	}
	secondTuple := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte{'a', ',', typeByte, 'x'}),
		sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte{'y'}),
	}
	keySet := map[uint64]struct{}{}

	require.NoError(t, writesetKeysForChange(plan, "t1", nil, firstTuple, keySet))
	require.NoError(t, writesetKeysForChange(plan, "t1", nil, secondTuple, keySet))
	require.Len(t, keySet, 2)
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
			"TABLE_NAME|CONSTRAINT_NAME|COLUMN_NAME|REFERENCED_TABLE_NAME|REFERENCED_COLUMN_NAME|CHILD_DATA_TYPE|CHILD_CHARACTER_SET_NAME|CHILD_COLLATION_NAME|CHILD_COLUMN_TYPE|PARENT_DATA_TYPE|PARENT_CHARACTER_SET_NAME|PARENT_COLLATION_NAME|PARENT_COLUMN_TYPE",
			"varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"child|fk_child_parent|parent_id|parent|id|int|||int|int|||int",
		"child|fk_child_parent|parent_id2|parent|id2|int|||int|int|||int",
		"other|fk_other_parent|parent_id|parent|id|int|||int|int|||int",
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

type maxRowsAssertingDBClient struct {
	result      *sqltypes.Result
	err         error
	assertQuery func(query string)
	assertRows  func(maxrows int) error
}

func (m *maxRowsAssertingDBClient) DBName() string  { return "db" }
func (m *maxRowsAssertingDBClient) Connect() error  { return nil }
func (m *maxRowsAssertingDBClient) Begin() error    { return nil }
func (m *maxRowsAssertingDBClient) Commit() error   { return nil }
func (m *maxRowsAssertingDBClient) Rollback() error { return nil }
func (m *maxRowsAssertingDBClient) Close()          {}
func (m *maxRowsAssertingDBClient) IsClosed() bool  { return false }
func (m *maxRowsAssertingDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	if m.assertQuery != nil {
		m.assertQuery(query)
	}
	if m.assertRows != nil {
		if err := m.assertRows(maxrows); err != nil {
			return nil, err
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.result, nil
}

func (m *maxRowsAssertingDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	qr, err := m.ExecuteFetch(query, maxrows)
	if err != nil {
		return nil, err
	}
	return []*sqltypes.Result{qr}, nil
}

func (m *maxRowsAssertingDBClient) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return false, nil
}

func TestQueryFKRefsFetchesAllRows(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	qr := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"TABLE_NAME|CONSTRAINT_NAME|COLUMN_NAME|REFERENCED_TABLE_NAME|REFERENCED_COLUMN_NAME|CHILD_DATA_TYPE|CHILD_CHARACTER_SET_NAME|CHILD_COLLATION_NAME|CHILD_COLUMN_TYPE|PARENT_DATA_TYPE|PARENT_CHARACTER_SET_NAME|PARENT_COLLATION_NAME|PARENT_COLUMN_TYPE",
			"varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"child|fk_child_parent|parent_id|parent|id|int|||int|int|||int",
	)
	client := newVDBClient(&maxRowsAssertingDBClient{
		result: qr,
		assertQuery: func(query string) {
			require.Contains(t, query, "JOIN information_schema.COLUMNS child_cols")
			require.Contains(t, query, "JOIN information_schema.COLUMNS parent_cols")
			require.NotContains(t, query, "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA")
		},
		assertRows: func(maxrows int) error {
			if maxrows != -1 {
				return fmt.Errorf("expected fetch-all maxrows, got %d", maxrows)
			}
			return nil
		},
	}, stats, 100)

	refs, err := queryFKRefs(client, "db")
	require.NoError(t, err)
	require.Len(t, refs["child"], 1)
	require.Equal(t, "parent", refs["child"][0].ParentTable)
	require.Equal(t, []string{"parent_id"}, refs["child"][0].ChildColumnNames)
	require.Equal(t, []string{"id"}, refs["child"][0].ReferencedColumnNames)
}

func TestQueryFKRefsRejectsHashIncompatibleFKColumnDefinitions(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	qr := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"TABLE_NAME|CONSTRAINT_NAME|COLUMN_NAME|REFERENCED_TABLE_NAME|REFERENCED_COLUMN_NAME|CHILD_DATA_TYPE|CHILD_CHARACTER_SET_NAME|CHILD_COLLATION_NAME|CHILD_COLUMN_TYPE|PARENT_DATA_TYPE|PARENT_CHARACTER_SET_NAME|PARENT_COLLATION_NAME|PARENT_COLUMN_TYPE",
			"varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"child|fk_child_parent|parent_id|parent|id|int|||int|bigint|||bigint",
	)

	client := newVDBClient(&stubDBClient{result: qr}, stats, 100)
	refs, err := queryFKRefs(client, "db")
	require.Error(t, err)
	require.ErrorContains(t, err, "incompatible FK column definitions")
	require.Nil(t, refs)
}

func TestQueryFKRefsAllowsCompatibleCharacterFKColumns(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	qr := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"TABLE_NAME|CONSTRAINT_NAME|COLUMN_NAME|REFERENCED_TABLE_NAME|REFERENCED_COLUMN_NAME|CHILD_DATA_TYPE|CHILD_CHARACTER_SET_NAME|CHILD_COLLATION_NAME|CHILD_COLUMN_TYPE|PARENT_DATA_TYPE|PARENT_CHARACTER_SET_NAME|PARENT_COLLATION_NAME|PARENT_COLUMN_TYPE",
			"varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"child|fk_child_parent|parent_code|parent|code|varchar|utf8mb4|utf8mb4_0900_ai_ci|varchar(64)|char|utf8mb4|utf8mb4_0900_ai_ci|char(32)",
	)

	client := newVDBClient(&stubDBClient{result: qr}, stats, 100)
	refs, err := queryFKRefs(client, "db")
	require.NoError(t, err)
	require.Len(t, refs["child"], 1)
	require.Equal(t, []string{"parent_code"}, refs["child"][0].ChildColumnNames)
	require.Equal(t, []string{"code"}, refs["child"][0].ReferencedColumnNames)
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
	err := writesetKeysForFKRef(ref, []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, fieldIdx, nil, vals, keySet)
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
	writesetKeysForFKRef(ref, childPlan.Fields, fieldIdx, nil, afterVals, keySet)
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

func TestBuildTxnWritesetWithCompositeParentFKRefsUsesIdentityColumnOrder(t *testing.T) {
	parentPlan := &TablePlan{
		TargetName:      "parent",
		Fields:          []*querypb.Field{{Name: "b", Type: querypb.Type_INT64}, {Name: "a", Type: querypb.Type_INT64}},
		IdentityColumns: []string{"a", "b"},
		PKIndices:       []bool{true, true},
	}
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_a", Type: querypb.Type_INT64},
			{Name: "parent_b", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{true, false, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {
			{ParentTable: "parent", ChildColumnNames: []string{"parent_a", "parent_b"}, ReferencedColumnNames: []string{"a", "b"}},
		},
	}
	parentRefs := buildParentFKRefs(fkRefs)
	tablePlans := map[string]*TablePlan{
		"parent": parentPlan,
		"child":  childPlan,
	}

	parentEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "parent", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("12"), Lengths: []int64{1, 1}},
		}}},
	}
	childEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("921"), Lengths: []int64{1, 1, 1}},
		}}},
	}

	parentKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{parentEvent})
	require.NoError(t, err)
	parentHash := testWritesetHash(
		"parent",
		sqltypes.MakeTrusted(querypb.Type_INT64, []byte("2")),
		sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")),
	)
	require.Equal(t, []uint64{parentHash}, parentKeys)

	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)
	require.Len(t, childKeys, 2)
	childPKHash := testWritesetHash("child", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("9")))
	assert.ElementsMatch(t, []uint64{childPKHash, parentHash}, childKeys)

	parentKeySet := map[uint64]struct{}{parentHash: {}}
	conflict := false
	for _, k := range childKeys {
		if _, ok := parentKeySet[k]; ok {
			conflict = true
			break
		}
	}
	require.True(t, conflict, "parent and child writesets should conflict on the parent identity hash")
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

func TestBuildTxnWritesetWithMixedCaseFKRefsUsesTargetTableNames(t *testing.T) {
	parentPlan := &TablePlan{
		TargetName: "Parent",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	childPlan := &TablePlan{
		TargetName: "Child",
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
	parentHash := testWritesetHash("Parent", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("42")))
	require.Equal(t, []uint64{parentHash}, parentKeys)

	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)
	require.Len(t, childKeys, 2)
	childPKHash := testWritesetHash("Child", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("5")))
	assert.ElementsMatch(t, []uint64{childPKHash, parentHash}, childKeys)

	parentKeySet := map[uint64]struct{}{parentHash: {}}
	conflict := false
	for _, k := range childKeys {
		if _, ok := parentKeySet[k]; ok {
			conflict = true
			break
		}
	}
	require.True(t, conflict, "mixed-case FK metadata should still conflict on the target parent hash")
}

func TestBuildTxnWritesetTextPrimaryKeyUsesCollationEquality(t *testing.T) {
	collationID := uint32(collations.MySQL8().LookupByName("utf8mb4_0900_ai_ci"))
	require.NotZero(t, collationID)

	plan := &TablePlan{
		TargetName: "emails",
		Fields: []*querypb.Field{{
			Name:    "email",
			Type:    querypb.Type_VARCHAR,
			Charset: collationID,
		}},
		PKIndices: []bool{true},
	}

	upperEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "emails", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("A"), Lengths: []int64{1}},
		}}},
	}
	lowerEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "emails", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("a"), Lengths: []int64{1}},
		}}},
	}

	upperKeys, err := buildTxnWriteset(map[string]*TablePlan{"emails": plan}, nil, nil, []*binlogdatapb.VEvent{upperEvent})
	require.NoError(t, err)
	lowerKeys, err := buildTxnWriteset(map[string]*TablePlan{"emails": plan}, nil, nil, []*binlogdatapb.VEvent{lowerEvent})
	require.NoError(t, err)
	require.Equal(t, upperKeys, lowerKeys, "text primary keys that compare equal under MySQL collation rules must hash identically")
}

func TestBuildTxnWritesetPadSpaceTextPrimaryKeyUsesTrailingSpaceEquality(t *testing.T) {
	collationID := uint32(collations.MySQL8().LookupByName("utf8mb4_general_ci"))
	require.NotZero(t, collationID)

	plan := &TablePlan{
		TargetName: "emails",
		Fields: []*querypb.Field{{
			Name:    "email",
			Type:    querypb.Type_VARCHAR,
			Charset: collationID,
		}},
		PKIndices: []bool{true},
	}

	trimmedEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "emails", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("a"), Lengths: []int64{1}},
		}}},
	}
	spacedEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "emails", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("a "), Lengths: []int64{2}},
		}}},
	}

	trimmedKeys, err := buildTxnWriteset(map[string]*TablePlan{"emails": plan}, nil, nil, []*binlogdatapb.VEvent{trimmedEvent})
	require.NoError(t, err)
	spacedKeys, err := buildTxnWriteset(map[string]*TablePlan{"emails": plan}, nil, nil, []*binlogdatapb.VEvent{spacedEvent})
	require.NoError(t, err)
	require.Equal(t, trimmedKeys, spacedKeys, "text primary keys that compare equal under PAD SPACE collation rules must hash identically")
}

func TestBuildTxnWritesetWithStringFKRefsUsesCollationEqualityAcrossCompatibleTypes(t *testing.T) {
	collationID := uint32(collations.MySQL8().LookupByName("utf8mb4_0900_ai_ci"))
	require.NotZero(t, collationID)

	parentPlan := &TablePlan{
		TargetName: "parent",
		Fields: []*querypb.Field{{
			Name:    "email",
			Type:    querypb.Type_CHAR,
			Charset: collationID,
		}},
		PKIndices: []bool{true},
	}
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_email", Type: querypb.Type_VARCHAR, Charset: collationID},
		},
		PKIndices: []bool{true, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {{ParentTable: "parent", ChildColumnNames: []string{"parent_email"}, ReferencedColumnNames: []string{"email"}}},
	}
	parentRefs := buildParentFKRefs(fkRefs)
	tablePlans := map[string]*TablePlan{
		"parent": parentPlan,
		"child":  childPlan,
	}

	parentEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "parent", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("A"), Lengths: []int64{1}},
		}}},
	}
	childEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("1a"), Lengths: []int64{1, 1}},
		}}},
	}

	parentKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{parentEvent})
	require.NoError(t, err)
	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)

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
	require.True(t, conflict, "compatible string FK values that compare equal under MySQL collation rules must conflict")
}

func TestBuildTxnWritesetWithPadSpaceStringFKRefsUsesTrailingSpaceEqualityAcrossCompatibleTypes(t *testing.T) {
	collationID := uint32(collations.MySQL8().LookupByName("utf8mb4_general_ci"))
	require.NotZero(t, collationID)

	parentPlan := &TablePlan{
		TargetName: "parent",
		Fields: []*querypb.Field{{
			Name:    "email",
			Type:    querypb.Type_CHAR,
			Charset: collationID,
		}},
		PKIndices: []bool{true},
	}
	childPlan := &TablePlan{
		TargetName: "child",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_email", Type: querypb.Type_VARCHAR, Charset: collationID},
		},
		PKIndices: []bool{true, false},
	}
	fkRefs := map[string][]fkConstraintRef{
		"child": {{ParentTable: "parent", ChildColumnNames: []string{"parent_email"}, ReferencedColumnNames: []string{"email"}}},
	}
	parentRefs := buildParentFKRefs(fkRefs)
	tablePlans := map[string]*TablePlan{
		"parent": parentPlan,
		"child":  childPlan,
	}

	parentEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "parent", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("A"), Lengths: []int64{1}},
		}}},
	}
	childEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{TableName: "child", RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Values: []byte("1a "), Lengths: []int64{1, 2}},
		}}},
	}

	parentKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{parentEvent})
	require.NoError(t, err)
	childKeys, err := buildTxnWriteset(tablePlans, fkRefs, parentRefs, []*binlogdatapb.VEvent{childEvent})
	require.NoError(t, err)

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
	require.True(t, conflict, "compatible PAD SPACE string FK values that compare equal under MySQL rules must conflict")
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

func TestBuildTxnWritesetBacktickedDirectColumnPlanStaysSupported(t *testing.T) {
	vttablet.InitVReplicationConfigDefaults()
	vr := &vreplicator{workflowConfig: vttablet.DefaultVReplicationConfig}
	plan, err := vr.buildReplicatorPlan(
		getSource(&binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select id, email from t1",
		}}}),
		map[string][]*ColumnInfo{"t1": {{Name: "id", IsPK: true}, {Name: "email"}}},
		nil,
		binlogplayer.NewStats(),
		collations.MySQL8(),
		sqlparser.NewTestParser(),
	)
	require.NoError(t, err)

	tplan, err := plan.buildExecutionPlan(&binlogdatapb.FieldEvent{
		TableName: "t1",
		Fields: []*querypb.Field{
			{Name: "`id`", Type: querypb.Type_INT64},
			{Name: "`email`", Type: querypb.Type_VARCHAR},
		},
	})
	require.NoError(t, err)
	assert.False(t, tplan.HasUnsupportedWritesetMapping)
	require.Len(t, tplan.Fields, 2)
	assert.Equal(t, "id", tplan.Fields[0].Name)
	assert.Equal(t, "email", tplan.Fields[1].Name)
}
