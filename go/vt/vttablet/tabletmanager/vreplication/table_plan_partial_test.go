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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

// buildTestTablePlan builds the full TablePlan for a single-rule filter from
// source table "src" into the given target, using the given fields as the FIELD
// event the vstreamer would send for it.
func buildTestTablePlan(t *testing.T, target, filter string, fields []*querypb.Field) *TablePlan {
	t.Helper()
	vttablet.InitVReplicationConfigDefaults()
	vr := &vreplicator{workflowConfig: vttablet.DefaultVReplicationConfig}
	rp, err := vr.buildReplicatorPlan(getSource(&binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{Match: target, Filter: filter}},
	}), map[string][]*ColumnInfo{
		target: {{Name: "id", IsPK: true}},
	}, nil, binlogplayer.NewStats(), collations.MySQL8(), sqlparser.NewTestParser())
	require.NoError(t, err)
	// FIELD events carry the source table's name.
	tp, err := rp.buildExecutionPlan(&binlogdatapb.FieldEvent{TableName: "src", Fields: fields})
	require.NoError(t, err)
	return tp
}

func bitmap(bits ...bool) *binlogdatapb.RowChange_Bitmap {
	bm := &binlogdatapb.RowChange_Bitmap{
		Count: int64(len(bits)),
		Cols:  make([]byte, (len(bits)+7)/8),
	}
	for i, b := range bits {
		setBit(bm.Cols, i, b)
	}
	return bm
}

func bitmapBits(bm *binlogdatapb.RowChange_Bitmap) []bool {
	bits := make([]bool, bm.Count)
	for i := range bits {
		bits[i] = isBitSet(bm.Cols, i)
	}
	return bits
}

// TestPartialQueryDataColumns confirms how the after image column presence
// bitmap of a row event is mapped onto the target column expressions before a
// partial insert/update query is generated from it. The projected
// AfterDataColumns is indexed by the streamed fields and mapped through the
// column names each target expression uses; the legacy DataColumns is used
// as-is, as before.
func TestPartialQueryDataColumns(t *testing.T) {
	// The source table is src(id, blb, val, txt). The filter reorders the
	// columns, drops txt, renames val through a convert expression, adds a
	// keyspace_id() and a constant. The vstreamer emits the convert expression
	// under the source column's name and the constant as "1".
	tp := buildTestTablePlan(t, "dst",
		"select blb, id, convert(val using utf8mb4) as val2, keyspace_id() as ksid, 1 as c from src",
		[]*querypb.Field{
			{Name: "blb", Type: querypb.Type_BLOB},
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "val", Type: querypb.Type_VARBINARY},
			{Name: "keyspace_id", Type: querypb.Type_VARBINARY},
			{Name: "1", Type: querypb.Type_INT64},
		})
	require.Len(t, tp.TablePlanBuilder.colExprs, 5)

	t.Run("projected bitmap is mapped onto the target expressions", func(t *testing.T) {
		// binlog_row_image=NOBLOB omitted the blob: streamed (blb, id, val, keyspace_id, 1).
		after := bitmap(false, true, true, true, true)
		got, err := tp.partialQueryDataColumns(&binlogdatapb.RowChange{AfterDataColumns: after})
		require.NoError(t, err)
		// Target (blb, id, val2, ksid, c): val2 follows val, not its alias.
		assert.Equal(t, []bool{false, true, true, true, true}, bitmapBits(got))
	})

	t.Run("keyspace_id absent when the vstreamer says so", func(t *testing.T) {
		after := bitmap(false, true, true, false, true)
		got, err := tp.partialQueryDataColumns(&binlogdatapb.RowChange{AfterDataColumns: after})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, true, true, false, true}, bitmapBits(got))
	})

	t.Run("projected bitmap wins over the legacy one", func(t *testing.T) {
		after := bitmap(false, true, true, true, true)
		legacy := bitmap(true, false, true, true) // source table order (id, blb, val, txt)
		got, err := tp.partialQueryDataColumns(&binlogdatapb.RowChange{DataColumns: legacy, AfterDataColumns: after})
		require.NoError(t, err)
		assert.Equal(t, []bool{false, true, true, true, true}, bitmapBits(got))
	})

	t.Run("legacy bitmap from an older source is used as-is", func(t *testing.T) {
		legacy := bitmap(true, false, true, true)
		got, err := tp.partialQueryDataColumns(&binlogdatapb.RowChange{DataColumns: legacy})
		require.NoError(t, err)
		assert.Same(t, legacy, got)
	})

	t.Run("no bitmap at all is an error", func(t *testing.T) {
		_, err := tp.partialQueryDataColumns(&binlogdatapb.RowChange{})
		require.ErrorContains(t, err, "no data columns bitmap")
	})

	t.Run("a column missing from the streamed fields is an error", func(t *testing.T) {
		broken := *tp
		broken.Fields = tp.Fields[:2] // val is not streamed
		_, err := broken.targetDataColumns(bitmap(true, true))
		require.ErrorContains(t, err, "column val used by target column val2")
	})
}

// TestPartialQueryDataColumnsSelectStar confirms the mapping for a "select *"
// filter, whose plan is built from the FIELD event: every target column follows
// the streamed field of the same name.
func TestPartialQueryDataColumnsSelectStar(t *testing.T) {
	tp := buildTestTablePlan(t, "src", "select * from src", []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT32},
		{Name: "blb", Type: querypb.Type_BLOB},
		{Name: "val", Type: querypb.Type_VARBINARY},
	})
	got, err := tp.partialQueryDataColumns(&binlogdatapb.RowChange{AfterDataColumns: bitmap(true, false, true)})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, true}, bitmapBits(got))
}

// TestStreamedDataColumns confirms which bitmap the paths that index by the
// streamed fields (tp.Fields) use.
func TestStreamedDataColumns(t *testing.T) {
	tp := &TablePlan{}
	legacy := bitmap(true, false)
	after := bitmap(false, true)
	assert.Nil(t, tp.streamedDataColumns(nil))
	assert.Nil(t, tp.streamedDataColumns(&binlogdatapb.RowChange{}))
	assert.Same(t, legacy, tp.streamedDataColumns(&binlogdatapb.RowChange{DataColumns: legacy}))
	assert.Same(t, after, tp.streamedDataColumns(&binlogdatapb.RowChange{DataColumns: legacy, AfterDataColumns: after}))
	assert.Same(t, after, tp.streamedDataColumns(&binlogdatapb.RowChange{AfterDataColumns: after}))
}

func applyChangeQueries(t *testing.T, tp *TablePlan, rowChange *binlogdatapb.RowChange) ([]string, error) {
	t.Helper()
	var executed []string
	_, err := tp.applyChange(rowChange, func(sql string) (*sqltypes.Result, error) {
		executed = append(executed, sql)
		return &sqltypes.Result{RowsAffected: 1}, nil
	})
	return executed, err
}

// TestApplyChangePartialProjectedFilter confirms that a partial UPDATE for a
// filter that reorders columns, renames one and adds a constant is applied
// using the projected bitmap: the omitted blob is left alone, the renamed
// column is updated and the constant is set. The legacy bitmap alone, as an
// older vstreamer sends it, has fewer bits than the target has columns and the
// partial update cannot be generated from it, which is the pre-existing
// behavior this change fixes.
func TestApplyChangePartialProjectedFilter(t *testing.T) {
	tp := buildTestTablePlan(t, "dst",
		"select blb, id, convert(val using utf8mb4) as val2, 1 as c from src",
		[]*querypb.Field{
			{Name: "blb", Type: querypb.Type_BLOB},
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "val", Type: querypb.Type_VARBINARY},
			{Name: "1", Type: querypb.Type_INT64},
		})
	tp.Stats = binlogplayer.NewStats()

	before := &querypb.Row{Lengths: []int64{-1, 1, 3, 1}, Values: []byte("1aaa1")}
	after := &querypb.Row{Lengths: []int64{-1, 1, 3, 1}, Values: []byte("1bbb1")}
	// Source table order (id, blb, val): only the blob is absent.
	legacy := bitmap(true, false, true)
	// Streamed order (blb, id, val, 1): only the blob is absent.
	projected := bitmap(false, true, true, true)

	executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
		Before: before, After: after, DataColumns: legacy, AfterDataColumns: projected,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"update dst set val2=convert(_binary'bbb' using utf8mb4), c=1 where id=1"}, executed)

	// A PK change on a partial row event cannot be rebuilt on the target
	// because the blob is missing; the projected bitmap identifies it.
	_, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{
		Before: before, DataColumns: legacy, AfterDataColumns: projected,
		After: &querypb.Row{Lengths: []int64{-1, 1, 3, 1}, Values: []byte("2bbb1")},
	})
	require.ErrorContains(t, err, "missing a needed value for dst.blb")

	// From an older vstreamer only the legacy bitmap arrives.
	_, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: after, DataColumns: legacy})
	require.ErrorContains(t, err, "unable to create partial update query for dst")
}

// TestApplyChangePartialLegacyBitmapMisaligned shows why the projected bitmap
// is needed even when the source and target have the same number of columns:
// for a reordering filter the legacy bitmap's bits describe the wrong target
// columns, so the omitted blob is taken as present and overwritten with NULL,
// while the projected bitmap leaves it alone.
func TestApplyChangePartialLegacyBitmapMisaligned(t *testing.T) {
	tp := buildTestTablePlan(t, "dst",
		"select blb, id, convert(val using utf8mb4) as val2 from src",
		[]*querypb.Field{
			{Name: "blb", Type: querypb.Type_BLOB},
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "val", Type: querypb.Type_VARBINARY},
		})
	tp.Stats = binlogplayer.NewStats()

	before := &querypb.Row{Lengths: []int64{-1, 1, 3}, Values: []byte("1aaa")}
	after := &querypb.Row{Lengths: []int64{-1, 1, 3}, Values: []byte("1bbb")}
	legacy := bitmap(true, false, true)    // source table order (id, blb, val)
	projected := bitmap(false, true, true) // streamed order (blb, id, val)

	executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: after, DataColumns: legacy})
	require.NoError(t, err)
	require.Equal(t, []string{"update dst set blb=null, val2=convert(_binary'bbb' using utf8mb4) where id=1"}, executed)

	executed, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: after, DataColumns: legacy, AfterDataColumns: projected})
	require.NoError(t, err)
	require.Equal(t, []string{"update dst set val2=convert(_binary'bbb' using utf8mb4) where id=1"}, executed)
}
