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
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

	// Both row changes carried the same streamed bitmap, mapped once.
	assert.Len(t, tp.PartialBitmaps, 1)

	// From an older vstreamer only the legacy bitmap arrives.
	_, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: after, DataColumns: legacy})
	require.ErrorContains(t, err, "unable to create partial update query for dst")
}

// TestApplyChangePartialLegacyInsertBitmapShort confirms that a short legacy
// DataColumns bitmap (older source, no AfterDataColumns) errors on the INSERT
// path instead of panicking in isBitSet. A Materialize-style filter with extra
// constants has more colExprs than dataColumns.Count, which is the case the
// UPDATE generator already guarded.
func TestApplyChangePartialLegacyInsertBitmapShort(t *testing.T) {
	tp := buildTestTablePlan(t, "dst",
		"select blb, id, convert(val using utf8mb4) as val2, 1 as c from src",
		[]*querypb.Field{
			{Name: "blb", Type: querypb.Type_BLOB},
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "val", Type: querypb.Type_VARBINARY},
			{Name: "1", Type: querypb.Type_INT64},
		})
	tp.Stats = binlogplayer.NewStats()

	after := &querypb.Row{Lengths: []int64{-1, 1, 3, 1}, Values: []byte("1aaa1")}
	// All bits present but fewer than the target has columns: the INSERT
	// completeness check must not fire, so the generators still report
	// INTERNAL for the layout mismatch.
	legacy := bitmap(true, true, true)
	require.Greater(t, len(tp.TablePlanBuilder.colExprs), int(legacy.Count))

	_, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{After: after, DataColumns: legacy})
	require.ErrorContains(t, err, "unable to create partial insert query for dst")
	assert.Equal(t, vtrpcpb.Code_INTERNAL, vterrors.Code(err))

	// Each INSERT generator must fail closed on its own; the lastpk
	// (copy-phase) select-from-dual form is a separate walk of the bitmap.
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	_, err = tp.TablePlanBuilder.generatePartialInsertPart(buf, legacy)
	require.ErrorContains(t, err, "unable to create partial insert query for dst")
	_, err = tp.TablePlanBuilder.generatePartialValuesPart(buf, bvf, legacy)
	require.ErrorContains(t, err, "unable to create partial insert query for dst")
	tp.TablePlanBuilder.lastpk = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id", "int32"),
		"10",
	)
	_, err = tp.TablePlanBuilder.generatePartialSelectPart(buf, bvf, legacy)
	require.ErrorContains(t, err, "unable to create partial insert query for dst")
}

// TestApplyChangePartialGroupedPlans confirms that partial INSERT and UPDATE
// statements for grouped plans keep the semantics of the full statements, so
// that a row event does not behave differently only because NOBLOB or
// PARTIAL_JSON marked it partial: a fully grouped plan (insertIgnore, first
// value wins) uses "insert ignore" for both, and a partially grouped plan
// (insertOnDup, last value wins) adds "on duplicate key update" over the
// present columns to its INSERT while its UPDATE stays a plain update.
func TestApplyChangePartialGroupedPlans(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT32},
		{Name: "val", Type: querypb.Type_VARBINARY},
		{Name: "blb", Type: querypb.Type_BLOB},
	}
	// Streamed order (id, val, blb) with the blob omitted.
	blobOmitted := bitmap(true, true, false)
	fullImage := bitmap(true, true, true)
	before := &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1aaa")}
	after := &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1bbb")}
	afterFull := &querypb.Row{Lengths: []int64{1, 3, 5}, Values: []byte("1bbbblob1")}

	t.Run("insertIgnore", func(t *testing.T) {
		tp := buildTestTablePlan(t, "dst", "select id, val, blb from src group by id, val, blb", fields)
		tp.Stats = binlogplayer.NewStats()

		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{After: after, AfterDataColumns: blobOmitted})
		require.NoError(t, err)
		require.Equal(t, []string{"insert ignore into dst(id,val) values (1,_binary'bbb')"}, executed, "partial insert")

		executed, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: after, AfterDataColumns: blobOmitted})
		require.NoError(t, err)
		require.Equal(t, []string{"insert ignore into dst(id,val) values (1,_binary'bbb')"}, executed, "partial update")

		executed, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: afterFull, AfterDataColumns: fullImage})
		require.NoError(t, err)
		require.Equal(t, []string{"insert ignore into dst(id,val,blb) values (1,_binary'bbb',_binary'blob1')"}, executed, "full update, still partial-marked")
	})

	t.Run("insertOnDup", func(t *testing.T) {
		tp := buildTestTablePlan(t, "dst", "select id, val, blb from src group by id", fields)
		tp.Stats = binlogplayer.NewStats()

		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{After: after, AfterDataColumns: blobOmitted})
		require.NoError(t, err)
		require.Equal(t, []string{"insert into dst(id,val) values (1,_binary'bbb') on duplicate key update val=values(val)"}, executed, "partial insert")

		executed, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{Before: before, After: after, AfterDataColumns: blobOmitted})
		require.NoError(t, err)
		require.Equal(t, []string{"update dst set val=_binary'bbb' where id=1"}, executed, "partial update")

		executed, err = applyChangeQueries(t, tp, &binlogdatapb.RowChange{After: afterFull, AfterDataColumns: fullImage})
		require.NoError(t, err)
		require.Equal(t, []string{"insert into dst(id,val,blb) values (1,_binary'bbb',_binary'blob1') on duplicate key update val=values(val), blb=values(blb)"}, executed, "full insert, still partial-marked")
	})
}

// TestApplyChangePartialInsertOmittedColumns pins down partial INSERTs. A
// column absent from a true INSERT's NOBLOB image was not set by the source
// statement (MySQL omits BLOB/TEXT columns that are not in the write set), so
// it is left out of the target INSERT and takes the target's default, as it did
// on the source. The vstreamer rejects the other after-only shape, an UPDATE
// moving a row into the key range, so vplayer does not have to tell them apart.
// An expression that mixes present and absent inputs cannot be computed and is
// rejected, as there is no before image to fall back on.
func TestApplyChangePartialInsertOmittedColumns(t *testing.T) {
	tp := buildTestTablePlan(t, "dst",
		"select blb, id, convert(val using utf8mb4) as val2, 1 as c from src",
		[]*querypb.Field{
			{Name: "blb", Type: querypb.Type_BLOB},
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "val", Type: querypb.Type_VARBINARY},
			{Name: "1", Type: querypb.Type_INT64},
		})
	tp.Stats = binlogplayer.NewStats()

	t.Run("omitted blob is left to the target default", func(t *testing.T) {
		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			After:            &querypb.Row{Lengths: []int64{-1, 1, 3, 1}, Values: []byte("1aaa1")},
			AfterDataColumns: bitmap(false, true, true, true), // streamed order (blb, id, val, 1)
		})
		require.NoError(t, err)
		require.Equal(t, []string{"insert into dst(id,val2,c) values (1,convert(_binary'aaa' using utf8mb4),1)"}, executed)
	})

	t.Run("full after image: inserted", func(t *testing.T) {
		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			After:            &querypb.Row{Lengths: []int64{5, 1, 3, 1}, Values: []byte("blob11aaa1")},
			AfterDataColumns: bitmap(true, true, true, true),
		})
		require.NoError(t, err)
		require.Equal(t, []string{"insert into dst(blb,id,val2,c) values (_binary'blob1',1,convert(_binary'aaa' using utf8mb4),1)"}, executed)
	})

	t.Run("mixed expression on insert: rejected", func(t *testing.T) {
		mixed := buildTestTablePlan(t, "dst", "select id, concat(val, blb) as c from src", []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "val", Type: querypb.Type_VARBINARY},
			{Name: "blb", Type: querypb.Type_BLOB},
		})
		mixed.Stats = binlogplayer.NewStats()
		executed, err := applyChangeQueries(t, mixed, &binlogdatapb.RowChange{
			After:            &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1aaa")},
			AfterDataColumns: bitmap(true, true, false),
		})
		require.ErrorContains(t, err, "missing a needed value for dst.c")
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		assert.Empty(t, executed)
	})
}

// TestApplyChangePartialNoWritableColumns confirms that a partial UPDATE whose
// image carries none of the target's writable columns is a no-op rather than an
// invalid "update dst set  where ..." statement. This happens when the source
// row change only touched columns the filter does not select: the projected
// bitmap then marks every non-PK target column as absent.
func TestApplyChangePartialNoWritableColumns(t *testing.T) {
	// Source is src(id, blb, val); the filter drops val.
	tp := buildTestTablePlan(t, "dst", "select blb, id from src", []*querypb.Field{
		{Name: "blb", Type: querypb.Type_BLOB},
		{Name: "id", Type: querypb.Type_INT32},
	})
	tp.Stats = binlogplayer.NewStats()

	// "update src set val = ..." under NOBLOB: the unchanged blob is omitted.
	rowChange := &binlogdatapb.RowChange{
		Before:           &querypb.Row{Lengths: []int64{-1, 1}, Values: []byte("1")},
		After:            &querypb.Row{Lengths: []int64{-1, 1}, Values: []byte("1")},
		DataColumns:      bitmap(true, false, true), // source order (id, blb, val)
		AfterDataColumns: bitmap(false, true),       // streamed order (blb, id)
	}
	executed, err := applyChangeQueries(t, tp, rowChange)
	require.NoError(t, err)
	assert.Empty(t, executed)
	assert.Empty(t, tp.PartialUpdates, "a no-op must not be cached")
	assert.Empty(t, tp.Stats.PartialQueryCount.Counts())

	// Once the blob is present again the update is generated as usual.
	rowChange.After = &querypb.Row{Lengths: []int64{5, 1}, Values: []byte("blob21")}
	rowChange.AfterDataColumns = bitmap(true, true)
	executed, err = applyChangeQueries(t, tp, rowChange)
	require.NoError(t, err)
	require.Equal(t, []string{"update dst set blb=_binary'blob2' where id=1"}, executed)
}

// TestApplyChangePartialAggregatePlansRejected confirms that a partial row
// event for a plan with aggregate expressions is rejected with a clear,
// non-retryable error on both the insert and the update path, whichever bitmap
// the source sends. The partial query generators only handle plain
// expressions, so such plans never worked with partial images (they produced
// invalid SQL such as "cnt=" without a value). Grouped plans without
// aggregates, like the Materialize filter of an owned lookup vindex backfill,
// only use plain expressions and keep working.
func TestApplyChangePartialAggregatePlansRejected(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT32},
		{Name: "val", Type: querypb.Type_VARBINARY},
	}
	before := &querypb.Row{Lengths: []int64{1, 3}, Values: []byte("1aaa")}
	after := &querypb.Row{Lengths: []int64{1, 3}, Values: []byte("1bbb")}

	for _, filter := range []string{
		"select id, count(*) as cnt from src group by id",
		"select id, sum(val) as total from src group by id",
		"select id, count(*) as cnt from src", // aggregate without group by is still an insertNormal plan
	} {
		t.Run(filter, func(t *testing.T) {
			tp := buildTestTablePlan(t, "dst", filter, fields)
			tp.Stats = binlogplayer.NewStats()
			require.False(t, tp.supportsPartialImages())

			for name, rowChange := range map[string]*binlogdatapb.RowChange{
				"projected": {AfterDataColumns: bitmap(true, true)},
				"legacy":    {DataColumns: bitmap(true, true)},
			} {
				rowChange.After = after
				_, err := applyChangeQueries(t, tp, rowChange)
				require.ErrorContains(t, err, "partial row image received for dst", "%s insert", name)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
				rowChange.Before = before
				_, err = applyChangeQueries(t, tp, rowChange)
				require.ErrorContains(t, err, "partial row image received for dst", "%s update", name)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			}
		})
	}

	t.Run("group by without aggregates is applied with the plan's semantics", func(t *testing.T) {
		// This is the shape of an owned lookup vindex backfill (insertIgnore
		// plan): first value wins, so an UPDATE is an insert ignore, as with a
		// full image. See TestApplyChangePartialGroupedPlans.
		tp := buildTestTablePlan(t, "dst", "select id, val from src group by id, val", fields)
		tp.Stats = binlogplayer.NewStats()
		require.True(t, tp.supportsPartialImages())
		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			Before: before, After: after, AfterDataColumns: bitmap(true, true),
		})
		require.NoError(t, err)
		require.Equal(t, []string{"insert ignore into dst(id,val) values (1,_binary'bbb')"}, executed)
	})

	t.Run("plain projection is fine", func(t *testing.T) {
		tp := buildTestTablePlan(t, "dst", "select id, val, 1 as c from src", fields)
		require.True(t, tp.supportsPartialImages())
	})
}

// TestApplyChangePartialMixedInputs pins down the rule for a target expression
// that uses both a present and an absent streamed column, e.g.
// concat(val, blb) under NOBLOB with the unchanged blob omitted. Its value
// cannot be computed from the image. When none of its present inputs changed,
// its value did not change either and it is left out of the partial update.
// When a present input did change, the row event is rejected with a
// non-retryable error rather than silently dropping the change (or writing
// concat(val, NULL), as the legacy path did).
func TestApplyChangePartialMixedInputs(t *testing.T) {
	tp := buildTestTablePlan(t, "dst", "select id, concat(val, blb) as c from src", []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT32},
		{Name: "val", Type: querypb.Type_VARBINARY},
		{Name: "blb", Type: querypb.Type_BLOB},
	})
	tp.Stats = binlogplayer.NewStats()
	blobOmitted := bitmap(true, true, false)

	t.Run("present input unchanged: nothing to apply", func(t *testing.T) {
		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			Before:           &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1aaa")},
			After:            &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1aaa")},
			AfterDataColumns: blobOmitted,
		})
		require.NoError(t, err)
		assert.Empty(t, executed)
	})

	t.Run("present input changed: rejected", func(t *testing.T) {
		_, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			Before:           &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1aaa")},
			After:            &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1bbb")},
			AfterDataColumns: blobOmitted,
		})
		require.ErrorContains(t, err, "missing a needed value for dst.c")
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	})

	t.Run("present input NULL-ness changed: rejected", func(t *testing.T) {
		_, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			Before:           &querypb.Row{Lengths: []int64{1, -1, -1}, Values: []byte("1")},
			After:            &querypb.Row{Lengths: []int64{1, 0, -1}, Values: []byte("1")},
			AfterDataColumns: blobOmitted,
		})
		require.ErrorContains(t, err, "missing a needed value for dst.c")
	})

	t.Run("present input omitted in before image: rejected", func(t *testing.T) {
		// Matt's two-BLOB repro (#21157 review): source (id, b1 BLOB, b2 BLOB, val),
		// filter select id, concat(b1, b2) as c, val from src. Under NOBLOB the
		// before image omits both blobs; UPDATE SET b1=NULL, val=2 then has
		// after-image b1 present as NULL and b2 absent. Without BeforeDataColumns
		// the omitted old b1 and the new NULL compare equal, so c is silently
		// dropped and the target keeps concat('a','b'). With the bitmap, b1 is
		// known to be missing from the before image and the event is rejected.
		tp2 := buildTestTablePlan(t, "dst", "select id, concat(b1, b2) as c, val from src", []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT32},
			{Name: "b1", Type: querypb.Type_BLOB},
			{Name: "b2", Type: querypb.Type_BLOB},
			{Name: "val", Type: querypb.Type_INT32},
		})
		tp2.Stats = binlogplayer.NewStats()
		_, err := applyChangeQueries(t, tp2, &binlogdatapb.RowChange{
			Before:            &querypb.Row{Lengths: []int64{1, -1, -1, 1}, Values: []byte("11")},
			After:             &querypb.Row{Lengths: []int64{1, -1, -1, 1}, Values: []byte("12")},
			AfterDataColumns:  bitmap(true, true, false, true),  // b1 present (NULL), b2 omitted
			BeforeDataColumns: bitmap(true, false, false, true), // both blobs omitted in before
		})
		require.ErrorContains(t, err, "missing a needed value for dst.c")
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	})

	t.Run("no before image to compare against: rejected", func(t *testing.T) {
		_, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			After:            &querypb.Row{Lengths: []int64{1, 3, -1}, Values: []byte("1bbb")},
			AfterDataColumns: blobOmitted,
		})
		require.ErrorContains(t, err, "missing a needed value for dst.c")
	})

	t.Run("all inputs present: applied", func(t *testing.T) {
		executed, err := applyChangeQueries(t, tp, &binlogdatapb.RowChange{
			Before:           &querypb.Row{Lengths: []int64{1, 3, 5}, Values: []byte("1aaablob1")},
			After:            &querypb.Row{Lengths: []int64{1, 3, 5}, Values: []byte("1bbbblob2")},
			AfterDataColumns: bitmap(true, true, true),
		})
		require.NoError(t, err)
		require.Equal(t, []string{"update dst set c=concat(_binary'bbb', _binary'blob2') where id=1"}, executed)
	})

	// The projection of each distinct streamed bitmap is computed once.
	assert.Len(t, tp.PartialBitmaps, 2)
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
