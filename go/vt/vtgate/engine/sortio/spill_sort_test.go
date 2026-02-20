/*
Copyright 2025 The Vitess Authors.

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

package sortio

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func toInt64(t *testing.T, v sqltypes.Value) int64 {
	t.Helper()
	i, err := v.ToInt64()
	require.NoError(t, err)
	return i
}

func testFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "name", Type: querypb.Type_VARCHAR},
	}
}

func testComparison() evalengine.Comparison {
	return evalengine.Comparison{
		evalengine.OrderByParams{
			Col:             0,
			WeightStringCol: -1,
			Type:            evalengine.NewType(querypb.Type_INT64, collations.CollationBinaryID),
		},
	}
}

func testComparisonDesc() evalengine.Comparison {
	return evalengine.Comparison{
		evalengine.OrderByParams{
			Col:             0,
			WeightStringCol: -1,
			Desc:            true,
			Type:            evalengine.NewType(querypb.Type_INT64, collations.CollationBinaryID),
		},
	}
}

func makeRow(id int64, name string) sqltypes.Row {
	return sqltypes.Row{
		sqltypes.NewInt64(id),
		sqltypes.NewVarChar(name),
	}
}

func collectRows(ctx context.Context, ss *SpillSorter) ([]sqltypes.Row, error) {
	var rows []sqltypes.Row
	err := ss.Finish(ctx, func(row sqltypes.Row) error {
		rows = append(rows, row)
		return nil
	})
	return rows, err
}

func TestSpillSorterNoSpill(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 1024*1024, "")
	defer ss.Close()

	require.NoError(t, ss.Add(makeRow(3, "charlie")))
	require.NoError(t, ss.Add(makeRow(1, "alice")))
	require.NoError(t, ss.Add(makeRow(2, "bob")))

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	assert.Equal(t, int64(1), toInt64(t, rows[0][0]))
	assert.Equal(t, int64(2), toInt64(t, rows[1][0]))
	assert.Equal(t, int64(3), toInt64(t, rows[2][0]))
}

func TestSpillSorterNoSpillDesc(t *testing.T) {
	fields := testFields()
	cmp := testComparisonDesc()

	ss := NewSpillSorter(cmp, fields, 1024*1024, "")
	defer ss.Close()

	require.NoError(t, ss.Add(makeRow(1, "alice")))
	require.NoError(t, ss.Add(makeRow(3, "charlie")))
	require.NoError(t, ss.Add(makeRow(2, "bob")))

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	assert.Equal(t, int64(3), toInt64(t, rows[0][0]))
	assert.Equal(t, int64(2), toInt64(t, rows[1][0]))
	assert.Equal(t, int64(1), toInt64(t, rows[2][0]))
}

func TestSpillSorterSingleSpill(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	// Use a tiny buffer to force spilling after a couple of rows
	ss := NewSpillSorter(cmp, fields, 50, "")
	defer ss.Close()

	require.NoError(t, ss.Add(makeRow(5, "eve")))
	require.NoError(t, ss.Add(makeRow(3, "charlie")))
	require.NoError(t, ss.Add(makeRow(1, "alice")))
	require.NoError(t, ss.Add(makeRow(4, "dave")))
	require.NoError(t, ss.Add(makeRow(2, "bob")))

	assert.True(t, ss.spilled, "should have spilled to disk")

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 5)

	for i := range rows {
		assert.Equal(t, int64(i+1), toInt64(t, rows[i][0]))
	}
}

func TestSpillSorterMultipleSpills(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	// Very small buffer forces many spills
	ss := NewSpillSorter(cmp, fields, 30, "")
	defer ss.Close()

	n := 50
	for i := n; i > 0; i-- {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("name_%d", i))))
	}

	assert.True(t, ss.spilled)
	assert.Greater(t, len(ss.runs), 2, "should have multiple runs")

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, n)

	for i := range rows {
		assert.Equal(t, int64(i+1), toInt64(t, rows[i][0]))
	}
}

func TestSpillSorterIntermediateMerge(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	// Force enough spills to trigger intermediate merge (>FinalMergeWay runs)
	ss := NewSpillSorter(cmp, fields, 30, "")
	defer ss.Close()

	n := 200
	perm := rand.Perm(n)
	for _, i := range perm {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("name_%d", i))))
	}

	assert.True(t, ss.spilled)

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, n)

	for i := range rows {
		assert.Equal(t, int64(i), toInt64(t, rows[i][0]))
	}
}

func TestSpillSorterDeepMerge(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	// Very tiny buffer to force many runs and multiple intermediate passes
	ss := NewSpillSorter(cmp, fields, 20, "")
	defer ss.Close()

	n := 500
	perm := rand.Perm(n)
	for _, i := range perm {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("n%d", i))))
	}

	assert.True(t, ss.spilled)
	assert.Greater(t, len(ss.runs), FinalMergeWay)

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, n)

	for i := range rows {
		assert.Equal(t, int64(i), toInt64(t, rows[i][0]))
	}
}

func TestSpillSorterEmpty(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 1024, "")
	defer ss.Close()

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestSpillSorterSingleRow(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 1024, "")
	defer ss.Close()

	require.NoError(t, ss.Add(makeRow(42, "answer")))

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(42), toInt64(t, rows[0][0]))
}

func TestSpillSorterNullHandling(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 50, "")
	defer ss.Close()

	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewInt64(3), sqltypes.NewVarChar("charlie")}))
	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NULL, sqltypes.NewVarChar("null_row")}))
	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewInt64(1), sqltypes.NULL}))

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	// NULL sorts lowest
	assert.True(t, rows[0][0].IsNull())
	assert.Equal(t, int64(1), toInt64(t, rows[1][0]))
	assert.Equal(t, int64(3), toInt64(t, rows[2][0]))
}

func TestSpillSorterLimit(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	// Use tiny buffer to force spill
	ss := NewSpillSorter(cmp, fields, 30, "")
	defer ss.Close()

	for i := 10; i > 0; i-- {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("name_%d", i))))
	}

	var rows []sqltypes.Row
	limit := 3
	err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
		rows = append(rows, row)
		if len(rows) >= limit {
			return io.EOF
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, limit)

	assert.Equal(t, int64(1), toInt64(t, rows[0][0]))
	assert.Equal(t, int64(2), toInt64(t, rows[1][0]))
	assert.Equal(t, int64(3), toInt64(t, rows[2][0]))
}

func TestSpillSorterLimitNoSpill(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 1024*1024, "")
	defer ss.Close()

	for i := 10; i > 0; i-- {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("name_%d", i))))
	}

	var rows []sqltypes.Row
	limit := 2
	err := ss.Finish(context.Background(), func(row sqltypes.Row) error {
		rows = append(rows, row)
		if len(rows) >= limit {
			return io.EOF
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, limit)

	assert.Equal(t, int64(1), toInt64(t, rows[0][0]))
	assert.Equal(t, int64(2), toInt64(t, rows[1][0]))
}

func TestSpillSorterLargeRowExceedsBuffer(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "data", Type: querypb.Type_VARCHAR},
	}
	cmp := evalengine.Comparison{
		evalengine.OrderByParams{
			Col:             0,
			WeightStringCol: -1,
			Type:            evalengine.NewType(querypb.Type_VARCHAR, collations.CollationBinaryID),
		},
	}

	// Buffer is 50 bytes, but we add a single row that's much larger
	ss := NewSpillSorter(cmp, fields, 50, "")
	defer ss.Close()

	largeValue := make([]byte, 200)
	for i := range largeValue {
		largeValue[i] = 'a'
	}
	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewVarChar(string(largeValue))}))

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

func TestSpillSorterContextCancellation(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 30, "")
	defer ss.Close()

	for i := range 20 {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("name_%d", i))))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := collectRows(ctx, ss)
	assert.Error(t, err)
}

func TestSpillSorterCleanup(t *testing.T) {
	fields := testFields()
	cmp := testComparison()

	ss := NewSpillSorter(cmp, fields, 30, "")

	for i := range 20 {
		require.NoError(t, ss.Add(makeRow(int64(i), fmt.Sprintf("name_%d", i))))
	}

	assert.True(t, ss.spilled)
	ss.Close()
	assert.Nil(t, ss.files[0])
	assert.Nil(t, ss.files[1])
	assert.Nil(t, ss.buffer)
	assert.Nil(t, ss.runs)
}

func TestSpillSorterMultiColumnSort(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "group", Type: querypb.Type_INT64},
		{Name: "name", Type: querypb.Type_VARCHAR},
	}
	cmp := evalengine.Comparison{
		evalengine.OrderByParams{
			Col:             0,
			WeightStringCol: -1,
			Type:            evalengine.NewType(querypb.Type_INT64, collations.CollationBinaryID),
		},
		evalengine.OrderByParams{
			Col:             1,
			WeightStringCol: -1,
			Desc:            true,
			Type:            evalengine.NewType(querypb.Type_VARCHAR, collations.CollationBinaryID),
		},
	}

	ss := NewSpillSorter(cmp, fields, 50, "")
	defer ss.Close()

	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewInt64(1), sqltypes.NewVarChar("b")}))
	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewInt64(1), sqltypes.NewVarChar("a")}))
	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewInt64(2), sqltypes.NewVarChar("c")}))
	require.NoError(t, ss.Add(sqltypes.Row{sqltypes.NewInt64(1), sqltypes.NewVarChar("c")}))

	rows, err := collectRows(context.Background(), ss)
	require.NoError(t, err)
	require.Len(t, rows, 4)

	// group ASC, name DESC
	assert.Equal(t, int64(1), toInt64(t, rows[0][0]))
	assert.Equal(t, "c", rows[0][1].ToString())
	assert.Equal(t, int64(1), toInt64(t, rows[1][0]))
	assert.Equal(t, "b", rows[1][1].ToString())
	assert.Equal(t, int64(1), toInt64(t, rows[2][0]))
	assert.Equal(t, "a", rows[2][1].ToString())
	assert.Equal(t, int64(2), toInt64(t, rows[3][0]))
	assert.Equal(t, "c", rows[3][1].ToString())
}
