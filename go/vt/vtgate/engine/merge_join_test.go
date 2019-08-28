/*
Copyright 2018 The Vitess Authors.

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

package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestMergeJoinExecute(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2",
					"int64|varchar",
				),
				"1|one",
				"2|two",
				"3|three",
			),
		},
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col4|col5",
					"int64|varchar",
				),
				"2|dos",
				"3|tres",
				"4|cuatro",
			),
		},
	}
	bv := map[string]*querypb.BindVariable{}

	// Inner merge join
	jn := &MergeJoin{
		Opcode:        InnerJoin,
		Left:          leftPrim,
		Right:         rightPrim,
		Cols:          []int{-1, -2, 2},
		LeftJoinCols:  []int{0},
		RightJoinCols: []int{0},
	}
	r, err := jn.Execute(noopVCursor{}, bv, true)
	if err != nil {
		t.Fatal(err)
	}
	expectes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col5",
			"int64|varchar|varchar",
		),
		"2|two|dos",
		"3|three|tres",
	)
	expectResult(t, "jn.Execute", r, expectes)
}

func TestMergeJoinWithOneRowOnLHSThatMatchesMultipleRowsOnTheRHS(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2",
					"int64|varchar",
				),
				"1|one",
				"1|oneB",
				"2|two",
				"3|three",
			),
		},
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col4|col5",
					"int64|varchar",
				),
				"1|uno",
				"3|tres",
				"3|tresB",
				"4|cuatro",
			),
		},
	}
	bv := map[string]*querypb.BindVariable{}

	// Inner merge join
	jn := &MergeJoin{
		Opcode:        InnerJoin,
		Left:          leftPrim,
		Right:         rightPrim,
		Cols:          []int{-1, -2, 2},
		LeftJoinCols:  []int{0},
		RightJoinCols: []int{0},
	}
	r, err := jn.Execute(noopVCursor{}, bv, true)
	if err != nil {
		t.Fatal(err)
	}
	expected := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col5",
			"int64|varchar|varchar",
		),
		"1|one|uno",
		"1|oneB|uno",
		"3|three|tres",
		"3|three|tresB",
	)
	expectResult(t, "jn.Execute", r, expected)
}

func TestCursorA(t *testing.T) {
	values := make([][]sqltypes.Value, 0)
	c, err := newCursor(values, []int{0})
	assert.NoError(t, err)
	assert.False(t, c.hasData(), "empty cursor is empty")
}

func TestCursorWithMultipleChunks(t *testing.T) {
	values := [][]sqltypes.Value{
		{sqltypes.NewInt32(0), sqltypes.NewVarChar("zero")},
		{sqltypes.NewInt32(0), sqltypes.NewVarChar("zeroB")},
		{sqltypes.NewInt32(1), sqltypes.NewVarChar("uno")},
		{sqltypes.NewInt32(2), sqltypes.NewVarChar("dos")},
		{sqltypes.NewInt32(3), sqltypes.NewVarChar("tres")},
		{sqltypes.NewInt32(3), sqltypes.NewVarChar("tresB")},
		{sqltypes.NewInt32(3), sqltypes.NewVarChar("tresC")},
	}
	c, err := newCursor(values, []int{0})
	assert.NoError(t, err)
	assert.True(t, c.hasData())
	assert.Equal(t, [][]sqltypes.Value{
		{sqltypes.NewInt32(0), sqltypes.NewVarChar("zero")},
		{sqltypes.NewInt32(0), sqltypes.NewVarChar("zeroB")},
	}, c.current)

	err = c.fetchNextChunk()
	assert.NoError(t, err)
	assert.True(t, c.hasData())
	assert.Equal(t, [][]sqltypes.Value{
		{sqltypes.NewInt32(1), sqltypes.NewVarChar("uno")},
	}, c.current)

	err = c.fetchNextChunk()
	assert.NoError(t, err)
	assert.True(t, c.hasData())
	assert.Equal(t, [][]sqltypes.Value{
		{sqltypes.NewInt32(2), sqltypes.NewVarChar("dos")},
	}, c.current)

	err = c.fetchNextChunk()
	assert.NoError(t, err)
	assert.True(t, c.hasData())
	assert.Equal(t, [][]sqltypes.Value{
		{sqltypes.NewInt32(3), sqltypes.NewVarChar("tres")},
		{sqltypes.NewInt32(3), sqltypes.NewVarChar("tresB")},
		{sqltypes.NewInt32(3), sqltypes.NewVarChar("tresC")},
	}, c.current)

	err = c.fetchNextChunk()
	assert.NoError(t, err)
	assert.False(t, c.hasData())
}
