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

package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func TestPulloutSubqueryValueGood(t *testing.T) {
	// Test one case with actual bind vars.
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}

	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	underlyingResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col",
			"int64",
		),
		"0",
	)
	ufp := &fakePrimitive{
		results: []*sqltypes.Result{underlyingResult},
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
		Outer:          ufp,
	}

	result, err := ps.TryExecute(t.Context(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v false`, sqltypes.Int64BindVariable(1))})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v sq: %v false`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(1))})
	expectResult(t, result, underlyingResult)

	// A row of more than one column binds each column to its own variable, leaving out any extra columns the
	// subquery returns, such as a weight_string for its ordering.
	rowResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|weight_string(col2)",
			"int64|varchar|varbinary",
		),
		"1|a|A",
	)
	sfp = &fakePrimitive{
		results: []*sqltypes.Result{rowResult},
	}
	ufp.rewind()
	ps.Subquery = sfp
	ps.RowResults = []string{"sq_0", "sq_1"}

	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v sq_0: %v sq_1: %v false`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(1), sqltypes.StringBindVariable("a"))})
}

func TestPulloutSubqueryValueNone(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
		Outer:          ufp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute sq:  false`})

	// For a row of more than one column, each column is NULL, which is how MySQL compares a row with an empty
	// row subquery.
	sfp.rewind()
	ufp.rewind()
	ps.RowResults = []string{"sq_0", "sq_1"}
	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	ufp.ExpectLog(t, []string{`Execute sq_0:  sq_1:  false`})
}

func TestPulloutSubqueryValueBadRows(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
		"2",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "subquery returned more than one row")

	// A row that is narrower than the comparison it is bound for is a planner bug, for a value comparison and for
	// IN alike.
	narrowResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	ps.Subquery = &fakePrimitive{results: []*sqltypes.Result{narrowResult}}
	ps.RowResults = []string{"sq_0", "sq_1"}
	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "VT13001: [BUG] subquery returned 1 columns for a row of 2")

	ps.Subquery = &fakePrimitive{results: []*sqltypes.Result{narrowResult}}
	ps.Opcode = PulloutIn
	ps.HasValues = "has_values"
	ps.RowResults = nil
	ps.RowColumns = 2
	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "VT13001: [BUG] subquery returned 1 columns for a row of 2")
}

func TestPulloutSubqueryInNotinGood(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
		"2",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutIn,
		SubqueryResult: "sq",
		HasValues:      "has_values",
		Subquery:       sfp,
		Outer:          ufp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v sq: %v false`, sqltypes.Int64BindVariable(1), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("1")}, {Type: querypb.Type_INT64, Value: []byte("2")}}})})

	// Test the NOT IN case just once even though it's common code.
	sfp.rewind()
	ufp.rewind()
	ps.Opcode = PulloutNotIn
	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v sq: %v false`, sqltypes.Int64BindVariable(1), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("1")}, {Type: querypb.Type_INT64, Value: []byte("2")}}})})

	// Rows of more than one column are bound as a list of tuples, leaving out any extra columns the subquery
	// returns.
	rowResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|weight_string(col2)",
			"int64|varchar|varbinary",
		),
		"1|a|A",
		"2|b|B",
	)
	sfp = &fakePrimitive{
		results: []*sqltypes.Result{rowResult},
	}
	ufp.rewind()
	ps.Subquery = sfp
	ps.RowColumns = 2
	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	rows := &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
		sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarChar("a")}),
		sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(2), sqltypes.NewVarChar("b")}),
	}}
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v sq: %v false`, sqltypes.Int64BindVariable(1), rows)})
}

func TestPulloutSubqueryInNone(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutIn,
		SubqueryResult: "sq",
		HasValues:      "has_values",
		Subquery:       sfp,
		Outer:          ufp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v sq: %v false`, sqltypes.Int64BindVariable(0), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}})})

	// For rows of more than one column, the placeholder is a tuple as wide as the row: MySQL rejects anything
	// else, even when the has-values guard is false.
	sfp.rewind()
	ufp.rewind()
	ps.RowColumns = 2
	_, err = ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	placeholder := &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
		sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(0), sqltypes.NewInt64(0)}),
	}}
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v sq: %v false`, sqltypes.Int64BindVariable(0), placeholder)})
}

func TestPulloutSubqueryExists(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:    PulloutExists,
		HasValues: "has_values",
		Subquery:  sfp,
		Outer:     ufp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v false`, sqltypes.Int64BindVariable(1))})
}

func TestPulloutSubqueryExistsNone(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:    PulloutExists,
		HasValues: "has_values",
		Subquery:  sfp,
		Outer:     ufp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute has_values: %v false`, sqltypes.Int64BindVariable(0))})
}

func TestPulloutSubqueryError(t *testing.T) {
	sfp := &fakePrimitive{
		sendErr: errors.New("err"),
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutExists,
		SubqueryResult: "sq",
		Subquery:       sfp,
	}

	_, err := ps.TryExecute(t.Context(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "err")
}

func TestPulloutSubqueryStream(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	underlyingResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col",
			"int64",
		),
		"0",
	)
	ufp := &fakePrimitive{
		results: []*sqltypes.Result{underlyingResult},
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
		Outer:          ufp,
	}

	result, err := wrapStreamExecute(ps, &noopVCursor{}, bindVars, true)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v false`, sqltypes.Int64BindVariable(1))})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`StreamExecute aa: %v sq: %v true`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(1))})
	expectResult(t, result, underlyingResult)
}

func TestPulloutSubqueryGetFields(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		HasValues:      "has_values",
		Outer:          ufp,
	}

	_, err := ps.GetFields(t.Context(), nil, bindVars)
	require.NoError(t, err)
	ufp.ExpectLog(t, []string{
		fmt.Sprintf(`GetFields aa: %v sq: `, sqltypes.Int64BindVariable(1)),
		fmt.Sprintf(`Execute aa: %v sq:  true`, sqltypes.Int64BindVariable(1)),
	})

	ufp.rewind()
	ps.Opcode = PulloutIn
	_, err = ps.GetFields(t.Context(), nil, bindVars)
	require.NoError(t, err)
	ufp.ExpectLog(t, []string{
		fmt.Sprintf(`GetFields aa: %v has_values: %v sq: %v`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(0), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}}),
		fmt.Sprintf(`Execute aa: %v has_values: %v sq: %v true`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(0), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}}),
	})

	ufp.rewind()
	ps.Opcode = PulloutNotIn
	_, err = ps.GetFields(t.Context(), nil, bindVars)
	require.NoError(t, err)
	ufp.ExpectLog(t, []string{
		fmt.Sprintf(`GetFields aa: %v has_values: %v sq: %v`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(0), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}}),
		fmt.Sprintf(`Execute aa: %v has_values: %v sq: %v true`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(0), &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}}),
	})

	ufp.rewind()
	ps.Opcode = PulloutExists
	_, err = ps.GetFields(t.Context(), nil, bindVars)
	require.NoError(t, err)
	ufp.ExpectLog(t, []string{
		fmt.Sprintf(`GetFields aa: %v has_values: %v`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(0)),
		fmt.Sprintf(`Execute aa: %v has_values: %v true`, sqltypes.Int64BindVariable(1), sqltypes.Int64BindVariable(0)),
	})
}
