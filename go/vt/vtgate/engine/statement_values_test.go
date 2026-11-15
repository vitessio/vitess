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

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// latin1Result is a result with an INT and a latin1_swedish_ci VARCHAR(10)
// with 'Müller' as a tablet with --queryserver-config-raw-result-charset
// returns it: 4D FC 6C 6C 65 72 with the collation 8.
func latin1Result() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int64, Charset: 63},
			{Name: "l", Type: sqltypes.VarChar, Charset: 8, ColumnLength: 10},
		},
		Rows: []sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller"))}},
	}
}

// TestJoinStatementValues checks the bind variable that a join makes of a
// value of a left row for its right side. With --convert-result-charset the
// value of a latin1 column is latin1 in the row, and has to be utf8mb4, 4D C3
// BC 6C 6C 65 72, in the bind variable, which goes into a statement. The row
// that the join returns keeps the value in latin1, with its field. Without the
// flag the value is the bind variable as it is.
func TestJoinStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				leftPrim := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
				rightPrim := &fakePrimitive{results: []*sqltypes.Result{
					sqltypes.MakeTestResult(sqltypes.MakeTestFields("col4", "int64"), "4"),
				}}
				jn := &Join{
					Opcode: InnerJoin,
					Left:   leftPrim,
					Right:  rightPrim,
					Cols:   []int{-2, 1},
					Vars:   map[string]int{"bv": 1},
				}
				vcursor := &noopVCursor{convertResultCharset: convert}

				var r *sqltypes.Result
				var err error
				if stream {
					r, err = wrapStreamExecute(jn, vcursor, map[string]*querypb.BindVariable{}, true)
				} else {
					r, err = jn.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true)
				}
				require.NoError(t, err)

				want := "M\xFCller"
				if convert {
					want = "Müller"
				}
				method := "Execute"
				if stream {
					method = "StreamExecute"
				}
				rightPrim.ExpectLog(t, []string{
					fmt.Sprintf(`%s %v true`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(want)})),
				})

				require.Len(t, r.Rows, 1)
				assert.Equal(t, uint32(8), r.Fields[0].Charset)
				assert.Equal(t, "M\xFCller", r.Rows[0][0].ToString())
			})
		}
	}
}

// TestSemiJoinStatementValues checks the bind variable that a semi-join makes
// of a value of a left row for its subquery. With --convert-result-charset the
// value of a latin1 column is utf8mb4 in the bind variable, and the left row
// that the semi-join returns keeps it in latin1. The semi-join is asked for no
// fields, so that it has to ask for the fields of its left side itself.
func TestSemiJoinStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				leftPrim := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
				rightPrim := &fakePrimitive{results: []*sqltypes.Result{
					sqltypes.MakeTestResult(sqltypes.MakeTestFields("col4", "int64"), "4"),
				}}
				jn := &SemiJoin{Left: leftPrim, Right: rightPrim, Vars: map[string]int{"bv": 1}}
				vcursor := &noopVCursor{convertResultCharset: convert}

				var r *sqltypes.Result
				var err error
				if stream {
					r, err = wrapStreamExecute(jn, vcursor, map[string]*querypb.BindVariable{}, false)
				} else {
					r, err = jn.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
				}
				require.NoError(t, err)

				method := "Execute"
				if stream {
					method = "StreamExecute"
				}
				rightPrim.ExpectLog(t, []string{
					fmt.Sprintf(`%s %v false`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(wantStatementValue(convert))})),
				})

				require.Len(t, r.Rows, 1)
				assert.Equal(t, "M\xFCller", r.Rows[0][1].ToString())
			})
		}
	}
}

// wantStatementValue is 'Müller' of latin1Result as it has to be in a bind
// variable: utf8mb4 with --convert-result-charset, and the bytes of the row
// without it.
func wantStatementValue(convert bool) string {
	if convert {
		return "Müller"
	}
	return "M\xFCller"
}

// TestRecurseCTEStatementValues checks the bind variables that a recursive CTE
// makes of the values of the rows of its seed and of each recursion, for the
// next recursion. With --convert-result-charset 'Müller' of the seed and 'é'
// of the first recursion, both of a latin1 column, are utf8mb4 in the bind
// variable, and the rows that the CTE returns keep them in latin1. The CTE is
// asked for no fields, so that it has to ask for the fields itself.
func TestRecurseCTEStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				seed := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
				recursion := latin1Result()
				recursion.Rows = []sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xE9"))}}
				end := latin1Result()
				end.Rows = nil
				term := &fakePrimitive{results: []*sqltypes.Result{recursion, end}}
				cte := &RecurseCTE{Seed: seed, Term: term, Vars: map[string]int{"bv": 1}}
				vcursor := &noopVCursor{convertResultCharset: convert}

				var r *sqltypes.Result
				var err error
				method := "Execute"
				if stream {
					method = "StreamExecute"
					r, err = wrapStreamExecute(cte, vcursor, map[string]*querypb.BindVariable{}, false)
				} else {
					r, err = cte.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
				}
				require.NoError(t, err)

				first, second := "M\xFCller", "\xE9"
				if convert {
					first, second = "Müller", "é"
				}
				term.ExpectLog(t, []string{
					fmt.Sprintf(`%s %v %v`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(first)}), convert),
					fmt.Sprintf(`%s %v %v`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(second)}), convert),
				})

				require.Len(t, r.Rows, 2)
				assert.Equal(t, "M\xFCller", r.Rows[0][1].ToString())
				assert.Equal(t, "\xE9", r.Rows[1][1].ToString())
			})
		}
	}
}

// TestValuesJoinStatementValues checks the rows of the VALUES that a values
// join makes of its left rows for its right side. With --convert-result-charset
// the value of a latin1 column is utf8mb4 in the row of the VALUES, and the
// row that the join returns keeps it in latin1. The join is asked for no
// fields, so that it has to ask for the fields of its left side itself.
func TestValuesJoinStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			leftPrim := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
			rightPrim := &fakePrimitive{results: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("x|id", "int64|int64"), "4|0"),
			}}
			jv := &ValuesJoin{
				Left:              leftPrim,
				Right:             rightPrim,
				Vars:              []int{1},
				RowConstructorArg: "v",
				Cols:              []int{-2, 1},
				ColNames:          []string{"l", "x"},
			}
			vcursor := &noopVCursor{convertResultCharset: convert}

			bindVars := map[string]*querypb.BindVariable{}
			r, err := jv.TryExecute(t.Context(), vcursor, bindVars, false)
			require.NoError(t, err)

			want := sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(0), sqltypes.NewVarChar(wantStatementValue(convert))})
			require.Len(t, bindVars["v"].Values, 1)
			assert.True(t, proto.Equal(want, bindVars["v"].Values[0]), "got %v", bindVars["v"].Values[0])

			require.Len(t, r.Rows, 1)
			assert.Equal(t, "M\xFCller", r.Rows[0][0].ToString())
		})
	}
}

// TestUncorrelatedSubqueryStatementValues checks the bind variables that a
// pulled-out subquery makes of the values of its rows for the outer query: a
// value, the columns of a row, a list of values and a list of rows. With
// --convert-result-charset 'Müller' of a latin1 column is utf8mb4 in each of
// them. Without the flag the values are the bind variables as they are.
func TestUncorrelatedSubqueryStatementValues(t *testing.T) {
	// The subquery returns the latin1 column first, and the INT second.
	subqueryResult := func() *sqltypes.Result {
		r := latin1Result()
		r.Fields[0], r.Fields[1] = r.Fields[1], r.Fields[0]
		r.Rows[0][0], r.Rows[0][1] = r.Rows[0][1], r.Rows[0][0]
		return r
	}
	for _, convert := range []bool{true, false} {
		want := wantStatementValue(convert)
		tuple := func(values ...*querypb.Value) *querypb.BindVariable {
			return &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: values}
		}
		tests := []struct {
			name string
			ps   *UncorrelatedSubquery
			want map[string]*querypb.BindVariable
		}{{
			name: "value",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutValue, SubqueryResult: "sq"},
			want: map[string]*querypb.BindVariable{"sq": sqltypes.StringBindVariable(want)},
		}, {
			name: "row",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutValue, SubqueryResult: "sq", RowResults: []string{"sq_0", "sq_1"}},
			want: map[string]*querypb.BindVariable{"sq_0": sqltypes.StringBindVariable(want), "sq_1": sqltypes.Int64BindVariable(1)},
		}, {
			name: "in",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutIn, SubqueryResult: "sq", HasValues: "has_values"},
			want: map[string]*querypb.BindVariable{
				"has_values": sqltypes.Int64BindVariable(1),
				"sq":         tuple(sqltypes.ValueToProto(sqltypes.NewVarChar(want))),
			},
		}, {
			name: "in rows",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutIn, SubqueryResult: "sq", HasValues: "has_values", RowColumns: 2},
			want: map[string]*querypb.BindVariable{
				"has_values": sqltypes.Int64BindVariable(1),
				"sq":         tuple(sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewVarChar(want), sqltypes.NewInt64(1)})),
			},
		}}
		for _, tc := range tests {
			t.Run(fmt.Sprintf("convert %v, %s", convert, tc.name), func(t *testing.T) {
				outer := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("x", "int64"), "1")}}
				tc.ps.Subquery = &fakePrimitive{results: []*sqltypes.Result{subqueryResult()}}
				tc.ps.Outer = outer
				vcursor := &noopVCursor{convertResultCharset: convert}

				_, err := tc.ps.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
				require.NoError(t, err)
				outer.ExpectLog(t, []string{fmt.Sprintf("Execute %v false", deprecatedPrintBindVars(tc.want))})
			})
		}
	}
}

// TestDMLWithInputStatementValues checks the bind variables that a DML with
// input makes of the values of the rows of its input for its DMLs: the list
// of the values of the rows, and the bind variables of the values of one row.
// With --convert-result-charset 'Müller' of a latin1 column is utf8mb4 in both.
func TestDMLWithInputStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		want := sqltypes.NewVarChar(wantStatementValue(convert))
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			literal := &fakePrimitive{results: []*sqltypes.Result{{RowsAffected: 1}}}
			nonLiteral := &fakePrimitive{results: []*sqltypes.Result{{RowsAffected: 1}}}
			dml := &DMLWithInput{
				Input:      &fakePrimitive{results: []*sqltypes.Result{latin1Result()}},
				DMLs:       []Primitive{literal, nonLiteral},
				OutputCols: [][]int{{1}, {0}},
				BVList:     []map[string]int{nil, {"bv": 1}},
			}
			vcursor := &loggingVCursor{convertResultCharset: convert}

			_, err := dml.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			literal.ExpectLog(t, []string{fmt.Sprintf("Execute %v false", deprecatedPrintBindVars(map[string]*querypb.BindVariable{
				DmlVals: {Type: querypb.Type_TUPLE, Values: []*querypb.Value{sqltypes.ValueToProto(want)}},
			}))})
			nonLiteral.ExpectLog(t, []string{fmt.Sprintf("Execute %v false", deprecatedPrintBindVars(map[string]*querypb.BindVariable{
				DmlVals: {Type: querypb.Type_TUPLE, Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(1))}},
				"bv":    sqltypes.ValueBindVariable(want),
			}))})
		})
	}
}

// TestFkCascadeStatementValues checks the bind variables that a foreign key
// cascade makes of the values of the rows of its selection for its children:
// the list of the values of the foreign key, and the new value of an update
// that is not a literal. With --convert-result-charset 'Müller' and 'é' of
// latin1 columns are utf8mb4 in them.
func TestFkCascadeStatementValues(t *testing.T) {
	selection := func() *sqltypes.Result {
		return &sqltypes.Result{
			Fields: []*querypb.Field{
				{Name: "l", Type: sqltypes.VarChar, Charset: 8, ColumnLength: 10},
				{Name: "unchanged", Type: sqltypes.Int64, Charset: 63},
				{Name: "new_l", Type: sqltypes.VarChar, Charset: 8, ColumnLength: 10},
			},
			Rows: []sqltypes.Row{{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")),
				sqltypes.NewInt64(0),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xE9")),
			}},
		}
	}
	for _, convert := range []bool{true, false} {
		first, second := sqltypes.NewVarChar("M\xFCller"), sqltypes.NewVarChar("\xE9")
		if convert {
			first, second = sqltypes.NewVarChar("Müller"), sqltypes.NewVarChar("é")
		}
		list := &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{sqltypes.TupleToProto([]sqltypes.Value{first})}}
		t.Run(fmt.Sprintf("convert %v, literal", convert), func(t *testing.T) {
			child := &fakePrimitive{results: []*sqltypes.Result{{}}}
			fkc := &FkCascade{
				Selection: &fakePrimitive{results: []*sqltypes.Result{selection()}},
				Children:  []*FkChild{{BVName: "__vals", Cols: []int{0}, Exec: child}},
				Parent:    &fakePrimitive{results: []*sqltypes.Result{{}}},
			}
			_, err := fkc.TryExecute(t.Context(), &noopVCursor{convertResultCharset: convert}, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			child.ExpectLog(t, []string{fmt.Sprintf("Execute %v false", deprecatedPrintBindVars(map[string]*querypb.BindVariable{"__vals": list}))})
		})
		t.Run(fmt.Sprintf("convert %v, non-literal", convert), func(t *testing.T) {
			child := &fakePrimitive{results: []*sqltypes.Result{{}}}
			fkc := &FkCascade{
				Selection: &fakePrimitive{results: []*sqltypes.Result{selection()}},
				Children: []*FkChild{{
					BVName:         "__vals",
					Cols:           []int{0},
					NonLiteralInfo: []NonLiteralUpdateInfo{{CompExprCol: 1, UpdateExprCol: 2, UpdateExprBvName: "fkc_upd"}},
					Exec:           child,
				}},
				Parent: &fakePrimitive{results: []*sqltypes.Result{{}}},
			}
			_, err := fkc.TryExecute(t.Context(), &noopVCursor{convertResultCharset: convert}, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			child.ExpectLog(t, []string{fmt.Sprintf("Execute %v false", deprecatedPrintBindVars(map[string]*querypb.BindVariable{
				"__vals":  list,
				"fkc_upd": sqltypes.ValueBindVariable(second),
			}))})
		})
	}
}

// TestInsertSelectStatementValues checks the bind variables that an INSERT ...
// SELECT makes of the values of the rows of its select for the insert, and the
// values that it gives to an owned lookup vindex, which inserts them into its
// table. With --convert-result-charset 'Müller' of a latin1 column is utf8mb4
// in both. The select is asked for no fields, so that the insert has to ask
// for them itself.
func TestInsertSelectStatementValues(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {Type: "hash"},
					"lkp": {
						Type:   "lookup",
						Params: map[string]string{"table": "lkp", "from": "l", "to": "keyspace_id"},
						Owner:  "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {ColumnVindexes: []*vschemapb.ColumnVindex{
						{Name: "hash", Columns: []string{"id"}},
						{Name: "lkp", Columns: []string{"l"}},
					}},
				},
			},
			"unsharded": {Tables: map[string]*vschemapb.Table{"t2": {}}},
		},
	}
	vs := vindexes.BuildVSchema(invschema, sqlparser.NewTestParser())

	for _, convert := range []bool{true, false} {
		want := sqltypes.StringBindVariable(wantStatementValue(convert))
		for _, stream := range []bool{false, true} {
			for _, sharded := range []bool{true, false} {
				t.Run(fmt.Sprintf("convert %v, stream %v, sharded %v", convert, stream, sharded), func(t *testing.T) {
					input := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
					var ins *InsertSelect
					if sharded {
						ks := vs.Keyspaces["sharded"]
						ins = newInsertSelect(false, ks.Keyspace, ks.Tables["t1"], "prefix ", nil, [][]int{{0}, {1}}, input)
					} else {
						ks := vs.Keyspaces["unsharded"]
						ins = newInsertSelect(false, ks.Keyspace, ks.Tables["t2"], "prefix ", nil, nil, input)
					}
					vc := newTestVCursor("0")
					if sharded {
						vc = newTestVCursor("-20", "20-")
						vc.shardForKsid = []string{"-20"}
					}
					vc.convertResultCharset = convert

					var err error
					if stream {
						err = ins.TryStreamExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false, func(*sqltypes.Result) error { return nil })
					} else {
						_, err = ins.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
					}
					require.NoError(t, err)

					var lookupInsert, insert string
					for _, line := range vc.log {
						switch {
						case strings.HasPrefix(line, "Execute insert into lkp"):
							lookupInsert = line
						case strings.HasPrefix(line, "ExecuteMultiShard"):
							insert = line
						}
					}
					if sharded {
						assert.Contains(t, lookupInsert, fmt.Sprintf("l_0: %v ", want))
					}
					assert.Contains(t, insert, fmt.Sprintf("_c0_1: %v}", want))
				})
			}
		}
	}
}

// ownedVindexResult is the result of the query of the old values of a row of
// t1 of buildTestVSchema for its owned vindexes, with c3, the column of the
// lookup vindex onecol, a latin1 VARCHAR(10) with 'Müller', and the columns
// that say whether twocol and onecol change.
func ownedVindexResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int64, Charset: 63},
			{Name: "c1", Type: sqltypes.Int64, Charset: 63},
			{Name: "c2", Type: sqltypes.Int64, Charset: 63},
			{Name: "c3", Type: sqltypes.VarChar, Charset: 8, ColumnLength: 10},
			{Name: "twocol", Type: sqltypes.Int64, Charset: 63},
			{Name: "onecol", Type: sqltypes.Int64, Charset: 63},
		},
		Rows: []sqltypes.Row{{
			sqltypes.NewInt64(1), sqltypes.NewInt64(4), sqltypes.NewInt64(5),
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")),
			sqltypes.NewInt64(0), sqltypes.NewInt64(0),
		}},
	}
}

// TestDeleteStatementValues checks the values of a row that a DELETE gives to
// an owned lookup vindex, which deletes them from its table. With
// --convert-result-charset 'Müller' of a latin1 column is utf8mb4 in the
// statement of the vindex.
func TestDeleteStatementValues(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	for _, convert := range []bool{true, false} {
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			del := &Delete{DML: &DML{
				RoutingParameters: &RoutingParameters{
					Opcode:   Equal,
					Keyspace: ks.Keyspace,
					Vindex:   ks.Vindexes["hash"],
					Values:   []evalengine.Expr{evalengine.NewLiteralInt(1)},
				},
				Query:            "dummy_delete",
				TableNames:       []string{ks.Tables["t1"].Name.String()},
				Vindexes:         ks.Tables["t1"].Owned,
				OwnedVindexQuery: "dummy_subquery",
				KsidVindex:       ks.Vindexes["hash"],
				KsidLength:       1,
			}}
			vc := newTestVCursor("-20", "20-")
			vc.convertResultCharset = convert
			vc.results = []*sqltypes.Result{ownedVindexResult()}

			_, err := del.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			assert.Contains(t, vc.log, fmt.Sprintf(`Execute delete from lkp1 where from = :from and toc = :toc from: %v toc: %v true`,
				sqltypes.StringBindVariable(wantStatementValue(convert)), &querypb.BindVariable{Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")}))
		})
	}
}

// TestUpdateStatementValues checks the old values of a row that an UPDATE
// gives to an owned lookup vindex that changes, which deletes them from its
// table and inserts the new ones. With --convert-result-charset 'Müller' of a
// latin1 column is utf8mb4 in the statement of the vindex. The new value, 'é',
// is a literal and utf8mb4 either way.
func TestUpdateStatementValues(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	for _, convert := range []bool{true, false} {
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			upd := &Update{
				DML: &DML{
					RoutingParameters: &RoutingParameters{
						Opcode:   Equal,
						Keyspace: ks.Keyspace,
						Vindex:   ks.Vindexes["hash"],
						Values:   []evalengine.Expr{evalengine.NewLiteralInt(1)},
					},
					Query:            "dummy_update",
					TableNames:       []string{ks.Tables["t1"].Name.String()},
					Vindexes:         ks.Tables["t1"].Owned,
					OwnedVindexQuery: "dummy_subquery",
					KsidVindex:       ks.Vindexes["hash"],
					KsidLength:       1,
				},
				ChangedVindexValues: map[string]*VindexValues{
					"onecol": {
						EvalExprMap: map[string]evalengine.Expr{
							"c3": evalengine.NewLiteralString([]byte("é"), collations.SystemCollation),
						},
						Offset: 5,
					},
				},
			}
			vc := newTestVCursor("-20", "20-")
			vc.convertResultCharset = convert
			vc.results = []*sqltypes.Result{ownedVindexResult()}

			_, err := upd.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			ksid := &querypb.BindVariable{Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")}
			assert.Contains(t, vc.log, fmt.Sprintf(`Execute delete from lkp1 where from = :from and toc = :toc from: %v toc: %v true`,
				sqltypes.StringBindVariable(wantStatementValue(convert)), ksid))
			assert.Contains(t, vc.log, fmt.Sprintf(`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: %v toc_0: %v true`,
				sqltypes.StringBindVariable("é"), ksid))
		})
	}
}

// TestVindexLookupStatementValues checks how a lookup by a vindex with
// batch_lookup matches the rows of the lookup query to the values it looks up:
// by the value of their first column, which is compared with a value of the
// query, in the connection character set. With --convert-result-charset the
// lookup table returns 'Müller' of a latin1 column in latin1, and it is
// converted to match 'Müller' of the query, so that the row is routed to its
// keyspace ID. Without the flag the tablet returns utf8mb4.
func TestVindexLookupStatementValues(t *testing.T) {
	vdx, err := vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table":        "lkp",
		"from":         "l",
		"to":           "keyspace_id",
		"batch_lookup": "true",
	})
	require.NoError(t, err)
	planable := vdx.(vindexes.LookupPlanable)
	_, args := planable.Query()

	for _, convert := range []bool{true, false} {
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			from := []byte("Müller")
			charset := uint32(255)
			if convert {
				from = []byte("M\xFCller")
				charset = 8
			}
			lookup := &fakePrimitive{results: []*sqltypes.Result{{
				Fields: []*querypb.Field{
					{Name: "l", Type: sqltypes.VarChar, Charset: charset, ColumnLength: 10},
					{Name: "keyspace_id", Type: sqltypes.VarBinary, Charset: 63},
				},
				Rows: []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, from), sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("\x10"))}},
			}}}
			vl := &VindexLookup{
				Opcode:    EqualUnique,
				Keyspace:  ks,
				Vindex:    planable,
				Arguments: args,
				Values:    []evalengine.Expr{evalengine.NewLiteralString([]byte("Müller"), collations.SystemCollation)},
				Lookup:    lookup,
				SendTo:    NewRoute(ByDestination, ks, "dummy_select", "dummy_select_field"),
			}
			vc := &loggingVCursor{convertResultCharset: convert, shards: []string{"-20", "20-"}, results: []*sqltypes.Result{{}}}

			_, err := vl.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, []string{
				fmt.Sprintf(`ResolveDestinations ks [%v] Destinations:DestinationKeyspaceID(10)`, sqltypes.StringBindVariable("Müller")),
				`ExecuteMultiShard ks.-20: dummy_select {} false false`,
			})
		})
	}
}

// TestSysVarReservedConnStatementValues checks the value of a system variable
// that a shard evaluates, and that goes into the text of a SET statement and
// into the session, which replays it in a SET statement. With --convert-result-charset a tablet returns the value
// of _latin1 x'E9' as E9, with the collation latin1_swedish_ci, as MySQL 8.0.46
// and 8.4.13 send it with character_set_results = NULL, and the value has to be
// 'é' in utf8mb4 in the statement.
func TestSysVarReservedConnStatementValues(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "ks", Sharded: true}
	evaluated := func() *sqltypes.Result {
		return &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "v", Type: sqltypes.VarChar, Charset: 8, ColumnLength: 1}},
			Rows:   []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xE9"))}},
		}
	}
	for _, convert := range []bool{true, false} {
		want := "'\xE9'"
		if convert {
			want = "'é'"
		}
		for _, targeted := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, targeted %v", convert, targeted), func(t *testing.T) {
				op := &SysVarReservedConn{Name: "x", Keyspace: ks, Expr: "_latin1 x'E9'"}
				if targeted {
					op.TargetDestination = key.DestinationAnyShard{}
				}
				set := &Set{Ops: []SetOp{op}, Input: &SingleRow{}}
				vc := &loggingVCursor{
					convertResultCharset: convert,
					shards:               []string{"-20"},
					results:              []*sqltypes.Result{evaluated(), {}},
					shardSession:         []*srvtopo.ResolvedShard{{Target: &querypb.Target{Keyspace: "ks", Shard: "-20"}}},
				}
				_, err := set.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
				require.NoError(t, err)
				// An untargeted SET sends its expression to the shards, and the
				// session replays the value on the next reserved connections.
				if targeted {
					assert.Contains(t, vc.log, fmt.Sprintf("ExecuteMultiShard ks.-20: set x = %s {} false false", want))
				}
				assert.Contains(t, vc.log, fmt.Sprintf("SysVar set with (x,%s)", want))
			})
		}
	}
}

// TestSetUserDefinedVariableStatementValues checks the value that SET @x =
// (SELECT l FROM t) stores in a user-defined variable, which becomes a bind
// variable of the statements that use @x. The shard evaluates the subquery,
// and the evaluation engine takes its value as an untyped column, in the
// connection character set. With --convert-result-charset 'Müller' of a
// latin1 column has to be stored in utf8mb4.
func TestSetUserDefinedVariableStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			input := latin1Result()
			input.Fields = input.Fields[1:]
			input.Rows[0] = input.Rows[0][1:]
			set := &Set{
				Ops:   []SetOp{&UserDefinedVariable{Name: "x", Expr: evalengine.NewColumn(0, evalengine.Type{}, nil)}},
				Input: &fakePrimitive{results: []*sqltypes.Result{input}},
			}
			vc := &loggingVCursor{convertResultCharset: convert}

			_, err := set.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, []string{fmt.Sprintf("UDV set with (x,%v)", sqltypes.NewVarChar(wantStatementValue(convert)))})
		})
	}
}
