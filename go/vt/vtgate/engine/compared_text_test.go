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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// The tests of this file give the primitives of vtgate the rows of
//
//	create table sorted (id int primary key, l varchar(10) character set latin1 collate latin1_german2_ci);
//	insert into sorted values (1, 'é'), (2, 'd'), (3, 'f'), (4, 'Müller'), (5, 'MUELLER'), (6, 'z');
//
// as a tablet sends them, in utf8mb4 and with a utf8mb4 collation in the
// field, and as they are in the column, in latin1 and with the collation of
// the column in the field. The plan has the collation of the column for both.
// The expected results are the ones of MySQL 8.0.46 for that table: in
// latin1_german2_ci 'é' equals 'e' and 'Müller' equals 'MUELLER'.
//
//	select id from sorted order by l, id                  2, 1, 3, 4, 5, 6
//	select count(*) from sorted group by l order by l     1, 1, 1, 2, 1
//	select count(distinct l), min(l), max(l) from sorted  5, 'd', 'z'
//	select id from sorted where l = 'mueller'             4, 5
//	select id from sorted where l < 'e'                   2
//	select hex(l), upper(l) from sorted where id = 4      4DFC6C6C6572, 'MÜLLER'
//	select a.id, b.id from sorted a join sorted b on a.l = b.l
//	    (1, 1), (2, 2), (3, 3), (4, 4), (4, 5), (5, 4), (5, 5), (6, 6)
//
// The utf8mb4 bytes of 'é', C3 A9, are the latin1 text 'Ã©', which sorts
// before 'd', and those of 'Müller' do not equal 'MUELLER'.

var comparedTextCollation = collations.MySQL8().LookupByName("latin1_german2_ci")

// comparedTextRows returns the rows of the table with these ids, in utf8mb4
// when raw is false and in latin1 when it is true.
func comparedTextRows(raw bool, ids ...int) *sqltypes.Result {
	utf8 := map[int]string{1: "é", 2: "d", 3: "f", 4: "Müller", 5: "MUELLER", 6: "z"}
	latin1 := map[int]string{1: "\xe9", 2: "d", 3: "f", 4: "M\xfcller", 5: "MUELLER", 6: "z"}

	text, collation := utf8, collations.MySQL8().DefaultConnectionCharset()
	if raw {
		text, collation = latin1, comparedTextCollation
	}
	result := &sqltypes.Result{Fields: []*querypb.Field{
		{Name: "id", Type: sqltypes.Int32, Charset: uint32(collations.CollationBinaryID)},
		{Name: "l", Type: sqltypes.VarChar, Charset: uint32(collation), ColumnLength: 10},
	}}
	for _, id := range ids {
		result.Rows = append(result.Rows, sqltypes.Row{sqltypes.NewInt32(int32(id)), sqltypes.MakeTrusted(sqltypes.VarChar, []byte(text[id]))})
	}
	return result
}

// comparedTextRun runs a primitive, with Execute and with StreamExecute, for
// the rows in utf8mb4 and in latin1. It asks for no fields: the primitive has
// to ask for the ones that it needs itself.
func comparedTextRun(t *testing.T, primitive func(raw bool) Primitive, check func(t *testing.T, rows []sqltypes.Row)) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw %v, execute", raw), func(t *testing.T) {
			qr, err := primitive(raw).TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			check(t, qr.Rows)
		})
		t.Run(fmt.Sprintf("raw %v, stream", raw), func(t *testing.T) {
			var rows []sqltypes.Row
			err := primitive(raw).TryStreamExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false, func(qr *sqltypes.Result) error {
				rows = append(rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			check(t, rows)
		})
	}
}

// comparedTextColumn returns the values of one column of rows.
func comparedTextColumn(rows []sqltypes.Row, col int) []string {
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, row[col].ToString())
	}
	return values
}

func comparedTextOrder() evalengine.Comparison {
	return evalengine.Comparison{
		{Col: 1, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarChar, comparedTextCollation), CollationEnv: collations.MySQL8()},
		{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.Int32, collations.CollationBinaryID), CollationEnv: collations.MySQL8()},
	}
}

func TestComparedTextMemorySort(t *testing.T) {
	comparedTextRun(t, func(raw bool) Primitive {
		return &MemorySort{
			OrderBy: comparedTextOrder(),
			Input:   &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 1, 2, 3, 4, 5, 6)}},
		}
	}, func(t *testing.T, rows []sqltypes.Row) {
		assert.Equal(t, []string{"2", "1", "3", "4", "5", "6"}, comparedTextColumn(rows, 0))
	})
}

// TestComparedTextMergeSort merges two shards, each sorted by MySQL. The row
// with 'é' is the first of its shard, and comes after the 'd' of the other.
func TestComparedTextMergeSort(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw %v", raw), func(t *testing.T) {
			shard := func(ids ...int) StreamExecutor {
				rows := comparedTextRows(raw, ids...)
				return &shardResult{results: []*sqltypes.Result{{Fields: rows.Fields}, {Rows: rows.Rows}}}
			}
			ms := MergeSort{
				Primitives: []StreamExecutor{shard(2, 3, 6), shard(1, 4, 5)},
				OrderBy:    comparedTextOrder(),
			}
			var rows []sqltypes.Row
			err := ms.TryStreamExecute(t.Context(), &noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
				rows = append(rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"2", "1", "3", "4", "5", "6"}, comparedTextColumn(rows, 0))
		})
	}
}

// TestComparedTextGroupBy groups rows that MySQL sorted. The rows with
// 'Müller' and with 'MUELLER' are one group.
func TestComparedTextGroupBy(t *testing.T) {
	groupBy := func() []*GroupByParams {
		return []*GroupByParams{{KeyCol: 1, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarChar, comparedTextCollation), CollationEnv: collations.MySQL8()}}
	}
	t.Run("with an aggregation", func(t *testing.T) {
		comparedTextRun(t, func(raw bool) Primitive {
			return &OrderedAggregate{
				Aggregates:  []*AggregateParams{NewAggregateParam(opcode.AggregateCountStar, 0, nil, "count(*)", collations.MySQL8())},
				GroupByKeys: groupBy(),
				Input:       &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 2, 1, 3, 4, 5, 6)}},
			}
		}, func(t *testing.T, rows []sqltypes.Row) {
			assert.Equal(t, []string{"1", "1", "1", "2", "1"}, comparedTextColumn(rows, 0))
		})
	})
	t.Run("without an aggregation", func(t *testing.T) {
		comparedTextRun(t, func(raw bool) Primitive {
			return &OrderedAggregate{
				GroupByKeys: groupBy(),
				Input:       &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 2, 1, 3, 4, 5, 6)}},
			}
		}, func(t *testing.T, rows []sqltypes.Row) {
			assert.Equal(t, []string{"2", "1", "3", "5", "6"}, comparedTextColumn(rows, 0))
		})
	})
}

// TestComparedTextAggregates counts the distinct values of rows that MySQL
// sorted, and finds the smallest and the largest value. 'é' is not smaller
// than 'd': MIN(l) of the table is 'd', and MAX(l) of the rows 1 and 2 is 'é'.
func TestComparedTextAggregates(t *testing.T) {
	tests := []struct {
		name   string
		code   opcode.AggregateOpcode
		ids    []int
		values []string
	}{
		{"count distinct", opcode.AggregateCountDistinct, []int{2, 1, 3, 4, 5, 6}, []string{"5"}},
		{"min", opcode.AggregateMin, []int{1, 2, 3, 4, 5, 6}, []string{"d"}},
		// The value is returned as it came, in utf8mb4 or in latin1.
		{"max", opcode.AggregateMax, []int{1, 2}, []string{"é", "\xe9"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			comparedTextRun(t, func(raw bool) Primitive {
				param := NewAggregateParam(tc.code, 1, nil, "", collations.MySQL8())
				param.Type = evalengine.NewType(sqltypes.VarChar, comparedTextCollation)
				return &ScalarAggregate{
					Aggregates: []*AggregateParams{param},
					Input:      &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, tc.ids...)}},
				}
			}, func(t *testing.T, rows []sqltypes.Row) {
				require.Len(t, rows, 1)
				assert.Contains(t, tc.values, rows[0][1].ToString())
			})
		})
	}
}

func TestComparedTextDistinct(t *testing.T) {
	comparedTextRun(t, func(raw bool) Primitive {
		return &Distinct{
			CheckCols: []CheckCol{{Col: 1, Type: evalengine.NewType(sqltypes.VarChar, comparedTextCollation), CollationEnv: collations.MySQL8()}},
			Source:    &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 1, 2, 3, 4, 5, 6)}},
		}
	}, func(t *testing.T, rows []sqltypes.Row) {
		assert.Equal(t, []string{"1", "2", "3", "4", "6"}, comparedTextColumn(rows, 0))
	})
}

// TestComparedTextHashJoin joins the table with itself. Each side can be in
// another character set.
func TestComparedTextHashJoin(t *testing.T) {
	for _, rawLeft := range []bool{false, true} {
		t.Run(fmt.Sprintf("left raw %v", rawLeft), func(t *testing.T) {
			comparedTextRun(t, func(raw bool) Primitive {
				return &HashJoin{
					Opcode:         InnerJoin,
					Left:           &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(rawLeft, 1, 2, 3, 4, 5, 6)}},
					Right:          &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 1, 2, 3, 4, 5, 6)}},
					Cols:           []int{-1, 1},
					LHSKey:         1,
					RHSKey:         1,
					Collation:      comparedTextCollation,
					ComparisonType: sqltypes.VarChar,
					CollationEnv:   collations.MySQL8(),
				}
			}, func(t *testing.T, rows []sqltypes.Row) {
				var pairs []string
				for _, row := range rows {
					pairs = append(pairs, row[0].ToString()+"-"+row[1].ToString())
				}
				assert.ElementsMatch(t, []string{"1-1", "2-2", "3-3", "4-4", "4-5", "5-4", "5-5", "6-6"}, pairs)
			})
		})
	}
}

// TestComparedTextFilter filters with a predicate on the column, which the
// planner typed with the collation of the column.
func TestComparedTextFilter(t *testing.T) {
	predicate := func(t *testing.T, sql string) evalengine.Expr {
		expr, err := sqlparser.NewTestParser().ParseExpr(sql)
		require.NoError(t, err)
		translated, err := evalengine.Translate(expr, &evalengine.Config{
			Environment: vtenv.NewTestEnv(),
			Collation:   collations.MySQL8().DefaultConnectionCharset(),
			ResolveColumn: func(*sqlparser.ColName) (int, error) {
				return 1, nil
			},
			ResolveType: func(sqlparser.Expr) (evalengine.Type, bool) {
				return evalengine.NewType(sqltypes.VarChar, comparedTextCollation), true
			},
		})
		require.NoError(t, err)
		return translated
	}
	tests := []struct {
		predicate string
		ids       []string
	}{
		{"l = 'mueller'", []string{"4", "5"}},
		{"l < 'e'", []string{"2"}},
	}
	for _, tc := range tests {
		t.Run(tc.predicate, func(t *testing.T) {
			comparedTextRun(t, func(raw bool) Primitive {
				return &Filter{
					Predicate: predicate(t, tc.predicate),
					Input:     &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 1, 2, 3, 4, 5, 6)}},
				}
			}, func(t *testing.T, rows []sqltypes.Row) {
				assert.Equal(t, tc.ids, comparedTextColumn(rows, 0))
			})
		})
	}
}

// TestComparedTextProjection evaluates expressions of the column, which the
// planner typed with the collation of the column. The values are returned in
// the connection character set, utf8mb4.
func TestComparedTextProjection(t *testing.T) {
	expression := func(t *testing.T, sql string) evalengine.Expr {
		expr, err := sqlparser.NewTestParser().ParseExpr(sql)
		require.NoError(t, err)
		translated, err := evalengine.Translate(expr, &evalengine.Config{
			Environment: vtenv.NewTestEnv(),
			Collation:   collations.MySQL8().DefaultConnectionCharset(),
			ResolveColumn: func(*sqlparser.ColName) (int, error) {
				return 1, nil
			},
			ResolveType: func(sqlparser.Expr) (evalengine.Type, bool) {
				return evalengine.NewType(sqltypes.VarChar, comparedTextCollation), true
			},
		})
		require.NoError(t, err)
		return translated
	}
	tests := []struct {
		expression string
		value      string
	}{
		{"hex(l)", "4DFC6C6C6572"},
		{"upper(l)", "MÜLLER"},
	}
	for _, tc := range tests {
		t.Run(tc.expression, func(t *testing.T) {
			comparedTextRun(t, func(raw bool) Primitive {
				return &Projection{
					Cols:  []string{"e"},
					Exprs: []evalengine.Expr{expression(t, tc.expression)},
					Input: &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(raw, 4)}},
				}
			}, func(t *testing.T, rows []sqltypes.Row) {
				assert.Equal(t, []string{tc.value}, comparedTextColumn(rows, 0))
			})
		})
	}
}

// TestComparedTextAsksForFields checks which primitives ask their input for
// the fields of its rows when they were not asked for them: the ones that
// compare in a collation of another character set than utf8mb4. A sort of a
// utf8mb4 column runs as it did.
func TestComparedTextAsksForFields(t *testing.T) {
	tests := []struct {
		collation  string
		wantfields bool
	}{
		{"latin1_german2_ci", true},
		{"gbk_chinese_ci", true},
		{"utf8mb4_0900_ai_ci", false},
		{"utf8mb4_general_ci", false},
		{"utf8mb3_general_ci", false},
		{"binary", false},
	}
	for _, tc := range tests {
		t.Run(tc.collation, func(t *testing.T) {
			collation := collations.MySQL8().LookupByName(tc.collation)
			order := evalengine.Comparison{{Col: 1, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarChar, collation), CollationEnv: collations.MySQL8()}}
			log := fmt.Sprintf("Execute  %v", tc.wantfields)
			streamLog := fmt.Sprintf("StreamExecute  %v", tc.wantfields)

			input := &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(false, 1, 2)}}
			_, err := (&MemorySort{OrderBy: order, Input: input}).TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			input.ExpectLog(t, []string{log})

			input = &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(false, 1, 2)}}
			distinct := &Distinct{CheckCols: []CheckCol{{Col: 1, Type: evalengine.NewType(sqltypes.VarChar, collation), CollationEnv: collations.MySQL8()}}, Source: input}
			err = distinct.TryStreamExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false, func(*sqltypes.Result) error { return nil })
			require.NoError(t, err)
			input.ExpectLog(t, []string{streamLog})

			expr, err := sqlparser.NewTestParser().ParseExpr("l = 'x'")
			require.NoError(t, err)
			predicate, err := evalengine.Translate(expr, &evalengine.Config{
				Environment:   vtenv.NewTestEnv(),
				Collation:     collations.MySQL8().DefaultConnectionCharset(),
				ResolveColumn: func(*sqlparser.ColName) (int, error) { return 1, nil },
				ResolveType: func(sqlparser.Expr) (evalengine.Type, bool) {
					return evalengine.NewType(sqltypes.VarChar, collation), true
				},
			})
			require.NoError(t, err)
			input = &fakePrimitive{results: []*sqltypes.Result{comparedTextRows(false, 1, 2)}}
			_, err = (&Filter{Predicate: predicate, Input: input}).TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			input.ExpectLog(t, []string{log})
		})
	}
}
