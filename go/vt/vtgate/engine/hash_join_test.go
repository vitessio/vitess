/*
Copyright 2021 The Vitess Authors.

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
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func TestHashJoinVariations(t *testing.T) {
	// This test tries the different variations of hash-joins:
	// comparing values of same type and different types, and both left and right outer joins
	lhs := func() Primitive {
		return &fakePrimitive{
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"col1|col2",
						"int64|varchar",
					),
					"1|1",
					"2|2",
					"3|b",
					"null|b",
				),
			},
		}
	}
	rhs := func() Primitive {
		return &fakePrimitive{
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"col4|col5",
						"int64|varchar",
					),
					"1|1",
					"3|2",
					"5|null",
					"4|b",
				),
			},
		}
	}

	rows := func(r ...string) []string { return r }

	tests := []struct {
		name     string
		typ      JoinOpcode
		lhs, rhs int
		expected []string
		reverse  bool
	}{{
		name:     "inner join, same type",
		typ:      InnerJoin,
		lhs:      0,
		rhs:      0,
		expected: rows("1|1|1|1", "3|b|3|2"),
	}, {
		name:     "inner join, coercion",
		typ:      InnerJoin,
		lhs:      0,
		rhs:      1,
		expected: rows("1|1|1|1", "2|2|3|2"),
	}, {
		name:     "left join, same type",
		typ:      LeftJoin,
		lhs:      0,
		rhs:      0,
		expected: rows("1|1|1|1", "3|b|3|2", "2|2|null|null", "null|b|null|null"),
	}, {
		name:     "left join, coercion",
		typ:      LeftJoin,
		lhs:      0,
		rhs:      1,
		expected: rows("1|1|1|1", "2|2|3|2", "3|b|null|null", "null|b|null|null"),
	}, {
		name:     "right join, same type",
		typ:      LeftJoin,
		lhs:      0,
		rhs:      0,
		expected: rows("1|1|1|1", "3|2|3|b", "4|b|null|null", "5|null|null|null"),
		reverse:  true,
	}, {
		name:     "right join, coercion",
		typ:      LeftJoin,
		lhs:      0,
		rhs:      1,
		reverse:  true,
		expected: rows("1|1|1|1", "3|2|null|null", "4|b|null|null", "5|null|null|null"),
	}}

	for _, tc := range tests {
		var fields []*querypb.Field
		var first, last func() Primitive
		if tc.reverse {
			first, last = rhs, lhs
			fields = sqltypes.MakeTestFields(
				"col4|col5|col1|col2",
				"int64|varchar|int64|varchar",
			)
		} else {
			first, last = lhs, rhs
			fields = sqltypes.MakeTestFields(
				"col1|col2|col4|col5",
				"int64|varchar|int64|varchar",
			)
		}

		expected := sqltypes.MakeTestResult(fields, tc.expected...)

		typ, err := evalengine.CoerceTypes(typeForOffset(tc.lhs), typeForOffset(tc.rhs), collations.MySQL8())
		require.NoError(t, err)

		jn := &HashJoin{
			Opcode:         tc.typ,
			Cols:           []int{-1, -2, 1, 2},
			LHSKey:         tc.lhs,
			RHSKey:         tc.rhs,
			Collation:      typ.Collation(),
			ComparisonType: typ.Type(),
			CollationEnv:   collations.MySQL8(),
		}

		t.Run(tc.name, func(t *testing.T) {
			jn.Left = first()
			jn.Right = last()
			r, err := jn.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
			require.NoError(t, err)
			expectResultAnyOrder(t, r, expected)
		})
		t.Run("Streaming "+tc.name, func(t *testing.T) {
			jn.Left = first()
			jn.Right = last()
			r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
			require.NoError(t, err)
			expectResultAnyOrder(t, r, expected)
		})
	}
}

func typeForOffset(i int) evalengine.Type {
	switch i {
	case 0:
		return evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)
	case 1:
		return evalengine.NewType(sqltypes.VarChar, collations.MySQL8().DefaultConnectionCharset())
	default:
		panic(i)
	}
}

// TestHashJoinJSONKeys checks that a hash join on JSON columns matches rows by
// value. The probe table hands back every row in a bucket without comparing the
// keys, so documents that share a shape but not a value must not share a bucket.
func TestHashJoinJSONKeys(t *testing.T) {
	lhs := func() Primitive {
		return &fakePrimitive{
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("col1", "json"),
					`[1]`,
					`[2]`,
					`{"a":1}`,
				),
			},
		}
	}
	rhs := func() Primitive {
		return &fakePrimitive{
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("col2", "json"),
					`[1]`,
					`{"a":1}`,
					`{"b":2}`,
				),
			},
		}
	}

	jsonType := evalengine.NewType(sqltypes.TypeJSON, collations.CollationBinaryID)
	typ, err := evalengine.CoerceTypes(jsonType, jsonType, collations.MySQL8())
	require.NoError(t, err)

	jn := &HashJoin{
		Opcode:         InnerJoin,
		Cols:           []int{-1, 1},
		LHSKey:         0,
		RHSKey:         0,
		Collation:      typ.Collation(),
		ComparisonType: typ.Type(),
		CollationEnv:   collations.MySQL8(),
	}

	expected := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2", "json|json"),
		`[1]|[1]`,
		`{"a":1}|{"a":1}`,
	)

	t.Run("Execute", func(t *testing.T) {
		jn.Left = lhs()
		jn.Right = rhs()
		r, err := jn.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
		require.NoError(t, err)
		expectResultAnyOrder(t, r, expected)
	})

	t.Run("StreamExecute", func(t *testing.T) {
		jn.Left = lhs()
		jn.Right = rhs()
		r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
		require.NoError(t, err)
		expectResultAnyOrder(t, r, expected)
	})
}

// TestHashJoinKeyCharsets joins the rows of
//
//	create table sorted (id int primary key, l varchar(10) character set latin1 collate latin1_german2_ci);
//	insert into sorted values (1, 'é'), (2, 'd'), (3, 'f'), (4, 'Müller'), (5, 'MUELLER'), (6, 'z');
//
// with themselves on l, with each side as a tablet with
// --queryserver-config-raw-result-charset sends it, in latin1 with the
// collation of the column in the field, or as a tablet without the flag sends
// it, in utf8mb4. MySQL 8.0.46 joins
//
//	on a.l = b.l                         1-1, 2-2, 3-3, 4-4, 4-5, 5-4, 5-5, 6-6
//	on convert(a.l using utf8mb4) =
//	   convert(b.l using utf8mb4)        1-1, 2-2, 3-3, 4-4, 5-5, 6-6
//
// In latin1_german2_ci 'Müller' equals 'MUELLER', and in utf8mb4_0900_ai_ci
// it does not. The key of each side is hashed in the collation of the join,
// whatever character set its rows are in. It asks for no fields: the join has
// to ask for the ones that it needs itself.
func TestHashJoinKeyCharsets(t *testing.T) {
	latin1German2 := collations.MySQL8().LookupByName("latin1_german2_ci")
	utf8mb4 := collations.MySQL8().DefaultConnectionCharset()
	rows := func(raw bool) *sqltypes.Result {
		text, collation := []string{"é", "d", "f", "Müller", "MUELLER", "z"}, utf8mb4
		if raw {
			text, collation = []string{"\xe9", "d", "f", "M\xfcller", "MUELLER", "z"}, latin1German2
		}
		qr := &sqltypes.Result{Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32, Charset: uint32(collations.CollationBinaryID)},
			{Name: "l", Type: sqltypes.VarChar, Charset: uint32(collation), ColumnLength: 10},
		}}
		for i, value := range text {
			qr.Rows = append(qr.Rows, sqltypes.Row{sqltypes.NewInt32(int32(i + 1)), sqltypes.MakeTrusted(sqltypes.VarChar, []byte(value))})
		}
		return qr
	}

	tests := []struct {
		name      string
		collation collations.ID
		pairs     []string
	}{
		{"latin1_german2_ci", latin1German2, []string{"1-1", "2-2", "3-3", "4-4", "4-5", "5-4", "5-5", "6-6"}},
		{"utf8mb4_0900_ai_ci", utf8mb4, []string{"1-1", "2-2", "3-3", "4-4", "5-5", "6-6"}},
	}
	for _, tc := range tests {
		for _, rawLeft := range []bool{false, true} {
			for _, rawRight := range []bool{false, true} {
				for _, stream := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s, left raw %v, right raw %v, stream %v", tc.name, rawLeft, rawRight, stream), func(t *testing.T) {
						hj := &HashJoin{
							Opcode:         InnerJoin,
							Left:           &fakePrimitive{results: []*sqltypes.Result{rows(rawLeft)}},
							Right:          &fakePrimitive{results: []*sqltypes.Result{rows(rawRight)}},
							Cols:           []int{-1, 1},
							LHSKey:         1,
							RHSKey:         1,
							Collation:      tc.collation,
							ComparisonType: sqltypes.VarChar,
							CollationEnv:   collations.MySQL8(),
						}
						vcursor := &noopVCursor{convertResultCharset: true}
						var qr *sqltypes.Result
						var err error
						if stream {
							qr, err = wrapStreamExecute(hj, vcursor, map[string]*querypb.BindVariable{}, false)
						} else {
							qr, err = hj.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
						}
						require.NoError(t, err)
						var pairs []string
						for _, row := range qr.Rows {
							pairs = append(pairs, row[0].ToString()+"-"+row[1].ToString())
						}
						assert.ElementsMatch(t, tc.pairs, pairs)
					})
				}
			}
		}
	}
}
