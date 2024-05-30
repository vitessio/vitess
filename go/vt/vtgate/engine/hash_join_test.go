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
	"context"
	"testing"

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
			r, err := jn.TryExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
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
