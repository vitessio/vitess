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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestHashJoinExecuteSameType(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
			),
		},
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col4|col5|col6",
					"int64|varchar|varchar",
				),
				"1|d|dd",
				"3|e|ee",
				"4|f|ff",
				"3|g|gg",
			),
		},
	}

	// Normal join
	jn := &HashJoin{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		LHSKey: 0,
		RHSKey: 0,
	}
	r, err := jn.TryExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|1|d",
		"3|c|3|e",
		"3|c|3|g",
	))
}

func TestHashJoinExecuteDifferentType(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
				"5|c|cc",
			),
		},
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col4|col5|col6",
					"varchar|varchar|varchar",
				),
				"1.00|d|dd",
				"3|e|ee",
				"2.89|z|zz",
				"4|f|ff",
				"3|g|gg",
				" 5.0toto|g|gg",
				"w|ww|www",
			),
		},
	}

	// Normal join
	jn := &HashJoin{
		Opcode:         InnerJoin,
		Left:           leftPrim,
		Right:          rightPrim,
		Cols:           []int{-1, -2, 1, 2},
		LHSKey:         0,
		RHSKey:         0,
		ComparisonType: querypb.Type_FLOAT64,
	}
	r, err := jn.TryExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|varchar|varchar",
		),
		"1|a|1.00|d",
		"3|c|3|e",
		"3|c|3|g",
		"5|c| 5.0toto|g",
	))
}
