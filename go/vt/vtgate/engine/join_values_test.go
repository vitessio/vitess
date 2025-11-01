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

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestJoinValuesExecute(t *testing.T) {
	/*
		select col1, col2, col3, col4, col5, col6 from left join right on left.col1 = right.col4
		LHS: select col1, col2, col3 from left
		RHS: select col5, col6, id from (values row(1,2), ...) left(id,col1) join right on left.col1 = right.col4
	*/

	leftPrim := &fakePrimitive{
		useNewPrintBindVars: true,
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
				"4|d|dd",
			),
		},
	}
	rightPrim := &fakePrimitive{
		useNewPrintBindVars: true,
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col5|col6|id",
					"varchar|varchar|int64",
				),
				"d|dd|0",
				"e|ee|1",
				"f|ff|2",
				"g|gg|3",
			),
		},
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(10),
	}

	vjn := &ValuesJoin{
		Left:              leftPrim,
		Right:             rightPrim,
		Vars:              []int{0},
		RowConstructorArg: "v",
		Cols:              []int{-1, -2, -3, -1, 1, 2},
		ColNames:          []string{"col1", "col2", "col3", "col4", "col5", "col6"},
	}

	r, err := vjn.TryExecute(context.Background(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute a: %v true`, sqltypes.Int64BindVariable(10)),
	})
	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute a: %v v: [[INT64(0) INT64(1)][INT64(1) INT64(2)][INT64(2) INT64(3)][INT64(3) INT64(4)]] true`, sqltypes.Int64BindVariable(10)),
	})

	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col3|col4|col5|col6",
			"int64|varchar|varchar|int64|varchar|varchar",
		),
		"1|a|aa|1|d|dd",
		"2|b|bb|2|e|ee",
		"3|c|cc|3|f|ff",
		"4|d|dd|4|g|gg",
	)
	expectResult(t, r, result)
}
