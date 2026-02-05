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
	"fmt"
	"sync"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSemiJoinExecute(t *testing.T) {
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
	rightFields := sqltypes.MakeTestFields(
		"col4|col5|col6",
		"int64|varchar|varchar",
	)
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				rightFields,
				"4|d|dd",
			),
			sqltypes.MakeTestResult(
				rightFields,
			),
			sqltypes.MakeTestResult(
				rightFields,
				"5|e|ee",
				"6|f|ff",
				"7|g|gg",
			),
		},
	}
	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(10),
	}

	jn := &SemiJoin{
		Left:  leftPrim,
		Right: rightPrim,
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.TryExecute(context.Background(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute a: %v true`, sqltypes.Int64BindVariable(10)),
	})
	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute a: %v bv: %v false`, sqltypes.Int64BindVariable(10), &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("a")}),
		fmt.Sprintf(`Execute a: %v bv: %v false`, sqltypes.Int64BindVariable(10), &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("b")}),
		fmt.Sprintf(`Execute a: %v bv: %v false`, sqltypes.Int64BindVariable(10), &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("c")}),
	})
	utils.MustMatch(t, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col3",
			"int64|varchar|varchar",
		),
		"1|a|aa",
		"3|c|cc",
	), r)
}

func TestSemiJoinStreamExecute(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
			), sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"3|c|cc",
				"4|d|dd",
			),
		},
		allResultsInOneCall: true,
	}
	rightFields := sqltypes.MakeTestFields(
		"col4|col5|col6",
		"int64|varchar|varchar",
	)
	rightPrim := &fakePrimitive{
		// we'll return non-empty results for rows 2 and 4
		results: sqltypes.MakeTestStreamingResults(rightFields,
			"4|d|dd",
			"---",
			"---",
			"5|e|ee",
			"6|f|ff",
			"7|g|gg",
		),
	}

	jn := &SemiJoin{
		Left:  leftPrim,
		Right: rightPrim,
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`StreamExecute bv: %v false`, &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("a")}),
		fmt.Sprintf(`StreamExecute bv: %v false`, &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("b")}),
		fmt.Sprintf(`StreamExecute bv: %v false`, &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("c")}),
		fmt.Sprintf(`StreamExecute bv: %v false`, &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("d")}),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col3",
			"int64|varchar|varchar",
		),
		"2|b|bb",
		"4|d|dd",
	))
}

// TestSemiJoinStreamExecuteParallelExecution tests SemiJoin stream execution with parallel execution
// to ensure we have no data races.
func TestSemiJoinStreamExecuteParallelExecution(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
			), sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"3|c|cc",
				"4|d|dd",
			),
		},
		async: true,
	}
	rightFields := sqltypes.MakeTestFields(
		"col4|col5|col6",
		"int64|varchar|varchar",
	)
	rightPrim := &fakePrimitive{
		// we'll return non-empty results for rows 2 and 4
		results: sqltypes.MakeTestStreamingResults(rightFields,
			"4|d|dd",
			"---",
			"---",
			"5|e|ee",
			"6|f|ff",
			"7|g|gg",
		),
		async: true,
		noLog: true,
	}

	jn := &SemiJoin{
		Left:  leftPrim,
		Right: rightPrim,
		Vars: map[string]int{
			"bv": 1,
		},
	}
	var res *sqltypes.Result
	var mu sync.Mutex
	err := jn.TryStreamExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if res == nil {
			res = result
		} else {
			res.Rows = append(res.Rows, result.Rows...)
		}
		return nil
	})
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	// We'll get all the rows back in left primitive, since we're returning the same set of rows
	// from the right primitive that makes them all qualify.
	expectResultAnyOrder(t, res, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col3",
			"int64|varchar|varchar",
		),
		"1|a|aa",
		"2|b|bb",
		"3|c|cc",
		"4|d|dd",
	))
}
