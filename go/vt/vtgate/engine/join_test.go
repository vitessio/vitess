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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestJoinExecute(t *testing.T) {
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

	// Normal join
	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute %v true`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10)})),
	})
	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute %v true`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10), "bv": sqltypes.StringBindVariable("a")})),
		fmt.Sprintf(`Execute %v false`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10), "bv": sqltypes.StringBindVariable("b")})),
		fmt.Sprintf(`Execute %v false`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10), "bv": sqltypes.StringBindVariable("c")})),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|4|d",
		"3|c|5|e",
		"3|c|6|f",
		"3|c|7|g",
	))

	// Left Join
	leftPrim.rewind()
	rightPrim.rewind()
	jn.Opcode = LeftJoin
	r, err = jn.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute %v true`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10)})),
	})
	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute %v true`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10), "bv": sqltypes.StringBindVariable("a")})),
		fmt.Sprintf(`Execute %v false`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10), "bv": sqltypes.StringBindVariable("b")})),
		fmt.Sprintf(`Execute %v false`, printBindVars(map[string]*querypb.BindVariable{"a": sqltypes.Int64BindVariable(10), "bv": sqltypes.StringBindVariable("c")})),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|4|d",
		"2|b|null|null",
		"3|c|5|e",
		"3|c|6|f",
		"3|c|7|g",
	))
}

func TestJoinExecuteMaxMemoryRows(t *testing.T) {
	saveMax := testMaxMemoryRows
	saveIgnore := testIgnoreMaxMemoryRows
	testMaxMemoryRows = 3
	defer func() {
		testMaxMemoryRows = saveMax
		testIgnoreMaxMemoryRows = saveIgnore
	}()

	testCases := []struct {
		ignoreMaxMemoryRows bool
		err                 string
	}{
		{true, ""},
		{false, "in-memory row count exceeded allowed limit of 3"},
	}
	for _, test := range testCases {
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

		// Normal join
		jn := &Join{
			Opcode: InnerJoin,
			Left:   leftPrim,
			Right:  rightPrim,
			Cols:   []int{-1, -2, 1, 2},
			Vars: map[string]int{
				"bv": 1,
			},
		}
		testIgnoreMaxMemoryRows = test.ignoreMaxMemoryRows
		_, err := jn.TryExecute(t.Context(), &noopVCursor{}, bv, true)
		if testIgnoreMaxMemoryRows {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, test.err)
		}
	}
}

func TestJoinExecuteNoResult(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
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
			),
		},
	}

	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
	})
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
	)
	expectResult(t, r, wantResult)
}

func TestJoinExecuteErrors(t *testing.T) {
	// Error on left query
	leftPrim := &fakePrimitive{
		sendErr: errors.New("left err"),
	}

	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
	}
	_, err := jn.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.EqualError(t, err, "left err")

	// Error on right query
	leftPrim = &fakePrimitive{
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
		sendErr: errors.New("right err"),
	}

	jn = &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err = jn.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.EqualError(t, err, "right err")

	// Error on right getfields
	leftPrim = &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
			),
		},
	}
	rightPrim = &fakePrimitive{
		sendErr: errors.New("right err"),
	}

	jn = &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err = jn.TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.EqualError(t, err, "right err")
}

func TestJoinStreamExecute(t *testing.T) {
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

	// Normal join
	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
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
		fmt.Sprintf(`StreamExecute %v true`, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("a")})),
		fmt.Sprintf(`StreamExecute %v false`, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("b")})),
		fmt.Sprintf(`StreamExecute %v false`, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("c")})),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|4|d",
		"3|c|5|e",
		"3|c|6|f",
		"3|c|7|g",
	))

	// Left Join
	leftPrim.rewind()
	rightPrim.rewind()
	jn.Opcode = LeftJoin
	r, err = wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`StreamExecute %v true`, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("a")})),
		fmt.Sprintf(`StreamExecute %v false`, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("b")})),
		fmt.Sprintf(`StreamExecute %v false`, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("c")})),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|4|d",
		"2|b|null|null",
		"3|c|5|e",
		"3|c|6|f",
		"3|c|7|g",
	))
}

// TestJoinStreamExecuteEmptyLeft covers the post-stream field path when the
// left streams its fields but no rows (an empty result set). The fields are
// derived after the left stream completes, so the left is streamed once and
// the right's fields come from a single GetFields.
func TestJoinStreamExecuteEmptyLeft(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("col1|col2|col3", "int64|varchar|varchar"),
			),
		},
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("col4|col5|col6", "int64|varchar|varchar"),
			),
		},
	}
	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars:   map[string]int{"bv": 1},
	}

	r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar"),
	))
}

// TestJoinStreamExecuteEmptyLeftWithoutFieldsCallback covers the degenerate
// case where the left stream returns without ever invoking the callback (no
// fields, no rows). The join must still derive fields via GetFields on both
// sides rather than panic.
func TestJoinStreamExecuteEmptyLeftWithoutFieldsCallback(t *testing.T) {
	leftPrim := &emptyLeftStreamPrimitive{}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("col4|col5|col6", "int64|varchar|varchar"),
			),
		},
	}
	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars:   map[string]int{"bv": 1},
	}

	r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute true`,
		`GetFields`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar"),
	))
}

// TestJoinStreamExecuteRightFieldsAndRowsInOneChunk covers a right side that
// emits fields and rows in a single callback, as the Rows primitive does. The
// joined fields and the first joined rows must come out in the same chunk.
func TestJoinStreamExecuteRightFieldsAndRowsInOneChunk(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("col1|col2|col3", "int64|varchar|varchar"),
				"1|a|aa",
				"2|b|bb",
			),
		},
	}
	rightResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col4|col5|col6", "int64|varchar|varchar"),
		"4|d|dd",
	)
	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  NewRowsPrimitive(rightResult.Rows, rightResult.Fields),
		Cols:   []int{-1, -2, 1, 2},
		Vars:   map[string]int{"bv": 1},
	}

	r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar"),
		"1|a|4|d",
		"2|b|4|d",
	))
}

// TestJoinStreamExecuteAsyncLeft covers concurrent left-side callback
// delivery. The write-once capture of the left fields must hold up under
// concurrent chunks, and the joined fields must be emitted exactly once, in
// the first chunk. Run with -race to validate the synchronization.
func TestJoinStreamExecuteAsyncLeft(t *testing.T) {
	leftFields := sqltypes.MakeTestFields("col1|col2|col3", "int64|varchar|varchar")
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(leftFields, "1|a|aa"),
			sqltypes.MakeTestResult(leftFields, "2|b|bb"),
		},
		async: true,
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("col4|col5|col6", "int64|varchar|varchar"),
				"4|d|dd",
			),
		},
		async: true,
		noLog: true,
	}
	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars:   map[string]int{"bv": 1},
	}

	var res *sqltypes.Result
	var mu sync.Mutex
	err := jn.TryStreamExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if res == nil {
			assert.NotEmpty(t, result.Fields, "the first chunk must carry the joined fields")
			res = result
		} else {
			assert.Empty(t, result.Fields, "only the first chunk may carry fields")
			res.Rows = append(res.Rows, result.Rows...)
		}
		return nil
	})
	require.NoError(t, err)
	expectResultAnyOrder(t, res, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar"),
		"1|a|4|d",
		"2|b|4|d",
	))
}

func TestGetFields(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
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
			),
		},
	}

	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.GetFields(t.Context(), nil, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`GetFields `,
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
	))
}

func TestGetFieldsErrors(t *testing.T) {
	leftPrim := &fakePrimitive{
		sendErr: errors.New("left err"),
	}
	rightPrim := &fakePrimitive{
		sendErr: errors.New("right err"),
	}

	jn := &Join{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err := jn.GetFields(t.Context(), nil, map[string]*querypb.BindVariable{})
	require.EqualError(t, err, "left err")

	jn.Left = &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
			),
		},
	}
	_, err = jn.GetFields(t.Context(), nil, map[string]*querypb.BindVariable{})
	require.EqualError(t, err, "right err")
}

// emptyLeftStreamPrimitive is a left input whose stream returns without ever
// invoking the callback, but whose GetFields still reports fields.
type emptyLeftStreamPrimitive struct {
	log []string
}

func (p *emptyLeftStreamPrimitive) TryExecute(context.Context, VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	return sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2|col3", "int64|varchar|varchar"),
	), nil
}

func (p *emptyLeftStreamPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	p.log = append(p.log, fmt.Sprintf("StreamExecute %v", wantfields))
	return nil
}

func (p *emptyLeftStreamPrimitive) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	p.log = append(p.log, "GetFields")
	return sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2|col3", "int64|varchar|varchar"),
	), nil
}

func (p *emptyLeftStreamPrimitive) NeedsTransaction() bool {
	return false
}

func (p *emptyLeftStreamPrimitive) Inputs() ([]Primitive, []map[string]any) {
	return nil, nil
}

func (p *emptyLeftStreamPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "emptyLeftStream"}
}

func (p *emptyLeftStreamPrimitive) ExpectLog(t *testing.T, want []string) {
	t.Helper()
	require.Equal(t, strings.Join(want, "\n"), strings.Join(p.log, "\n"))
}
