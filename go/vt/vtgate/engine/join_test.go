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
	"sync/atomic"
	"testing"
	"time"

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

func TestJoinStreamExecuteSerializesRightSideStreams(t *testing.T) {
	jn := &Join{
		Opcode: InnerJoin,
		Left:   &concurrentLeftStreamPrimitive{},
		Right:  &nonConcurrentRightStreamPrimitive{},
		Cols:   []int{-1, 1},
	}

	r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	expectResult(t, &sqltypes.Result{Fields: r.Fields}, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1|col2", "int64|int64"),
	))
	require.Len(t, r.Rows, 2)
	require.ElementsMatch(t, []string{
		"[INT64(1) INT64(10)]",
		"[INT64(2) INT64(10)]",
	}, []string{
		fmt.Sprintf("%v", r.Rows[0]),
		fmt.Sprintf("%v", r.Rows[1]),
	})
}

func TestJoinStreamExecuteEmptyLeftWithoutFieldsCallback(t *testing.T) {
	leftPrim := &emptyLeftStreamPrimitive{}
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
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
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

type concurrentLeftStreamPrimitive struct{}

func (p *concurrentLeftStreamPrimitive) TryExecute(context.Context, VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	panic("not implemented")
}

func (p *concurrentLeftStreamPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	fields := sqltypes.MakeTestFields("col1", "int64")
	rows := [][]sqltypes.Value{
		{sqltypes.NewInt64(1)},
		{sqltypes.NewInt64(2)},
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(rows))
	start := make(chan struct{})
	for _, row := range rows {
		wg.Go(func() {
			<-start
			errs <- callback(&sqltypes.Result{Fields: fields, Rows: [][]sqltypes.Value{row}})
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *concurrentLeftStreamPrimitive) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1", "int64")), nil
}

func (p *concurrentLeftStreamPrimitive) NeedsTransaction() bool {
	return false
}

func (p *concurrentLeftStreamPrimitive) Inputs() ([]Primitive, []map[string]any) {
	return nil, nil
}

func (p *concurrentLeftStreamPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "concurrentLeft"}
}

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

type nonConcurrentRightStreamPrimitive struct {
	active atomic.Int32
}

func (p *nonConcurrentRightStreamPrimitive) TryExecute(context.Context, VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	panic("not implemented")
}

func (p *nonConcurrentRightStreamPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if active := p.active.Add(1); active != 1 {
		p.active.Add(-1)
		return errors.New("concurrent right stream")
	}
	defer p.active.Add(-1)
	time.Sleep(20 * time.Millisecond)
	return callback(sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col2", "int64"),
		"10",
	))
}

func (p *nonConcurrentRightStreamPrimitive) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return sqltypes.MakeTestResult(sqltypes.MakeTestFields("col2", "int64")), nil
}

func (p *nonConcurrentRightStreamPrimitive) NeedsTransaction() bool {
	return false
}

func (p *nonConcurrentRightStreamPrimitive) Inputs() ([]Primitive, []map[string]any) {
	return nil, nil
}

func (p *nonConcurrentRightStreamPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "nonConcurrentRight"}
}
