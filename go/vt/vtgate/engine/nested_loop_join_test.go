/*
Copyright 2018 The Vitess Authors.

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
	"testing"

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
	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.Execute(noopVCursor{}, bv, true)
	if err != nil {
		t.Fatal(err)
	}
	leftPrim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"10"  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"a"  true`,
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"b"  false`,
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"c"  false`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
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
	r, err = jn.Execute(noopVCursor{}, bv, true)
	if err != nil {
		t.Fatal(err)
	}
	leftPrim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"10"  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"a"  true`,
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"b"  false`,
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"c"  false`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
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
	save := testMaxMemoryRows
	testMaxMemoryRows = 3
	defer func() { testMaxMemoryRows = save }()

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
	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err := jn.Execute(noopVCursor{}, bv, true)
	want := "in-memory row count exceeded allowed limit of 3"
	if err == nil || err.Error() != want {
		t.Errorf("Execute(): %v, want %v", err, want)
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

	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.Execute(noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	if err != nil {
		t.Fatal(err)
	}
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
	expectResult(t, "jn.Execute", r, wantResult)
}

func TestJoinExecuteErrors(t *testing.T) {
	// Error on left query
	leftPrim := &fakePrimitive{
		sendErr: errors.New("left err"),
	}

	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
	}
	_, err := jn.Execute(noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	expectError(t, "jn.Execute", err, "left err")

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

	jn = &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err = jn.Execute(noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	expectError(t, "jn.Execute", err, "right err")

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

	jn = &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err = jn.Execute(noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	expectError(t, "jn.Execute", err, "right err")
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
			// First right query will always be a GetFields.
			sqltypes.MakeTestResult(
				rightFields,
			),
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
	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := wrapStreamExecute(jn, nil, map[string]*querypb.BindVariable{}, true)
	if err != nil {
		t.Fatal(err)
	}
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
		`StreamExecute bv: type:VARCHAR value:"a"  false`,
		`StreamExecute bv: type:VARCHAR value:"b"  false`,
		`StreamExecute bv: type:VARCHAR value:"c"  false`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
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
	r, err = wrapStreamExecute(jn, nil, map[string]*querypb.BindVariable{}, true)
	if err != nil {
		t.Fatal(err)
	}
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
		`StreamExecute bv: type:VARCHAR value:"a"  false`,
		`StreamExecute bv: type:VARCHAR value:"b"  false`,
		`StreamExecute bv: type:VARCHAR value:"c"  false`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
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

	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	r, err := jn.GetFields(nil, map[string]*querypb.BindVariable{})
	if err != nil {
		t.Fatal(err)
	}
	leftPrim.ExpectLog(t, []string{
		`GetFields `,
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`GetFields bv: `,
		`Execute bv:  true`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
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

	jn := &NestedLoopJoin{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	_, err := jn.GetFields(nil, map[string]*querypb.BindVariable{})
	expectError(t, "jn.GetFields", err, "left err")

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
	_, err = jn.GetFields(nil, map[string]*querypb.BindVariable{})
	expectError(t, "jn.GetFields", err, "right err")
}
