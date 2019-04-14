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

func TestSubqueryExecute(t *testing.T) {
	prim := &fakePrimitive{
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

	sq := &Subquery{
		Cols:     []int{0, 2},
		Subquery: prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := sq.Execute(nil, bv, true)
	if err != nil {
		t.Fatal(err)
	}
	prim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"1"  true`,
	})
	expectResult(t, "sq.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col3",
			"int64|varchar",
		),
		"1|aa",
		"2|bb",
		"3|cc",
	))

	// Error case.
	sq.Subquery = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = sq.Execute(nil, bv, true)
	expectError(t, "sq.Execute", err, "err")
}

func TestSubqueryStreamExecute(t *testing.T) {
	prim := &fakePrimitive{
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

	sq := &Subquery{
		Cols:     []int{0, 2},
		Subquery: prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := wrapStreamExecute(sq, nil, bv, true)
	if err != nil {
		t.Fatal(err)
	}
	prim.ExpectLog(t, []string{
		`StreamExecute a: type:INT64 value:"1"  true`,
	})
	expectResult(t, "sq.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col3",
			"int64|varchar",
		),
		"1|aa",
		"2|bb",
		"3|cc",
	))

	// Error case.
	sq.Subquery = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = wrapStreamExecute(sq, nil, bv, true)
	expectError(t, "sq.Execute", err, "err")
}

func TestSubqueryGetFields(t *testing.T) {
	prim := &fakePrimitive{
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

	sq := &Subquery{
		Cols:     []int{0, 2},
		Subquery: prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := sq.GetFields(nil, bv)
	if err != nil {
		t.Fatal(err)
	}
	prim.ExpectLog(t, []string{
		`GetFields a: type:INT64 value:"1" `,
		`Execute a: type:INT64 value:"1"  true`,
	})
	expectResult(t, "sq.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col3",
			"int64|varchar",
		),
	))

	// Error case.
	sq.Subquery = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = sq.GetFields(nil, bv)
	expectError(t, "sq.Execute", err, "err")
}
