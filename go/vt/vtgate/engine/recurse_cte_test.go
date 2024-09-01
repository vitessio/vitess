/*
Copyright 2024 The Vitess Authors.

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

func TestRecurseDualQuery(t *testing.T) {
	// Test that the RecurseCTE primitive works as expected.
	// The test is testing something like this:
	// WITH RECURSIVE cte AS (SELECT 1 as col1 UNION SELECT col1+1 FROM cte WHERE col1 < 5) SELECT * FROM cte;
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1",
					"int64",
				),
				"1",
			),
		},
	}
	rightFields := sqltypes.MakeTestFields(
		"col4",
		"int64",
	)

	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				rightFields,
				"2",
			),
			sqltypes.MakeTestResult(
				rightFields,
				"3",
			),
			sqltypes.MakeTestResult(
				rightFields,
				"4",
			), sqltypes.MakeTestResult(
				rightFields,
			),
		},
	}
	bv := map[string]*querypb.BindVariable{}

	cte := &RecurseCTE{
		Seed: leftPrim,
		Term: rightPrim,
		Vars: map[string]int{"col1": 0},
	}

	r, err := cte.TryExecute(context.Background(), &noopVCursor{}, bv, true)
	require.NoError(t, err)

	rightPrim.ExpectLog(t, []string{
		`Execute col1: type:INT64 value:"1" false`,
		`Execute col1: type:INT64 value:"2" false`,
		`Execute col1: type:INT64 value:"3" false`,
		`Execute col1: type:INT64 value:"4" false`,
	})

	wantRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
		"2",
		"3",
		"4",
	)
	expectResult(t, r, wantRes)

	// testing the streaming mode.

	leftPrim.rewind()
	rightPrim.rewind()

	r, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)

	rightPrim.ExpectLog(t, []string{
		`StreamExecute col1: type:INT64 value:"1" false`,
		`StreamExecute col1: type:INT64 value:"2" false`,
		`StreamExecute col1: type:INT64 value:"3" false`,
		`StreamExecute col1: type:INT64 value:"4" false`,
	})
	expectResult(t, r, wantRes)

	// testing the streaming mode with transaction

	leftPrim.rewind()
	rightPrim.rewind()

	r, err = wrapStreamExecute(cte, &noopVCursor{inTx: true}, bv, true)
	require.NoError(t, err)

	rightPrim.ExpectLog(t, []string{
		`Execute col1: type:INT64 value:"1" false`,
		`Execute col1: type:INT64 value:"2" false`,
		`Execute col1: type:INT64 value:"3" false`,
		`Execute col1: type:INT64 value:"4" false`,
	})
	expectResult(t, r, wantRes)

}
