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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestUncorrelatedSubqueryOneRow(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1", "int64"),
		"1",
	)
	sfp := &fakePrimitive{results: []*sqltypes.Result{sqResult}}
	underlyingResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col", "int64"),
		"0",
	)
	ufp := &fakePrimitive{results: []*sqltypes.Result{underlyingResult}}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		NeedsScalar:  true,
		Subquery:     sfp,
		Outer:        ufp,
	}

	result, err := ps.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v false`, sqltypes.Int64BindVariable(1))})
	// Verify outer gets all 3 bind vars
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v sq: %v sq_has_values: %v sq_list: %v false`,
		sqltypes.Int64BindVariable(1),
		sqltypes.Int64BindVariable(1),
		sqltypes.Int64BindVariable(1),
		&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("1")}}},
	)})
	expectResult(t, result, underlyingResult)
}

func TestUncorrelatedSubqueryNoRows(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1", "int64"))
	sfp := &fakePrimitive{results: []*sqltypes.Result{sqResult}}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		NeedsScalar:  true,
		Subquery:     sfp,
		Outer:        ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute sq:  sq_has_values: %v sq_list: %v false`,
		sqltypes.Int64BindVariable(0),
		&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}},
	)})
}

func TestUncorrelatedSubqueryMultiRowScalarError(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1", "int64"),
		"1", "2",
	)
	sfp := &fakePrimitive{results: []*sqltypes.Result{sqResult}}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		NeedsScalar:  true,
		Subquery:     sfp,
	}

	_, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "subquery returned more than one row")
}

func TestUncorrelatedSubqueryMultiRowNoScalar(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1", "int64"),
		"1", "2",
	)
	sfp := &fakePrimitive{results: []*sqltypes.Result{sqResult}}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		NeedsScalar:  false, // no scalar usage — multiple rows OK
		Subquery:     sfp,
		Outer:        ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`Execute sq:  sq_has_values: %v sq_list: %v false`,
		sqltypes.Int64BindVariable(1),
		&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
			{Type: querypb.Type_INT64, Value: []byte("1")},
			{Type: querypb.Type_INT64, Value: []byte("2")},
		}},
	)})
}

func TestUncorrelatedSubqueryError(t *testing.T) {
	sfp := &fakePrimitive{sendErr: errors.New("err")}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		Subquery:     sfp,
	}

	_, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "err")
}

func TestUncorrelatedSubqueryStream(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col1", "int64"),
		"1",
	)
	sfp := &fakePrimitive{results: []*sqltypes.Result{sqResult}}
	underlyingResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("col", "int64"),
		"0",
	)
	ufp := &fakePrimitive{results: []*sqltypes.Result{underlyingResult}}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		NeedsScalar:  true,
		Subquery:     sfp,
		Outer:        ufp,
	}

	result, err := wrapStreamExecute(ps, &noopVCursor{}, bindVars, true)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{fmt.Sprintf(`Execute aa: %v false`, sqltypes.Int64BindVariable(1))})
	ufp.ExpectLog(t, []string{fmt.Sprintf(`StreamExecute aa: %v sq: %v sq_has_values: %v sq_list: %v true`,
		sqltypes.Int64BindVariable(1),
		sqltypes.Int64BindVariable(1),
		sqltypes.Int64BindVariable(1),
		&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("1")}}},
	)})
	expectResult(t, result, underlyingResult)
}

func TestUncorrelatedSubqueryGetFields(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		ScalarResult: "sq",
		ListResult:   "sq_list",
		HasValues:    "sq_has_values",
		Outer:        ufp,
	}

	if _, err := ps.GetFields(context.Background(), nil, bindVars); err != nil {
		t.Error(err)
	}
	ufp.ExpectLog(t, []string{
		fmt.Sprintf(`GetFields aa: %v sq:  sq_has_values: %v sq_list: %v`,
			sqltypes.Int64BindVariable(1),
			sqltypes.Int64BindVariable(0),
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}}),
		fmt.Sprintf(`Execute aa: %v sq:  sq_has_values: %v sq_list: %v true`,
			sqltypes.Int64BindVariable(1),
			sqltypes.Int64BindVariable(0),
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_INT64, Value: []byte("0")}}}),
	})
}
