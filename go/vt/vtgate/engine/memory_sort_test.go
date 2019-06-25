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
	"encoding/json"
	"reflect"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestMemorySortExecute(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			Col: 1,
		}},
		Input: fp,
	}

	result, err := ms.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"b|2",
		"c|3",
		"c|4",
	)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("oa.Execute:\n%v, want\n%v", result, wantResult)
	}

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewValArg([]byte(":__upper_limit")))
	if err != nil {
		t.Fatal(err)
	}
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.Execute(nil, bv, false)
	if err != nil {
		t.Fatal(err)
	}

	wantResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"b|2",
	)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("oa.Execute:\n%v, want\n%v", result, wantResult)
	}
}

func TestMemorySortStreamExecute(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			Col: 1,
		}},
		Input: fp,
	}

	var results []*sqltypes.Result
	err := ms.StreamExecute(noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"a|1",
		"b|2",
		"c|3",
		"c|4",
	)
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("oa.Execute:\n%v, want\n%v", results, wantResults)
	}

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewValArg([]byte(":__upper_limit")))
	if err != nil {
		t.Fatal(err)
	}
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	results = nil
	err = ms.StreamExecute(noopVCursor{}, bv, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	wantResults = sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"a|1",
		"b|2",
	)
	if !reflect.DeepEqual(results, wantResults) {
		r, _ := json.Marshal(results)
		w, _ := json.Marshal(wantResults)
		t.Errorf("oa.Execute:\n%s, want\n%s", r, w)
	}
}

func TestMemorySortGetFields(t *testing.T) {
	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2",
			"int64|varchar",
		),
	)
	fp := &fakePrimitive{results: []*sqltypes.Result{result}}

	ms := &MemorySort{Input: fp}

	got, err := ms.GetFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, result) {
		t.Errorf("l.GetFields:\n%v, want\n%v", got, result)
	}
}

func TestMemorySortMultiColumn(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"b|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			Col: 1,
		}, {
			Col:  0,
			Desc: true,
		}},
		Input: fp,
	}

	result, err := ms.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	wantResult := sqltypes.MakeTestResult(
		fields,
		"b|1",
		"a|1",
		"b|2",
		"c|3",
		"c|4",
	)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("oa.Execute:\n%v, want\n%v", result, wantResult)
	}

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewValArg([]byte(":__upper_limit")))
	if err != nil {
		t.Fatal(err)
	}
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.Execute(nil, bv, false)
	if err != nil {
		t.Fatal(err)
	}

	wantResult = sqltypes.MakeTestResult(
		fields,
		"b|1",
		"a|1",
		"b|2",
	)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("oa.Execute:\n%v, want\n%v", result, wantResult)
	}
}

func TestMemorySortMaxMemoryRows(t *testing.T) {
	save := testMaxMemoryRows
	testMaxMemoryRows = 3
	defer func() { testMaxMemoryRows = save }()

	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			Col: 1,
		}},
		Input: fp,
	}

	err := ms.StreamExecute(noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		return nil
	})
	want := "in-memory row count exceeded allowed limit of 3"
	if err == nil || err.Error() != want {
		t.Errorf("StreamExecute err: %v, want %v", err, want)
	}
}

func TestMemorySortExecuteNoVarChar(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varchar|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			Col: 0,
		}},
		Input: fp,
	}

	_, err := ms.Execute(nil, nil, false)
	want := "types are not comparable: VARCHAR vs VARCHAR"
	if err == nil || err.Error() != want {
		t.Errorf("Execute err: %v, want %v", err, want)
	}

	fp.rewind()
	err = ms.StreamExecute(noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		return nil
	})
	if err == nil || err.Error() != want {
		t.Errorf("StreamExecute err: %v, want %v", err, want)
	}
}
