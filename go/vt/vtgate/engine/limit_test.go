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
	"errors"
	"reflect"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestLimitExecute(t *testing.T) {
	bindVars := make(map[string]*querypb.BindVariable)
	fields := sqltypes.MakeTestFields(
		"col1|col2",
		"int64|varchar",
	)
	inputResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l := &Limit{
		Count: int64PlanValue(2),
		Input: fp,
	}

	// Test with limit smaller than input.
	result, err := l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
	)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n%v, want\n%v", result, wantResult)
	}

	// Test with limit equal to input.
	wantResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
	)
	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}
	l = &Limit{
		Count: int64PlanValue(3),
		Input: fp,
	}

	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, inputResult) {
		t.Errorf("l.Execute:\n%v, want\n%v", result, wantResult)
	}

	// Test with limit higher than input.
	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}
	l = &Limit{
		Count: int64PlanValue(4),
		Input: fp,
	}

	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n%v, want\n%v", result, wantResult)
	}

	// Test with bind vars.
	wantResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
	)
	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}
	l = &Limit{
		Count: sqltypes.PlanValue{Key: "l"},
		Input: fp,
	}

	result, err = l.Execute(nil, map[string]*querypb.BindVariable{"l": sqltypes.Int64BindVariable(2)}, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n%v, want\n%v", result, wantResult)
	}
}

func TestLimitOffsetExecute(t *testing.T) {
	bindVars := make(map[string]*querypb.BindVariable)
	fields := sqltypes.MakeTestFields(
		"col1|col2",
		"int64|varchar",
	)
	inputResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l := &Limit{
		Count:  int64PlanValue(2),
		Offset: int64PlanValue(0),
		Input:  fp,
	}

	// Test with offset 0
	result, err := l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
	)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n%v, want\n%v", result, wantResult)
	}

	// Test with offset set

	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l = &Limit{
		Count:  int64PlanValue(2),
		Offset: int64PlanValue(1),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"b|2",
		"c|3",
	)
	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n got %v, want\n%v", result, wantResult)
	}

	// Works on boundary condition (elements == limit + offset)
	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l = &Limit{
		Count:  int64PlanValue(2),
		Offset: int64PlanValue(4),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"c|5",
		"c|6",
	)
	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n got %v, want\n%v", result, wantResult)
	}

	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l = &Limit{
		Count:  int64PlanValue(4),
		Offset: int64PlanValue(2),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n got %v, want\n%v", result, wantResult)
	}

	// test when limit is beyond the number of available elements
	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l = &Limit{
		Count:  int64PlanValue(2),
		Offset: int64PlanValue(5),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"c|6",
	)
	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n got %v, want\n%v", result, wantResult)
	}

	// Works when offset is beyond the response
	inputResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l = &Limit{
		Count:  int64PlanValue(2),
		Offset: int64PlanValue(7),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
	)
	result, err = l.Execute(nil, bindVars, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n got %v, want\n%v", result, wantResult)
	}

	// works with bindvars
	inputResult = sqltypes.MakeTestResult(
		fields,
		"x|1",
		"z|2",
	)
	wantResult = sqltypes.MakeTestResult(
		fields,
		"z|2",
	)

	fp = &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l = &Limit{
		Count:  sqltypes.PlanValue{Key: "l"},
		Offset: sqltypes.PlanValue{Key: "o"},
		Input:  fp,
	}
	result, err = l.Execute(nil, map[string]*querypb.BindVariable{"l": sqltypes.Int64BindVariable(1), "o": sqltypes.Int64BindVariable(1)}, false)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("l.Execute:\n got %v, want\n%v", result, wantResult)
	}
}

func TestLimitStreamExecute(t *testing.T) {
	bindVars := make(map[string]*querypb.BindVariable)
	fields := sqltypes.MakeTestFields(
		"col1|col2",
		"int64|varchar",
	)
	inputResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
		"c|3",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l := &Limit{
		Count: int64PlanValue(2),
		Input: fp,
	}

	// Test with limit smaller than input.
	var results []*sqltypes.Result
	err := l.StreamExecute(nil, bindVars, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"b|2",
	)
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}

	// Test with bind vars.
	fp.rewind()
	l.Count = sqltypes.PlanValue{Key: "l"}
	results = nil
	err = l.StreamExecute(nil, map[string]*querypb.BindVariable{"l": sqltypes.Int64BindVariable(2)}, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}

	// Test with limit equal to input
	fp.rewind()
	l.Count = int64PlanValue(3)
	results = nil
	err = l.StreamExecute(nil, bindVars, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	wantResults = sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"b|2",
		"---",
		"c|3",
	)
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}

	// Test with limit higher than input.
	fp.rewind()
	l.Count = int64PlanValue(4)
	results = nil
	err = l.StreamExecute(nil, bindVars, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	// wantResults is same as before.
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}
}

func TestLimitGetFields(t *testing.T) {
	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2",
			"int64|varchar",
		),
	)
	fp := &fakePrimitive{results: []*sqltypes.Result{result}}

	l := &Limit{Input: fp}

	got, err := l.GetFields(nil, nil)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, result) {
		t.Errorf("l.GetFields:\n%v, want\n%v", got, result)
	}
}

func TestLimitInputFail(t *testing.T) {
	bindVars := make(map[string]*querypb.BindVariable)
	fp := &fakePrimitive{sendErr: errors.New("input fail")}

	l := &Limit{Count: int64PlanValue(1), Input: fp}

	want := "input fail"
	if _, err := l.Execute(nil, bindVars, false); err == nil || err.Error() != want {
		t.Errorf("l.Execute(): %v, want %s", err, want)
	}

	fp.rewind()
	err := l.StreamExecute(nil, bindVars, false, func(_ *sqltypes.Result) error { return nil })
	if err == nil || err.Error() != want {
		t.Errorf("l.StreamExecute(): %v, want %s", err, want)
	}

	fp.rewind()
	if _, err := l.GetFields(nil, nil); err == nil || err.Error() != want {
		t.Errorf("l.GetFields(): %v, want %s", err, want)
	}
}

func TestLimitInvalidCount(t *testing.T) {
	l := &Limit{
		Count: sqltypes.PlanValue{Key: "l"},
	}
	_, err := l.fetchCount(nil)
	want := "missing bind var l"
	if err == nil || err.Error() != want {
		t.Errorf("fetchCount: %v, want %s", err, want)
	}

	l.Count = sqltypes.PlanValue{Value: sqltypes.NewFloat64(1.2)}
	_, err = l.fetchCount(nil)
	want = "could not parse value: '1.2'"
	if err == nil || err.Error() != want {
		t.Errorf("fetchCount: %v, want %s", err, want)
	}

	l.Count = sqltypes.PlanValue{Value: sqltypes.NewUint64(18446744073709551615)}
	_, err = l.fetchCount(nil)
	want = "requested limit is out of range: 18446744073709551615"
	if err == nil || err.Error() != want {
		t.Errorf("fetchCount: %v, want %s", err, want)
	}

	// When going through the API, it should return the same error.
	_, err = l.Execute(nil, nil, false)
	if err == nil || err.Error() != want {
		t.Errorf("l.Execute: %v, want %s", err, want)
	}

	err = l.StreamExecute(nil, nil, false, func(_ *sqltypes.Result) error { return nil })
	if err == nil || err.Error() != want {
		t.Errorf("l.Execute: %v, want %s", err, want)
	}
}

func int64PlanValue(v int64) sqltypes.PlanValue {
	return sqltypes.PlanValue{Value: sqltypes.NewInt64(v)}
}
