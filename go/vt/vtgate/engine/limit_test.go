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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/require"

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
		Count: evalengine.NewLiteralInt(2),
		Input: fp,
	}

	// Test with limit smaller than input.
	result, err := l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
	)
	if !result.Equal(wantResult) {
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
		Count: evalengine.NewLiteralInt(3),
		Input: fp,
	}

	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(inputResult) {
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
		Count: evalengine.NewLiteralInt(4),
		Input: fp,
	}

	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count: evalengine.NewBindVar("l", collations.TypedCollation{}),
		Input: fp,
	}

	result, err = l.TryExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{"l": sqltypes.Int64BindVariable(2)}, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewLiteralInt(2),
		Offset: evalengine.NewLiteralInt(0),
		Input:  fp,
	}

	// Test with offset 0
	result, err := l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"b|2",
	)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewLiteralInt(2),
		Offset: evalengine.NewLiteralInt(1),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"b|2",
		"c|3",
	)
	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewLiteralInt(2),
		Offset: evalengine.NewLiteralInt(4),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"c|5",
		"c|6",
	)
	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewLiteralInt(4),
		Offset: evalengine.NewLiteralInt(2),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"c|3",
		"c|4",
		"c|5",
		"c|6",
	)
	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewLiteralInt(2),
		Offset: evalengine.NewLiteralInt(5),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
		"c|6",
	)
	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewLiteralInt(2),
		Offset: evalengine.NewLiteralInt(7),
		Input:  fp,
	}
	wantResult = sqltypes.MakeTestResult(
		fields,
	)
	result, err = l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count:  evalengine.NewBindVar("l", collations.TypedCollation{}),
		Offset: evalengine.NewBindVar("o", collations.TypedCollation{}),
		Input:  fp,
	}
	result, err = l.TryExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{"l": sqltypes.Int64BindVariable(1), "o": sqltypes.Int64BindVariable(1)}, false)
	require.NoError(t, err)
	if !result.Equal(wantResult) {
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
		Count: evalengine.NewLiteralInt(2),
		Input: fp,
	}

	// Test with limit smaller than input.
	var results []*sqltypes.Result
	err := l.TryStreamExecute(context.Background(), &noopVCursor{}, bindVars, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)
	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"b|2",
	)
	require.Len(t, results, len(wantResults))
	for i, result := range results {
		if !result.Equal(wantResults[i]) {
			t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
		}
	}

	// Test with bind vars.
	fp.rewind()
	l.Count = evalengine.NewBindVar("l", collations.TypedCollation{})
	results = nil
	err = l.TryStreamExecute(context.Background(), &noopVCursor{}, map[string]*querypb.BindVariable{"l": sqltypes.Int64BindVariable(2)}, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, results, len(wantResults))
	for i, result := range results {
		if !result.Equal(wantResults[i]) {
			t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
		}
	}

	// Test with limit equal to input
	fp.rewind()
	l.Count = evalengine.NewLiteralInt(3)
	results = nil
	err = l.TryStreamExecute(context.Background(), &noopVCursor{}, bindVars, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)
	wantResults = sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"b|2",
		"---",
		"c|3",
	)
	require.Len(t, results, len(wantResults))
	for i, result := range results {
		if !result.Equal(wantResults[i]) {
			t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
		}
	}

	// Test with limit higher than input.
	fp.rewind()
	l.Count = evalengine.NewLiteralInt(4)
	results = nil
	err = l.TryStreamExecute(context.Background(), &noopVCursor{}, bindVars, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)
	// wantResults is same as before.
	require.Len(t, results, len(wantResults))
	for i, result := range results {
		if !result.Equal(wantResults[i]) {
			t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
		}
	}
}

func TestOffsetStreamExecute(t *testing.T) {
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
		"d|4",
		"e|5",
		"f|6",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{inputResult},
	}

	l := &Limit{
		Offset: evalengine.NewLiteralInt(2),
		Count:  evalengine.NewLiteralInt(3),
		Input:  fp,
	}

	var results []*sqltypes.Result
	err := l.TryStreamExecute(context.Background(), &noopVCursor{}, bindVars, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)
	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"c|3",
		"d|4",
		"---",
		"e|5",
	)
	require.Len(t, results, len(wantResults))
	for i, result := range results {
		if !result.Equal(wantResults[i]) {
			t.Errorf("l.StreamExecute:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
		}
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

	got, err := l.GetFields(context.Background(), nil, nil)
	require.NoError(t, err)
	if !got.Equal(result) {
		t.Errorf("l.GetFields:\n%v, want\n%v", got, result)
	}
}

func TestLimitInputFail(t *testing.T) {
	bindVars := make(map[string]*querypb.BindVariable)
	fp := &fakePrimitive{sendErr: errors.New("input fail")}

	l := &Limit{Count: evalengine.NewLiteralInt(1), Input: fp}

	want := "input fail"
	if _, err := l.TryExecute(context.Background(), &noopVCursor{}, bindVars, false); err == nil || err.Error() != want {
		t.Errorf("l.Execute(): %v, want %s", err, want)
	}

	fp.rewind()
	err := l.TryStreamExecute(context.Background(), &noopVCursor{}, bindVars, false, func(_ *sqltypes.Result) error { return nil })
	if err == nil || err.Error() != want {
		t.Errorf("l.StreamExecute(): %v, want %s", err, want)
	}

	fp.rewind()
	if _, err := l.GetFields(context.Background(), nil, nil); err == nil || err.Error() != want {
		t.Errorf("l.GetFields(): %v, want %s", err, want)
	}
}

func TestLimitInvalidCount(t *testing.T) {
	l := &Limit{
		Count: evalengine.NewBindVar("l", collations.TypedCollation{}),
	}
	_, _, err := l.getCountAndOffset(&noopVCursor{}, nil)
	assert.EqualError(t, err, "query arguments missing for l")

	l.Count = evalengine.NewLiteralFloat(1.2)
	_, _, err = l.getCountAndOffset(&noopVCursor{}, nil)
	assert.EqualError(t, err, "Cannot convert value to desired type")

	l.Count = evalengine.NewLiteralUint(18446744073709551615)
	_, _, err = l.getCountAndOffset(&noopVCursor{}, nil)
	assert.EqualError(t, err, "requested limit is out of range: 18446744073709551615")

	// When going through the API, it should return the same error.
	_, err = l.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.EqualError(t, err, "requested limit is out of range: 18446744073709551615")

	err = l.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(_ *sqltypes.Result) error { return nil })
	assert.EqualError(t, err, "requested limit is out of range: 18446744073709551615")
}
