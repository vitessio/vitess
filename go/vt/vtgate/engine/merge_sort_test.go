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
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestMergeSortNormal tests the normal flow of a merge
// sort where all shards return ascending rows.
func TestMergeSortNormal(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	shardResults := []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"1|a",
			"7|g",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"2|b",
			"---",
			"3|c",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"4|d",
			"6|f",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"4|d",
			"---",
			"8|h",
		),
	}}
	orderBy := []OrderbyParams{{
		WeightStringCol: -1,
		Col:             0,
	}}

	var results []*sqltypes.Result
	err := testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	// Results are returned one row at a time.
	wantResults := sqltypes.MakeTestStreamingResults(idColFields,
		"1|a",
		"---",
		"2|b",
		"---",
		"3|c",
		"---",
		"4|d",
		"---",
		"4|d",
		"---",
		"6|f",
		"---",
		"7|g",
		"---",
		"8|h",
	)
	utils.MustMatch(t, wantResults, results)
}

func TestMergeSortWeightString(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "varbinary|varchar")
	shardResults := []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"1|a",
			"7|g",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"2|b",
			"---",
			"3|c",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"4|d",
			"6|f",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"4|d",
			"---",
			"8|h",
		),
	}}
	orderBy := []OrderbyParams{{
		WeightStringCol: 0,
		Col:             1,
	}}

	var results []*sqltypes.Result
	err := testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	// Results are returned one row at a time.
	wantResults := sqltypes.MakeTestStreamingResults(idColFields,
		"1|a",
		"---",
		"2|b",
		"---",
		"3|c",
		"---",
		"4|d",
		"---",
		"4|d",
		"---",
		"6|f",
		"---",
		"7|g",
		"---",
		"8|h",
	)
	utils.MustMatch(t, wantResults, results)
}

// TestMergeSortDescending tests the normal flow of a merge
// sort where all shards return descending rows.
func TestMergeSortDescending(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	shardResults := []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"7|g",
			"1|a",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"3|c",
			"---",
			"2|b",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"6|f",
			"4|d",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"8|h",
			"---",
			"4|d",
		),
	}}
	orderBy := []OrderbyParams{{
		WeightStringCol: -1,
		Col:             0,
		Desc:            true,
	}}

	var results []*sqltypes.Result
	err := testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	// Results are returned one row at a time.
	wantResults := sqltypes.MakeTestStreamingResults(idColFields,
		"8|h",
		"---",
		"7|g",
		"---",
		"6|f",
		"---",
		"4|d",
		"---",
		"4|d",
		"---",
		"3|c",
		"---",
		"2|b",
		"---",
		"1|a",
	)
	utils.MustMatch(t, wantResults, results)
}

func TestMergeSortEmptyResults(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	shardResults := []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"1|a",
			"7|g",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"4|d",
			"6|f",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields),
	}}
	orderBy := []OrderbyParams{{
		WeightStringCol: -1,
		Col:             0,
	}}

	var results []*sqltypes.Result
	err := testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	// Results are returned one row at a time.
	wantResults := sqltypes.MakeTestStreamingResults(idColFields,
		"1|a",
		"---",
		"4|d",
		"---",
		"6|f",
		"---",
		"7|g",
	)
	utils.MustMatch(t, wantResults, results)
}

// TestMergeSortResultFailures tests failures at various
// stages of result return.
func TestMergeSortResultFailures(t *testing.T) {
	orderBy := []OrderbyParams{{
		WeightStringCol: -1,
		Col:             0,
	}}

	// Test early error.
	shardResults := []*shardResult{{
		sendErr: errors.New("early error"),
	}}
	err := testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error { return nil })
	want := "early error"
	require.EqualError(t, err, want)

	// Test fail after fields.
	idFields := sqltypes.MakeTestFields("id", "int32")
	shardResults = []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idFields),
		sendErr: errors.New("fail after fields"),
	}}
	err = testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error { return nil })
	want = "fail after fields"
	require.EqualError(t, err, want)

	// Test fail after first row.
	shardResults = []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idFields, "1"),
		sendErr: errors.New("fail after first row"),
	}}
	err = testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error { return nil })
	want = "fail after first row"
	require.EqualError(t, err, want)
}

func TestMergeSortDataFailures(t *testing.T) {
	// The first row being bad fails in a different code path than
	// the case of subsequent rows. So, test the two cases separately.
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	shardResults := []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"1|a",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"2.1|b",
		),
	}}
	orderBy := []OrderbyParams{{
		WeightStringCol: -1,
		Col:             0,
	}}

	err := testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error { return nil })
	want := `strconv.ParseInt: parsing "2.1": invalid syntax`
	require.EqualError(t, err, want)

	// Create a new VCursor because the previous MergeSort will still
	// have lingering goroutines that can cause data race.
	shardResults = []*shardResult{{
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"1|a",
			"1.1|a",
		),
	}, {
		results: sqltypes.MakeTestStreamingResults(idColFields,
			"2|b",
		),
	}}
	err = testMergeSort(shardResults, orderBy, func(qr *sqltypes.Result) error { return nil })
	want = `strconv.ParseInt: parsing "1.1": invalid syntax`
	require.EqualError(t, err, want)
}

func testMergeSort(shardResults []*shardResult, orderBy []OrderbyParams, callback func(qr *sqltypes.Result) error) error {
	prims := make([]StreamExecutor, 0, len(shardResults))
	for _, sr := range shardResults {
		prims = append(prims, sr)
	}
	ms := MergeSort{
		Primitives: prims,
		OrderBy:    orderBy,
	}
	return ms.StreamExecute(&noopVCursor{}, nil, true, callback)
}

type shardResult struct {
	// shardRoute helps us avoid redefining the Primitive functions.
	shardRoute

	results []*sqltypes.Result
	sendErr error
}

func (sr *shardResult) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	for _, r := range sr.results {
		if err := callback(r); err != nil {
			return err
		}
	}
	return sr.sendErr
}
