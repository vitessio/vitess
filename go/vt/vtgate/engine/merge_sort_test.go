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
	"vitess.io/vitess/go/vt/srvtopo"
)

// TestMergeSortNormal tests the normal flow of a merge
// sort where all shards return ascending rows.
func TestMergeSortNormal(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &streamVCursor{
		shardResults: map[string]*shardResult{
			"0": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"1|a",
				"7|g",
			)},
			"1": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"2|b",
				"---",
				"3|c",
			)},
			"2": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"4|d",
				"6|f",
			)},
			"3": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"4|d",
				"---",
				"8|h",
			)},
		},
	}
	orderBy := []OrderbyParams{{
		Col: 0,
	}}
	rss := []*srvtopo.ResolvedShard{
		{Target: &querypb.Target{Shard: "0"}},
		{Target: &querypb.Target{Shard: "1"}},
		{Target: &querypb.Target{Shard: "2"}},
		{Target: &querypb.Target{Shard: "3"}},
	}
	bvs := []map[string]*querypb.BindVariable{nil, nil, nil, nil}

	var results []*sqltypes.Result
	err := mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	// Results are retuned one row at a time.
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
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("mergeSort:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}
}

// TestMergeSortDescending tests the normal flow of a merge
// sort where all shards return descending rows.
func TestMergeSortDescending(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &streamVCursor{
		shardResults: map[string]*shardResult{
			"0": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"7|g",
				"1|a",
			)},
			"1": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"3|c",
				"---",
				"2|b",
			)},
			"2": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"6|f",
				"4|d",
			)},
			"3": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"8|h",
				"---",
				"4|d",
			)},
		},
	}
	orderBy := []OrderbyParams{{
		Col:  0,
		Desc: true,
	}}
	rss := []*srvtopo.ResolvedShard{
		{Target: &querypb.Target{Shard: "0"}},
		{Target: &querypb.Target{Shard: "1"}},
		{Target: &querypb.Target{Shard: "2"}},
		{Target: &querypb.Target{Shard: "3"}},
	}
	bvs := []map[string]*querypb.BindVariable{nil, nil, nil, nil}

	var results []*sqltypes.Result
	err := mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	// Results are retuned one row at a time.
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
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("mergeSort:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}
}

func TestMergeSortEmptyResults(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &streamVCursor{
		shardResults: map[string]*shardResult{
			"0": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"1|a",
				"7|g",
			)},
			"1": {results: sqltypes.MakeTestStreamingResults(idColFields)},
			"2": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"4|d",
				"6|f",
			)},
			"3": {results: sqltypes.MakeTestStreamingResults(idColFields)},
		},
	}
	orderBy := []OrderbyParams{{
		Col: 0,
	}}
	rss := []*srvtopo.ResolvedShard{
		{Target: &querypb.Target{Shard: "0"}},
		{Target: &querypb.Target{Shard: "1"}},
		{Target: &querypb.Target{Shard: "2"}},
		{Target: &querypb.Target{Shard: "3"}},
	}
	bvs := []map[string]*querypb.BindVariable{nil, nil, nil, nil}

	var results []*sqltypes.Result
	err := mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	// Results are retuned one row at a time.
	wantResults := sqltypes.MakeTestStreamingResults(idColFields,
		"1|a",
		"---",
		"4|d",
		"---",
		"6|f",
		"---",
		"7|g",
	)
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("mergeSort:\n%s, want\n%s", sqltypes.PrintResults(results), sqltypes.PrintResults(wantResults))
	}
}

// TestMergeSortResultFailures tests failures at various
// stages of result return.
func TestMergeSortResultFailures(t *testing.T) {
	vc := &streamVCursor{
		shardResults: make(map[string]*shardResult),
	}
	orderBy := []OrderbyParams{{
		Col: 0,
	}}
	rss := []*srvtopo.ResolvedShard{
		{Target: &querypb.Target{Shard: "0"}},
	}
	bvs := []map[string]*querypb.BindVariable{nil}

	// Test early error.
	vc.shardResults["0"] = &shardResult{
		sendErr: errors.New("early error"),
	}
	err := mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error { return nil })
	want := "early error"
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	// Test fail after fields.
	idFields := sqltypes.MakeTestFields("id", "int32")
	vc.shardResults["0"] = &shardResult{
		results: sqltypes.MakeTestStreamingResults(idFields),
		sendErr: errors.New("fail after fields"),
	}
	err = mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error { return nil })
	want = "fail after fields"
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	// Test fail after first row.
	vc.shardResults["0"] = &shardResult{
		results: sqltypes.MakeTestStreamingResults(idFields, "1"),
		sendErr: errors.New("fail after first row"),
	}
	err = mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error { return nil })
	want = "fail after first row"
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}
}

func TestMergeSortDataFailures(t *testing.T) {
	// The first row being bad fails in a different code path than
	// the case of subsequent rows. So, test the two cases separately.
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &streamVCursor{
		shardResults: map[string]*shardResult{
			"0": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"1|a",
			)},
			"1": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"2.1|b",
			)},
		},
	}
	orderBy := []OrderbyParams{{
		Col: 0,
	}}
	rss := []*srvtopo.ResolvedShard{
		{Target: &querypb.Target{Shard: "0"}},
		{Target: &querypb.Target{Shard: "1"}},
	}
	bvs := []map[string]*querypb.BindVariable{nil, nil}

	err := mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error { return nil })
	want := `strconv.ParseInt: parsing "2.1": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	// Create a new VCursor because the previous mergeSort will still
	// have lingering goroutines that can cause data race.
	vc = &streamVCursor{
		shardResults: map[string]*shardResult{
			"0": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"1|a",
				"1.1|a",
			)},
			"1": {results: sqltypes.MakeTestStreamingResults(idColFields,
				"2|b",
			)},
		},
	}
	err = mergeSort(vc, "", orderBy, rss, bvs, func(qr *sqltypes.Result) error { return nil })
	want = `strconv.ParseInt: parsing "1.1": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}
}

type shardResult struct {
	results []*sqltypes.Result
	// sendErr is sent at the end of the stream if it's set.
	sendErr error
}

// streamVCursor fakes a VCursor that supports
// a single-shard streaming query through StreamExecuteMulti.
type streamVCursor struct {
	noopVCursor

	shardResults map[string]*shardResult
}

// StreamExecuteMulti streams a result from the specified shard.
// The shard is specified by the only entry in shardVars. At the
// end of a stream, if sendErr is set, that error is returned.
func (t *streamVCursor) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	shard := rss[0].Target.Shard
	for _, r := range t.shardResults[shard].results {
		if err := callback(r); err != nil {
			return err
		}
	}
	if t.shardResults[shard].sendErr != nil {
		return t.shardResults[shard].sendErr
	}
	return nil
}
