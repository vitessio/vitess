/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestMergeSortAscending tests the normal flow of a merge
// sort where all shards return ascending rows.
func TestMergeSortNormal(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &fakeVcursor{
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
	params := &scatterParams{
		shardVars: map[string]map[string]interface{}{
			"0": nil,
			"1": nil,
			"2": nil,
			"3": nil,
		},
	}

	var results []*sqltypes.Result
	err := mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error {
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

// TestMergeSortNormal tests the normal flow of a merge
// sort where all shards return descending rows.
func TestMergeSortDescending(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &fakeVcursor{
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
	params := &scatterParams{
		shardVars: map[string]map[string]interface{}{
			"0": nil,
			"1": nil,
			"2": nil,
			"3": nil,
		},
	}

	var results []*sqltypes.Result
	err := mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error {
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
	vc := &fakeVcursor{
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
	params := &scatterParams{
		shardVars: map[string]map[string]interface{}{
			"0": nil,
			"1": nil,
			"2": nil,
			"3": nil,
		},
	}

	var results []*sqltypes.Result
	err := mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error {
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

func TestMergeSortResultFailures(t *testing.T) {
	vc := &fakeVcursor{
		shardResults: make(map[string]*shardResult),
	}
	orderBy := []OrderbyParams{{
		Col: 0,
	}}
	params := &scatterParams{
		shardVars: map[string]map[string]interface{}{
			"0": nil,
		},
	}

	vc.shardResults["0"] = &shardResult{
		sendErr: errors.New("early error"),
	}
	err := mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error { return nil })
	want := "early error"
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	idFields := sqltypes.MakeTestFields("id", "int32")
	vc.shardResults["0"] = &shardResult{
		results: sqltypes.MakeTestStreamingResults(idFields),
		sendErr: errors.New("fail after fields"),
	}
	err = mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error { return nil })
	want = "fail after fields"
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	vc.shardResults["0"] = &shardResult{
		results: sqltypes.MakeTestStreamingResults(idFields, "1"),
		sendErr: errors.New("fail after first row"),
	}
	err = mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error { return nil })
	want = "fail after first row"
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	vc.shardResults["0"] = &shardResult{
		results: sqltypes.MakeTestStreamingResults(idFields, "1"),
		sendErr: errors.New("fail after first row"),
	}
}

func TestMergeSortDataFailures(t *testing.T) {
	idColFields := sqltypes.MakeTestFields("id|col", "int32|varchar")
	vc := &fakeVcursor{
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
	params := &scatterParams{
		shardVars: map[string]map[string]interface{}{
			"0": nil,
			"1": nil,
		},
	}

	err := mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error { return nil })
	want := `strconv.ParseInt: parsing "2.1": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("mergeSort(): %v, want %v", err, want)
	}

	// Create a new VCursor because the previous mergeSort will still
	// have lingering goroutines that can cause data race.
	vc = &fakeVcursor{
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
	err = mergeSort(vc, "", orderBy, params, func(qr *sqltypes.Result) error { return nil })
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

// fakeVCursor fakes a VCursor. Currently, the only supported functionality
// is a single-shard streaming query through StreamExecuteMulti.
type fakeVcursor struct {
	shardResults map[string]*shardResult
}

func (t *fakeVcursor) Context() context.Context {
	return context.Background()
}

func (t *fakeVcursor) Execute(query string, bindvars map[string]interface{}, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *fakeVcursor) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *fakeVcursor) ExecuteStandalone(query string, bindvars map[string]interface{}, keyspace, shard string) (*sqltypes.Result, error) {
	panic("unimplemented")
}

// StreamExecuteMulti streams a result from the specified shard.
// The shard is specifed by the only entry in shardVars. At the
// end of a stream, if sendErr is set, that error is returned.
func (t *fakeVcursor) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
	var shard string
	for k := range shardVars {
		shard = k
		break
	}
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

func (t *fakeVcursor) GetKeyspaceShards(vkeyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error) {
	panic("unimplemented")
}

func (t *fakeVcursor) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	panic("unimplemented")
}
