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
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestMergeSortNormal tests the normal flow of a merge
// sort where all shards return ordered rows with no errors.
func TestMergeSortNormal(t *testing.T) {
	vc := &testVCursor{
		shardResults: map[string]*shardResult{
			"0": {
				results: []*sqltypes.Result{
					fieldResult,
					{
						Rows: [][]sqltypes.Value{
							makeRow("1", "a"),
							makeRow("7", "g"),
						},
					},
				},
			},
			"1": {
				results: []*sqltypes.Result{
					fieldResult,
					{
						Rows: [][]sqltypes.Value{
							makeRow("2", "b"),
						},
					},
					{
						Rows: [][]sqltypes.Value{
							makeRow("3", "c"),
						},
					},
				},
			},
			"2": {
				results: []*sqltypes.Result{
					fieldResult,
					{
						Rows: [][]sqltypes.Value{
							makeRow("4", "d"),
							makeRow("6", "f"),
						},
					},
				},
			},
			"3": {
				results: []*sqltypes.Result{
					fieldResult,
					{
						Rows: [][]sqltypes.Value{
							makeRow("4", "d"),
							makeRow("8", "h"),
						},
					},
				},
			},
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
	wantResults := []*sqltypes.Result{
		fieldResult,
		{
			Rows: [][]sqltypes.Value{
				makeRow("1", "a"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("2", "b"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("3", "c"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("4", "d"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("4", "d"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("6", "f"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("7", "g"),
			},
		},
		{
			Rows: [][]sqltypes.Value{
				makeRow("8", "h"),
			},
		},
	}
	if !reflect.DeepEqual(results, wantResults) {
		t.Errorf("merge-sort:\n%s, want\n%s", printResults(results), printResults(wantResults))
	}
}

func printResults(results []*sqltypes.Result) string {
	b := new(bytes.Buffer)
	delim := ""
	for _, r := range results {
		fmt.Fprintf(b, "%s%v", delim, r)
		delim = ", "
	}
	return b.String()
}

var fieldResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{Name: "id", Type: sqltypes.Int32},
		{Name: "col", Type: sqltypes.VarChar},
	},
}

func makeRow(id, col string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.Int32, []byte(id)),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte(col)),
	}
}

type shardResult struct {
	results []*sqltypes.Result
	// sendErr is sent at the end of the stream if it's set.
	sendErr error
	// recvErr is set if a callback returned an error.
	recvErr error
}

type testVCursor struct {
	shardResults map[string]*shardResult
}

func (t *testVCursor) Execute(query string, bindvars map[string]interface{}, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *testVCursor) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *testVCursor) ExecuteStandalone(query string, bindvars map[string]interface{}, keyspace, shard string) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *testVCursor) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
	var shard string
	for k := range shardVars {
		shard = k
		break
	}
	for _, r := range t.shardResults[shard].results {
		if err := callback(r); err != nil {
			t.shardResults[shard].recvErr = err
			return err
		}
	}
	if t.shardResults[shard].sendErr != nil {
		return t.shardResults[shard].sendErr
	}
	return nil
}

func (t *testVCursor) GetKeyspaceShards(vkeyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error) {
	panic("unimplemented")
}

func (t *testVCursor) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	panic("unimplemented")
}
