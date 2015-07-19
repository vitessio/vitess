// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

func TestKeyRangeToShardMap(t *testing.T) {
	ts := new(sandboxTopo)
	var testCases = []struct {
		keyspace string
		keyRange string
		shards   []string
	}{
		{keyspace: KsTestSharded, keyRange: "20-40", shards: []string{"20-40"}},
		// check for partial keyrange, spanning one shard
		{keyspace: KsTestSharded, keyRange: "10-18", shards: []string{"-20"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: KsTestSharded, keyRange: "10-40", shards: []string{"-20", "20-40"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: KsTestSharded, keyRange: "1c-2a", shards: []string{"-20", "20-40"}},
		// check for keyrange where kr.End is Max Key ""
		{keyspace: KsTestSharded, keyRange: "80-", shards: []string{"80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: KsTestSharded, keyRange: "", shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for unsharded, non-partial keyrange spanning the entire space.
		{keyspace: KsTestUnsharded, keyRange: "", shards: []string{"0"}},
	}

	for _, testCase := range testCases {
		var keyRange key.KeyRange
		var err error
		if testCase.keyRange == "" {
			keyRange = key.KeyRange{Start: "", End: ""}
		} else {
			krArray, err := key.ParseShardingSpec(testCase.keyRange)
			if err != nil {
				t.Errorf("Got error while parsing sharding spec %v", err)
			}
			keyRange = krArray[0]
		}
		_, _, allShards, err := getKeyspaceShards(context.Background(), ts, "", testCase.keyspace, topo.TYPE_MASTER)
		gotShards, err := resolveKeyRangeToShards(allShards, keyRange)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		if !reflect.DeepEqual(testCase.shards, gotShards) {
			t.Errorf("want \n%#v, got \n%#v", testCase.shards, gotShards)
		}
	}
}

func TestMapExactShards(t *testing.T) {
	ts := new(sandboxTopo)
	var testCases = []struct {
		keyspace string
		keyRange string
		shards   []string
		err      string
	}{
		{keyspace: KsTestSharded, keyRange: "20-40", shards: []string{"20-40"}},
		// check for partial keyrange, spanning one shard
		{keyspace: KsTestSharded, keyRange: "10-18", shards: nil, err: "keyrange {Start: 10, End: 18} does not exactly match shards"},
		// check for keyrange intersecting with multiple shards
		{keyspace: KsTestSharded, keyRange: "10-40", shards: nil, err: "keyrange {Start: 10, End: 40} does not exactly match shards"},
		// check for keyrange intersecting with multiple shards
		{keyspace: KsTestSharded, keyRange: "1c-2a", shards: nil, err: "keyrange {Start: 1c, End: 2a} does not exactly match shards"},
		// check for keyrange where kr.End is Max Key ""
		{keyspace: KsTestSharded, keyRange: "80-", shards: []string{"80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: KsTestSharded, keyRange: "", shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
	}

	for _, testCase := range testCases {
		var keyRange key.KeyRange
		var err error
		if testCase.keyRange == "" {
			keyRange = key.KeyRange{Start: "", End: ""}
		} else {
			krArray, err := key.ParseShardingSpec(testCase.keyRange)
			if err != nil {
				t.Errorf("Got error while parsing sharding spec %v", err)
			}
			keyRange = krArray[0]
		}
		_, gotShards, err := mapExactShards(context.Background(), ts, "", testCase.keyspace, topo.TYPE_MASTER, keyRange)
		if err != nil && err.Error() != testCase.err {
			t.Errorf("gotShards: %v, want %s", err, testCase.err)
		}
		if !reflect.DeepEqual(testCase.shards, gotShards) {
			t.Errorf("want \n%#v, got \n%#v", testCase.shards, gotShards)
		}
	}
}

func TestBoundShardQueriesToScatterBatchRequest(t *testing.T) {
	var testCases = []struct {
		boundQueries []proto.BoundShardQuery
		requests     *scatterBatchRequest
	}{
		{
			boundQueries: []proto.BoundShardQuery{
				{
					Sql:           "q1",
					BindVariables: map[string]interface{}{"q1var": 1},
					Keyspace:      "ks1",
					Shards:        []string{"0", "1"},
				}, {
					Sql:           "q2",
					BindVariables: map[string]interface{}{"q2var": 2},
					Keyspace:      "ks1",
					Shards:        []string{"1"},
				}, {
					Sql:           "q3",
					BindVariables: map[string]interface{}{"q3var": 3},
					Keyspace:      "ks2",
					Shards:        []string{"1"},
				},
			},
			requests: &scatterBatchRequest{
				Length: 3,
				Requests: map[string]*shardBatchRequest{
					"ks1:0": &shardBatchRequest{
						Queries: []tproto.BoundQuery{
							{
								Sql:           "q1",
								BindVariables: map[string]interface{}{"q1var": 1},
							},
						},
						Keyspace:      "ks1",
						Shard:         "0",
						ResultIndexes: []int{0},
					},
					"ks1:1": &shardBatchRequest{
						Queries: []tproto.BoundQuery{
							{
								Sql:           "q1",
								BindVariables: map[string]interface{}{"q1var": 1},
							}, {
								Sql:           "q2",
								BindVariables: map[string]interface{}{"q2var": 2},
							},
						},
						Keyspace:      "ks1",
						Shard:         "1",
						ResultIndexes: []int{0, 1},
					},
					"ks2:1": &shardBatchRequest{
						Queries: []tproto.BoundQuery{
							{
								Sql:           "q3",
								BindVariables: map[string]interface{}{"q3var": 3},
							},
						},
						Keyspace:      "ks2",
						Shard:         "1",
						ResultIndexes: []int{2},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		scatterRequest, err := boundShardQueriesToScatterBatchRequest(testCase.boundQueries)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(testCase.requests, scatterRequest) {
			got, _ := json.Marshal(scatterRequest)
			want, _ := json.Marshal(testCase.requests)
			t.Errorf("Bound Query: %#v\nResponse:   %s\nExepecting: %s", testCase.boundQueries, got, want)
		}
	}
}
