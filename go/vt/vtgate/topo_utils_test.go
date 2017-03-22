// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

func TestMapKeyRangesToShards(t *testing.T) {
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
		var keyRange *topodatapb.KeyRange
		var err error
		if testCase.keyRange == "" {
			keyRange = &topodatapb.KeyRange{}
		} else {
			krArray, err := key.ParseShardingSpec(testCase.keyRange)
			if err != nil {
				t.Errorf("Got error while parsing sharding spec %v", err)
			}
			keyRange = krArray[0]
		}
		krs := []*topodatapb.KeyRange{keyRange}
		_, gotShards, err := mapKeyRangesToShards(context.Background(), ts, "", testCase.keyspace, topodatapb.TabletType_MASTER, krs)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		sort.Strings(gotShards)
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
		{keyspace: KsTestSharded, keyRange: "10-18", shards: nil, err: "keyrange 10-18 does not exactly match shards"},
		// check for keyrange intersecting with multiple shards
		{keyspace: KsTestSharded, keyRange: "10-40", shards: nil, err: "keyrange 10-40 does not exactly match shards"},
		// check for keyrange intersecting with multiple shards
		{keyspace: KsTestSharded, keyRange: "1c-2a", shards: nil, err: "keyrange 1c-2a does not exactly match shards"},
		// check for keyrange where kr.End is Max Key ""
		{keyspace: KsTestSharded, keyRange: "80-", shards: []string{"80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: KsTestSharded, keyRange: "", shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
	}

	for _, testCase := range testCases {
		var keyRange *topodatapb.KeyRange
		var err error
		if testCase.keyRange == "" {
			keyRange = &topodatapb.KeyRange{}
		} else {
			krArray, err := key.ParseShardingSpec(testCase.keyRange)
			if err != nil {
				t.Errorf("Got error while parsing sharding spec %v", err)
			}
			keyRange = krArray[0]
		}
		_, gotShards, err := mapExactShards(context.Background(), ts, "", testCase.keyspace, topodatapb.TabletType_MASTER, keyRange)
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
		boundQueries []*vtgatepb.BoundShardQuery
		requests     *scatterBatchRequest
	}{
		{
			boundQueries: []*vtgatepb.BoundShardQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "q1",
						BindVariables: map[string]*querypb.BindVariable{
							"q1var": {
								Type:  sqltypes.Int64,
								Value: []byte("1"),
							},
						},
					},
					Keyspace: "ks1",
					Shards:   []string{"0", "1"},
				}, {
					Query: &querypb.BoundQuery{
						Sql: "q2",
						BindVariables: map[string]*querypb.BindVariable{
							"q2var": {
								Type:  sqltypes.Int64,
								Value: []byte("2"),
							},
						},
					},
					Keyspace: "ks1",
					Shards:   []string{"1"},
				}, {
					Query: &querypb.BoundQuery{
						Sql: "q3",
						BindVariables: map[string]*querypb.BindVariable{
							"q3var": {
								Type:  sqltypes.Int64,
								Value: []byte("3"),
							},
						},
					},
					Keyspace: "ks2",
					Shards:   []string{"1"},
				},
			},
			requests: &scatterBatchRequest{
				Length: 3,
				Requests: map[string]*shardBatchRequest{
					"ks1:0": {
						Queries: []querytypes.BoundQuery{
							{
								Sql: "q1",
								BindVariables: map[string]interface{}{
									"q1var": &querypb.BindVariable{
										Type:  sqltypes.Int64,
										Value: []byte("1"),
									},
								},
							},
						},
						Keyspace:      "ks1",
						Shard:         "0",
						ResultIndexes: []int{0},
					},
					"ks1:1": {
						Queries: []querytypes.BoundQuery{
							{
								Sql: "q1",
								BindVariables: map[string]interface{}{
									"q1var": &querypb.BindVariable{
										Type:  sqltypes.Int64,
										Value: []byte("1"),
									},
								},
							}, {
								Sql: "q2",
								BindVariables: map[string]interface{}{
									"q2var": &querypb.BindVariable{
										Type:  sqltypes.Int64,
										Value: []byte("2"),
									},
								},
							},
						},
						Keyspace:      "ks1",
						Shard:         "1",
						ResultIndexes: []int{0, 1},
					},
					"ks2:1": {
						Queries: []querytypes.BoundQuery{
							{
								Sql: "q3",
								BindVariables: map[string]interface{}{
									"q3var": &querypb.BindVariable{
										Type:  sqltypes.Int64,
										Value: []byte("3"),
									},
								},
							},
						},
						Keyspace:      "ks2",
						Shard:         "1",
						ResultIndexes: []int{2},
					},
				},
			},
		},
		{
			boundQueries: []*vtgatepb.BoundShardQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "q1",
						BindVariables: map[string]*querypb.BindVariable{
							"q1var": {
								Type:  sqltypes.Int64,
								Value: []byte("1"),
							},
						},
					},
					Keyspace: "ks1",
					Shards:   []string{"0", "0"},
				},
			},
			requests: &scatterBatchRequest{
				Length: 1,
				Requests: map[string]*shardBatchRequest{
					"ks1:0": {
						Queries: []querytypes.BoundQuery{
							{
								Sql: "q1",
								BindVariables: map[string]interface{}{
									"q1var": &querypb.BindVariable{
										Type:  sqltypes.Int64,
										Value: []byte("1"),
									},
								},
							},
						},
						Keyspace:      "ks1",
						Shard:         "0",
						ResultIndexes: []int{0},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		scatterRequest, err := boundShardQueriesToScatterBatchRequest(testCase.boundQueries)
		if err != nil {
			t.Errorf("boundShardQueriesToScatterBatchRequest failed: %v", err)
			continue
		}
		if !reflect.DeepEqual(testCase.requests, scatterRequest) {
			got, _ := json.Marshal(scatterRequest)
			want, _ := json.Marshal(testCase.requests)
			t.Errorf("Bound Query: %#v\nResponse:  %s\nExpecting: %s", testCase.boundQueries, got, want)
		}
	}
}

func TestBoundKeyspaceIdQueriesToBoundShardQueries(t *testing.T) {
	ts := new(sandboxTopo)
	kid10 := []byte{0x10}
	kid25 := []byte{0x25}
	var testCases = []struct {
		idQueries    []*vtgatepb.BoundKeyspaceIdQuery
		shardQueries []*vtgatepb.BoundShardQuery
	}{
		{
			idQueries: []*vtgatepb.BoundKeyspaceIdQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "q1",
						BindVariables: map[string]*querypb.BindVariable{
							"q1var": {
								Type:  sqltypes.Int64,
								Value: []byte("1"),
							},
						},
					},
					Keyspace:    KsTestSharded,
					KeyspaceIds: [][]byte{kid10, kid25},
				}, {
					Query: &querypb.BoundQuery{
						Sql: "q2",
						BindVariables: map[string]*querypb.BindVariable{
							"q2var": {
								Type:  sqltypes.Int64,
								Value: []byte("2"),
							},
						},
					},
					Keyspace:    KsTestSharded,
					KeyspaceIds: [][]byte{kid25, kid25},
				},
			},
			shardQueries: []*vtgatepb.BoundShardQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "q1",
						BindVariables: map[string]*querypb.BindVariable{
							"q1var": {
								Type:  sqltypes.Int64,
								Value: []byte("1"),
							},
						},
					},
					Keyspace: KsTestSharded,
					Shards:   []string{"-20", "20-40"},
				}, {
					Query: &querypb.BoundQuery{
						Sql: "q2",
						BindVariables: map[string]*querypb.BindVariable{
							"q2var": {
								Type:  sqltypes.Int64,
								Value: []byte("2"),
							},
						},
					},
					Keyspace: KsTestSharded,
					Shards:   []string{"20-40"},
				},
			},
		},
	}

	for _, testCase := range testCases {
		shardQueries, err := boundKeyspaceIDQueriesToBoundShardQueries(context.Background(), ts, "", topodatapb.TabletType_MASTER, testCase.idQueries)
		if err != nil {
			t.Error(err)
		}
		// Sort shards, because they're random otherwise.
		for _, shardQuery := range shardQueries {
			sort.Strings(shardQuery.Shards)
		}
		if !reflect.DeepEqual(testCase.shardQueries, shardQueries) {
			got, _ := json.Marshal(shardQueries)
			want, _ := json.Marshal(testCase.shardQueries)
			t.Errorf("idQueries: %#v\nResponse:   %s\nExpecting: %s", testCase.idQueries, got, want)
		}
	}
}

func BenchmarkResolveKeyRangeToShards(b *testing.B) {
	ts := new(sandboxTopo)
	kr := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x60, 0, 0, 0, 0, 0, 0, 0},
	}
	_, _, allShards, err := getKeyspaceShards(context.Background(), ts, "", KsTestSharded, topodatapb.TabletType_MASTER)
	if err != nil {
		b.Fatal(err)
	}
	uniqueShards := map[string]bool{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolveKeyRangeToShards(allShards, uniqueShards, kr)
	}
}
