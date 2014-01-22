// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestKeyRangeToShardMap(t *testing.T) {
	ts := new(sandboxTopo)
	var testCases = []struct {
		keyspace  string
		keyRanges []key.KeyRange
		shards    []string
	}{
		{keyspace: TEST_SHARDED, keyRanges: []key.KeyRange{{Start: "20", End: "40"}}, shards: []string{"20-40"}},
		// check for partial keyrange, spanning one shard each
		{keyspace: TEST_SHARDED, keyRanges: []key.KeyRange{{Start: "10", End: "18"}, {Start: "18", End: "20"}}, shards: []string{"-20", "-20"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: TEST_SHARDED, keyRanges: []key.KeyRange{{Start: "10", End: "40"}}, shards: []string{"-20", "20-40"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: TEST_SHARDED, keyRanges: []key.KeyRange{{Start: "1c", End: "2a"}}, shards: []string{"-20", "20-40"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: TEST_SHARDED, keyRanges: []key.KeyRange{{Start: "", End: ""}}, shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for unsharded, non-partial keyrange spanning the entire space.
		{keyspace: TEST_UNSHARDED, keyRanges: []key.KeyRange{{Start: "", End: ""}}, shards: []string{"0"}},
	}

	for _, testCase := range testCases {
		krShardMap, err := resolveKeyRangesToShards(ts, "", testCase.keyspace, topo.TYPE_MASTER, testCase.keyRanges)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		gotShards := make([]string, 0, 1)
		for _, kr := range testCase.keyRanges {
			if shards, ok := krShardMap[kr]; ok {
				gotShards = append(gotShards, shards...)
			}
		}
		if len(testCase.shards) != len(gotShards) {
			t.Errorf("want num of shards %v, got %v", len(testCase.shards), len(gotShards))
		}
		for j, testCaseShard := range testCase.shards {
			if testCaseShard != gotShards[j] {
				t.Errorf("For key range %v want shard %v, got %v", testCase.keyRanges, testCaseShard, gotShards[j])
			}
		}
	}
}
