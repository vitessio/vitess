// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestKeyRangeToShardMap(t *testing.T) {
	ts := new(sandboxTopo)
	var testCases = []struct {
		keyspace string
		keyRange key.KeyRange
		shards   []string
	}{
		{keyspace: TEST_SHARDED, keyRange: key.KeyRange{Start: "20", End: "40"}, shards: []string{"20-40"}},
		// check for partial keyrange, spanning one shard
		{keyspace: TEST_SHARDED, keyRange: key.KeyRange{Start: "10", End: "18"}, shards: []string{"-20"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: TEST_SHARDED, keyRange: key.KeyRange{Start: "10", End: "40"}, shards: []string{"-20", "20-40"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: TEST_SHARDED, keyRange: key.KeyRange{Start: "1c", End: "2a"}, shards: []string{"-20", "20-40"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: TEST_SHARDED, keyRange: key.KeyRange{Start: "", End: ""}, shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for unsharded, non-partial keyrange spanning the entire space.
		{keyspace: TEST_UNSHARDED, keyRange: key.KeyRange{Start: "", End: ""}, shards: []string{"0"}},
	}

	for _, testCase := range testCases {
		gotShards, err := resolveKeyRangeToShards(ts, "", testCase.keyspace, topo.TYPE_MASTER, testCase.keyRange)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		if !reflect.DeepEqual(testCase.shards, gotShards) {
			t.Errorf("want \n%#v, got \n%#v", testCase.shards, gotShards)
		}
	}
}
