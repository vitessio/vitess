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

package key

import (
	"reflect"
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// initShardArray returns the ShardReference array for the provided
// sharding spec.
func initShardArray(t *testing.T, shardingSpec string) []*topodatapb.ShardReference {
	// unsharded keyspace, we use ""
	if shardingSpec == "" {
		return []*topodatapb.ShardReference{
			{
				Name:     "0",
				KeyRange: &topodatapb.KeyRange{},
			},
		}
	}

	// custom sharded keyspace, we use "custom"
	if shardingSpec == "custom" {
		return []*topodatapb.ShardReference{
			{
				Name:     "0",
				KeyRange: &topodatapb.KeyRange{},
			},
			{
				Name:     "1",
				KeyRange: &topodatapb.KeyRange{},
			},
		}
	}

	shardKrArray, err := ParseShardingSpec(shardingSpec)
	if err != nil {
		t.Fatalf("ParseShardingSpec failed: %v", err)
	}

	result := make([]*topodatapb.ShardReference, len(shardKrArray))
	for i, kr := range shardKrArray {
		shard := KeyRangeString(kr)
		result[i] = &topodatapb.ShardReference{
			Name:     shard,
			KeyRange: kr,
		}
	}
	return result
}

func TestDestinationExactKeyRange(t *testing.T) {
	var testCases = []struct {
		keyspace string
		keyRange string
		shards   []string
		err      string
	}{
		{
			// success case, spanning one shard.
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "20-40",
			shards:   []string{"20-40"},
		},
		{
			// check for partial keyrange, spanning one shard
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "10-18",
			shards:   nil,
			err:      "keyrange 10-18 does not exactly match shards",
		},
		{
			// check for keyrange intersecting with multiple shards
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "10-40",
			shards:   nil,
			err:      "keyrange 10-40 does not exactly match shards",
		},
		{
			// check for keyrange intersecting with multiple shards
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "1c-2a",
			shards:   nil,
			err:      "keyrange 1c-2a does not exactly match shards",
		},
		{
			// check for keyrange where kr.End is Max Key ""
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "80-",
			shards:   []string{"80-a0", "a0-c0", "c0-e0", "e0-"},
		},
		{
			// test for sharded, non-partial keyrange spanning the entire space.
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "",
			shards:   []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"},
		},
		{
			// test for unsharded keyspace
			keyspace: "",
			keyRange: "",
			shards:   []string{"0"},
		},
		{
			// test for custom sharded keyspace
			// FIXME(alainjobart) this is wrong, it should return "0", "1"
			// Note this was wrong before the refactor that moved the code here.
			keyspace: "custom",
			keyRange: "",
			shards:   []string{"0"},
		},
	}

	for _, testCase := range testCases {
		allShards := initShardArray(t, testCase.keyspace)

		var keyRange *topodatapb.KeyRange
		var err error
		if testCase.keyRange == "" {
			keyRange = &topodatapb.KeyRange{}
		} else {
			krArray, err := ParseShardingSpec(testCase.keyRange)
			if err != nil {
				t.Errorf("Got error while parsing sharding spec %v", err)
			}
			keyRange = krArray[0]
		}
		dkr := DestinationExactKeyRange{KeyRange: keyRange}
		var gotShards []string
		err = dkr.Resolve(allShards, func(shard string) error {
			gotShards = append(gotShards, shard)
			return nil
		})
		if err != nil && err.Error() != testCase.err {
			t.Errorf("gotShards: %v, want %s", err, testCase.err)
		}
		if !reflect.DeepEqual(testCase.shards, gotShards) {
			t.Errorf("want \n%#v, got \n%#v", testCase.shards, gotShards)
		}
	}
}

func TestDestinationKeyRange(t *testing.T) {
	var testCases = []struct {
		keyspace string
		keyRange string
		shards   []string
	}{
		{
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "20-40",
			shards:   []string{"20-40"},
		},
		{
			// check for partial keyrange, spanning one shard
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "10-18",
			shards:   []string{"-20"},
		},
		{
			// check for keyrange intersecting with multiple shards
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "10-40",
			shards:   []string{"-20", "20-40"},
		},
		{
			// check for keyrange intersecting with multiple shards
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "1c-2a",
			shards:   []string{"-20", "20-40"},
		},
		{
			// check for keyrange where kr.End is Max Key ""
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "80-",
			shards:   []string{"80-a0", "a0-c0", "c0-e0", "e0-"},
		},
		{
			// test for sharded, non-partial keyrange spanning the entire space.
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "",
			shards:   []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"},
		},
		{
			// test for sharded, non-partial keyrange spanning the entire space,
			// with nil keyrange.
			keyspace: "-20-40-60-80-a0-c0-e0-",
			keyRange: "nil",
			shards:   []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"},
		},
		{
			// test for unsharded, non-partial keyrange spanning the entire space.
			keyspace: "",
			keyRange: "",
			shards:   []string{"0"},
		},
		{
			// test for unsharded, non-partial keyrange spanning the entire space,
			// with nil keyrange.
			keyspace: "",
			keyRange: "nil",
			shards:   []string{"0"},
		},
		{
			// custom sharding
			keyspace: "custom",
			keyRange: "",
			shards:   []string{"0", "1"},
		},
		{
			// custom sharding, with nil keyrange.
			keyspace: "custom",
			keyRange: "nil",
			shards:   []string{"0", "1"},
		},
	}

	for _, testCase := range testCases {
		allShards := initShardArray(t, testCase.keyspace)

		var keyRange *topodatapb.KeyRange
		if testCase.keyRange == "nil" {
		} else if testCase.keyRange == "" {
			keyRange = &topodatapb.KeyRange{}
		} else {
			krArray, err := ParseShardingSpec(testCase.keyRange)
			if err != nil {
				t.Errorf("Got error while parsing sharding spec %v", err)
			}
			keyRange = krArray[0]
		}
		dkr := DestinationKeyRange{KeyRange: keyRange}
		var gotShards []string
		if err := dkr.Resolve(allShards, func(shard string) error {
			gotShards = append(gotShards, shard)
			return nil
		}); err != nil {
			t.Errorf("want nil, got %v", err)
		}
		if !reflect.DeepEqual(testCase.shards, gotShards) {
			t.Errorf("want \n%#v, got \n%#v", testCase.shards, gotShards)
		}
	}
}
