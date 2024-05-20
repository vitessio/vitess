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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "ParseShardingSpec failed")

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
			assert.NoError(t, err, "Got error while parsing sharding spec")
			keyRange = krArray[0]
		}
		dkr := DestinationExactKeyRange{KeyRange: keyRange}
		var gotShards []string
		err = dkr.Resolve(allShards, func(shard string) error {
			gotShards = append(gotShards, shard)
			return nil
		})
		if testCase.err != "" {
			assert.ErrorContains(t, err, testCase.err)
		}
		assert.Equal(t, testCase.shards, gotShards)
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
			assert.NoError(t, err, "Got error while parsing sharding spec")
			keyRange = krArray[0]
		}
		dkr := DestinationKeyRange{KeyRange: keyRange}
		var gotShards []string
		err := dkr.Resolve(allShards, func(shard string) error {
			gotShards = append(gotShards, shard)
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, testCase.shards, gotShards)
	}
}

func TestDestinationsString(t *testing.T) {
	kr2040 := &topodatapb.KeyRange{
		Start: []byte{0x20},
		End:   []byte{0x40},
	}

	got := DestinationsString([]Destination{
		DestinationShard("2"),
		DestinationShards{"2", "3"},
		DestinationExactKeyRange{KeyRange: kr2040},
		DestinationKeyRange{KeyRange: kr2040},
		DestinationKeyspaceID{1, 2},
		DestinationKeyspaceIDs{
			{1, 2},
			{2, 3},
		},
		DestinationAllShards{},
		DestinationNone{},
		DestinationAnyShard{},
	})
	want := "Destinations:DestinationShard(2),DestinationShards(2,3),DestinationExactKeyRange(20-40),DestinationKeyRange(20-40),DestinationKeyspaceID(0102),DestinationKeyspaceIDs(0102,0203),DestinationAllShards(),DestinationNone(),DestinationAnyShard()"
	assert.Equal(t, want, got)
}

func TestDestinationShardResolve(t *testing.T) {
	allShards := initShardArray(t, "")

	ds := DestinationShard("test-destination-shard")

	var calledVar string
	err := ds.Resolve(allShards, func(shard string) error {
		calledVar = shard
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "test-destination-shard", calledVar)
}

func TestDestinationShardsResolve(t *testing.T) {
	allShards := initShardArray(t, "")

	ds := DestinationShards{"ds1", "ds2"}

	var calledVar []string
	err := ds.Resolve(allShards, func(shard string) error {
		calledVar = append(calledVar, shard)
		return nil
	})
	assert.NoError(t, err)

	want := []string{"ds1", "ds2"}
	assert.ElementsMatch(t, want, calledVar)
}

func TestDestinationKeyspaceIDResolve(t *testing.T) {
	allShards := initShardArray(t, "60-80-90")

	testCases := []struct {
		keyspaceID string
		want       string
		err        string
	}{
		{"59", "", "KeyspaceId 59 didn't match any shards"},
		// Should include start limit of keyRange
		{"60", "60-80", ""},
		{"79", "60-80", ""},
		{"80", "80-90", ""},
		{"89", "80-90", ""},
		// Shouldn't include end limit of keyRange
		{"90", "", "KeyspaceId 90 didn't match any shards"},
	}

	for _, tc := range testCases {
		t.Run(tc.keyspaceID, func(t *testing.T) {
			k, err := hex.DecodeString(tc.keyspaceID)
			assert.NoError(t, err)

			ds := DestinationKeyspaceID(k)

			var calledVar string
			addShard := func(shard string) error {
				calledVar = shard
				return nil
			}

			err = ds.Resolve(allShards, addShard)
			if tc.err != "" {
				assert.ErrorContains(t, err, tc.err)
				return
			}

			assert.Equal(t, tc.want, calledVar)
		})
	}

	// Expect error when allShards is empty
	ds := DestinationKeyspaceID("80")
	err := ds.Resolve([]*topodatapb.ShardReference{}, func(_ string) error {
		return nil
	})
	assert.ErrorContains(t, err, "no shard in keyspace")
}

func TestDestinationKeyspaceIDsResolve(t *testing.T) {
	allShards := initShardArray(t, "60-80-90")

	k1, err := hex.DecodeString("82")
	assert.NoError(t, err)

	k2, err := hex.DecodeString("61")
	assert.NoError(t, err)

	k3, err := hex.DecodeString("89")
	assert.NoError(t, err)

	ds := DestinationKeyspaceIDs{k1, k2, k3}

	var calledVar []string
	addShard := func(shard string) error {
		calledVar = append(calledVar, shard)
		return nil
	}

	err = ds.Resolve(allShards, addShard)
	assert.NoError(t, err)

	want := []string{"80-90", "60-80", "80-90"}
	assert.Equal(t, want, calledVar)
}

func TestDestinationAllShardsResolve(t *testing.T) {
	allShards := initShardArray(t, "60-80-90")

	ds := DestinationAllShards{}

	var calledVar []string
	addShard := func(shard string) error {
		calledVar = append(calledVar, shard)
		return nil
	}

	err := ds.Resolve(allShards, addShard)
	assert.NoError(t, err)

	want := []string{"60-80", "80-90"}
	assert.ElementsMatch(t, want, calledVar)
}

func TestDestinationNoneResolve(t *testing.T) {
	allShards := initShardArray(t, "60-80-90")

	ds := DestinationNone{}

	var called bool
	addShard := func(shard string) error {
		called = true
		return nil
	}

	err := ds.Resolve(allShards, addShard)
	assert.NoError(t, err)
	assert.False(t, called, "addShard shouldn't be called in the case of DestinationNone")
}

func TestDestinationAnyShardResolve(t *testing.T) {
	allShards := initShardArray(t, "custom")

	ds := DestinationAnyShard{}

	var calledVar string
	addShard := func(shard string) error {
		calledVar = shard
		return nil
	}

	err := ds.Resolve(allShards, addShard)
	assert.NoError(t, err)

	possibleShards := []string{"0", "1"}
	assert.Contains(t, possibleShards, calledVar)

	// Expect error when allShards is empty
	err = ds.Resolve([]*topodatapb.ShardReference{}, addShard)
	assert.ErrorContains(t, err, "no shard in keyspace")
}
