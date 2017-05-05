/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topotools

import (
	"encoding/hex"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// helper methods for tests to be shorter

func hki(hexValue string) []byte {
	k, err := hex.DecodeString(hexValue)
	if err != nil {
		panic(err)
	}
	return k
}

func si(start, end string) *topo.ShardInfo {
	s := hki(start)
	e := hki(end)
	return topo.NewShardInfo("keyspace", start+"-"+end, &topodatapb.Shard{
		KeyRange: &topodatapb.KeyRange{
			Start: s,
			End:   e,
		},
	}, 0)
}

type expectedOverlappingShard struct {
	left  []string
	right []string
}

func overlappingShardMatch(ol []*topo.ShardInfo, or []*topo.ShardInfo, e expectedOverlappingShard) bool {
	if len(ol)+1 != len(e.left) {
		return false
	}
	if len(or)+1 != len(e.right) {
		return false
	}
	for i, l := range ol {
		if l.ShardName() != e.left[i]+"-"+e.left[i+1] {
			return false
		}
	}
	for i, r := range or {
		if r.ShardName() != e.right[i]+"-"+e.right[i+1] {
			return false
		}
	}
	return true
}

func compareResultLists(t *testing.T, os []*OverlappingShards, expected []expectedOverlappingShard) {
	if len(os) != len(expected) {
		t.Errorf("Unexpected result length, got %v, want %v", len(os), len(expected))
		return
	}

	for _, o := range os {
		found := false
		for _, e := range expected {
			if overlappingShardMatch(o.Left, o.Right, e) {
				found = true
			}
			if overlappingShardMatch(o.Right, o.Left, e) {
				found = true
			}
		}
		if !found {
			t.Errorf("OverlappingShard %v not found in expected %v", o, expected)
			return
		}
	}
}

func TestFindOverlappingShardsNoOverlap(t *testing.T) {
	var shardMap map[string]*topo.ShardInfo
	var os []*OverlappingShards
	var err error

	// no shards
	shardMap = map[string]*topo.ShardInfo{}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 0 || err != nil {
		t.Errorf("empty shard map: %v %v", os, err)
	}

	// just one shard, full keyrange
	shardMap = map[string]*topo.ShardInfo{
		"0": {},
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 0 || err != nil {
		t.Errorf("just one shard, full keyrange: %v %v", os, err)
	}

	// just one shard, partial keyrange
	shardMap = map[string]*topo.ShardInfo{
		"-80": si("", "80"),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 0 || err != nil {
		t.Errorf("just one shard, partial keyrange: %v %v", os, err)
	}

	// two non-overlapping shards
	shardMap = map[string]*topo.ShardInfo{
		"-80": si("", "80"),
		"80":  si("80", ""),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 0 || err != nil {
		t.Errorf("two non-overlapping shards: %v %v", os, err)
	}

	// shards with holes
	shardMap = map[string]*topo.ShardInfo{
		"-80": si("", "80"),
		"80":  si("80", ""),
		"-20": si("", "20"),
		// HOLE: "20-40": si("20", "40"),
		"40-60": si("40", "60"),
		"60-80": si("60", "80"),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 0 || err != nil {
		t.Errorf("shards with holes: %v %v", os, err)
	}

	// shards not overlapping
	shardMap = map[string]*topo.ShardInfo{
		"-80": si("", "80"),
		"80":  si("80", ""),
		// MISSING: "-20": si("", "20"),
		"20-40": si("20", "40"),
		"40-60": si("40", "60"),
		"60-80": si("60", "80"),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 0 || err != nil {
		t.Errorf("shards not overlapping: %v %v", os, err)
	}
}

func TestFindOverlappingShardsOverlap(t *testing.T) {
	var shardMap map[string]*topo.ShardInfo
	var os []*OverlappingShards
	var err error

	// split in progress
	shardMap = map[string]*topo.ShardInfo{
		"-80":   si("", "80"),
		"80":    si("80", ""),
		"-40":   si("", "40"),
		"40-80": si("40", "80"),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 1 || err != nil {
		t.Errorf("split in progress: %v %v", os, err)
	}
	compareResultLists(t, os, []expectedOverlappingShard{
		{
			left:  []string{"", "80"},
			right: []string{"", "40", "80"},
		},
	})

	// 1 to 4 split
	shardMap = map[string]*topo.ShardInfo{
		"-":     si("", ""),
		"-40":   si("", "40"),
		"40-80": si("40", "80"),
		"80-c0": si("80", "c0"),
		"c0-":   si("c0", ""),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 1 || err != nil {
		t.Errorf("1 to 4 split: %v %v", os, err)
	}
	compareResultLists(t, os, []expectedOverlappingShard{
		{
			left:  []string{"", ""},
			right: []string{"", "40", "80", "c0", ""},
		},
	})

	// 2 to 3 split
	shardMap = map[string]*topo.ShardInfo{
		"-40":   si("", "40"),
		"40-80": si("40", "80"),
		"80-":   si("80", ""),
		"-30":   si("", "30"),
		"30-60": si("30", "60"),
		"60-80": si("60", "80"),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 1 || err != nil {
		t.Errorf("2 to 3 split: %v %v", os, err)
	}
	compareResultLists(t, os, []expectedOverlappingShard{
		{
			left:  []string{"", "40", "80"},
			right: []string{"", "30", "60", "80"},
		},
	})

	// multiple concurrent splits
	shardMap = map[string]*topo.ShardInfo{
		"-80":   si("", "80"),
		"80-":   si("80", ""),
		"-40":   si("", "40"),
		"40-80": si("40", "80"),
		"80-c0": si("80", "c0"),
		"c0-":   si("c0", ""),
	}
	os, err = findOverlappingShards(shardMap)
	if len(os) != 2 || err != nil {
		t.Errorf("2 to 3 split: %v %v", os, err)
	}
	compareResultLists(t, os, []expectedOverlappingShard{
		{
			left:  []string{"", "80"},
			right: []string{"", "40", "80"},
		},
		{
			left:  []string{"80", ""},
			right: []string{"80", "c0", ""},
		},
	})

	// find a shard in there
	if o := OverlappingShardsForShard(os, "-60"); o != nil {
		t.Errorf("Found a shard where I shouldn't have!")
	}
	if o := OverlappingShardsForShard(os, "-40"); o == nil {
		t.Errorf("Found no shard where I should have!")
	} else {
		compareResultLists(t, []*OverlappingShards{o},
			[]expectedOverlappingShard{
				{
					left:  []string{"", "80"},
					right: []string{"", "40", "80"},
				},
			})
	}
}

func TestFindOverlappingShardsErrors(t *testing.T) {
	var shardMap map[string]*topo.ShardInfo
	var err error

	// 3 overlapping shards
	shardMap = map[string]*topo.ShardInfo{
		"-20": si("", "20"),
		"-40": si("", "40"),
		"-80": si("", "80"),
	}
	_, err = findOverlappingShards(shardMap)
	if err == nil {
		t.Errorf("3 overlapping shards with no error")
	}
}
