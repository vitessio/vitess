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

package srvtopo

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topotools"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func initTopo(name string) (*ResilientServer, error) {
	ctx := context.Background()
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	rs := NewResilientServer(ts, name)

	// Create sharded keyspace and shards.
	if err := ts.CreateKeyspace(ctx, "sks", &topodatapb.Keyspace{}); err != nil {
		return nil, fmt.Errorf("CreateKeyspace(sks) failed: %v", err)
	}
	shardKrArray, err := key.ParseShardingSpec("-20-40-60-80-a0-c0-e0-")
	if err != nil {
		return nil, fmt.Errorf("key.ParseShardingSpec failed: %v", err)
	}
	for _, kr := range shardKrArray {
		shard := key.KeyRangeString(kr)
		if err := ts.CreateShard(ctx, "sks", shard); err != nil {
			return nil, fmt.Errorf("CreateShard(\"%v\") failed: %v", shard, err)
		}
		if _, err := ts.UpdateShardFields(ctx, "sks", shard, func(si *topo.ShardInfo) error {
			si.Shard.Cells = []string{"cell1"}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("UpdateShardFields(\"%v\") failed: %v", shard, err)
		}
	}

	// Create unsharded keyspace and shard.
	if err := ts.CreateKeyspace(ctx, "uks", &topodatapb.Keyspace{}); err != nil {
		return nil, fmt.Errorf("CreateKeyspace(uks) failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "uks", "0"); err != nil {
		return nil, fmt.Errorf("CreateShard(0) failed: %v", err)
	}
	if _, err := ts.UpdateShardFields(ctx, "uks", "0", func(si *topo.ShardInfo) error {
		si.Shard.Cells = []string{"cell1"}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("UpdateShardFields(0) failed: %v", err)
	}

	// And rebuild both.
	for _, keyspace := range []string{"sks", "uks"} {
		if err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, keyspace, []string{cell}); err != nil {
			return nil, fmt.Errorf("RebuildKeyspace(%v) failed: %v", keyspace, err)
		}
	}
	return rs, nil
}

func TestMapKeyRangesToShards(t *testing.T) {
	ctx := context.Background()
	rs, err := initTopo("TestMapKeyRangesToShards")
	if err != nil {
		t.Fatal(err)
	}

	var testCases = []struct {
		keyspace string
		keyRange string
		shards   []string
	}{
		{keyspace: "sks", keyRange: "20-40", shards: []string{"20-40"}},
		// check for partial keyrange, spanning one shard
		{keyspace: "sks", keyRange: "10-18", shards: []string{"-20"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: "sks", keyRange: "10-40", shards: []string{"-20", "20-40"}},
		// check for keyrange intersecting with multiple shards
		{keyspace: "sks", keyRange: "1c-2a", shards: []string{"-20", "20-40"}},
		// check for keyrange where kr.End is Max Key ""
		{keyspace: "sks", keyRange: "80-", shards: []string{"80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: "sks", keyRange: "", shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for unsharded, non-partial keyrange spanning the entire space.
		{keyspace: "uks", keyRange: "", shards: []string{"0"}},
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
		_, gotShards, err := MapKeyRangesToShards(ctx, rs, "cell1", testCase.keyspace, topodatapb.TabletType_MASTER, krs)
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
	ctx := context.Background()
	rs, err := initTopo("TestMapExactShards")
	if err != nil {
		t.Fatal(err)
	}

	var testCases = []struct {
		keyspace string
		keyRange string
		shards   []string
		err      string
	}{
		{keyspace: "sks", keyRange: "20-40", shards: []string{"20-40"}},
		// check for partial keyrange, spanning one shard
		{keyspace: "sks", keyRange: "10-18", shards: nil, err: "keyrange 10-18 does not exactly match shards"},
		// check for keyrange intersecting with multiple shards
		{keyspace: "sks", keyRange: "10-40", shards: nil, err: "keyrange 10-40 does not exactly match shards"},
		// check for keyrange intersecting with multiple shards
		{keyspace: "sks", keyRange: "1c-2a", shards: nil, err: "keyrange 1c-2a does not exactly match shards"},
		// check for keyrange where kr.End is Max Key ""
		{keyspace: "sks", keyRange: "80-", shards: []string{"80-a0", "a0-c0", "c0-e0", "e0-"}},
		// test for sharded, non-partial keyrange spanning the entire space.
		{keyspace: "sks", keyRange: "", shards: []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}},
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
		_, gotShards, err := MapExactShards(ctx, rs, "cell1", testCase.keyspace, topodatapb.TabletType_MASTER, keyRange)
		if err != nil && err.Error() != testCase.err {
			t.Errorf("gotShards: %v, want %s", err, testCase.err)
		}
		if !reflect.DeepEqual(testCase.shards, gotShards) {
			t.Errorf("want \n%#v, got \n%#v", testCase.shards, gotShards)
		}
	}
}

func BenchmarkResolveKeyRangeToShards(b *testing.B) {
	ctx := context.Background()
	rs, err := initTopo("BenchmarkResolveKeyRangeToShards")
	if err != nil {
		b.Fatal(err)
	}

	kr := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x60, 0, 0, 0, 0, 0, 0, 0},
	}
	_, _, allShards, err := GetKeyspaceShards(ctx, rs, "", "sks", topodatapb.TabletType_MASTER)
	if err != nil {
		b.Fatal(err)
	}
	uniqueShards := map[string]bool{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ResolveKeyRangeToShards(allShards, uniqueShards, kr)
	}
}
