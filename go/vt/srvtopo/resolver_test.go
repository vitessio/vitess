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
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// fakeStats implements TargetStats.
type fakeStats struct{}

func (s *fakeStats) GetAggregateStats(target *querypb.Target) (*querypb.AggregateStats, queryservice.QueryService, error) {
	return &querypb.AggregateStats{}, nil, nil
}

func (s *fakeStats) GetMasterCell(keyspace, shard string) (cell string, qs queryservice.QueryService, err error) {
	return "", nil, nil
}

func initResolver(t *testing.T, name string) *Resolver {
	ctx := context.Background()
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	rs := NewResilientServer(name, ts)

	// Create sharded keyspace and shards.
	if err := ts.CreateKeyspace(ctx, "sks", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace(sks) failed: %v", err)
	}
	shardKrArray, err := key.ParseShardingSpec("-20-40-60-80-a0-c0-e0-")
	if err != nil {
		t.Fatalf("key.ParseShardingSpec failed: %v", err)
	}
	for _, kr := range shardKrArray {
		shard := key.KeyRangeString(kr)
		if err := ts.CreateShard(ctx, "sks", shard); err != nil {
			t.Fatalf("CreateShard(\"%v\") failed: %v", shard, err)
		}
	}

	// Create unsharded keyspace and shard.
	if err := ts.CreateKeyspace(ctx, "uks", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace(uks) failed: %v", err)
	}
	if err := ts.CreateShard(ctx, "uks", "0"); err != nil {
		t.Fatalf("CreateShard(0) failed: %v", err)
	}

	// And rebuild both.
	for _, keyspace := range []string{"sks", "uks"} {
		if err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, keyspace, []string{cell}); err != nil {
			t.Fatalf("RebuildKeyspace(%v) failed: %v", keyspace, err)
		}
	}

	return NewResolver(rs, &fakeStats{}, cell)
}

func TestResolveDestinations(t *testing.T) {
	resolver := initResolver(t, "TestResolveDestinations")

	id1 := &querypb.Value{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}
	id2 := &querypb.Value{
		Type:  sqltypes.VarChar,
		Value: []byte("2"),
	}

	kr2040 := &topodatapb.KeyRange{
		Start: []byte{0x20},
		End:   []byte{0x40},
	}
	kr80a0 := &topodatapb.KeyRange{
		Start: []byte{0x80},
		End:   []byte{0xa0},
	}
	kr2830 := &topodatapb.KeyRange{
		Start: []byte{0x28},
		End:   []byte{0x30},
	}

	var testCases = []struct {
		name           string
		keyspace       string
		ids            []*querypb.Value
		destinations   []key.Destination
		errString      string
		expectedShards []string
		expectedValues [][]*querypb.Value
	}{
		{
			name:     "unsharded keyspace, regular shard, no ids",
			keyspace: "uks",
			destinations: []key.Destination{
				key.DestinationShard("0"),
			},
			expectedShards: []string{"0"},
		},
		{
			name:     "unsharded keyspace, regular shard, with ids",
			keyspace: "uks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.Destination{
				key.DestinationShard("0"),
				key.DestinationShard("0"),
			},
			expectedShards: []string{"0"},
			expectedValues: [][]*querypb.Value{
				{id1, id2},
			},
		},
		{
			name:     "sharded keyspace, keyrange destinations, with ids",
			keyspace: "sks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.Destination{
				key.DestinationExactKeyRange{KeyRange: kr2040},
				key.DestinationExactKeyRange{KeyRange: kr80a0},
			},
			expectedShards: []string{"20-40", "80-a0"},
			expectedValues: [][]*querypb.Value{
				{id1},
				{id2},
			},
		},
		{
			name:     "sharded keyspace, keyspace id destinations, with ids",
			keyspace: "sks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.Destination{
				key.DestinationKeyspaceID{0x28},
				key.DestinationKeyspaceID{0x78, 0x23},
			},
			expectedShards: []string{"20-40", "60-80"},
			expectedValues: [][]*querypb.Value{
				{id1},
				{id2},
			},
		},
		{
			name:     "sharded keyspace, multi keyspace id destinations, with ids",
			keyspace: "sks",
			ids:      []*querypb.Value{id1, id2},
			destinations: []key.Destination{
				key.DestinationKeyspaceIDs{
					{0x28},
					{0x47},
				},
				key.DestinationKeyspaceIDs{
					{0x78},
					{0x23},
				},
			},
			expectedShards: []string{"20-40", "40-60", "60-80"},
			expectedValues: [][]*querypb.Value{
				{id1, id2},
				{id1},
				{id2},
			},
		},
		{
			name:     "using non-mapping keyranges should fail",
			keyspace: "sks",
			destinations: []key.Destination{
				key.DestinationExactKeyRange{
					KeyRange: kr2830,
				},
			},
			errString: "keyrange 28-30 does not exactly match shards",
		},
	}
	for _, testCase := range testCases {
		ctx := context.Background()
		rss, values, err := resolver.ResolveDestinations(ctx, testCase.keyspace, topodatapb.TabletType_REPLICA, testCase.ids, testCase.destinations)
		if err != nil {
			if testCase.errString == "" {
				t.Errorf("%v: expected success but got error: %v", testCase.name, err)
			} else {
				if err.Error() != testCase.errString {
					t.Errorf("%v: expected error '%v' but got error: %v", testCase.name, testCase.errString, err)
				}
			}
			continue
		}

		if testCase.errString != "" {
			t.Errorf("%v: expected error '%v' but got success", testCase.name, testCase.errString)
			continue
		}

		// Check the ResolvedShard are correct.
		if len(rss) != len(testCase.expectedShards) {
			t.Errorf("%v: expected %v ResolvedShard, but got: %v", testCase.name, len(testCase.expectedShards), rss)
			continue
		}
		badShards := false
		for i, rs := range rss {
			if rs.Target.Shard != testCase.expectedShards[i] {
				t.Errorf("%v: expected rss[%v] to be '%v', but got: %v", testCase.name, i, testCase.expectedShards[i], rs.Target.Shard)
				badShards = true
			}
		}
		if badShards {
			continue
		}

		// Check the values are correct, if we passed some in.
		if testCase.ids == nil {
			continue
		}
		if len(values) != len(rss) {
			t.Errorf("%v: len(values) != len(rss): %v != %v", testCase.name, len(values), len(rss))
		}
		if !ValuesEqual(values, testCase.expectedValues) {
			t.Errorf("%v: values != testCase.expectedValues: got values=%v", testCase.name, values)
		}
	}
}
