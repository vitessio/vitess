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

package test

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// checkVSchema runs the tests on the VSchema part of the API
func checkVSchema(t *testing.T, ts topo.Impl) {
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	shard := &topodatapb.Shard{
		KeyRange: newKeyRange("b0-c0"),
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0", shard); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	got, err := ts.GetVSchema(ctx, "test_keyspace")
	want := &vschemapb.Keyspace{}
	if err != topo.ErrNoNode {
		t.Error(err)
	}

	err = ts.SaveVSchema(ctx, "test_keyspace", &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"stfu1": {
				Type: "stfu",
				Params: map[string]string{
					"stfu1": "1",
				},
				Owner: "t1",
			},
			"stln1": {
				Type:  "stln",
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "c1",
						Name:   "stfu1",
					}, {
						Column: "c2",
						Name:   "stln1",
					},
				},
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx, "test_keyspace")
	if err != nil {
		t.Error(err)
	}
	want = &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"stfu1": {
				Type: "stfu",
				Params: map[string]string{
					"stfu1": "1",
				},
				Owner: "t1",
			},
			"stln1": {
				Type:  "stln",
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "c1",
						Name:   "stfu1",
					}, {
						Column: "c2",
						Name:   "stln1",
					},
				},
			},
		},
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, "test_keyspace", &vschemapb.Keyspace{
		Sharded: true,
	})
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx, "test_keyspace")
	if err != nil {
		t.Error(err)
	}
	want = &vschemapb.Keyspace{
		Sharded: true,
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	// Make sure the vschema is not returned as a shard name,
	// because they share the same directory location.
	shards, err := ts.GetShardNames(ctx, "test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 || shards[0] != "b0-c0" {
		t.Errorf(`GetShardNames: want [ "b0-c0" ], got %v`, shards)
	}
}
