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
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// checkVSchema runs the tests on the VSchema part of the API
func checkVSchema(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	_, err := ts.GetVSchema(ctx, "test_keyspace")
	if !topo.IsErrType(err, topo.NoNode) {
		t.Error(err)
	}

	err = ts.SaveVSchema(ctx, "test_keyspace", &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"unsharded": {},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetVSchema(ctx, "test_keyspace")
	if err != nil {
		t.Error(err)
	}
	want := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"unsharded": {},
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

// checkRoutingRules runs the tests on the routing rules part of the API
func checkRoutingRules(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	if _, err := ts.GetRoutingRules(ctx); err != nil {
		t.Fatal(err)
	}

	want := &vschemapb.RoutingRules{
		Rules: []*vschemapb.RoutingRule{{
			FromTable: "t1",
			ToTables:  []string{"t2", "t3"},
		}},
	}
	if err := ts.SaveRoutingRules(ctx, want); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetRoutingRules(ctx)
	if err != nil {
		t.Error(err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetRoutingRules: %v, want %v", got, want)
	}
}
