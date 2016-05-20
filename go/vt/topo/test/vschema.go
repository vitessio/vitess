// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// CheckVSchema runs the tests on the VSchema part of the API
func CheckVSchema(ctx context.Context, t *testing.T, ts topo.Impl) {
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
	if err != nil {
		t.Error(err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetVSchema: %s, want nil", got)
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

	err = ts.SaveVSchema(ctx, "test_keyspace", &vschemapb.Keyspace{})
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx, "test_keyspace")
	if err != nil {
		t.Error(err)
	}
	want = &vschemapb.Keyspace{}
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

// CheckWatchVSchema makes sure WatchVSchema works as expected
func CheckWatchVSchema(ctx context.Context, t *testing.T, ts topo.Impl) {
	keyspace := "test_keyspace"

	// start watching, should get nil first
	ctx, cancel := context.WithCancel(ctx)
	notifications, err := ts.WatchVSchema(ctx, keyspace)
	if err != nil {
		t.Fatalf("WatchVSchema failed: %v", err)
	}
	vs, ok := <-notifications
	if !ok || vs != nil {
		t.Fatalf("first value is wrong: %v %v", vs, ok)
	}

	// update the VSchema, should get a notification
	newContents := &vschemapb.Keyspace{}
	if err := ts.SaveVSchema(ctx, keyspace, newContents); err != nil {
		t.Fatalf("SaveVSchema failed: %v", err)
	}
	for {
		vs, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if vs == nil {
			// duplicate notification of the first value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !proto.Equal(vs, newContents) {
			t.Fatalf("first value is wrong: got %v expected %v", vs, newContents)
		}
		break
	}

	// empty the VSchema, should get a notification
	if err := ts.SaveVSchema(ctx, keyspace, nil); err != nil {
		t.Fatalf("SaveVSchema failed: %v", err)
	}
	for {
		vs, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if vs == nil {
			break
		}

		// duplicate notification of the first value, that's OK,
		// but value better be good.
		if !proto.Equal(vs, newContents) {
			t.Fatalf("duplicate notification value is bad: %v", vs)
		}
	}

	// re-create the value, a bit different, should get a notification
	newContents = &vschemapb.Keyspace{Sharded: true}
	if err := ts.SaveVSchema(ctx, keyspace, newContents); err != nil {
		t.Fatalf("SaveVSchema failed: %v", err)
	}
	for {
		vs, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if vs == nil {
			// duplicate notification of the closed value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if !proto.Equal(vs, newContents) {
			t.Fatalf("value after delete / re-create is wrong: %v", vs)
		}
		break
	}

	// close the context, should eventually get a closed
	// notifications channel too
	cancel()
	for {
		vs, ok := <-notifications
		if !ok {
			break
		}
		if !proto.Equal(vs, newContents) {
			t.Fatalf("duplicate notification value is bad: %v", vs)
		}
	}
}
