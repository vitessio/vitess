// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	if err != nil {
		t.Error(err)
	}
	want := "{}"
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, "test_keyspace", `{ "Sharded": true }`)
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx, "test_keyspace")
	if err != nil {
		t.Error(err)
	}
	want = `{ "Sharded": true }`
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, "test_keyspace", `{ "Sharded": false }`)
	if err != nil {
		t.Error(err)
	}

	got, err = ts.GetVSchema(ctx, "test_keyspace")
	if err != nil {
		t.Error(err)
	}
	want = `{ "Sharded": false }`
	if got != want {
		t.Errorf("GetVSchema: %s, want %s", got, want)
	}

	err = ts.SaveVSchema(ctx, "test_keyspace", "invalid")
	want = "Unmarshal failed:"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("SaveVSchema: %v, must start with %s", err, want)
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
	if !ok || vs != "{}" {
		t.Fatalf("first value is wrong: %v %v", vs, ok)
	}

	// update the VSchema, should get a notification
	newContents := `{ "Sharded": false }`
	if err := ts.SaveVSchema(ctx, keyspace, newContents); err != nil {
		t.Fatalf("SaveVSchema failed: %v", err)
	}
	for {
		vs, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if vs == "{}" {
			// duplicate notification of the first value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if vs != newContents {
			t.Fatalf("first value is wrong: got %v expected %v", vs, newContents)
		}
		break
	}

	// empty the VSchema, should get a notification
	if err := ts.SaveVSchema(ctx, keyspace, "{}"); err != nil {
		t.Fatalf("SaveVSchema failed: %v", err)
	}
	for {
		vs, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if vs == "{}" {
			break
		}

		// duplicate notification of the first value, that's OK,
		// but value better be good.
		if vs != newContents {
			t.Fatalf("duplicate notification value is bad: %v", vs)
		}
	}

	// re-create the value, a bit different, should get a notification
	newContents = `{ "Sharded": true }`
	if err := ts.SaveVSchema(ctx, keyspace, newContents); err != nil {
		t.Fatalf("SaveVSchema failed: %v", err)
	}
	for {
		vs, ok := <-notifications
		if !ok {
			t.Fatalf("watch channel is closed???")
		}
		if vs == "{}" {
			// duplicate notification of the closed value, that's OK
			continue
		}
		// non-empty value, that one should be ours
		if vs != newContents {
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
		if vs != newContents {
			t.Fatalf("duplicate notification value is bad: %v", vs)
		}
	}
}
