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

	// test GetShardNames
	shards, err := ts.GetShardNames(ctx, "test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 || shards[0] != "b0-c0" {
		t.Errorf(`GetShardNames: want [ "b0-c0" ], got %v`, shards)
	}
}
