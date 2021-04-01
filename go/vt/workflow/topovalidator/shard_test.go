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

package topovalidator

import (
	"path"
	"strings"
	"testing"

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains tests for the shard.go file.

func TestShard(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	shard := "sh1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// Create a Keyspace / Shard
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}

	// Hack the zookeeper backend to create an error for GetShard.
	// 'a' is not a valid proto-encoded value.
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell() failed: %v", err)
	}
	if _, err := conn.Update(ctx, path.Join("/keyspaces", keyspace, "shards", shard, "Shard"), []byte{'a'}, nil); err != nil {
		t.Fatalf("failed to hack the shard: %v", err)
	}

	// Create the workflow, run the validator.
	w := &Workflow{
		logger: logutil.NewMemoryLogger(),
	}
	sv := &ShardValidator{}
	if err := sv.Audit(ctx, ts, w); err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(w.fixers) != 1 {
		t.Fatalf("fixer not added: %v", w.fixers)
	}
	if !strings.Contains(w.fixers[0].message, "bad shard data") {
		t.Errorf("bad message: %v ", w.fixers[0].message)
	}

	// Run Delete, make sure the entry is removed.
	if err := w.fixers[0].fixer.Action(ctx, "Delete"); err != nil {
		t.Fatalf("Action failed: %v", err)
	}
	shards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil || len(shards) != 0 {
		t.Errorf("bad GetShardNames output: %v %v ", shards, err)
	}
}
