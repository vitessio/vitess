package topovalidator

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	if _, err := ts.Impl.Update(ctx, topo.GlobalCell, path.Join("/keyspaces", keyspace, "shards", shard, "Shard"), []byte{'a'}, nil); err != nil {
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
