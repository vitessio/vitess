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

// This file contains tests for the keyspace.go file.

func TestKeyspace(t *testing.T) {
	cell := "cell1"
	keyspace := "ks1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// Create a Keyspace
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	// Hack the zookeeper backend to create an error for GetKeyspace.
	// 'a' is not a valid proto-encoded value.
	if _, err := ts.Impl.Update(ctx, topo.GlobalCell, path.Join("/keyspaces", keyspace, "Keyspace"), []byte{'a'}, nil); err != nil {
		t.Fatalf("failed to hack the keyspace: %v", err)
	}

	// Create the workflow, run the validator.
	w := &Workflow{
		logger: logutil.NewMemoryLogger(),
	}
	kv := &KeyspaceValidator{}
	if err := kv.Audit(ctx, ts, w); err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(w.fixers) != 1 {
		t.Fatalf("fixer not added: %v", w.fixers)
	}
	if !strings.Contains(w.fixers[0].message, "bad keyspace data") {
		t.Errorf("bad message: %v ", w.fixers[0].message)
	}

	// Run Delete, make sure the entry is removed.
	if err := w.fixers[0].fixer.Action(ctx, "Delete"); err != nil {
		t.Fatalf("Action failed: %v", err)
	}
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil || len(keyspaces) != 0 {
		t.Errorf("bad GetKeyspaces output: %v %v ", keyspaces, err)
	}
}
