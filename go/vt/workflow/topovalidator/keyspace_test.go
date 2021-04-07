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

	// Hack the topology backend to create an error for GetKeyspace.
	// 'a' is not a valid proto-encoded value.
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell() failed: %v", err)
	}
	if _, err := conn.Update(ctx, path.Join("/keyspaces", keyspace, "Keyspace"), []byte{'a'}, nil); err != nil {
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
