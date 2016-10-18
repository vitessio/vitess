package topovalidator

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the Keyspace validator. It uses GetKeyspaces to
// find all the keyspaces, and tries to read them. If any error occurs
// during the reading, it adds a fixer to either Delete or Create the
// keyspace.

// RegisterKeyspaceValidator registers the Keyspace Validator.
func RegisterKeyspaceValidator() {
	RegisterValidator("Keyspace Validator", &KeyspaceValidator{})
}

// KeyspaceValidator implements Validator.
type KeyspaceValidator struct{}

// Audit is part of the Validator interface.
func (kv *KeyspaceValidator) Audit(ctx context.Context, ts topo.Server, w *Workflow) error {
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return err
	}

	for _, keyspace := range keyspaces {
		_, err := ts.GetKeyspace(ctx, keyspace)
		if err != nil {
			w.AddFixer(keyspace, fmt.Sprintf("Error: %v", err), &KeyspaceFixer{
				ts:       ts,
				keyspace: keyspace,
			}, []string{"Create", "Delete"})
		}
	}
	return nil
}

// KeyspaceFixer implements Fixer.
type KeyspaceFixer struct {
	ts       topo.Server
	keyspace string
}

// Action is part of the Fixer interface.
func (kf *KeyspaceFixer) Action(ctx context.Context, name string) error {
	if name == "Create" {
		return kf.ts.CreateKeyspace(ctx, kf.keyspace, &topodatapb.Keyspace{})
	}
	if name == "Delete" {
		return kf.ts.DeleteKeyspace(ctx, kf.keyspace)
	}
	return fmt.Errorf("unknown KeyspaceFixer action: %v", name)
}
