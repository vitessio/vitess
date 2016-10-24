package topovalidator

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the Shard validator. It uses GetKeyspaces to
// find all the keyspaces, then uses GetShardNames to read all shards, then tries to read them. If any error occurs
// during the reading, it adds a fixer to either Delete or Create the
// shard.

// RegisterShardValidator registers the Shard Validator.
func RegisterShardValidator() {
	RegisterValidator("Shard Validator", &ShardValidator{})
}

// ShardValidator implements Validator.
type ShardValidator struct{}

// Audit is part of the Validator interface.
func (kv *ShardValidator) Audit(ctx context.Context, ts topo.Server, w *Workflow) error {
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return err
	}

	for _, keyspace := range keyspaces {
		shards, err := ts.GetShardNames(ctx, keyspace)
		if err != nil {
			return err
		}
		for _, shard := range shards {
			_, err := ts.GetShard(ctx, keyspace, shard)
			if err != nil {
				w.AddFixer(fmt.Sprintf("%v/%v", keyspace, shard), fmt.Sprintf("Error: %v", err), &ShardFixer{
					ts:       ts,
					keyspace: keyspace,
					shard:    shard,
				}, []string{"Create", "Delete"})
			}
		}
	}
	return nil
}

// ShardFixer implements Fixer.
type ShardFixer struct {
	ts       topo.Server
	keyspace string
	shard    string
}

// Action is part of the Fixer interface.
func (sf *ShardFixer) Action(ctx context.Context, name string) error {
	if name == "Create" {
		return sf.ts.CreateShard(ctx, sf.keyspace, sf.shard)
	}
	if name == "Delete" {
		return sf.ts.DeleteShard(ctx, sf.keyspace, sf.shard)
	}
	return fmt.Errorf("unknown ShardFixer action: %v", name)
}
