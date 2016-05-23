// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateShard will create the shard, while holding the keyspace lock
func CreateShard(ctx context.Context, ts topo.Server, keyspace, shard string) (err error) {
	// Lock the keyspace, because we're looking at ServedTypes.
	actionNode := actionnode.KeyspaceCreateShard()
	ctx, unlock, lockErr := actionNode.LockKeyspaceContext(ctx, ts, keyspace)
	if lockErr != nil {
		return lockErr
	}
	defer unlock(ctx, &err)

	// and try to create within the lock, may already exist
	return ts.CreateShard(ctx, keyspace, shard)
}

// GetOrCreateShard will return the shard object, or create one if it doesn't
// already exist. Note the shard creation is protected by a keyspace Lock.
func GetOrCreateShard(ctx context.Context, ts topo.Server, keyspace, shard string) (si *topo.ShardInfo, err error) {
	si, err = ts.GetShard(ctx, keyspace, shard)
	if err != topo.ErrNoNode {
		return
	}

	// create the keyspace, maybe it already exists
	if err = ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil && err != topo.ErrNodeExists {
		return nil, fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
	}

	// now we can lock the keyspace
	actionNode := actionnode.KeyspaceCreateShard()
	ctx, unlock, lockErr := actionNode.LockKeyspaceContext(ctx, ts, keyspace)
	if lockErr != nil {
		return nil, fmt.Errorf("LockKeyspace failed: %v", lockErr)
	}
	defer unlock(ctx, &err)

	// now try to create within the lock, may already exist
	if err = ts.CreateShard(ctx, keyspace, shard); err != nil && err != topo.ErrNodeExists {
		return nil, fmt.Errorf("CreateShard(%v/%v) failed: %v", keyspace, shard, err)
	}

	// try to read the shard again, maybe someone created it
	// in between the original GetShard and the LockKeyspace
	return ts.GetShard(ctx, keyspace, shard)
}
