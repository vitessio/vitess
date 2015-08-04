// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateShard will create the shard, while holding the keyspace lock
func CreateShard(ctx context.Context, ts topo.Server, keyspace, shard string) error {
	// Lock the keyspace
	node := actionnode.KeyspaceCreateShard()
	lockPath, err := node.LockKeyspace(ctx, ts, keyspace)
	if err != nil {
		return fmt.Errorf("LockKeyspace failed: %v", err)
	}

	// now try to create within the lock, may already exist
	err = topo.CreateShard(ctx, ts, keyspace, shard)

	// and unlock and return
	return node.UnlockKeyspace(ctx, ts, keyspace, lockPath, err)
}

// GetOrCreateShard will return the shard object, or create one if it doesn't
// already exist. Note the shard creation is protected by a keyspace Lock.
func GetOrCreateShard(ctx context.Context, ts topo.Server, keyspace, shard string) (*topo.ShardInfo, error) {
	si, finalErr := ts.GetShard(ctx, keyspace, shard)
	if finalErr == topo.ErrNoNode {
		// create the keyspace, maybe it already exists
		if err := ts.CreateKeyspace(ctx, keyspace, &pb.Keyspace{}); err != nil && err != topo.ErrNodeExists {
			return nil, fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
		}

		// now we can lock the keyspace
		node := actionnode.KeyspaceCreateShard()
		lockPath, err := node.LockKeyspace(ctx, ts, keyspace)
		if err != nil {
			return nil, fmt.Errorf("LockKeyspace failed: %v", err)
		}

		// now try to create within the lock, may already exist
		if err := topo.CreateShard(ctx, ts, keyspace, shard); err != nil && err != topo.ErrNodeExists {
			return nil, node.UnlockKeyspace(ctx, ts, keyspace, lockPath, fmt.Errorf("CreateShard(%v/%v) failed: %v", keyspace, shard, err))
		}

		// try to read the shard again, maybe someone created it
		// in between the original GetShard and the LockKeyspace
		si, finalErr = ts.GetShard(ctx, keyspace, shard)

		// and unlock
		if err := node.UnlockKeyspace(ctx, ts, keyspace, lockPath, finalErr); err != nil {
			return nil, fmt.Errorf("UnlockKeyspace failed: %v", err)
		}
	}
	return si, finalErr
}
