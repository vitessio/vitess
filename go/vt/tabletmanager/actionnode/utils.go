// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

// This file contains utility functions to be used with actionnode /
// topology server.

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

var (
	// DefaultLockTimeout is a good value to use as a default for
	// locking a shard / keyspace.
	DefaultLockTimeout = 30 * time.Second
)

// LockKeyspace will lock the keyspace in the topology server.
// UnlockKeyspace should be called if this returns no error.
func (n *ActionNode) LockKeyspace(ctx context.Context, ts topo.Server, keyspace string) (lockPath string, err error) {
	log.Infof("Locking keyspace %v for action %v", keyspace, n.Action)

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockKeyspaceForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	j, err := n.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockKeyspaceForAction(ctx, keyspace, j)
}

// UnlockKeyspace unlocks a previously locked keyspace.
func (n *ActionNode) UnlockKeyspace(ctx context.Context, ts topo.Server, keyspace string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockKeyspaceForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking keyspace %v for action %v with error %v", keyspace, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ActionStateFailed
	} else {
		log.Infof("Unlocking keyspace %v for successful action %v", keyspace, n.Action)
		n.Error = ""
		n.State = ActionStateDone
	}
	j, err := n.ToJSON()
	if err != nil {
		if actionError != nil {
			// this will be masked
			log.Warningf("node.ToJSON failed: %v", err)
			return actionError
		}
		return err
	}
	err = ts.UnlockKeyspaceForAction(ctx, keyspace, lockPath, j)
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockKeyspaceForAction failed: %v", err)
		}
		return actionError
	}
	return err
}

// LockShard will lock the shard in the topology server.
// UnlockShard should be called if this returns no error.
func (n *ActionNode) LockShard(ctx context.Context, ts topo.Server, keyspace, shard string) (lockPath string, err error) {
	log.Infof("Locking shard %v/%v for action %v", keyspace, shard, n.Action)

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockShardForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	j, err := n.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockShardForAction(ctx, keyspace, shard, j)
}

// UnlockShard unlocks a previously locked shard.
func (n *ActionNode) UnlockShard(ctx context.Context, ts topo.Server, keyspace, shard string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockShardForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking shard %v/%v for action %v with error %v", keyspace, shard, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ActionStateFailed
	} else {
		log.Infof("Unlocking shard %v/%v for successful action %v", keyspace, shard, n.Action)
		n.Error = ""
		n.State = ActionStateDone
	}
	j, err := n.ToJSON()
	if err != nil {
		if actionError != nil {
			// this will be masked
			log.Warningf("node.ToJSON failed: %v", err)
			return actionError
		}
		return err
	}
	err = ts.UnlockShardForAction(ctx, keyspace, shard, lockPath, j)
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockShardForAction failed: %v", err)
		}
		return actionError
	}
	return err
}

// LockSrvShard will lock the serving shard in the topology server.
// UnlockSrvShard should be called if this returns no error.
func (n *ActionNode) LockSrvShard(ctx context.Context, ts topo.Server, cell, keyspace, shard string) (lockPath string, err error) {
	log.Infof("Locking serving shard %v/%v/%v for action %v", cell, keyspace, shard, n.Action)

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.LockSrvShardForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("cell", cell)
	defer span.Finish()

	j, err := n.ToJSON()
	if err != nil {
		return "", err
	}
	return ts.LockSrvShardForAction(ctx, cell, keyspace, shard, j)
}

// UnlockSrvShard unlocks a previously locked serving shard.
func (n *ActionNode) UnlockSrvShard(ctx context.Context, ts topo.Server, cell, keyspace, shard string, lockPath string, actionError error) error {
	// Detach from the parent timeout, but copy the trace span.
	// We need to still release the lock even if the parent context timed out.
	ctx = trace.CopySpan(context.TODO(), ctx)
	ctx, cancel := context.WithTimeout(ctx, DefaultLockTimeout)
	defer cancel()

	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UnlockSrvShardForAction")
	span.Annotate("action", n.Action)
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("cell", cell)
	defer span.Finish()

	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking serving shard %v/%v/%v for action %v with error %v", cell, keyspace, shard, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ActionStateFailed
	} else {
		log.Infof("Unlocking serving shard %v/%v/%v for successful action %v", cell, keyspace, shard, n.Action)
		n.Error = ""
		n.State = ActionStateDone
	}
	j, err := n.ToJSON()
	if err != nil {
		if actionError != nil {
			// this will be masked
			log.Warningf("node.ToJSON failed: %v", err)
			return actionError
		}
		return err
	}
	err = ts.UnlockSrvShardForAction(ctx, cell, keyspace, shard, lockPath, j)
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockSrvShardForAction failed: %v", err)
		}
		return actionError
	}
	return err
}
