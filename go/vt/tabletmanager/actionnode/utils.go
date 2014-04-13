// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package actionnode

// This file contains utility functions to be used with actionnode /
// topology server.

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	// DefaultLockTimeout is a good value to use as a default for
	// locking a shard / keyspace.
	DefaultLockTimeout = 15 * time.Second
)

// LockKeyspace will lock the keyspace in the topology server.
// UnlockKeyspace should be called if this returns no error.
func (n *ActionNode) LockKeyspace(ts topo.Server, keyspace string, lockTimeout time.Duration, interrupted chan struct{}) (lockPath string, err error) {
	log.Infof("Locking keyspace %v for action %v", keyspace, n.Action)
	return ts.LockKeyspaceForAction(keyspace, n.ToJson(), lockTimeout, interrupted)
}

// UnlockKeyspace unlocks a previously locked keyspace.
func (n *ActionNode) UnlockKeyspace(ts topo.Server, keyspace string, lockPath string, actionError error) error {
	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking keyspace %v for action %v with error %v", keyspace, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ACTION_STATE_FAILED
	} else {
		log.Infof("Unlocking keyspace %v for successful action %v", keyspace, n.Action)
		n.Error = ""
		n.State = ACTION_STATE_DONE
	}
	err := ts.UnlockKeyspaceForAction(keyspace, lockPath, n.ToJson())
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
func (n *ActionNode) LockShard(ts topo.Server, keyspace, shard string, lockTimeout time.Duration, interrupted chan struct{}) (lockPath string, err error) {
	log.Infof("Locking shard %v/%v for action %v", keyspace, shard, n.Action)
	return ts.LockShardForAction(keyspace, shard, n.ToJson(), lockTimeout, interrupted)
}

// UnlockShard unlocks a previously locked shard.
func (n *ActionNode) UnlockShard(ts topo.Server, keyspace, shard string, lockPath string, actionError error) error {
	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking shard %v/%v for action %v with error %v", keyspace, shard, n.Action, actionError)
		n.Error = actionError.Error()
		n.State = ACTION_STATE_FAILED
	} else {
		log.Infof("Unlocking shard %v/%v for successful action %v", keyspace, shard, n.Action)
		n.Error = ""
		n.State = ACTION_STATE_DONE
	}
	err := ts.UnlockShardForAction(keyspace, shard, lockPath, n.ToJson())
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockShardForAction failed: %v", err)
		}
		return actionError
	}
	return err
}
