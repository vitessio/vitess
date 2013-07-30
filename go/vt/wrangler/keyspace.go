// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"github.com/youtube/vitess/go/relog"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
)

// keyspace related methods for Wrangler

func (wr *Wrangler) lockKeyspace(keyspace string, actionNode *tm.ActionNode) (lockPath string, err error) {
	relog.Info("Locking keyspace %v for action %v", keyspace, actionNode.Action)
	return wr.ts.LockKeyspaceForAction(keyspace, tm.ActionNodeToJson(actionNode), wr.lockTimeout, interrupted)
}

func (wr *Wrangler) unlockKeyspace(keyspace string, actionNode *tm.ActionNode, lockPath string, actionError error) error {
	// first update the actionNode
	if actionError != nil {
		relog.Info("Unlocking keyspace %v for action %v with error %v", keyspace, actionNode.Action, actionError)
		actionNode.Error = actionError.Error()
		actionNode.State = tm.ACTION_STATE_FAILED
	} else {
		relog.Info("Unlocking keyspace %v for successful action %v", keyspace, actionNode.Action)
		actionNode.Error = ""
		actionNode.State = tm.ACTION_STATE_DONE
	}
	err := wr.ts.UnlockKeyspaceForAction(keyspace, lockPath, tm.ActionNodeToJson(actionNode))
	if actionError != nil {
		if err != nil {
			// this will be masked
			relog.Warning("UnlockKeyspaceForAction failed: %v", err)
		}
		return actionError
	}
	return err
}
