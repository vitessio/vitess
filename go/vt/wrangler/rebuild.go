// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"fmt"

	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

// Rebuild the serving data while locking out other changes.
func (wr *Wrangler) RebuildShard(zkShardPath string) (actionPath string, err error) {
	tm.MustBeShardPath(zkShardPath)

	actionPath, err = wr.ai.RebuildShard(zkShardPath)
	if err != nil {
		return
	}

	// Make sure two of these don't get scheduled at the same time.
	ok, err := zk.ObtainQueueLock(wr.zconn, actionPath, false)
	if err != nil {
		return
	}

	if !ok {
		// just clean up for now, in the future we may want to try harder, or wait
		wr.zconn.Delete(actionPath, -1)
		panic(fmt.Errorf("failed to obtain action lock: %v", actionPath))
	}

	rebuildErr := tm.RebuildShard(wr.zconn, zkShardPath)

	if err == nil {
		err = zk.DeleteRecursive(wr.zconn, actionPath, -1)
	} else {
		data, stat, err := wr.zconn.Get(actionPath)
		if err == nil {
			var actionNode *tm.ActionNode
			actionNode, err = tm.ActionNodeFromJson(data, actionPath)
			if err == nil {
				actionNode.Error = rebuildErr.Error()
				data = tm.ActionNodeToJson(actionNode)
				_, err = wr.zconn.Set(actionPath, data, stat.Version())
			}
		}
	}

	if rebuildErr != nil {
		return actionPath, rebuildErr
	}

	return
}

// Rebuild the serving data while locking out other changes.
// FIXME(msolomon) reduce copy-paste code from above
func (wr *Wrangler) RebuildKeyspace(zkKeyspacePath string) (actionPath string, err error) {
	tm.MustBeKeyspacePath(zkKeyspacePath)

	actionPath, err = wr.ai.RebuildKeyspace(zkKeyspacePath)
	if err != nil {
		return
	}

	// Make sure two of these don't get scheduled at the same time.
	ok, err := zk.ObtainQueueLock(wr.zconn, actionPath, false)
	if err != nil {
		return
	}

	if !ok {
		// just clean up for now, in the future we may want to try harder, or wait
		wr.zconn.Delete(actionPath, -1)
		panic(fmt.Errorf("failed to obtain action lock: %v", actionPath))
	}

	rebuildErr := tm.RebuildKeyspace(wr.zconn, zkKeyspacePath)

	if err == nil {
		err = zk.DeleteRecursive(wr.zconn, actionPath, -1)
	} else {
		data, stat, err := wr.zconn.Get(actionPath)
		if err == nil {
			var actionNode *tm.ActionNode
			actionNode, err = tm.ActionNodeFromJson(data, actionPath)
			if err == nil {
				actionNode.Error = rebuildErr.Error()
				data = tm.ActionNodeToJson(actionNode)
				_, err = wr.zconn.Set(actionPath, data, stat.Version())
			}
		}
	}

	if rebuildErr != nil {
		return actionPath, rebuildErr
	}

	return
}
