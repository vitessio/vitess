// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"path"
	"time"

	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

const (
	DefaultActionTimeout = 30 * time.Second
	DefaultLockTimeout   = 30 * time.Second
)

type Wrangler struct {
	zconn       zk.Conn
	ai          *tm.ActionInitiator
	deadline    time.Time
	lockTimeout time.Duration
}

// actionTimeout: how long should we wait for an action to complete?
// lockTimeout: how long should we wait for the initial lock to start a complex action?
//   This is distinct from actionTimeout because most of the time, we want to immediately
//   know that out action will fail. However, automated action will need some time to
//   arbitrate the locks.
func NewWrangler(zconn zk.Conn, actionTimeout, lockTimeout time.Duration) *Wrangler {
	return &Wrangler{zconn, tm.NewActionInitiator(zconn), time.Now().Add(actionTimeout), lockTimeout}
}

func (wr *Wrangler) actionTimeout() time.Duration {
	return wr.deadline.Sub(time.Now())
}

func (wr *Wrangler) readTablet(zkTabletPath string) (*tm.TabletInfo, error) {
	return tm.ReadTablet(wr.zconn, zkTabletPath)
}

func (wr *Wrangler) ZkConn() zk.Conn {
	return wr.zconn
}

func (wr *Wrangler) ActionInitiator() *tm.ActionInitiator {
	return wr.ai
}

// Change the type of tablet and recompute all necessary derived paths in the
// serving graph.
// force: Bypass the vtaction system and make the data change directly, and
// do not run the idle_server_check nor live_server_check hooks
func (wr *Wrangler) ChangeType(zkTabletPath string, dbType tm.TabletType, force bool) error {
	// Load tablet to find keyspace and shard assignment.
	// Don't load after the ChangeType which might have unassigned
	// the tablet.
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	if force {
		// with --force, we do not run any hook
		err = tm.ChangeType(wr.zconn, zkTabletPath, dbType, false)
	} else {
		// the remote action will run the hooks
		var actionPath string
		actionPath, err = wr.ai.ChangeType(zkTabletPath, dbType)
		// You don't have a choice - you must wait for
		// completion before rebuilding.
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, DefaultActionTimeout)
		}
	}

	if err != nil {
		return err
	}

	// we rebuild if the tablet was serving, or if it is now
	var shardToRebuild string
	if rebuildRequired {
		shardToRebuild = ti.ShardPath()
	} else {
		// re-read the tablet, see if we become serving
		ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
		if err != nil {
			return err
		}
		if ti.Tablet.IsServingType() {
			rebuildRequired = true
			shardToRebuild = ti.ShardPath()
		}
	}

	if rebuildRequired {
		if _, err := wr.RebuildShardGraph(shardToRebuild); err != nil {
			return err
		}
	}
	return nil
}

// same as ChangeType, but assume we already have the shard lock,
// and do not have the option to force anything
// FIXME(alainjobart): doesn't rebuild the Keyspace, as that part has locks,
// so the local serving graphs will be wrong. To do that, I need to refactor
// some code, might be a bigger change.
// Mike says: Updating the shard should be good enough. I'm debating dropping the entire
// keyspace rollup, since I think that is adding complexity and feels like it might
// be a premature optimization.
func (wr *Wrangler) changeTypeInternal(zkTabletPath string, dbType tm.TabletType) error {
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	// change the type
	actionPath, err := wr.ai.ChangeType(ti.Path(), dbType)
	if err != nil {
		return err
	}
	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	if err != nil {
		return err
	}

	// rebuild if necessary
	if rebuildRequired {
		err = wr.rebuildShard(ti.ShardPath(), false)
		if err != nil {
			return err
		}
		// FIXME(alainjobart) We already have the lock on one shard, so this is not
		// possible. But maybe it's not necessary anyway.
		// We could pass in a shard path we already have the lock on, and skip it?
		//		err = wr.rebuildKeyspace(ti.KeyspacePath())
		//		if err != nil {
		//			return err
		//		}
	}
	return nil
}

// Wait for the queue lock, displays a nice error message if we cant get it
func (wr *Wrangler) obtainActionLock(actionPath string) error {
	err := zk.ObtainQueueLock(wr.zconn, actionPath, wr.lockTimeout)
	if err != nil {
		errToReturn := fmt.Errorf("failed to obtain action lock: %v", actionPath)

		// Regardless of the reason, try to cleanup.
		relog.Warning("Failed to obtain action lock: %v", err)
		wr.zconn.Delete(actionPath, -1)

		// Show the other actions in the directory
		dir := path.Dir(actionPath)
		children, _, err := wr.zconn.Children(dir)
		if err != nil {
			relog.Warning("Failed to get children of %v: %v", dir, err)
			return errToReturn
		}

		if len(children) == 0 {
			relog.Warning("No other action running, you may just try again now.")
			return errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := wr.zconn.Get(childPath)
		if err != nil {
			relog.Warning("Failed to get first action node %v (may have just ended): %v", childPath, err)
			return errToReturn
		}

		actionNode, err := tm.ActionNodeFromJson(data, childPath)
		if err != nil {
			relog.Warning("Failed to parse ActionNode %v: %v\n%v", childPath, err, data)
			return errToReturn
		}

		relog.Warning("------ Most likely blocking action: %v %v from %v", actionNode.Action, childPath, actionNode.ActionGuid)
		return errToReturn
	}

	return nil
}

// Cleanup an action node and write back status/error to zk.
// Only returns an error if something went wrong with zk.
func (wr *Wrangler) handleActionError(actionPath string, actionErr error, blockQueueOnError bool) error {
	// re-read the action node
	data, _, err := wr.zconn.Get(actionPath)
	if err != nil {
		return err
	}
	var actionNode *tm.ActionNode
	actionNode, err = tm.ActionNodeFromJson(data, actionPath)
	if err != nil {
		return err
	}

	// write what happened to the action log
	err = tm.StoreActionResponse(wr.zconn, actionNode, actionPath, actionErr)
	if err != nil {
		return err
	}

	// no error, we can unblock the action queue
	if actionErr == nil || !blockQueueOnError {
		return zk.DeleteRecursive(wr.zconn, actionPath, -1)
	}
	return nil
}

// Scrap a tablet. If force is used, we write to ZK directly and don't
// remote-execute the command.
func (wr *Wrangler) Scrap(zkTabletPath string, force, skipRebuild bool) (actionPath string, err error) {
	// load the tablet, see if we'll need to rebuild
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return "", err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	if force {
		err = tm.Scrap(wr.zconn, zkTabletPath, force)
	} else {
		actionPath, err = wr.ai.Scrap(zkTabletPath)
	}
	if err != nil {
		return "", err
	}

	if !rebuildRequired {
		relog.Info("Rebuild not required")
		return
	}
	if skipRebuild {
		relog.Warning("Rebuild required, but skipping it")
		return
	}

	// wait for the remote Scrap if necessary
	if actionPath != "" {
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		if err != nil {
			return "", err
		}
	}

	// and rebuild the original shard / keyspace
	return wr.RebuildShardGraph(ti.ShardPath())
}
