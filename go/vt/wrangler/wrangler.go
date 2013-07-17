// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
)

const (
	DefaultActionTimeout = 30 * time.Second
	DefaultLockTimeout   = 30 * time.Second
)

type Wrangler struct {
	ts          topo.Server
	ai          *tm.ActionInitiator
	deadline    time.Time
	lockTimeout time.Duration
}

// actionTimeout: how long should we wait for an action to complete?
// lockTimeout: how long should we wait for the initial lock to start a complex action?
//   This is distinct from actionTimeout because most of the time, we want to immediately
//   know that out action will fail. However, automated action will need some time to
//   arbitrate the locks.
func New(ts topo.Server, actionTimeout, lockTimeout time.Duration) *Wrangler {
	return &Wrangler{ts, tm.NewActionInitiator(ts), time.Now().Add(actionTimeout), lockTimeout}
}

func (wr *Wrangler) actionTimeout() time.Duration {
	return wr.deadline.Sub(time.Now())
}

func (wr *Wrangler) TopoServer() topo.Server {
	return wr.ts
}

func (wr *Wrangler) ActionInitiator() *tm.ActionInitiator {
	return wr.ai
}

// ResetActionTimeout should be used before every action on a wrangler
// object that is going to be re-used:
// - vtctl will not call this, as it does one action
// - vtctld will call this, as it re-uses the same wrangler for actions
func (wr *Wrangler) ResetActionTimeout(actionTimeout time.Duration) {
	wr.deadline = time.Now().Add(actionTimeout)
}

// Change the type of tablet and recompute all necessary derived paths in the
// serving graph.
// force: Bypass the vtaction system and make the data change directly, and
// do not run the remote hooks
func (wr *Wrangler) ChangeType(tabletAlias topo.TabletAlias, dbType topo.TabletType, force bool) error {
	// Load tablet to find keyspace and shard assignment.
	// Don't load after the ChangeType which might have unassigned
	// the tablet.
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	if force {
		// with --force, we do not run any hook
		err = tm.ChangeType(wr.ts, tabletAlias, dbType, false)
	} else {
		// the remote action will run the hooks
		var actionPath string
		actionPath, err = wr.ai.ChangeType(tabletAlias, dbType)
		// You don't have a choice - you must wait for
		// completion before rebuilding.
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		}
	}

	if err != nil {
		return err
	}

	// we rebuild if the tablet was serving, or if it is now
	var keyspaceToRebuild string
	var shardToRebuild string
	var cellToRebuild string
	if rebuildRequired {
		keyspaceToRebuild = ti.Keyspace
		shardToRebuild = ti.Shard
		cellToRebuild = ti.Cell
	} else {
		// re-read the tablet, see if we become serving
		ti, err := wr.ts.GetTablet(tabletAlias)
		if err != nil {
			return err
		}
		if ti.Tablet.IsServingType() {
			rebuildRequired = true
			keyspaceToRebuild = ti.Keyspace
			shardToRebuild = ti.Shard
			cellToRebuild = ti.Cell
		}
	}

	if rebuildRequired {
		if err := wr.RebuildShardGraph(keyspaceToRebuild, shardToRebuild, []string{cellToRebuild}); err != nil {
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
func (wr *Wrangler) changeTypeInternal(tabletAlias topo.TabletAlias, dbType topo.TabletType) error {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	// change the type
	actionPath, err := wr.ai.ChangeType(ti.Alias(), dbType)
	if err != nil {
		return err
	}
	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	if err != nil {
		return err
	}

	// rebuild if necessary
	if rebuildRequired {
		err = wr.rebuildShard(ti.Keyspace, ti.Shard, []string{ti.Cell})
		if err != nil {
			return err
		}
		// FIXME(alainjobart) We already have the lock on one shard, so this is not
		// possible. But maybe it's not necessary anyway.
		// We could pass in a shard path we already have the lock on, and skip it?
		//		err = wr.rebuildKeyspace(ti.Keyspace)
		//		if err != nil {
		//			return err
		//		}
	}
	return nil
}

// signal handling
var interrupted = make(chan struct{})

func SignalInterrupt() {
	close(interrupted)
}

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

func (wr *Wrangler) lockShard(keyspace, shard string, actionNode *tm.ActionNode) (lockPath string, err error) {
	relog.Info("Locking shard %v/%v for action %v", keyspace, shard, actionNode.Action)
	return wr.ts.LockShardForAction(keyspace, shard, tm.ActionNodeToJson(actionNode), wr.lockTimeout, interrupted)
}

func (wr *Wrangler) unlockShard(keyspace, shard string, actionNode *tm.ActionNode, lockPath string, actionError error) error {
	// first update the actionNode
	if actionError != nil {
		relog.Info("Unlocking shard %v/%v for action %v with error %v", keyspace, shard, actionNode.Action, actionError)
		actionNode.Error = actionError.Error()
		actionNode.State = tm.ACTION_STATE_FAILED
	} else {
		relog.Info("Unlocking keyspace %v/%v for successful action %v", keyspace, shard, actionNode.Action)
		actionNode.Error = ""
		actionNode.State = tm.ACTION_STATE_DONE
	}
	err := wr.ts.UnlockShardForAction(keyspace, shard, lockPath, tm.ActionNodeToJson(actionNode))
	if actionError != nil {
		if err != nil {
			// this will be masked
			relog.Warning("UnlockShardForAction failed: %v", err)
		}
		return actionError
	}
	return err
}

func (wr *Wrangler) getMasterAlias(keyspace, shard string) (topo.TabletAlias, error) {
	aliases, err := wr.ts.GetReplicationPaths(keyspace, shard, "")
	if err != nil {
		return topo.TabletAlias{}, err
	}
	if len(aliases) != 1 {
		return topo.TabletAlias{}, fmt.Errorf("More than one master in shard %v/%v: %v", keyspace, shard, aliases)
	}
	return aliases[0], nil
}

// InitTablet will create or update a tablet. If not parent is
// specified, and the tablet created is a slave type, we will find the
// appropriate parent.
func (wr *Wrangler) InitTablet(tabletAlias topo.TabletAlias, hostname, mysqlPort, port, keyspace, shardId, tabletType string, parentAlias topo.TabletAlias, dbNameOverride string, force, update bool) error {
	// if shardId contains a '-', we assume it's a range-based shard,
	// so we try to extract the KeyRange.
	var keyRange key.KeyRange
	var err error
	if strings.Contains(shardId, "-") {
		parts := strings.Split(shardId, "-")
		if len(parts) != 2 {
			return fmt.Errorf("Invalid shardId, can only contains one '-': %v", shardId)
		}

		keyRange.Start, err = key.HexKeyspaceId(parts[0]).Unhex()
		if err != nil {
			return err
		}

		keyRange.End, err = key.HexKeyspaceId(parts[1]).Unhex()
		if err != nil {
			return err
		}

		shardId = strings.ToUpper(shardId)
	}

	if parentAlias == (topo.TabletAlias{}) && topo.TabletType(tabletType) != topo.TYPE_MASTER && topo.TabletType(tabletType) != topo.TYPE_IDLE {
		parentAlias, err = wr.getMasterAlias(keyspace, shardId)
		if err != nil {
			return err
		}
	}

	tablet, err := topo.NewTablet(tabletAlias.Cell, tabletAlias.Uid, parentAlias, fmt.Sprintf("%v:%v", hostname, port), fmt.Sprintf("%v:%v", hostname, mysqlPort), keyspace, shardId, topo.TabletType(tabletType))
	if err != nil {
		return err
	}
	tablet.DbNameOverride = dbNameOverride
	tablet.KeyRange = keyRange

	err = topo.CreateTablet(wr.ts, tablet)
	if err != nil && err == topo.ErrNodeExists {
		// Try to update nicely, but if it fails fall back to force behavior.
		if update {
			oldTablet, err := wr.ts.GetTablet(tabletAlias)
			if err != nil {
				relog.Warning("failed reading tablet %v: %v", tabletAlias, err)
			} else {
				if oldTablet.Keyspace == tablet.Keyspace && oldTablet.Shard == tablet.Shard {
					*(oldTablet.Tablet) = *tablet
					err := topo.UpdateTablet(wr.ts, oldTablet)
					if err != nil {
						relog.Warning("failed updating tablet %v: %v", tabletAlias, err)
					} else {
						return nil
					}
				}
			}
		}
		if force {
			if _, err = wr.Scrap(tabletAlias, force, false); err != nil {
				relog.Error("failed scrapping tablet %v: %v", tabletAlias, err)
				return err
			}
			if err = wr.ts.DeleteTablet(tabletAlias); err != nil {
				// we ignore this
				relog.Error("failed deleting tablet %v: %v", tabletAlias, err)
			}
			err = topo.CreateTablet(wr.ts, tablet)
		}
	}
	return err
}

// Scrap a tablet. If force is used, we write to topo.Server
// directly and don't remote-execute the command.
func (wr *Wrangler) Scrap(tabletAlias topo.TabletAlias, force, skipRebuild bool) (actionPath string, err error) {
	// load the tablet, see if we'll need to rebuild
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return "", err
	}
	rebuildRequired := ti.Tablet.IsServingType()

	if force {
		err = tm.Scrap(wr.ts, ti.Alias(), force)
	} else {
		actionPath, err = wr.ai.Scrap(ti.Alias())
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
	return "", wr.RebuildShardGraph(ti.Keyspace, ti.Shard, []string{ti.Cell})
}
