// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"encoding/json"
	"path"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

const (
	DefaultActionTimeout = 30 * time.Second
)

type Wrangler struct {
	zconn    zk.Conn
	ai       *tm.ActionInitiator
	deadline time.Time
}

func NewWrangler(zconn zk.Conn, actionTimeout time.Duration) *Wrangler {
	return &Wrangler{zconn, tm.NewActionInitiator(zconn), time.Now().Add(actionTimeout)}
}

func (wr *Wrangler) actionTimeout() time.Duration {
	return wr.deadline.Sub(time.Now())
}

func (wr *Wrangler) readTablet(zkTabletPath string) (*tm.TabletInfo, error) {
	return tm.ReadTablet(wr.zconn, zkTabletPath)
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
		// with --force, we do not run the hooks
		err = tm.ChangeType(wr.zconn, zkTabletPath, dbType)
	} else {
		// if the tablet was idle, run the idle_server_check hook
		if ti.Tablet.Type == tm.TYPE_IDLE {
			err = wr.ExecuteOptionalTabletInfoHook(ti, tm.NewSimpleHook("idle_server_check"))
			if err != nil {
				return err
			}
		}

		// run the live_server_check hook unless we're going to scrap
		if dbType != tm.TYPE_SCRAP {
			err = wr.ExecuteOptionalTabletInfoHook(ti, tm.NewSimpleHook("live_server_check"))
			if err != nil {
				return err
			}
		}

		actionPath, err := wr.ai.ChangeType(zkTabletPath, dbType)
		// You don't have a choice - you must wait for completion before rebuilding.
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, DefaultActionTimeout)
		}
	}

	if err != nil {
		return err
	}

	if rebuildRequired {
		if _, err := wr.RebuildShardGraph(ti.ShardPath()); err != nil {
			return err
		}

		if _, err := wr.RebuildKeyspaceGraph(ti.KeyspacePath()); err != nil {
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

// Waits for the completion of a tablet action, and pulls the single
// result back into the given interface.
//
// - if replyPath is relative, we read it from the 'reply' area for
// the tablet, and then delete the directory for the reply.
// - if replyPath is absolute, we just read it, and don't delete
// anything (most likely it's a shard action and will be deleted with it)
//
// See tabletmanager/TabletActor.storeTabletActionResponse
func (wr *Wrangler) WaitForTabletActionResponse(actionPath, replyPath string, result interface{}, waitTime time.Duration) (err error) {
	err = wr.ai.WaitForCompletion(actionPath, waitTime)
	if err != nil {
		return err
	}
	isRelativeResponse := false
	if !strings.HasPrefix(replyPath, "/") {
		replyPath = tm.TabletActionToReplyPath(actionPath, replyPath)
		isRelativeResponse = true
	}
	data, _, err := wr.zconn.Get(replyPath)
	if err != nil {
		return err
	}
	if err = json.Unmarshal([]byte(data), result); err != nil {
		return err
	}
	if isRelativeResponse {
		if err = zk.DeleteRecursive(wr.zconn, path.Dir(replyPath), -1); err != nil {
			relog.Error("Cannot delete action reply %v", replyPath)
		}
	}
	return nil
}
