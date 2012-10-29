// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"time"

	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

const (
	DefaultActionTimeout = 30 * time.Second
)

type Wrangler struct {
	zconn         zk.Conn
	ai            *tm.ActionInitiator
	actionTimeout time.Duration
}

func NewWrangler(zconn zk.Conn, actionTimeout time.Duration) *Wrangler {
	return &Wrangler{zconn, tm.NewActionInitiator(zconn), actionTimeout}
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
		if _, err := wr.RebuildShard(ti.ShardPath()); err != nil {
			return err
		}

		if _, err := wr.RebuildKeyspace(ti.KeyspacePath()); err != nil {
			return err
		}
	}
	return nil
}
