// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

// forceMasterSnapshot: Normally a master is not a viable tablet to snapshot.
// However, there are degenerate cases where you need to override this, for
// instance the initial clone of a new master.
func (wr *Wrangler) PartialSnapshot(zkTabletPath, keyName string, startKey, endKey key.HexKeyspaceId, forceMasterSnapshot bool) error {
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return err
	}

	originalType := ti.Tablet.Type

	if ti.Tablet.Type == tm.TYPE_MASTER && forceMasterSnapshot {
		// In this case, we don't bother recomputing the serving graph.
		// All queries will have to fail anyway.
		relog.Info("force change type master -> backup: %v", zkTabletPath)
		// There is a legitimate reason to force in the case of a single
		// master.
		ti.Tablet.Type = tm.TYPE_BACKUP
		err = tm.UpdateTablet(wr.zconn, zkTabletPath, ti)
	} else {
		err = wr.ChangeType(zkTabletPath, tm.TYPE_BACKUP, false)
	}

	if err != nil {
		return err
	}

	actionPath, err := wr.ai.PartialSnapshot(zkTabletPath, keyName, startKey, endKey)
	if err != nil {
		return err
	}

	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout)
	if err != nil {
		return err
	}

	// Restore type
	relog.Info("change type after snapshot: %v %v", zkTabletPath, originalType)
	if ti.Tablet.Parent.Uid == tm.NO_TABLET && forceMasterSnapshot {
		relog.Info("force change type backup -> master: %v", zkTabletPath)
		ti.Tablet.Type = tm.TYPE_MASTER
		err = tm.UpdateTablet(wr.zconn, zkTabletPath, ti)
	} else {
		err = wr.ChangeType(zkTabletPath, originalType, false)
	}

	return err
}

func (wr *Wrangler) PartialRestore(zkSrcTabletPath, zkDstTabletPath string) error {
	err := wr.ChangeType(zkDstTabletPath, tm.TYPE_RESTORE, false)
	if err != nil {
		return err
	}

	actionPath, err := wr.ai.PartialRestore(zkDstTabletPath, zkSrcTabletPath)
	if err != nil {
		return err
	}

	if err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout); err != nil {
		return err
	}

	// Restore moves us into the replication graph as a spare. There are no
	// consequences to the replication or serving graphs, so no rebuild required.
	return nil
}

func (wr *Wrangler) PartialClone(zkSrcTabletPath, zkDstTabletPath, keyName string, startKey, endKey key.HexKeyspaceId, forceMasterSnapshot bool) error {
	if err := wr.PartialSnapshot(zkSrcTabletPath, keyName, startKey, endKey, forceMasterSnapshot); err != nil {
		return err
	}
	if err := wr.PartialRestore(zkSrcTabletPath, zkDstTabletPath); err != nil {
		return err
	}
	return nil
}
