// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

// forceMasterSnapshot: Normally a master is not a viable tablet to snapshot.
// However, there are degenerate cases where you need to override this, for
// instance the initial clone of a new master.
func (wr *Wrangler) Snapshot(zkTabletPath string, forceMasterSnapshot bool, concurrency int, serverMode bool) (manifest, parent string, slaveStartRequired, readOnly bool, originalType tm.TabletType, err error) {
	var ti *tm.TabletInfo
	ti, err = tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return
	}

	originalType = ti.Tablet.Type

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
		return
	}

	var actionPath string
	actionPath, err = wr.ai.Snapshot(zkTabletPath, &tm.SnapshotArgs{concurrency, serverMode})
	if err != nil {
		return
	}

	// wait for completion, and save the error
	results, actionErr := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	var reply *tm.SnapshotReply
	newType := originalType
	if actionErr != nil {
		relog.Error("snapshot failed, still restoring tablet type: %v", actionErr)
		reply = &tm.SnapshotReply{}
	} else {
		reply = results.(*tm.SnapshotReply)
		if serverMode {
			relog.Info("server mode specified, switching tablet to snapshot_source mode")
			newType = tm.TYPE_SNAPSHOT_SOURCE
		}
	}

	// Go back to original type, or go to SNAPSHOT_SOURCE
	relog.Info("change type after snapshot: %v %v", zkTabletPath, newType)
	if ti.Tablet.Parent.Uid == tm.NO_TABLET && forceMasterSnapshot && newType != tm.TYPE_SNAPSHOT_SOURCE {
		relog.Info("force change type backup -> master: %v", zkTabletPath)
		ti.Tablet.Type = tm.TYPE_MASTER
		err = tm.UpdateTablet(wr.zconn, zkTabletPath, ti)
	} else {
		err = wr.ChangeType(zkTabletPath, newType, false)
	}
	if err != nil {
		// failure in changing the zk type is probably worse,
		// so returning that (we logged actionErr anyway)
		return
	}
	return reply.ManifestPath, reply.ZkParentPath, reply.SlaveStartRequired, reply.ReadOnly, originalType, actionErr
}

func (wr *Wrangler) SnapshotSourceEnd(zkTabletPath string, slaveStartRequired, readWrite bool, originalType tm.TabletType) (err error) {
	var ti *tm.TabletInfo
	ti, err = tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return
	}

	var actionPath string
	actionPath, err = wr.ai.SnapshotSourceEnd(zkTabletPath, &tm.SnapshotSourceEndArgs{slaveStartRequired, !readWrite})
	if err != nil {
		return
	}

	// wait for completion, and save the error
	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	if err != nil {
		relog.Error("SnapshotSourceEnd failed (%v), leaving tablet type alone", err)
		return
	}

	if ti.Tablet.Parent.Uid == tm.NO_TABLET {
		ti.Tablet.Type = tm.TYPE_MASTER
		err = tm.UpdateTablet(wr.zconn, zkTabletPath, ti)
	} else {
		err = wr.ChangeType(zkTabletPath, originalType, false)
	}

	return err
}

func (wr *Wrangler) Restore(zkSrcTabletPath, srcFilePath, zkDstTabletPath, zkParentPath string, fetchConcurrency, fetchRetryCount int, dontWaitForSlaveStart bool) error {
	err := wr.ChangeType(zkDstTabletPath, tm.TYPE_RESTORE, false)
	if err != nil {
		return err
	}

	actionPath, err := wr.ai.Restore(zkDstTabletPath, &tm.RestoreArgs{zkSrcTabletPath, srcFilePath, zkParentPath, fetchConcurrency, fetchRetryCount, dontWaitForSlaveStart})
	if err != nil {
		return err
	}

	if err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout()); err != nil {
		return err
	}

	// Restore moves us into the replication graph as a spare. There are no
	// consequences to the replication or serving graphs, so no rebuild required.
	return nil
}

func (wr *Wrangler) Clone(zkSrcTabletPath, zkDstTabletPath string, forceMasterSnapshot bool, concurrency, fetchConcurrency, fetchRetryCount int, serverMode bool) error {
	// take the snapshot, or put the server in SnapshotSource mode
	srcFilePath, zkParentPath, slaveStartRequired, readWrite, originalType, err := wr.Snapshot(zkSrcTabletPath, forceMasterSnapshot, concurrency, serverMode)
	if err != nil {
		return err
	}

	// try to restore the snapshot
	// In serverMode, and in the case where we're replicating from
	// the master, we can't wait for replication, as the master is down.
	restoreErr := wr.Restore(zkSrcTabletPath, srcFilePath, zkDstTabletPath, zkParentPath, fetchConcurrency, fetchRetryCount, serverMode && originalType == tm.TYPE_MASTER)

	// in any case, fix the server
	if serverMode {
		if err = wr.SnapshotSourceEnd(zkSrcTabletPath, slaveStartRequired, readWrite, originalType); err != nil {
			return err
		}
	}

	return restoreErr
}
