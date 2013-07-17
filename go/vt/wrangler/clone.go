// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
)

// forceMasterSnapshot: Normally a master is not a viable tablet to snapshot.
// However, there are degenerate cases where you need to override this, for
// instance the initial clone of a new master.
func (wr *Wrangler) Snapshot(tabletAlias topo.TabletAlias, forceMasterSnapshot bool, snapshotConcurrency int, serverMode bool) (manifest string, parent topo.TabletAlias, slaveStartRequired, readOnly bool, originalType topo.TabletType, err error) {
	var ti *topo.TabletInfo
	ti, err = wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return
	}

	originalType = ti.Tablet.Type

	if ti.Tablet.Type == topo.TYPE_MASTER && forceMasterSnapshot {
		// In this case, we don't bother recomputing the serving graph.
		// All queries will have to fail anyway.
		relog.Info("force change type master -> backup: %v", tabletAlias)
		// There is a legitimate reason to force in the case of a single
		// master.
		ti.Tablet.Type = topo.TYPE_BACKUP
		err = topo.UpdateTablet(wr.ts, ti)
	} else {
		err = wr.ChangeType(ti.Alias(), topo.TYPE_BACKUP, false)
	}

	if err != nil {
		return
	}

	var actionPath string
	actionPath, err = wr.ai.Snapshot(tabletAlias, &tm.SnapshotArgs{snapshotConcurrency, serverMode})
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
		tm.BackfillAlias(reply.ZkParentPath, &reply.ParentAlias)
		if serverMode {
			relog.Info("server mode specified, switching tablet to snapshot_source mode")
			newType = topo.TYPE_SNAPSHOT_SOURCE
		}
	}

	// Go back to original type, or go to SNAPSHOT_SOURCE
	relog.Info("change type after snapshot: %v %v", tabletAlias, newType)
	if ti.Tablet.Parent.Uid == topo.NO_TABLET && forceMasterSnapshot && newType != topo.TYPE_SNAPSHOT_SOURCE {
		relog.Info("force change type backup -> master: %v", tabletAlias)
		ti.Tablet.Type = topo.TYPE_MASTER
		err = topo.UpdateTablet(wr.ts, ti)
	} else {
		err = wr.ChangeType(ti.Alias(), newType, false)
	}
	if err != nil {
		// failure in changing the topology type is probably worse,
		// so returning that (we logged actionErr anyway)
		return
	}
	return reply.ManifestPath, reply.ParentAlias, reply.SlaveStartRequired, reply.ReadOnly, originalType, actionErr
}

func (wr *Wrangler) SnapshotSourceEnd(tabletAlias topo.TabletAlias, slaveStartRequired, readWrite bool, originalType topo.TabletType) (err error) {
	var ti *topo.TabletInfo
	ti, err = wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return
	}

	var actionPath string
	actionPath, err = wr.ai.SnapshotSourceEnd(tabletAlias, &tm.SnapshotSourceEndArgs{slaveStartRequired, !readWrite})
	if err != nil {
		return
	}

	// wait for completion, and save the error
	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	if err != nil {
		relog.Error("SnapshotSourceEnd failed (%v), leaving tablet type alone", err)
		return
	}

	if ti.Tablet.Parent.Uid == topo.NO_TABLET {
		ti.Tablet.Type = topo.TYPE_MASTER
		err = topo.UpdateTablet(wr.ts, ti)
	} else {
		err = wr.ChangeType(ti.Alias(), originalType, false)
	}

	return err
}

func (wr *Wrangler) ReserveForRestore(srcTabletAlias, dstTabletAlias topo.TabletAlias) (err error) {
	// read our current tablet, verify its state before sending it
	// to the tablet itself
	tablet, err := wr.ts.GetTablet(dstTabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, dstTabletAlias)
	}

	var actionPath string
	actionPath, err = wr.ai.ReserveForRestore(dstTabletAlias, &tm.ReserveForRestoreArgs{"", srcTabletAlias})
	if err != nil {
		return
	}

	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}

func (wr *Wrangler) UnreserveForRestore(dstTabletAlias topo.TabletAlias) (err error) {
	tablet, err := wr.ts.GetTablet(dstTabletAlias)
	if err != nil {
		return err
	}
	err = wr.ts.DeleteReplicationPath(tablet.Keyspace, tablet.Shard, tablet.ReplicationPath())
	if err != nil {
		return err
	}

	return wr.ChangeType(tablet.Alias(), topo.TYPE_IDLE, false)
}

func (wr *Wrangler) Restore(srcTabletAlias topo.TabletAlias, srcFilePath string, dstTabletAlias, parentAlias topo.TabletAlias, fetchConcurrency, fetchRetryCount int, wasReserved, dontWaitForSlaveStart bool) error {
	// read our current tablet, verify its state before sending it
	// to the tablet itself
	tablet, err := wr.ts.GetTablet(dstTabletAlias)
	if err != nil {
		return err
	}
	if wasReserved {
		if tablet.Type != topo.TYPE_RESTORE {
			return fmt.Errorf("expected restore type, not %v: %v", tablet.Type, dstTabletAlias)
		}
	} else {
		if tablet.Type != topo.TYPE_IDLE {
			return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, dstTabletAlias)
		}
	}

	// do the work
	actionPath, err := wr.ai.Restore(dstTabletAlias, &tm.RestoreArgs{"", srcTabletAlias, srcFilePath, "", parentAlias, fetchConcurrency, fetchRetryCount, wasReserved, dontWaitForSlaveStart})
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

func (wr *Wrangler) UnreserveForRestoreMulti(dstTabletAliases []topo.TabletAlias) {
	for _, dstTabletAlias := range dstTabletAliases {
		ufrErr := wr.UnreserveForRestore(dstTabletAlias)
		if ufrErr != nil {
			relog.Error("Failed to UnreserveForRestore destination tablet after failed source snapshot: %v", ufrErr)
		} else {
			relog.Info("Un-reserved %v", dstTabletAlias)
		}
	}
}

func (wr *Wrangler) Clone(srcTabletAlias topo.TabletAlias, dstTabletAliases []topo.TabletAlias, forceMasterSnapshot bool, snapshotConcurrency, fetchConcurrency, fetchRetryCount int, serverMode bool) error {
	// make sure the destination can be restored into (otherwise
	// there is no point in taking the snapshot in the first place),
	// and reserve it.
	reserved := make([]topo.TabletAlias, 0, len(dstTabletAliases))
	for _, dstTabletAlias := range dstTabletAliases {
		err := wr.ReserveForRestore(srcTabletAlias, dstTabletAlias)
		if err != nil {
			wr.UnreserveForRestoreMulti(reserved)
			return err
		}
		reserved = append(reserved, dstTabletAlias)
		relog.Info("Successfully reserved %v for restore", dstTabletAlias)
	}

	// take the snapshot, or put the server in SnapshotSource mode
	srcFilePath, parentAlias, slaveStartRequired, readWrite, originalType, err := wr.Snapshot(srcTabletAlias, forceMasterSnapshot, snapshotConcurrency, serverMode)
	if err != nil {
		// The snapshot failed so un-reserve the destinations
		wr.UnreserveForRestoreMulti(reserved)
	} else {
		// try to restore the snapshot
		// In serverMode, and in the case where we're replicating from
		// the master, we can't wait for replication, as the master is down.
		wg := sync.WaitGroup{}
		er := concurrency.FirstErrorRecorder{}
		for _, dstTabletAlias := range dstTabletAliases {
			wg.Add(1)
			go func(dstTabletAlias topo.TabletAlias) {
				e := wr.Restore(srcTabletAlias, srcFilePath, dstTabletAlias, parentAlias, fetchConcurrency, fetchRetryCount, true, serverMode && originalType == topo.TYPE_MASTER)
				er.RecordError(e)
				wg.Done()
			}(dstTabletAlias)
		}
		wg.Wait()
		err = er.Error()
	}

	// in any case, fix the server
	if serverMode {
		resetErr := wr.SnapshotSourceEnd(srcTabletAlias, slaveStartRequired, readWrite, originalType)
		if resetErr != nil {
			if err == nil {
				// If there is no other error, this matters.
				err = resetErr
			} else {
				// In the context of a larger failure, just log a note to cleanup.
				relog.Error("Failed to reset snapshot source: %v - vtctl SnapshotSourceEnd is required", resetErr)
			}
		}
	}

	return err
}
