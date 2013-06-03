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
)

// forceMasterSnapshot: Normally a master is not a viable tablet to snapshot.
// However, there are degenerate cases where you need to override this, for
// instance the initial clone of a new master.
func (wr *Wrangler) Snapshot(zkTabletPath string, forceMasterSnapshot bool, snapshotConcurrency int, serverMode bool) (manifest, parent string, slaveStartRequired, readOnly bool, originalType tm.TabletType, err error) {
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
	actionPath, err = wr.ai.Snapshot(zkTabletPath, &tm.SnapshotArgs{snapshotConcurrency, serverMode})
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

func (wr *Wrangler) ReserveForRestore(zkSrcTabletPath, zkDstTabletPath string) (err error) {
	// read our current tablet, verify its state before sending it
	// to the tablet itself
	tablet, err := tm.ReadTablet(wr.zconn, zkDstTabletPath)
	if err != nil {
		return err
	}
	if tablet.Type != tm.TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, zkDstTabletPath)
	}

	var actionPath string
	actionPath, err = wr.ai.ReserveForRestore(zkDstTabletPath, &tm.ReserveForRestoreArgs{zkSrcTabletPath})
	if err != nil {
		return
	}

	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}

func (wr *Wrangler) UnreserveForRestore(zkDstTabletPath string) (err error) {
	tablet, err := tm.ReadTablet(wr.zconn, zkDstTabletPath)
	if err != nil {
		return err
	}
	rp, err := tablet.ReplicationPath()
	if err != nil {
		return err
	}
	err = wr.zconn.Delete(rp, -1)
	if err != nil {
		return err
	}

	return wr.ChangeType(zkDstTabletPath, tm.TYPE_IDLE, false)
}

func (wr *Wrangler) Restore(zkSrcTabletPath, srcFilePath, zkDstTabletPath, zkParentPath string, fetchConcurrency, fetchRetryCount int, wasReserved, dontWaitForSlaveStart bool) error {
	// read our current tablet, verify its state before sending it
	// to the tablet itself
	tablet, err := tm.ReadTablet(wr.zconn, zkDstTabletPath)
	if err != nil {
		return err
	}
	if wasReserved {
		if tablet.Type != tm.TYPE_RESTORE {
			return fmt.Errorf("expected restore type, not %v: %v", tablet.Type, zkDstTabletPath)
		}
	} else {
		if tablet.Type != tm.TYPE_IDLE {
			return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, zkDstTabletPath)
		}
	}

	// do the work
	actionPath, err := wr.ai.Restore(zkDstTabletPath, &tm.RestoreArgs{zkSrcTabletPath, srcFilePath, zkParentPath, fetchConcurrency, fetchRetryCount, wasReserved, dontWaitForSlaveStart})
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

func (wr *Wrangler) UnreserveForRestoreMulti(zkDstTabletPaths []string) {
	for _, zkDstTabletPath := range zkDstTabletPaths {
		ufrErr := wr.UnreserveForRestore(zkDstTabletPath)
		if ufrErr != nil {
			relog.Error("Failed to UnreserveForRestore destination tablet after failed source snapshot: %v", ufrErr)
		} else {
			relog.Info("Un-reserved %v", zkDstTabletPath)
		}
	}
}

func (wr *Wrangler) Clone(zkSrcTabletPath string, zkDstTabletPaths []string, forceMasterSnapshot bool, snapshotConcurrency, fetchConcurrency, fetchRetryCount int, serverMode bool) error {
	// make sure the destination can be restored into (otherwise
	// there is no point in taking the snapshot in the first place),
	// and reserve it.
	reserved := make([]string, 0, len(zkDstTabletPaths))
	for _, zkDstTabletPath := range zkDstTabletPaths {
		err := wr.ReserveForRestore(zkSrcTabletPath, zkDstTabletPath)
		if err != nil {
			wr.UnreserveForRestoreMulti(reserved)
			return err
		}
		reserved = append(reserved, zkDstTabletPath)
		relog.Info("Successfully reserved %v for restore", zkDstTabletPath)
	}

	// take the snapshot, or put the server in SnapshotSource mode
	srcFilePath, zkParentPath, slaveStartRequired, readWrite, originalType, err := wr.Snapshot(zkSrcTabletPath, forceMasterSnapshot, snapshotConcurrency, serverMode)
	if err != nil {
		// The snapshot failed so un-reserve the destinations
		wr.UnreserveForRestoreMulti(reserved)
	} else {
		// try to restore the snapshot
		// In serverMode, and in the case where we're replicating from
		// the master, we can't wait for replication, as the master is down.
		wg := sync.WaitGroup{}
		er := concurrency.FirstErrorRecorder{}
		for _, zkDstTabletPath := range zkDstTabletPaths {
			wg.Add(1)
			go func(zkDstTabletPath string) {
				e := wr.Restore(zkSrcTabletPath, srcFilePath, zkDstTabletPath, zkParentPath, fetchConcurrency, fetchRetryCount, true, serverMode && originalType == tm.TYPE_MASTER)
				er.RecordError(e)
				wg.Done()
			}(zkDstTabletPath)
		}
		wg.Wait()
		err = er.Error()
	}

	// in any case, fix the server
	if serverMode {
		resetErr := wr.SnapshotSourceEnd(zkSrcTabletPath, slaveStartRequired, readWrite, originalType)
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
