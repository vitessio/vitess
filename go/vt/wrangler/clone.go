// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// Snapshot takes a tablet snapshot.
//
// forceMasterSnapshot: Normally a master is not a viable tablet to snapshot.
// However, there are degenerate cases where you need to override this, for
// instance the initial clone of a new master.
//
// serverMode: if specified, the server will stop its mysqld, and be
// ready to serve the data files directly. Slaves can just download
// these and use them directly. Call SnapshotSourceEnd to return into
// serving mode. If not specified, the server will create an archive
// of the files, store them locally, and restart.
//
// If error is nil, returns the SnapshotReply from the remote host,
// and the original type the server was before the snapshot.
func (wr *Wrangler) Snapshot(tabletAlias topo.TabletAlias, forceMasterSnapshot bool, snapshotConcurrency int, serverMode bool) (*actionnode.SnapshotReply, topo.TabletType, error) {
	// read the tablet to be able to RPC to it, and also to get its
	// original type
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, "", err
	}
	originalType := ti.Tablet.Type

	// execute the remote action, log the results, save the error
	args := &actionnode.SnapshotArgs{
		Concurrency:         snapshotConcurrency,
		ServerMode:          serverMode,
		ForceMasterSnapshot: forceMasterSnapshot,
	}
	logStream, errFunc, err := wr.tmc.Snapshot(ti, args, wr.ActionTimeout())
	if err != nil {
		return nil, "", err
	}
	for e := range logStream {
		wr.Logger().Infof("Snapshot(%v): %v", tabletAlias, e)
	}
	reply, err := errFunc()
	return reply, originalType, err
}

// SnapshotSourceEnd will change the tablet back to its original type
// once it's done serving backups.
func (wr *Wrangler) SnapshotSourceEnd(tabletAlias topo.TabletAlias, slaveStartRequired, readWrite bool, originalType topo.TabletType) (err error) {
	var ti *topo.TabletInfo
	ti, err = wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return
	}

	args := &actionnode.SnapshotSourceEndArgs{
		SlaveStartRequired: slaveStartRequired,
		ReadOnly:           !readWrite,
		OriginalType:       originalType,
	}
	return wr.tmc.SnapshotSourceEnd(ti, args, wr.ActionTimeout())
}

// ReserveForRestore will make sure a tablet is ready to be used as a restore
// target.
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

	args := &actionnode.ReserveForRestoreArgs{
		SrcTabletAlias: srcTabletAlias,
	}
	return wr.tmc.ReserveForRestore(tablet, args, wr.ActionTimeout())
}

// UnreserveForRestore switches the tablet back to its original state,
// the restore won't happen.
func (wr *Wrangler) UnreserveForRestore(dstTabletAlias topo.TabletAlias) (err error) {
	tablet, err := wr.ts.GetTablet(dstTabletAlias)
	if err != nil {
		return err
	}
	err = topo.DeleteTabletReplicationData(wr.ts, tablet.Tablet)
	if err != nil {
		return err
	}

	return wr.ChangeType(tablet.Alias, topo.TYPE_IDLE, false)
}

// Restore actually performs the restore action on a tablet.
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

	// update the shard record if we need to, to update Cells
	srcTablet, err := wr.ts.GetTablet(srcTabletAlias)
	if err != nil {
		return err
	}
	si, err := wr.ts.GetShard(srcTablet.Keyspace, srcTablet.Shard)
	if err != nil {
		return fmt.Errorf("Cannot read shard: %v", err)
	}
	if err := wr.updateShardCellsAndMaster(si, tablet.Alias, topo.TYPE_SPARE, false); err != nil {
		return err
	}

	// do the work
	args := &actionnode.RestoreArgs{
		SrcTabletAlias:        srcTabletAlias,
		SrcFilePath:           srcFilePath,
		ParentAlias:           parentAlias,
		FetchConcurrency:      fetchConcurrency,
		FetchRetryCount:       fetchRetryCount,
		WasReserved:           wasReserved,
		DontWaitForSlaveStart: dontWaitForSlaveStart,
	}
	logStream, errFunc, err := wr.tmc.Restore(tablet, args, wr.ActionTimeout())
	if err != nil {
		return err
	}
	for e := range logStream {
		wr.Logger().Infof("Restore(%v): %v", dstTabletAlias, e)
	}
	if err := errFunc(); err != nil {
		return err
	}

	// Restore moves us into the replication graph as a
	// spare. There are no consequences to the replication or
	// serving graphs, so no rebuild required.
	return nil
}

// UnreserveForRestoreMulti calls UnreserveForRestore on all targets.
func (wr *Wrangler) UnreserveForRestoreMulti(dstTabletAliases []topo.TabletAlias) {
	for _, dstTabletAlias := range dstTabletAliases {
		ufrErr := wr.UnreserveForRestore(dstTabletAlias)
		if ufrErr != nil {
			wr.Logger().Errorf("Failed to UnreserveForRestore destination tablet after failed source snapshot: %v", ufrErr)
		} else {
			wr.Logger().Infof("Un-reserved %v", dstTabletAlias)
		}
	}
}

// Clone will do all the necessary actions to copy all the data from a
// source to a set of destinations.
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
		wr.Logger().Infof("Successfully reserved %v for restore", dstTabletAlias)
	}

	// take the snapshot, or put the server in SnapshotSource mode
	// srcFilePath, parentAlias, slaveStartRequired, readWrite
	sr, originalType, err := wr.Snapshot(srcTabletAlias, forceMasterSnapshot, snapshotConcurrency, serverMode)
	if err != nil {
		// The snapshot failed so un-reserve the destinations and return
		wr.UnreserveForRestoreMulti(reserved)
		return err
	}

	// try to restore the snapshot
	// In serverMode, and in the case where we're replicating from
	// the master, we can't wait for replication, as the master is down.
	wg := sync.WaitGroup{}
	rec := concurrency.FirstErrorRecorder{}
	for _, dstTabletAlias := range dstTabletAliases {
		wg.Add(1)
		go func(dstTabletAlias topo.TabletAlias) {
			e := wr.Restore(srcTabletAlias, sr.ManifestPath, dstTabletAlias, sr.ParentAlias, fetchConcurrency, fetchRetryCount, true, serverMode && originalType == topo.TYPE_MASTER)
			rec.RecordError(e)
			wg.Done()
		}(dstTabletAlias)
	}
	wg.Wait()
	err = rec.Error()

	// in any case, fix the server
	if serverMode {
		resetErr := wr.SnapshotSourceEnd(srcTabletAlias, sr.SlaveStartRequired, sr.ReadOnly, originalType)
		if resetErr != nil {
			if err == nil {
				// If there is no other error, this matters.
				err = resetErr
			} else {
				// In the context of a larger failure, just log a note to cleanup.
				wr.Logger().Errorf("Failed to reset snapshot source: %v - vtctl SnapshotSourceEnd is required", resetErr)
			}
		}
	}

	return err
}
