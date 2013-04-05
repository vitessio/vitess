// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

// replaceError replaces original with recent if recent is not nil,
// logging original if it wasn't nil. This should be used in deferred
// cleanup functions if they change the returned error.
func replaceError(original, recent error) error {
	if recent == nil {
		return original
	}
	if original != nil {
		relog.Error("One of multiple error: %v", original)
	}
	return recent
}

// forceMasterSnapshot: Normally a master is not a viable tablet to snapshot.
// However, there are degenerate cases where you need to override this, for
// instance the initial clone of a new master.
func (wr *Wrangler) PartialSnapshot(zkTabletPath, keyName string, startKey, endKey key.HexKeyspaceId, forceMasterSnapshot bool, concurrency int) (manifest, parent string, err error) {
	restoreAfterSnapshot, err := wr.prepareToSnapshot(zkTabletPath, forceMasterSnapshot)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, restoreAfterSnapshot())
	}()

	actionPath, err := wr.ai.PartialSnapshot(zkTabletPath, &tm.PartialSnapshotArgs{keyName, startKey, endKey, concurrency})
	if err != nil {
		return
	}

	results, actionErr := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	var reply *tm.SnapshotReply
	if actionErr != nil {
		relog.Error("PartialSnapshot failed, still restoring tablet type: %v", actionErr)
		reply = &tm.SnapshotReply{}
	} else {
		reply = results.(*tm.SnapshotReply)
	}

	return reply.ManifestPath, reply.ZkParentPath, actionErr
}

// prepareToSnapshot changes the type of the tablet to backup (when
// the original type is master, it will proceed only if
// forceMasterSnapshot is true). It returns a function that will
// restore the original state.
func (wr *Wrangler) prepareToSnapshot(zkTabletPath string, forceMasterSnapshot bool) (restoreAfterSnapshot func() error, err error) {
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return
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
		return
	}

	restoreAfterSnapshot = func() (err error) {
		relog.Info("change type after snapshot: %v %v", zkTabletPath, originalType)

		if ti.Tablet.Parent.Uid == tm.NO_TABLET && forceMasterSnapshot {
			relog.Info("force change type backup -> master: %v", zkTabletPath)
			ti.Tablet.Type = tm.TYPE_MASTER
			return tm.UpdateTablet(wr.zconn, zkTabletPath, ti)
		}

		return wr.ChangeType(zkTabletPath, originalType, false)
	}

	return

}

func (wr *Wrangler) RestoreFromMultiSnapshot(zkDstTabletPath string, sources []string, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, writeBinLogs bool, skipAutoIncrementOnTables []string) error {
	actionPath, err := wr.ai.RestoreFromMultiSnapshot(zkDstTabletPath, &tm.MultiRestoreArgs{
		ZkSrcTabletPaths:          sources,
		Concurrency:               concurrency,
		FetchConcurrency:          fetchConcurrency,
		InsertTableConcurrency:    insertTableConcurrency,
		FetchRetryCount:           fetchRetryCount,
		WriteBinLogs:              writeBinLogs,
		SkipAutoIncrementOnTables: skipAutoIncrementOnTables})
	if err != nil {
		return err
	}

	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}

func (wr *Wrangler) MultiSnapshot(keyRanges []key.KeyRange, zkTabletPath, keyName string, concurrency int, tables []string, forceMasterSnapshot, skipSlaveRestart bool, maximumFilesize uint64) (manifests []string, parent string, err error) {
	restoreAfterSnapshot, err := wr.prepareToSnapshot(zkTabletPath, forceMasterSnapshot)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, restoreAfterSnapshot())
	}()

	actionPath, err := wr.ai.MultiSnapshot(zkTabletPath, &tm.MultiSnapshotArgs{KeyName: keyName, KeyRanges: keyRanges, Concurrency: concurrency, Tables: tables, SkipSlaveRestart: skipSlaveRestart, MaximumFilesize: maximumFilesize})
	if err != nil {
		return
	}

	results, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return
	}

	reply := results.(*tm.MultiSnapshotReply)

	return reply.ManifestPaths, reply.ZkParentPath, nil
}

func (wr *Wrangler) PartialRestore(zkSrcTabletPath, srcFilePath, zkDstTabletPath, zkParentPath string, fetchConcurrency, fetchRetryCount int) error {
	// read our current tablet, verify its state before sending it
	// to the tablet itself
	tablet, err := tm.ReadTablet(wr.zconn, zkDstTabletPath)
	if err != nil {
		return err
	}
	if tablet.Type != tm.TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, zkDstTabletPath)
	}

	actionPath, err := wr.ai.PartialRestore(zkDstTabletPath, &tm.RestoreArgs{zkSrcTabletPath, srcFilePath, zkParentPath, fetchConcurrency, fetchRetryCount, false, false})
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

func (wr *Wrangler) PartialClone(zkSrcTabletPath, zkDstTabletPath, keyName string, startKey, endKey key.HexKeyspaceId, forceMasterSnapshot bool, concurrency, fetchConcurrency, fetchRetryCount int) error {
	srcFilePath, zkParentPath, err := wr.PartialSnapshot(zkSrcTabletPath, keyName, startKey, endKey, forceMasterSnapshot, concurrency)
	if err != nil {
		return err
	}
	if err := wr.PartialRestore(zkSrcTabletPath, srcFilePath, zkDstTabletPath, zkParentPath, fetchConcurrency, fetchRetryCount); err != nil {
		return err
	}
	return nil
}
