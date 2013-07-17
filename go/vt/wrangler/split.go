// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
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
func (wr *Wrangler) PartialSnapshot(tabletAlias topo.TabletAlias, keyName string, startKey, endKey key.HexKeyspaceId, forceMasterSnapshot bool, concurrency int) (manifest string, parent topo.TabletAlias, err error) {
	restoreAfterSnapshot, err := wr.prepareToSnapshot(tabletAlias, forceMasterSnapshot)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, restoreAfterSnapshot())
	}()

	actionPath, err := wr.ai.PartialSnapshot(tabletAlias, &tm.PartialSnapshotArgs{keyName, startKey, endKey, concurrency})
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
		tm.BackfillAlias(reply.ZkParentPath, &reply.ParentAlias)
	}

	return reply.ManifestPath, reply.ParentAlias, actionErr
}

// prepareToSnapshot changes the type of the tablet to backup (when
// the original type is master, it will proceed only if
// forceMasterSnapshot is true). It returns a function that will
// restore the original state.
func (wr *Wrangler) prepareToSnapshot(tabletAlias topo.TabletAlias, forceMasterSnapshot bool) (restoreAfterSnapshot func() error, err error) {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return
	}

	originalType := ti.Tablet.Type

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

	restoreAfterSnapshot = func() (err error) {
		relog.Info("change type after snapshot: %v %v", tabletAlias, originalType)

		if ti.Tablet.Parent.Uid == topo.NO_TABLET && forceMasterSnapshot {
			relog.Info("force change type backup -> master: %v", tabletAlias)
			ti.Tablet.Type = topo.TYPE_MASTER
			return topo.UpdateTablet(wr.ts, ti)
		}

		return wr.ChangeType(ti.Alias(), originalType, false)
	}

	return

}

func (wr *Wrangler) RestoreFromMultiSnapshot(dstTabletAlias topo.TabletAlias, sources []topo.TabletAlias, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) error {
	actionPath, err := wr.ai.RestoreFromMultiSnapshot(dstTabletAlias, &tm.MultiRestoreArgs{
		SrcTabletAliases:       sources,
		Concurrency:            concurrency,
		FetchConcurrency:       fetchConcurrency,
		InsertTableConcurrency: insertTableConcurrency,
		FetchRetryCount:        fetchRetryCount,
		Strategy:               strategy})
	if err != nil {
		return err
	}

	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}

func (wr *Wrangler) MultiSnapshot(keyRanges []key.KeyRange, tabletAlias topo.TabletAlias, keyName string, concurrency int, tables []string, forceMasterSnapshot, skipSlaveRestart bool, maximumFilesize uint64) (manifests []string, parent topo.TabletAlias, err error) {
	restoreAfterSnapshot, err := wr.prepareToSnapshot(tabletAlias, forceMasterSnapshot)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, restoreAfterSnapshot())
	}()

	actionPath, err := wr.ai.MultiSnapshot(tabletAlias, &tm.MultiSnapshotArgs{KeyName: keyName, KeyRanges: keyRanges, Concurrency: concurrency, Tables: tables, SkipSlaveRestart: skipSlaveRestart, MaximumFilesize: maximumFilesize})
	if err != nil {
		return
	}

	results, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return
	}

	reply := results.(*tm.MultiSnapshotReply)

	return reply.ManifestPaths, reply.ParentAlias, nil
}

func (wr *Wrangler) PartialRestore(srcTabletAlias topo.TabletAlias, srcFilePath string, dstTabletAlias, parentAlias topo.TabletAlias, fetchConcurrency, fetchRetryCount int) error {
	// read our current tablet, verify its state before sending it
	// to the tablet itself
	tablet, err := wr.ts.GetTablet(dstTabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type != topo.TYPE_IDLE {
		return fmt.Errorf("expected idle type, not %v: %v", tablet.Type, dstTabletAlias)
	}

	actionPath, err := wr.ai.PartialRestore(dstTabletAlias, &tm.RestoreArgs{"", srcTabletAlias, srcFilePath, "", parentAlias, fetchConcurrency, fetchRetryCount, false, false})
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

func (wr *Wrangler) PartialClone(srcTabletAlias, dstTabletAlias topo.TabletAlias, keyName string, startKey, endKey key.HexKeyspaceId, forceMasterSnapshot bool, concurrency, fetchConcurrency, fetchRetryCount int) error {
	srcFilePath, parentAlias, err := wr.PartialSnapshot(srcTabletAlias, keyName, startKey, endKey, forceMasterSnapshot, concurrency)
	if err != nil {
		return err
	}
	if err := wr.PartialRestore(srcTabletAlias, srcFilePath, dstTabletAlias, parentAlias, fetchConcurrency, fetchRetryCount); err != nil {
		return err
	}
	return nil
}
