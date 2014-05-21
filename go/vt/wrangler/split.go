// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	cc "github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// replaceError replaces original with recent if recent is not nil,
// logging original if it wasn't nil. This should be used in deferred
// cleanup functions if they change the returned error.
func replaceError(original, recent error) error {
	if recent == nil {
		return original
	}
	if original != nil {
		log.Errorf("One of multiple error: %v", original)
	}
	return recent
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
		log.Infof("force change type master -> backup: %v", tabletAlias)
		// There is a legitimate reason to force in the case of a single
		// master.
		ti.Tablet.Type = topo.TYPE_BACKUP
		err = topo.UpdateTablet(wr.ts, ti)
	} else {
		err = wr.ChangeType(ti.Alias, topo.TYPE_BACKUP, false)
	}

	if err != nil {
		return
	}

	restoreAfterSnapshot = func() (err error) {
		log.Infof("change type after snapshot: %v %v", tabletAlias, originalType)

		if ti.Tablet.Parent.Uid == topo.NO_TABLET && forceMasterSnapshot {
			log.Infof("force change type backup -> master: %v", tabletAlias)
			ti.Tablet.Type = topo.TYPE_MASTER
			return topo.UpdateTablet(wr.ts, ti)
		}

		return wr.ChangeType(ti.Alias, originalType, false)
	}

	return

}

func (wr *Wrangler) MultiRestore(dstTabletAlias topo.TabletAlias, sources []topo.TabletAlias, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) error {
	actionPath, err := wr.ai.MultiRestore(dstTabletAlias, &actionnode.MultiRestoreArgs{
		SrcTabletAliases:       sources,
		Concurrency:            concurrency,
		FetchConcurrency:       fetchConcurrency,
		InsertTableConcurrency: insertTableConcurrency,
		FetchRetryCount:        fetchRetryCount,
		Strategy:               strategy})
	if err != nil {
		return err
	}

	return wr.WaitForCompletion(actionPath)
}

func (wr *Wrangler) MultiSnapshot(keyRanges []key.KeyRange, tabletAlias topo.TabletAlias, concurrency int, tables []string, forceMasterSnapshot, skipSlaveRestart bool, maximumFilesize uint64) (manifests []string, parent topo.TabletAlias, err error) {
	restoreAfterSnapshot, err := wr.prepareToSnapshot(tabletAlias, forceMasterSnapshot)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, restoreAfterSnapshot())
	}()

	actionPath, err := wr.ai.MultiSnapshot(tabletAlias, &actionnode.MultiSnapshotArgs{KeyRanges: keyRanges, Concurrency: concurrency, Tables: tables, SkipSlaveRestart: skipSlaveRestart, MaximumFilesize: maximumFilesize})
	if err != nil {
		return
	}

	results, err := wr.WaitForCompletionReply(actionPath)
	if err != nil {
		return
	}

	reply := results.(*actionnode.MultiSnapshotReply)

	return reply.ManifestPaths, reply.ParentAlias, nil
}

func (wr *Wrangler) ShardMultiRestore(keyspace, shard string, sources []topo.TabletAlias, tables []string, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) error {

	// check parameters
	if len(tables) > 0 && len(sources) > 1 {
		return fmt.Errorf("ShardMultiRestore can only handle one source when tables are specified")
	}

	// lock the shard to perform the changes we need done
	actionNode := actionnode.ShardMultiRestore(&actionnode.MultiRestoreArgs{
		SrcTabletAliases:       sources,
		Concurrency:            concurrency,
		FetchConcurrency:       fetchConcurrency,
		InsertTableConcurrency: insertTableConcurrency,
		FetchRetryCount:        fetchRetryCount,
		Strategy:               strategy})
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	mrErr := wr.shardMultiRestore(keyspace, shard, sources, tables, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount, strategy)
	err = wr.unlockShard(keyspace, shard, actionNode, lockPath, mrErr)
	if err != nil {
		return err
	}
	if mrErr != nil {
		return mrErr
	}

	// find all tablets in the shard
	destTablets, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	// now launch MultiRestore on all tablets we need to do
	rec := cc.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, tabletAlias := range destTablets {
		wg.Add(1)
		go func(tabletAlias topo.TabletAlias) {
			log.Infof("Starting multirestore on tablet %v", tabletAlias)
			err := wr.MultiRestore(tabletAlias, sources, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount, strategy)
			log.Infof("Multirestore on tablet %v is done (err=%v)", tabletAlias, err)
			rec.RecordError(err)
			wg.Done()
		}(tabletAlias)
	}
	wg.Wait()

	return rec.Error()
}

func (wr *Wrangler) shardMultiRestore(keyspace, shard string, sources []topo.TabletAlias, tables []string, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) error {
	// read the shard
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// read the source tablets
	sourceTablets, err := topo.GetTabletMap(wr.TopoServer(), sources)
	if err != nil {
		return err
	}

	// Insert their KeyRange in the SourceShards array.
	// We use a linear 0-based id, that matches what mysqlctld/split.go
	// inserts into _vt.blp_checkpoint.
	shardInfo.SourceShards = make([]topo.SourceShard, len(sourceTablets))
	i := 0
	for _, ti := range sourceTablets {
		shardInfo.SourceShards[i] = topo.SourceShard{
			Uid:      uint32(i),
			Keyspace: ti.Keyspace,
			Shard:    ti.Shard,
			KeyRange: ti.KeyRange,
			Tables:   tables,
		}
		i++
	}

	// and write the shard
	if err = wr.ts.UpdateShard(shardInfo); err != nil {
		return err
	}

	return nil
}
