// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"code.google.com/p/go.net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	cc "github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools/events"
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
func (wr *Wrangler) prepareToSnapshot(ti *topo.TabletInfo, forceMasterSnapshot bool) (restoreAfterSnapshot func() error, err error) {
	originalType := ti.Tablet.Type

	if ti.Tablet.Type == topo.TYPE_MASTER && forceMasterSnapshot {
		// In this case, we don't bother recomputing the serving graph.
		// All queries will have to fail anyway.
		log.Infof("force change type master -> backup: %v", ti.Alias)
		// There is a legitimate reason to force in the case of a single
		// master.
		ti.Tablet.Type = topo.TYPE_BACKUP
		err = topo.UpdateTablet(context.TODO(), wr.ts, ti)
	} else {
		err = wr.ChangeType(ti.Alias, topo.TYPE_BACKUP, false)
	}

	if err != nil {
		return
	}

	restoreAfterSnapshot = func() (err error) {
		log.Infof("change type after snapshot: %v %v", ti.Alias, originalType)

		if ti.Tablet.Parent.Uid == topo.NO_TABLET && forceMasterSnapshot {
			log.Infof("force change type backup -> master: %v", ti.Alias)
			ti.Tablet.Type = topo.TYPE_MASTER
			return topo.UpdateTablet(context.TODO(), wr.ts, ti)
		}

		return wr.ChangeType(ti.Alias, originalType, false)
	}

	return
}

func (wr *Wrangler) MultiRestore(dstTabletAlias topo.TabletAlias, sources []topo.TabletAlias, concurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) (err error) {
	var ti *topo.TabletInfo
	ti, err = wr.ts.GetTablet(dstTabletAlias)
	if err != nil {
		return
	}

	args := &actionnode.MultiRestoreArgs{SrcTabletAliases: sources, Concurrency: concurrency, FetchConcurrency: fetchConcurrency, InsertTableConcurrency: insertTableConcurrency, FetchRetryCount: fetchRetryCount, Strategy: strategy}
	ev := &events.MultiRestore{
		Tablet: *ti.Tablet,
		Args:   *args,
	}
	event.DispatchUpdate(ev, "starting")
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	logStream, errFunc, err := wr.tmc.MultiRestore(context.TODO(), ti, args, wr.ActionTimeout())
	if err != nil {
		return err
	}
	for e := range logStream {
		wr.Logger().Infof("MultiRestore(%v): %v", dstTabletAlias, e)
	}
	if err := errFunc(); err != nil {
		return err
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

func (wr *Wrangler) MultiSnapshot(keyRanges []key.KeyRange, tabletAlias topo.TabletAlias, concurrency int, tables, excludeTables []string, forceMasterSnapshot, skipSlaveRestart bool, maximumFilesize uint64) (manifests []string, parent topo.TabletAlias, err error) {
	var ti *topo.TabletInfo
	ti, err = wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return
	}

	args := &actionnode.MultiSnapshotArgs{KeyRanges: keyRanges, Concurrency: concurrency, Tables: tables, ExcludeTables: excludeTables, SkipSlaveRestart: skipSlaveRestart, MaximumFilesize: maximumFilesize}
	ev := &events.MultiSnapshot{
		Tablet: *ti.Tablet,
		Args:   *args,
	}
	if len(tables) > 0 {
		event.DispatchUpdate(ev, "starting table")
	} else {
		event.DispatchUpdate(ev, "starting keyrange")
	}
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	restoreAfterSnapshot, err := wr.prepareToSnapshot(ti, forceMasterSnapshot)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, restoreAfterSnapshot())
	}()

	// execute the remote action, log the results, save the error
	logStream, errFunc, err := wr.tmc.MultiSnapshot(context.TODO(), ti, args, wr.ActionTimeout())
	if err != nil {
		return nil, topo.TabletAlias{}, err
	}
	for e := range logStream {
		wr.Logger().Infof("MultiSnapshot(%v): %v", tabletAlias, e)
	}
	var reply *actionnode.MultiSnapshotReply
	if reply, err = errFunc(); err != nil {
		return
	}

	event.DispatchUpdate(ev, "finished")
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

	mrErr := wr.SetSourceShards(keyspace, shard, sources, tables)
	err = wr.unlockShard(keyspace, shard, actionNode, lockPath, mrErr)
	if err != nil {
		if mrErr != nil {
			log.Errorf("unlockShard got error back: %v", err)
			return mrErr
		}
		return err
	}
	if mrErr != nil {
		return mrErr
	}

	// find all tablets in the shard
	destTablets, err := topo.FindAllTabletAliasesInShard(context.TODO(), wr.ts, keyspace, shard)
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

func (wr *Wrangler) SetSourceShards(keyspace, shard string, sources []topo.TabletAlias, tables []string) error {
	// read the shard
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// If the shard already has sources, maybe it's already been restored,
	// so let's be safe and abort right here.
	if len(shardInfo.SourceShards) > 0 {
		return fmt.Errorf("Shard %v/%v already has SourceShards, not overwriting them", keyspace, shard)
	}

	// read the source tablets
	sourceTablets, err := topo.GetTabletMap(context.TODO(), wr.TopoServer(), sources)
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
	if err = topo.UpdateShard(context.TODO(), wr.ts, shardInfo); err != nil {
		return err
	}

	return nil
}
