// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
)

// GetSchema uses an RPC to get the schema from a remote tablet
func (wr *Wrangler) GetSchema(tabletAlias topo.TabletAlias, tables, excludeTables []string, includeViews bool) (*myproto.SchemaDefinition, error) {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	return wr.ai.GetSchema(ti, tables, excludeTables, includeViews, wr.ActionTimeout())
}

// ReloadSchema forces the remote tablet to reload its schema.
func (wr *Wrangler) ReloadSchema(tabletAlias topo.TabletAlias) error {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	return wr.ai.ReloadSchema(ti, wr.ActionTimeout())
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(masterSchema *myproto.SchemaDefinition, masterTabletAlias, alias topo.TabletAlias, excludeTables []string, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering schema for %v", alias)
	slaveSchema, err := wr.GetSchema(alias, nil, excludeTables, includeViews)
	if err != nil {
		er.RecordError(err)
		return
	}

	log.Infof("Diffing schema for %v", alias)
	myproto.DiffSchema(masterTabletAlias.String(), masterSchema, alias.String(), slaveSchema, er)
}

// ValidateSchemaShard will diff the schema from all the tablets in the shard.
func (wr *Wrangler) ValidateSchemaShard(keyspace, shard string, excludeTables []string, includeViews bool) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// get schema from the master, or error
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shard)
	}
	log.Infof("Gathering schema for master %v", si.MasterAlias)
	masterSchema, err := wr.GetSchema(si.MasterAlias, nil, excludeTables, includeViews)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	// then diff with all slaves
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if alias == si.MasterAlias {
			continue
		}

		wg.Add(1)
		go wr.diffSchema(masterSchema, si.MasterAlias, alias, excludeTables, includeViews, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

// ValidateSchemaShard will diff the schema from all the tablets in
// the keyspace.
func (wr *Wrangler) ValidateSchemaKeyspace(keyspace string, excludeTables []string, includeViews bool) error {
	// find all the shards
	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace %v", keyspace)
	}
	sort.Strings(shards)
	if len(shards) == 1 {
		return wr.ValidateSchemaShard(keyspace, shards[0], excludeTables, includeViews)
	}

	// find the reference schema using the first shard's master
	si, err := wr.ts.GetShard(keyspace, shards[0])
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shards[0])
	}
	referenceAlias := si.MasterAlias
	log.Infof("Gathering schema for reference master %v", referenceAlias)
	referenceSchema, err := wr.GetSchema(referenceAlias, nil, excludeTables, includeViews)
	if err != nil {
		return err
	}

	// then diff with all other tablets everywhere
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// first diff the slaves in the reference shard 0
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shards[0])
	if err != nil {
		return err
	}

	for _, alias := range aliases {
		if alias == si.MasterAlias {
			continue
		}

		wg.Add(1)
		go wr.diffSchema(referenceSchema, referenceAlias, alias, excludeTables, includeViews, &wg, &er)
	}

	// then diffs all tablets in the other shards
	for _, shard := range shards[1:] {
		si, err := wr.ts.GetShard(keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		if si.MasterAlias.Uid == topo.NO_TABLET {
			er.RecordError(fmt.Errorf("No master in shard %v/%v", keyspace, shard))
			continue
		}

		aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			wg.Add(1)
			go wr.diffSchema(referenceSchema, referenceAlias, alias, excludeTables, includeViews, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

// PreflightSchema will try a schema change on the remote tablet.
func (wr *Wrangler) PreflightSchema(tabletAlias topo.TabletAlias, change string) (*myproto.SchemaChangeResult, error) {
	actionPath, err := wr.ai.PreflightSchema(tabletAlias, change)
	if err != nil {
		return nil, err
	}

	result, err := wr.WaitForCompletionReply(actionPath)
	if err != nil {
		return nil, err
	}
	return result.(*myproto.SchemaChangeResult), nil
}

// ApplySchema will apply a schema change on the remote tablet.
func (wr *Wrangler) ApplySchema(tabletAlias topo.TabletAlias, sc *myproto.SchemaChange) (*myproto.SchemaChangeResult, error) {
	actionPath, err := wr.ai.ApplySchema(tabletAlias, sc)

	// the timeout is for the entire action, so it might be too big
	// for an individual tablet
	results, err := wr.WaitForCompletionReply(actionPath)
	if err != nil {
		return nil, err
	}
	return results.(*myproto.SchemaChangeResult), nil
}

// Note for 'complex' mode (the 'simple' mode is easy enough that we
// don't need to handle recovery that much): this method is able to
// recover if interrupted in the middle, because it knows which server
// has the schema change already applied, and will just pass through them
// very quickly.
func (wr *Wrangler) ApplySchemaShard(keyspace, shard, change string, newParentTabletAlias topo.TabletAlias, simple, force bool) (*myproto.SchemaChangeResult, error) {
	// read the shard
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return nil, err
	}

	// preflight on the master, to get baseline
	// this assumes the master doesn't have the schema upgrade applied
	// If the master does, and some slaves don't, may have to
	// fix them manually one at a time, or re-clone them.
	// we do this outside of the shard lock because we can.
	log.Infof("Running Preflight on Master %v", shardInfo.MasterAlias)
	if err != nil {
		return nil, err
	}
	preflight, err := wr.PreflightSchema(shardInfo.MasterAlias, change)
	if err != nil {
		return nil, err
	}

	return wr.lockAndApplySchemaShard(shardInfo, preflight, keyspace, shard, shardInfo.MasterAlias, change, newParentTabletAlias, simple, force)
}

func (wr *Wrangler) lockAndApplySchemaShard(shardInfo *topo.ShardInfo, preflight *myproto.SchemaChangeResult, keyspace, shard string, masterTabletAlias topo.TabletAlias, change string, newParentTabletAlias topo.TabletAlias, simple, force bool) (*myproto.SchemaChangeResult, error) {
	// get a shard lock
	actionNode := actionnode.ApplySchemaShard(masterTabletAlias, change, simple)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return nil, err
	}

	scr, err := wr.applySchemaShard(shardInfo, preflight, masterTabletAlias, change, newParentTabletAlias, simple, force)
	return scr, wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

// local structure used to keep track of what we're doing
type TabletStatus struct {
	ti           *topo.TabletInfo
	lastError    error
	beforeSchema *myproto.SchemaDefinition
}

func (wr *Wrangler) applySchemaShard(shardInfo *topo.ShardInfo, preflight *myproto.SchemaChangeResult, masterTabletAlias topo.TabletAlias, change string, newParentTabletAlias topo.TabletAlias, simple, force bool) (*myproto.SchemaChangeResult, error) {

	// find all the shards we need to handle
	aliases, err := topo.FindAllTabletAliasesInShard(wr.ts, shardInfo.Keyspace(), shardInfo.ShardName())
	if err != nil {
		return nil, err
	}

	// build the array of TabletStatus we're going to use
	statusArray := make([]*TabletStatus, 0, len(aliases)-1)
	for _, alias := range aliases {
		if alias == masterTabletAlias {
			// we skip the master
			continue
		}

		ti, err := wr.ts.GetTablet(alias)
		if err != nil {
			return nil, err
		}
		if ti.Type == topo.TYPE_LAG {
			// lag tablets are usually behind, not replicating,
			// and a general pain. So let's just skip them
			// all together.
			// TODO(alainjobart) figure out other types to skip:
			// ValidateSchemaShard only does the serving types.
			// We do everything in the replication graph
			// but LAG. This seems fine for now.
			log.Infof("Skipping tablet %v as it is LAG", ti.Alias)
			continue
		}

		statusArray = append(statusArray, &TabletStatus{ti: ti})
	}

	// get schema on all tablets.
	log.Infof("Getting schema on all tablets for shard %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())
	wg := &sync.WaitGroup{}
	for _, status := range statusArray {
		wg.Add(1)
		go func(status *TabletStatus) {
			status.beforeSchema, status.lastError = wr.ai.GetSchema(status.ti, nil, nil, false, wr.ActionTimeout())
			wg.Done()
		}(status)
	}
	wg.Wait()

	// quick check for errors
	for _, status := range statusArray {
		if status.lastError != nil {
			return nil, fmt.Errorf("Error getting schema on tablet %v: %v", status.ti.Alias, status.lastError)
		}
	}

	// simple or complex?
	if simple {
		return wr.applySchemaShardSimple(statusArray, preflight, masterTabletAlias, change, force)
	}

	return wr.applySchemaShardComplex(statusArray, shardInfo, preflight, masterTabletAlias, change, newParentTabletAlias, force)
}

func (wr *Wrangler) applySchemaShardSimple(statusArray []*TabletStatus, preflight *myproto.SchemaChangeResult, masterTabletAlias topo.TabletAlias, change string, force bool) (*myproto.SchemaChangeResult, error) {
	// check all tablets have the same schema as the master's
	// BeforeSchema. If not, we shouldn't proceed
	log.Infof("Checking schema on all tablets")
	for _, status := range statusArray {
		diffs := myproto.DiffSchemaToArray("master", preflight.BeforeSchema, status.ti.Alias.String(), status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				log.Warningf("Tablet %v has inconsistent schema, ignoring: %v", status.ti.Alias, strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.ti.Alias, strings.Join(diffs, "\n"))
			}
		}
	}

	// we're good, just send to the master
	log.Infof("Applying schema change to master in simple mode")
	sc := &myproto.SchemaChange{Sql: change, Force: force, AllowReplication: true, BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}
	return wr.ApplySchema(masterTabletAlias, sc)
}

func (wr *Wrangler) applySchemaShardComplex(statusArray []*TabletStatus, shardInfo *topo.ShardInfo, preflight *myproto.SchemaChangeResult, masterTabletAlias topo.TabletAlias, change string, newParentTabletAlias topo.TabletAlias, force bool) (*myproto.SchemaChangeResult, error) {
	// apply the schema change to all replica / slave tablets
	for _, status := range statusArray {
		// if already applied, we skip this guy
		diffs := myproto.DiffSchemaToArray("after", preflight.AfterSchema, status.ti.Alias.String(), status.beforeSchema)
		if len(diffs) == 0 {
			log.Infof("Tablet %v already has the AfterSchema, skipping", status.ti.Alias)
			continue
		}

		// make sure the before schema matches
		diffs = myproto.DiffSchemaToArray("master", preflight.BeforeSchema, status.ti.Alias.String(), status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				log.Warningf("Tablet %v has inconsistent schema, ignoring: %v", status.ti.Alias, strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.ti.Alias, strings.Join(diffs, "\n"))
			}
		}

		// take this guy out of the serving graph if necessary
		ti, err := wr.ts.GetTablet(status.ti.Alias)
		if err != nil {
			return nil, err
		}
		typeChangeRequired := ti.Tablet.IsInServingGraph()
		if typeChangeRequired {
			// note we want to update the serving graph there
			err = wr.changeTypeInternal(ti.Alias, topo.TYPE_SCHEMA_UPGRADE)
			if err != nil {
				return nil, err
			}
		}

		// apply the schema change
		log.Infof("Applying schema change to slave %v in complex mode", status.ti.Alias)
		sc := &myproto.SchemaChange{Sql: change, Force: force, AllowReplication: false, BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}
		_, err = wr.ApplySchema(status.ti.Alias, sc)
		if err != nil {
			return nil, err
		}

		// put this guy back into the serving graph
		if typeChangeRequired {
			err = wr.changeTypeInternal(ti.Alias, ti.Tablet.Type)
			if err != nil {
				return nil, err
			}
		}
	}

	// if newParentTabletAlias is passed in, use that as the new master
	if !newParentTabletAlias.IsZero() {
		log.Infof("Reparenting with new master set to %v", newParentTabletAlias)
		tabletMap, err := topo.GetTabletMapForShard(wr.ts, shardInfo.Keyspace(), shardInfo.ShardName())
		if err != nil {
			return nil, err
		}

		slaveTabletMap, masterTabletMap := topotools.SortedTabletMap(tabletMap)
		newMasterTablet, err := wr.ts.GetTablet(newParentTabletAlias)
		if err != nil {
			return nil, err
		}

		// Create reusable Reparent event with available info
		ev := &events.Reparent{
			ShardInfo: *shardInfo,
			NewMaster: *newMasterTablet.Tablet,
		}

		if oldMasterTablet, ok := tabletMap[shardInfo.MasterAlias]; ok {
			ev.OldMaster = *oldMasterTablet.Tablet
		}

		err = wr.reparentShardGraceful(ev, shardInfo, slaveTabletMap, masterTabletMap, newMasterTablet /*leaveMasterReadOnly*/, false)
		if err != nil {
			return nil, err
		}

		// Here we would apply the schema change to the old
		// master, but after a reparent it's in Scrap state,
		// so no need to.  When/if reparent leaves the
		// original master in a different state (like replica
		// or rdonly), then we should apply the schema there
		// too.
		log.Infof("Skipping schema change on old master %v in complex mode, it's been Scrapped", masterTabletAlias)
	}
	return &myproto.SchemaChangeResult{BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}, nil
}

// apply a schema change to an entire keyspace.
// take a keyspace lock to do this.
// first we will validate the Preflight works the same on all shard masters
// and fail if not (unless force is specified)
// if simple, we just do it on all masters.
// if complex, we do the shell game in parallel on all shards
func (wr *Wrangler) ApplySchemaKeyspace(keyspace string, change string, simple, force bool) (*myproto.SchemaChangeResult, error) {
	actionNode := actionnode.ApplySchemaKeyspace(change, simple)
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return nil, err
	}

	scr, err := wr.applySchemaKeyspace(keyspace, change, simple, force)
	return scr, wr.unlockKeyspace(keyspace, actionNode, lockPath, err)
}

func (wr *Wrangler) applySchemaKeyspace(keyspace string, change string, simple, force bool) (*myproto.SchemaChangeResult, error) {
	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return nil, err
	}

	// corner cases
	if len(shards) == 0 {
		return nil, fmt.Errorf("No shards in keyspace %v", keyspace)
	}
	if len(shards) == 1 {
		log.Infof("Only one shard in keyspace %v, using ApplySchemaShard", keyspace)
		return wr.ApplySchemaShard(keyspace, shards[0], change, topo.TabletAlias{}, simple, force)
	}

	// Get schema on all shard masters in parallel
	log.Infof("Getting schema on all shards")
	beforeSchemas := make([]*myproto.SchemaDefinition, len(shards))
	shardInfos := make([]*topo.ShardInfo, len(shards))
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	getErrs := make([]string, 0, 5)
	for i, shard := range shards {
		wg.Add(1)
		go func(i int, shard string) {
			var err error
			defer func() {
				if err != nil {
					mu.Lock()
					getErrs = append(getErrs, err.Error())
					mu.Unlock()
				}
				wg.Done()
			}()

			shardInfos[i], err = wr.ts.GetShard(keyspace, shard)
			if err != nil {
				return
			}

			beforeSchemas[i], err = wr.GetSchema(shardInfos[i].MasterAlias, nil, nil, false)
		}(i, shard)
	}
	wg.Wait()
	if len(getErrs) > 0 {
		return nil, fmt.Errorf("Error(s) getting schema: %v", strings.Join(getErrs, ", "))
	}

	// check they all match, or use the force flag
	log.Infof("Checking starting schemas match on all shards")
	for i, beforeSchema := range beforeSchemas {
		if i == 0 {
			continue
		}
		diffs := myproto.DiffSchemaToArray("shard 0", beforeSchemas[0], fmt.Sprintf("shard %v", i), beforeSchema)
		if len(diffs) > 0 {
			if force {
				log.Warningf("Shard %v has inconsistent schema, ignoring: %v", i, strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Shard %v has inconsistent schema: %v", i, strings.Join(diffs, "\n"))
			}
		}
	}

	// preflight on shard 0 master, to get baseline
	// this assumes shard 0 master doesn't have the schema upgrade applied
	// if it does, we'll have to fix the slaves and other shards manually.
	log.Infof("Running Preflight on Shard 0 Master")
	preflight, err := wr.PreflightSchema(shardInfos[0].MasterAlias, change)
	if err != nil {
		return nil, err
	}

	// for each shard, apply the change
	log.Infof("Applying change on all shards")
	var applyErr error
	for i, shard := range shards {
		wg.Add(1)
		go func(i int, shard string) {
			defer wg.Done()

			_, err := wr.lockAndApplySchemaShard(shardInfos[i], preflight, keyspace, shard, shardInfos[i].MasterAlias, change, topo.TabletAlias{}, simple, force)
			if err != nil {
				mu.Lock()
				applyErr = err
				mu.Unlock()
				return
			}
		}(i, shard)
	}
	wg.Wait()
	if applyErr != nil {
		return nil, applyErr
	}

	return &myproto.SchemaChangeResult{BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}, nil
}
