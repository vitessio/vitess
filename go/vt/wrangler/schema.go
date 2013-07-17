// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/topo"
)

func (wr *Wrangler) GetSchema(tabletAlias topo.TabletAlias, tables []string, includeViews bool) (*mysqlctl.SchemaDefinition, error) {
	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	return wr.GetSchemaTablet(ti, tables, includeViews)
}

func (wr *Wrangler) GetSchemaTablet(tablet *topo.TabletInfo, tables []string, includeViews bool) (*mysqlctl.SchemaDefinition, error) {
	return wr.ai.RpcGetSchemaTablet(tablet, tables, includeViews, wr.actionTimeout())
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(masterSchema *mysqlctl.SchemaDefinition, masterTabletAlias, alias topo.TabletAlias, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	relog.Info("Gathering schema for %v", alias)
	slaveSchema, err := wr.GetSchema(alias, nil, includeViews)
	if err != nil {
		er.RecordError(err)
		return
	}

	relog.Info("Diffing schema for %v", alias)
	mysqlctl.DiffSchema(masterTabletAlias.String(), masterSchema, alias.String(), slaveSchema, er)
}

func (wr *Wrangler) ValidateSchemaShard(keyspace, shard string, includeViews bool) error {
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// get schema from the master, or error
	if si.MasterAlias.Uid == topo.NO_TABLET {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shard)
	}
	relog.Info("Gathering schema for master %v", si.MasterAlias)
	masterSchema, err := wr.GetSchema(si.MasterAlias, nil, includeViews)
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
		go wr.diffSchema(masterSchema, si.MasterAlias, alias, includeViews, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) ValidateSchemaKeyspace(keyspace string, includeViews bool) error {
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
		return wr.ValidateSchemaShard(keyspace, shards[0], includeViews)
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
	relog.Info("Gathering schema for reference master %v", referenceAlias)
	referenceSchema, err := wr.GetSchema(referenceAlias, nil, includeViews)
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
		go wr.diffSchema(referenceSchema, referenceAlias, alias, includeViews, &wg, &er)
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
			go wr.diffSchema(referenceSchema, referenceAlias, alias, includeViews, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) PreflightSchema(tabletAlias topo.TabletAlias, change string) (*mysqlctl.SchemaChangeResult, error) {
	actionPath, err := wr.ai.PreflightSchema(tabletAlias, change)
	if err != nil {
		return nil, err
	}

	result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return nil, err
	}
	return result.(*mysqlctl.SchemaChangeResult), nil
}

func (wr *Wrangler) ApplySchema(tabletAlias topo.TabletAlias, sc *mysqlctl.SchemaChange) (*mysqlctl.SchemaChangeResult, error) {
	actionPath, err := wr.ai.ApplySchema(tabletAlias, sc)

	// the timeout is for the entire action, so it might be too big
	// for an individual tablet
	results, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return nil, err
	}
	return results.(*mysqlctl.SchemaChangeResult), nil
}

// Note for 'complex' mode (the 'simple' mode is easy enough that we
// don't need to handle recovery that much): this method is able to
// recover if interrupted in the middle, because it knows which server
// has the schema change already applied, and will just pass through them
// very quickly.
func (wr *Wrangler) ApplySchemaShard(keyspace, shard, change string, newParentTabletAlias topo.TabletAlias, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
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
	relog.Info("Running Preflight on Master %v", shardInfo.MasterAlias)
	if err != nil {
		return nil, err
	}
	preflight, err := wr.PreflightSchema(shardInfo.MasterAlias, change)
	if err != nil {
		return nil, err
	}

	return wr.lockAndApplySchemaShard(shardInfo, preflight, keyspace, shard, shardInfo.MasterAlias, change, newParentTabletAlias, simple, force)
}

func (wr *Wrangler) lockAndApplySchemaShard(shardInfo *topo.ShardInfo, preflight *mysqlctl.SchemaChangeResult, keyspace, shard string, masterTabletAlias topo.TabletAlias, change string, newParentTabletAlias topo.TabletAlias, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// get a shard lock
	actionNode := wr.ai.ApplySchemaShard(masterTabletAlias, change, simple)
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
	beforeSchema *mysqlctl.SchemaDefinition
}

func (wr *Wrangler) applySchemaShard(shardInfo *topo.ShardInfo, preflight *mysqlctl.SchemaChangeResult, masterTabletAlias topo.TabletAlias, change string, newParentTabletAlias topo.TabletAlias, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {

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
			relog.Info("Skipping tablet %v as it is LAG", ti.Alias())
			continue
		}

		statusArray = append(statusArray, &TabletStatus{ti: ti})
	}

	// get schema on all tablets.
	relog.Info("Getting schema on all tablets for shard %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())
	wg := &sync.WaitGroup{}
	for _, status := range statusArray {
		wg.Add(1)
		go func(status *TabletStatus) {
			status.beforeSchema, status.lastError = wr.GetSchemaTablet(status.ti, nil, false)
			wg.Done()
		}(status)
	}
	wg.Wait()

	// quick check for errors
	for _, status := range statusArray {
		if status.lastError != nil {
			return nil, fmt.Errorf("Error getting schema on tablet %v: %v", status.ti.Alias(), status.lastError)
		}
	}

	// simple or complex?
	if simple {
		return wr.applySchemaShardSimple(statusArray, preflight, masterTabletAlias, change, force)
	}

	return wr.applySchemaShardComplex(statusArray, shardInfo, preflight, masterTabletAlias, change, newParentTabletAlias, force)
}

func (wr *Wrangler) applySchemaShardSimple(statusArray []*TabletStatus, preflight *mysqlctl.SchemaChangeResult, masterTabletAlias topo.TabletAlias, change string, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// check all tablets have the same schema as the master's
	// BeforeSchema. If not, we shouldn't proceed
	relog.Info("Checking schema on all tablets")
	for _, status := range statusArray {
		diffs := mysqlctl.DiffSchemaToArray("master", preflight.BeforeSchema, status.ti.Alias().String(), status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				relog.Warning("Tablet %v has inconsistent schema, ignoring: %v", status.ti.Alias(), strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.ti.Alias(), strings.Join(diffs, "\n"))
			}
		}
	}

	// we're good, just send to the master
	relog.Info("Applying schema change to master in simple mode")
	sc := &mysqlctl.SchemaChange{Sql: change, Force: force, AllowReplication: true, BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}
	return wr.ApplySchema(masterTabletAlias, sc)
}

func (wr *Wrangler) applySchemaShardComplex(statusArray []*TabletStatus, shardInfo *topo.ShardInfo, preflight *mysqlctl.SchemaChangeResult, masterTabletAlias topo.TabletAlias, change string, newParentTabletAlias topo.TabletAlias, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// apply the schema change to all replica / slave tablets
	for _, status := range statusArray {
		// if already applied, we skip this guy
		diffs := mysqlctl.DiffSchemaToArray("after", preflight.AfterSchema, status.ti.Alias().String(), status.beforeSchema)
		if len(diffs) == 0 {
			relog.Info("Tablet %v already has the AfterSchema, skipping", status.ti.Alias())
			continue
		}

		// make sure the before schema matches
		diffs = mysqlctl.DiffSchemaToArray("master", preflight.BeforeSchema, status.ti.Alias().String(), status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				relog.Warning("Tablet %v has inconsistent schema, ignoring: %v", status.ti.Alias(), strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.ti.Alias(), strings.Join(diffs, "\n"))
			}
		}

		// take this guy out of the serving graph if necessary
		ti, err := wr.ts.GetTablet(status.ti.Alias())
		if err != nil {
			return nil, err
		}
		typeChangeRequired := ti.Tablet.IsServingType()
		if typeChangeRequired {
			// note we want to update the serving graph there
			err = wr.changeTypeInternal(ti.Alias(), topo.TYPE_SCHEMA_UPGRADE)
			if err != nil {
				return nil, err
			}
		}

		// apply the schema change
		relog.Info("Applying schema change to slave %v in complex mode", status.ti.Alias())
		sc := &mysqlctl.SchemaChange{Sql: change, Force: force, AllowReplication: false, BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}
		_, err = wr.ApplySchema(status.ti.Alias(), sc)
		if err != nil {
			return nil, err
		}

		// put this guy back into the serving graph
		if typeChangeRequired {
			err = wr.changeTypeInternal(ti.Alias(), ti.Tablet.Type)
			if err != nil {
				return nil, err
			}
		}
	}

	// if newParentTabletAlias is passed in, use that as the new master
	if newParentTabletAlias != (topo.TabletAlias{}) {
		relog.Info("Reparenting with new master set to %v", newParentTabletAlias)
		tabletMap, err := GetTabletMapForShard(wr.ts, shardInfo.Keyspace(), shardInfo.ShardName())
		if err != nil {
			return nil, err
		}

		slaveTabletMap, foundMaster, err := slaveTabletMap(tabletMap)
		if err != nil {
			return nil, err
		}

		newMasterTablet, err := wr.ts.GetTablet(newParentTabletAlias)
		if err != nil {
			return nil, err
		}

		err = wr.reparentShardGraceful(slaveTabletMap, foundMaster, newMasterTablet /*leaveMasterReadOnly*/, false)
		if err != nil {
			return nil, err
		}

		// Here we would apply the schema change to the old
		// master, but after a reparent it's in Scrap state,
		// so no need to.  When/if reparent leaves the
		// original master in a different state (like replica
		// or rdonly), then we should apply the schema there
		// too.
		relog.Info("Skipping schema change on old master %v in complex mode, it's been Scrapped", masterTabletAlias)
	}
	return &mysqlctl.SchemaChangeResult{BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}, nil
}

// apply a schema change to an entire keyspace.
// take a keyspace lock to do this.
// first we will validate the Preflight works the same on all shard masters
// and fail if not (unless force is specified)
// if simple, we just do it on all masters.
// if complex, we do the shell game in parallel on all shards
func (wr *Wrangler) ApplySchemaKeyspace(keyspace string, change string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	actionNode := wr.ai.ApplySchemaKeyspace(change, simple)
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return nil, err
	}

	scr, err := wr.applySchemaKeyspace(keyspace, change, simple, force)
	return scr, wr.unlockKeyspace(keyspace, actionNode, lockPath, err)
}

func (wr *Wrangler) applySchemaKeyspace(keyspace string, change string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return nil, err
	}

	// corner cases
	if len(shards) == 0 {
		return nil, fmt.Errorf("No shards in keyspace %v", keyspace)
	}
	if len(shards) == 1 {
		relog.Info("Only one shard in keyspace %v, using ApplySchemaShard", keyspace)
		return wr.ApplySchemaShard(keyspace, shards[0], change, topo.TabletAlias{}, simple, force)
	}

	// Get schema on all shard masters in parallel
	relog.Info("Getting schema on all shards")
	beforeSchemas := make([]*mysqlctl.SchemaDefinition, len(shards))
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

			beforeSchemas[i], err = wr.GetSchema(shardInfos[i].MasterAlias, nil, false)
		}(i, shard)
	}
	wg.Wait()
	if len(getErrs) > 0 {
		return nil, fmt.Errorf("Error(s) getting schema: %v", strings.Join(getErrs, ", "))
	}

	// check they all match, or use the force flag
	relog.Info("Checking starting schemas match on all shards")
	for i, beforeSchema := range beforeSchemas {
		if i == 0 {
			continue
		}
		diffs := mysqlctl.DiffSchemaToArray("shard 0", beforeSchemas[0], fmt.Sprintf("shard %v", i), beforeSchema)
		if len(diffs) > 0 {
			if force {
				relog.Warning("Shard %v has inconsistent schema, ignoring: %v", i, strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Shard %v has inconsistent schema: %v", i, strings.Join(diffs, "\n"))
			}
		}
	}

	// preflight on shard 0 master, to get baseline
	// this assumes shard 0 master doesn't have the schema upgrade applied
	// if it does, we'll have to fix the slaves and other shards manually.
	relog.Info("Running Preflight on Shard 0 Master")
	preflight, err := wr.PreflightSchema(shardInfos[0].MasterAlias, change)
	if err != nil {
		return nil, err
	}

	// for each shard, apply the change
	relog.Info("Applying change on all shards")
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

	return &mysqlctl.SchemaChangeResult{BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}, nil
}
