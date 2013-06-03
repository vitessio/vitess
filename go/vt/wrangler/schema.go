// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

func (wr *Wrangler) GetSchema(zkTabletPath string, tables []string, includeViews bool) (*mysqlctl.SchemaDefinition, error) {
	return wr.ai.RpcGetSchema(zkTabletPath, tables, includeViews, wr.actionTimeout())
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(masterSchema *mysqlctl.SchemaDefinition, zkMasterTabletPath string, alias tm.TabletAlias, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	zkTabletPath := tm.TabletPathForAlias(alias)
	relog.Info("Gathering schema for %v", zkTabletPath)
	slaveSchema, err := wr.GetSchema(zkTabletPath, nil, includeViews)
	if err != nil {
		er.RecordError(err)
		return
	}

	relog.Info("Diffing schema for %v", zkTabletPath)
	mysqlctl.DiffSchema(zkMasterTabletPath, masterSchema, zkTabletPath, slaveSchema, er)
}

func (wr *Wrangler) ValidateSchemaShard(zkShardPath string, includeViews bool) error {
	si, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	// get schema from the master, or error
	if si.MasterAlias.Uid == tm.NO_TABLET {
		return fmt.Errorf("No master in shard " + zkShardPath)
	}
	zkMasterTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	relog.Info("Gathering schema for master %v", zkMasterTabletPath)
	masterSchema, err := wr.GetSchema(zkMasterTabletPath, nil, includeViews)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, si.ShardPath())
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
		go wr.diffSchema(masterSchema, zkMasterTabletPath, alias, includeViews, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) ValidateSchemaKeyspace(zkKeyspacePath string, includeViews bool) error {
	// find all the shards
	zkShardsPath := path.Join(zkKeyspacePath, "shards")
	shards, _, err := wr.zconn.Children(zkShardsPath)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace " + zkKeyspacePath)
	}
	sort.Strings(shards)
	referenceShardPath := path.Join(zkShardsPath, shards[0])
	if len(shards) == 1 {
		return wr.ValidateSchemaShard(referenceShardPath, includeViews)
	}

	// find the reference schema using the first shard's master
	si, err := tm.ReadShard(wr.zconn, referenceShardPath)
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == tm.NO_TABLET {
		return fmt.Errorf("No master in shard " + referenceShardPath)
	}
	zkReferenceTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	relog.Info("Gathering schema for reference master %v", zkReferenceTabletPath)
	referenceSchema, err := wr.GetSchema(zkReferenceTabletPath, nil, includeViews)
	if err != nil {
		return err
	}

	// then diff with all other tablets everywhere
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// first diff the slaves in the reference shard 0
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, si.ShardPath())
	if err != nil {
		return err
	}

	for _, alias := range aliases {
		if alias == si.MasterAlias {
			continue
		}

		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, includeViews, &wg, &er)
	}

	// then diffs all tablets in the other shards
	for _, shard := range shards[1:] {
		shardPath := path.Join(zkShardsPath, shard)
		si, err := tm.ReadShard(wr.zconn, shardPath)
		if err != nil {
			er.RecordError(err)
			continue
		}

		if si.MasterAlias.Uid == tm.NO_TABLET {
			er.RecordError(fmt.Errorf("No master in shard %v", shardPath))
			continue
		}

		aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, si.ShardPath())
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			wg.Add(1)
			go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, includeViews, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

func (wr *Wrangler) PreflightSchema(zkTabletPath string, change string) (*mysqlctl.SchemaChangeResult, error) {
	if err := tm.IsTabletPath(zkTabletPath); err != nil {
		return nil, err
	}
	actionPath, err := wr.ai.PreflightSchema(zkTabletPath, change)
	if err != nil {
		return nil, err
	}

	result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return nil, err
	}
	return result.(*mysqlctl.SchemaChangeResult), nil
}

func (wr *Wrangler) ApplySchema(zkTabletPath string, sc *mysqlctl.SchemaChange) (*mysqlctl.SchemaChangeResult, error) {
	if err := tm.IsTabletPath(zkTabletPath); err != nil {
		return nil, err
	}
	actionPath, err := wr.ai.ApplySchema(zkTabletPath, sc)

	// FIXME(alainjobart) the timeout value is wrong here, we need
	// a longer one
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
func (wr *Wrangler) ApplySchemaShard(zkShardPath string, change, zkNewParentTabletPath string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	if err := tm.IsShardPath(zkShardPath); err != nil {
		return nil, err
	}
	if zkNewParentTabletPath != "" {
		if err := tm.IsTabletPath(zkNewParentTabletPath); err != nil {
			return nil, err
		}
	}

	// read the shard from zk
	shardInfo, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return nil, err
	}

	// preflight on the master, to get baseline
	// this assumes the master doesn't have the schema upgrade applied
	// If the master does, and some slaves don't, may have to
	// fix them manually one at a time, or re-clone them.
	// we do this outside of the shard lock because we can.
	relog.Info("Running Preflight on Master")
	zkMasterTabletPath, err := shardInfo.MasterTabletPath()
	if err != nil {
		return nil, err
	}
	preflight, err := wr.PreflightSchema(zkMasterTabletPath, change)
	if err != nil {
		return nil, err
	}

	return wr.lockAndApplySchemaShard(shardInfo, preflight, zkShardPath, zkMasterTabletPath, change, zkNewParentTabletPath, simple, force)
}

func (wr *Wrangler) lockAndApplySchemaShard(shardInfo *tm.ShardInfo, preflight *mysqlctl.SchemaChangeResult, shardPath, zkMasterTabletPath, change, zkNewParentTabletPath string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// get a shard lock
	actionPath, err := wr.ai.ApplySchemaShard(shardPath, zkNewParentTabletPath, change, simple)
	if err != nil {
		return nil, err
	}

	// Make sure two of these don't get scheduled at the same time.
	if err = wr.obtainActionLock(actionPath); err != nil {
		return nil, err
	}

	scr, schemaErr := wr.applySchemaShard(shardInfo, preflight, zkMasterTabletPath, change, zkNewParentTabletPath, simple, force)
	relog.Info("applySchemaShard finished on %v error=%v", shardPath, schemaErr)

	err = wr.handleActionError(actionPath, schemaErr, false)
	if err != nil {
		relog.Warning("handleActionError failed: %v", err)
	}
	// schemaErr has higher priority
	if schemaErr != nil {
		return nil, schemaErr
	}
	if err != nil {
		return nil, err
	}
	return scr, nil
}

// local structure used to keep track of what we're doing
type TabletStatus struct {
	zkTabletPath string
	lastError    error
	beforeSchema *mysqlctl.SchemaDefinition
}

func (wr *Wrangler) applySchemaShard(shardInfo *tm.ShardInfo, preflight *mysqlctl.SchemaChangeResult, zkMasterTabletPath, change, zkNewParentTabletPath string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {

	// find all the shards we need to handle
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, shardInfo.ShardPath())
	if err != nil {
		return nil, err
	}

	// build the array of TabletStatus we're going to use
	statusArray := make([]*TabletStatus, 0, len(aliases)-1)
	for _, alias := range aliases {
		tabletPath := tm.TabletPathForAlias(alias)
		if tabletPath == zkMasterTabletPath {
			// we skip the master
			continue
		}

		ti, err := tm.ReadTablet(wr.zconn, tabletPath)
		if err != nil {
			return nil, err
		}
		if ti.Type == tm.TYPE_LAG {
			// lag tablets are usually behind, not replicating,
			// and a general pain. So let's just skip them
			// all together.
			// TODO(alainjobart) figure out other types to skip:
			// ValidateSchemaShard only does the serving types.
			// We do everything in the replication graph
			// but LAG. This seems fine for now.
			relog.Info("Skipping tablet %v as it is LAG", ti.Path())
			continue
		}

		statusArray = append(statusArray, &TabletStatus{zkTabletPath: tabletPath})
	}

	// get schema on all tablets. This is an action, so returning
	// from this guarantees all tablets are ready for an action.
	// In particular, if we had interrupted a schema change
	// before, and some tablets are still applying it, this would
	// wait until they're done.
	relog.Info("Getting schema on all tablets for shard %v", shardInfo.ShardPath())
	wg := &sync.WaitGroup{}
	for _, status := range statusArray {
		wg.Add(1)
		go func(status *TabletStatus) {
			status.beforeSchema, status.lastError = wr.GetSchema(status.zkTabletPath, nil, false)
			wg.Done()
		}(status)
	}
	wg.Wait()

	// quick check for errors
	for _, status := range statusArray {
		if status.lastError != nil {
			return nil, fmt.Errorf("Error getting schema on tablet %v: %v", status.zkTabletPath, status.lastError)
		}
	}

	// simple or complex?
	if simple {
		return wr.applySchemaShardSimple(statusArray, preflight, zkMasterTabletPath, change, force)
	}

	return wr.applySchemaShardComplex(statusArray, shardInfo, preflight, zkMasterTabletPath, change, zkNewParentTabletPath, force)
}

func (wr *Wrangler) applySchemaShardSimple(statusArray []*TabletStatus, preflight *mysqlctl.SchemaChangeResult, zkMasterTabletPath, change string, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// check all tablets have the same schema as the master's
	// BeforeSchema. If not, we shouldn't proceed
	relog.Info("Checking schema on all tablets")
	for _, status := range statusArray {
		diffs := mysqlctl.DiffSchemaToArray("master", preflight.BeforeSchema, status.zkTabletPath, status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				relog.Warning("Tablet %v has inconsistent schema, ignoring: %v", status.zkTabletPath, strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.zkTabletPath, strings.Join(diffs, "\n"))
			}
		}
	}

	// we're good, just send to the master
	relog.Info("Applying schema change to master in simple mode")
	sc := &mysqlctl.SchemaChange{Sql: change, Force: force, AllowReplication: true, BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}
	return wr.ApplySchema(zkMasterTabletPath, sc)
}

func (wr *Wrangler) applySchemaShardComplex(statusArray []*TabletStatus, shardInfo *tm.ShardInfo, preflight *mysqlctl.SchemaChangeResult, zkMasterTabletPath, change, zkNewParentTabletPath string, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// apply the schema change to all replica / slave tablets
	for _, status := range statusArray {
		// if already applied, we skip this guy
		diffs := mysqlctl.DiffSchemaToArray("after", preflight.AfterSchema, status.zkTabletPath, status.beforeSchema)
		if len(diffs) == 0 {
			relog.Info("Tablet %v already has the AfterSchema, skipping", status.zkTabletPath)
			continue
		}

		// make sure the before schema matches
		diffs = mysqlctl.DiffSchemaToArray("master", preflight.BeforeSchema, status.zkTabletPath, status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				relog.Warning("Tablet %v has inconsistent schema, ignoring: %v", status.zkTabletPath, strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.zkTabletPath, strings.Join(diffs, "\n"))
			}
		}

		// take this guy out of the serving graph if necessary
		ti, err := tm.ReadTablet(wr.zconn, status.zkTabletPath)
		if err != nil {
			return nil, err
		}
		typeChangeRequired := ti.Tablet.IsServingType()
		if typeChangeRequired {
			// note we want to update the serving graph there
			err = wr.changeTypeInternal(ti.Path(), tm.TYPE_SCHEMA_UPGRADE)
			if err != nil {
				return nil, err
			}
		}

		// apply the schema change
		relog.Info("Applying schema change to slave %v in complex mode", status.zkTabletPath)
		sc := &mysqlctl.SchemaChange{Sql: change, Force: force, AllowReplication: false, BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}
		_, err = wr.ApplySchema(status.zkTabletPath, sc)
		if err != nil {
			return nil, err
		}

		// put this guy back into the serving graph
		if typeChangeRequired {
			err = wr.changeTypeInternal(ti.Path(), ti.Tablet.Type)
			if err != nil {
				return nil, err
			}
		}
	}

	// if zkNewParentTabletPath is passed in, use that as the new master
	if zkNewParentTabletPath != "" {
		relog.Info("Reparenting with new master set to %v", zkNewParentTabletPath)
		tabletMap, err := GetTabletMapForShard(wr.zconn, shardInfo.ShardPath())
		if err != nil {
			return nil, err
		}

		slaveTabletMap, foundMaster, err := slaveTabletMap(tabletMap)
		if err != nil {
			return nil, err
		}

		newMasterTablet, err := wr.readTablet(zkNewParentTabletPath)
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
		relog.Info("Skipping schema change on old master %v in complex mode, it's been Scrapped", zkMasterTabletPath)
	}
	return &mysqlctl.SchemaChangeResult{BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}, nil
}

// apply a schema change to an entire keyspace.
// take a keyspace lock to do this.
// first we will validate the Preflight works the same on all shard masters
// and fail if not (unless force is specified)
// if simple, we just do it on all masters.
// if complex, we do the shell game in parallel on all shards
func (wr *Wrangler) ApplySchemaKeyspace(zkKeyspacePath string, change string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	relog.Info("Reading keyspace and getting lock")
	if err := tm.IsKeyspacePath(zkKeyspacePath); err != nil {
		return nil, err
	}
	actionPath, err := wr.ai.ApplySchemaKeyspace(zkKeyspacePath, change, simple)
	if err != nil {
		return nil, err
	}

	// Make sure two of these don't get scheduled at the same time.
	if err = wr.obtainActionLock(actionPath); err != nil {
		return nil, err
	}

	scr, schemaErr := wr.applySchemaKeyspace(zkKeyspacePath, change, simple, force)
	err = wr.handleActionError(actionPath, schemaErr, false)
	if schemaErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return nil, schemaErr
	}
	return scr, err
}

func (wr *Wrangler) applySchemaKeyspace(zkKeyspacePath string, change string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	zkShardsPath := path.Join(zkKeyspacePath, "shards")
	shards, _, err := wr.zconn.Children(zkShardsPath)
	if err != nil {
		return nil, err
	}

	// corner cases
	if len(shards) == 0 {
		return nil, fmt.Errorf("No shards in keyspace " + zkKeyspacePath)
	}
	if len(shards) == 1 {
		relog.Info("Only one shard in keyspace %v, using ApplySchemaShard", zkKeyspacePath)
		return wr.ApplySchemaShard(path.Join(zkShardsPath, shards[0]), change, "", simple, force)
	}

	// Get schema on all shard masters in parallel
	relog.Info("Getting schema on all shards")
	beforeSchemas := make([]*mysqlctl.SchemaDefinition, len(shards))
	zkMasterTabletPaths := make([]string, len(shards))
	shardInfos := make([]*tm.ShardInfo, len(shards))
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	getErrs := make([]string, 0, 5)
	for i, shard := range shards {
		shardPath := path.Join(zkShardsPath, shard)
		wg.Add(1)
		go func(i int) {
			var err error
			defer func() {
				if err != nil {
					mu.Lock()
					getErrs = append(getErrs, err.Error())
					mu.Unlock()
				}
				wg.Done()
			}()

			shardInfos[i], err = tm.ReadShard(wr.zconn, shardPath)
			if err != nil {
				return
			}

			zkMasterTabletPaths[i], err = shardInfos[i].MasterTabletPath()
			if err != nil {
				return
			}

			beforeSchemas[i], err = wr.GetSchema(zkMasterTabletPaths[i], nil, false)
		}(i)
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
	preflight, err := wr.PreflightSchema(zkMasterTabletPaths[0], change)
	if err != nil {
		return nil, err
	}

	// for each shard, apply the change
	relog.Info("Applying change on all shards")
	var applyErr error
	for i, shard := range shards {
		shardPath := path.Join(zkShardsPath, shard)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			_, err := wr.lockAndApplySchemaShard(shardInfos[i], preflight, shardPath, zkMasterTabletPaths[i], change, "", simple, force)
			if err != nil {
				mu.Lock()
				applyErr = err
				mu.Unlock()
				return
			}
		}(i)
	}
	wg.Wait()
	if applyErr != nil {
		return nil, applyErr
	}

	return &mysqlctl.SchemaChangeResult{BeforeSchema: preflight.BeforeSchema, AfterSchema: preflight.AfterSchema}, nil
}
