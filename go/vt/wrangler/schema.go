// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

func (wr *Wrangler) GetSchema(zkTabletPath string) (*mysqlctl.SchemaDefinition, error) {
	tm.MustBeTabletPath(zkTabletPath)
	actionPath, err := wr.ai.GetSchema(zkTabletPath)

	sd := new(mysqlctl.SchemaDefinition)
	if err = wr.WaitForActionResult(actionPath, sd, wr.actionTimeout()); err != nil {
		return nil, err
	}
	return sd, nil
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(masterSchema *mysqlctl.SchemaDefinition, zkMasterTabletPath string, alias tm.TabletAlias, wg *sync.WaitGroup, result chan string) {
	defer wg.Done()
	zkTabletPath := tm.TabletPathForAlias(alias)
	slaveSchema, err := wr.GetSchema(zkTabletPath)
	if err != nil {
		result <- err.Error()
		return
	}

	masterSchema.DiffSchema(zkMasterTabletPath, zkTabletPath, slaveSchema, result)
}

func channelToError(stream chan string) error {
	result := ""
	for text := range stream {
		if result != "" {
			result += "\n"
		}
		result += text
	}
	if result == "" {
		return nil
	}

	return fmt.Errorf("Schema diffs:\n%v", result)
}

func (wr *Wrangler) ValidateSchemaShard(zkShardPath string) error {
	si, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	// get schema from the master, or error
	if si.MasterAlias.Uid == 0 {
		return fmt.Errorf("No master in shard " + zkShardPath)
	}
	zkMasterTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	masterSchema, err := wr.GetSchema(zkMasterTabletPath)
	if err != nil {
		return err
	}

	// then diff with all slaves
	result := make(chan string, 10)
	wg := &sync.WaitGroup{}
	for _, alias := range si.ReplicaAliases {
		wg.Add(1)
		go wr.diffSchema(masterSchema, zkMasterTabletPath, alias, wg, result)
	}
	for _, alias := range si.RdonlyAliases {
		wg.Add(1)
		go wr.diffSchema(masterSchema, zkMasterTabletPath, alias, wg, result)
	}

	wg.Wait()
	close(result)
	return channelToError(result)
}

func (wr *Wrangler) ValidateSchemaKeyspace(zkKeyspacePath string) error {
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
	referenceShardPath := path.Join(zkShardsPath, shards[0])
	if len(shards) == 1 {
		return wr.ValidateSchemaShard(referenceShardPath)
	}

	// find the reference schema using the first shard's master
	si, err := tm.ReadShard(wr.zconn, referenceShardPath)
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == 0 {
		return fmt.Errorf("No master in shard " + referenceShardPath)
	}
	zkReferenceTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	referenceSchema, err := wr.GetSchema(zkReferenceTabletPath)
	if err != nil {
		return err
	}

	//
	// then diff with all slaves
	result := make(chan string, 10)
	wg := &sync.WaitGroup{}

	// first diff the slaves in the main shard
	for _, alias := range si.ReplicaAliases {
		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
	}
	for _, alias := range si.RdonlyAliases {
		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
	}

	// then diffs the masters in the other shards, along with
	// their slaves
	for _, shard := range shards[1:] {
		shardPath := path.Join(zkShardsPath, shard)
		si, err := tm.ReadShard(wr.zconn, shardPath)
		if err != nil {
			result <- err.Error()
			continue
		}

		if si.MasterAlias.Uid == 0 {
			result <- "No master in shard " + shardPath
			continue
		}

		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, si.MasterAlias, wg, result)
		for _, alias := range si.ReplicaAliases {
			wg.Add(1)
			go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
		}
		for _, alias := range si.RdonlyAliases {
			wg.Add(1)
			go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
		}
	}

	wg.Wait()
	close(result)
	return channelToError(result)
}

func (wr *Wrangler) PreflightSchema(zkTabletPath string, change string) (*mysqlctl.SchemaChangeResult, error) {
	tm.MustBeTabletPath(zkTabletPath)
	actionPath, err := wr.ai.PreflightSchema(zkTabletPath, change)

	scr := new(mysqlctl.SchemaChangeResult)
	if err = wr.WaitForActionResult(actionPath, scr, wr.actionTimeout()); err != nil {
		return nil, err
	}
	return scr, nil
}

func (wr *Wrangler) ApplySchema(zkTabletPath string, sc *mysqlctl.SchemaChange) (*mysqlctl.SchemaChangeResult, error) {
	tm.MustBeTabletPath(zkTabletPath)
	actionPath, err := wr.ai.ApplySchema(zkTabletPath, sc)

	// FIXME(alainjobart) the timeout value is wrong here, we need
	// a longer one
	scr := new(mysqlctl.SchemaChangeResult)
	if err = wr.WaitForActionResult(actionPath, scr, wr.actionTimeout()); err != nil {
		return nil, err
	}
	return scr, nil
}

// Note for 'complex' mode (the 'simple' mode is easy enough that we
// don't need to handle recovery that much): this method is able to
// recover if interrupted in the middle, because it knows which server
// has the schema change already applied, and will just pass through them
// very quickly.
func (wr *Wrangler) ApplySchemaShard(zkShardPath string, change, zkNewParentTabletPath string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {
	tm.MustBeShardPath(zkShardPath)
	if zkNewParentTabletPath != "" {
		tm.MustBeTabletPath(zkNewParentTabletPath)
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

	// We think we can do this, let's get a shard lock
	actionPath, err := wr.ai.ApplySchemaShard(zkShardPath)
	if err != nil {
		return nil, err
	}

	// Make sure two of these don't get scheduled at the same time.
	ok, err := zk.ObtainQueueLock(wr.zconn, actionPath, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		// just clean up for now, in the future we may want to try harder, or wait
		wr.zconn.Delete(actionPath, -1)
		return nil, fmt.Errorf("ApplySchemaShard failed to obtain shard action lock")
	}

	scr, schemaErr := wr.applySchemaShard(shardInfo, preflight, zkMasterTabletPath, actionPath, change, zkNewParentTabletPath, simple, force)
	relog.Info("applySchemaShard finished error=%v", schemaErr)

	err = wr.handleActionError(actionPath, schemaErr)
	if schemaErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return nil, schemaErr
	}
	return scr, err
}

// local structure used to keep track of what we're doing
type TabletStatus struct {
	zkTabletPath string
	lastError    error
	beforeSchema *mysqlctl.SchemaDefinition
}

func (wr *Wrangler) applySchemaShard(shardInfo *tm.ShardInfo, preflight *mysqlctl.SchemaChangeResult, zkMasterTabletPath, actionPath, change, zkNewParentTabletPath string, simple, force bool) (*mysqlctl.SchemaChangeResult, error) {

	// find all the shards we need to handle
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, shardInfo.ShardPath())
	if err != nil {
		return nil, err
	}

	// build the array of TabletStatus we're going to use
	statusArray := make([]*TabletStatus, 0, len(aliases)-1)
	for _, alias := range aliases {
		tabletPath := tm.TabletPathForAlias(alias)
		if tabletPath != zkMasterTabletPath {
			statusArray = append(statusArray, &TabletStatus{zkTabletPath: tabletPath})
		}
	}

	// get schema on all tablets. This is an action, so returning
	// from this guarantees all tablets are ready for an action.
	// In particular, if we had interrupted a schema change
	// before, and some tablets are still applying it, this would
	// wait until they're done.
	relog.Info("Getting schema on all tablets")
	wg := &sync.WaitGroup{}
	for _, status := range statusArray {
		wg.Add(1)
		go func(status *TabletStatus) {
			status.beforeSchema, status.lastError = wr.GetSchema(status.zkTabletPath)
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

	return wr.applySchemaShardComplex(statusArray, shardInfo, preflight, zkMasterTabletPath, actionPath, change, zkNewParentTabletPath, force)
}

func (wr *Wrangler) applySchemaShardSimple(statusArray []*TabletStatus, preflight *mysqlctl.SchemaChangeResult, zkMasterTabletPath, change string, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// check all tablets have the same schema as the master's
	// BeforeSchema. If not, we shouldn't proceed
	relog.Info("Checking schema on all tablets")
	for _, status := range statusArray {
		diffs := preflight.BeforeSchema.DiffSchemaToArray("master", status.zkTabletPath, status.beforeSchema)
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

func (wr *Wrangler) applySchemaShardComplex(statusArray []*TabletStatus, shardInfo *tm.ShardInfo, preflight *mysqlctl.SchemaChangeResult, zkMasterTabletPath, actionPath, change, zkNewParentTabletPath string, force bool) (*mysqlctl.SchemaChangeResult, error) {
	// apply the schema change to all replica / slave tablets
	for _, status := range statusArray {
		// if already applied, we skip this guy
		diffs := preflight.AfterSchema.DiffSchemaToArray("after", status.zkTabletPath, status.beforeSchema)
		if len(diffs) == 0 {
			relog.Info("Tablet %v already has the AfterSchema, skipping", status.zkTabletPath)
			continue
		}

		// make sure the before schema matches
		diffs = preflight.BeforeSchema.DiffSchemaToArray("master", status.zkTabletPath, status.beforeSchema)
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
		scr, err := wr.ApplySchema(status.zkTabletPath, sc)
		if err != nil || scr.Error != "" {
			return scr, err
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
		newMasterTablet, err := wr.readTablet(zkNewParentTabletPath)
		if err != nil {
			return nil, err
		}
		err = wr.reparentShard(shardInfo, newMasterTablet, actionPath /*leaveMasterReadOnly*/, false)
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
