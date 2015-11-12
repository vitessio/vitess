// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"bytes"
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/schemamanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// GetSchema uses an RPC to get the schema from a remote tablet
func (wr *Wrangler) GetSchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}

	return wr.tmc.GetSchema(ctx, ti, tables, excludeTables, includeViews)
}

// ReloadSchema forces the remote tablet to reload its schema.
func (wr *Wrangler) ReloadSchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	return wr.tmc.ReloadSchema(ctx, ti)
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(ctx context.Context, masterSchema *tabletmanagerdatapb.SchemaDefinition, masterTabletAlias, alias *topodatapb.TabletAlias, excludeTables []string, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering schema for %v", topoproto.TabletAliasString(alias))
	slaveSchema, err := wr.GetSchema(ctx, alias, nil, excludeTables, includeViews)
	if err != nil {
		er.RecordError(err)
		return
	}

	log.Infof("Diffing schema for %v", topoproto.TabletAliasString(alias))
	tmutils.DiffSchema(topoproto.TabletAliasString(masterTabletAlias), masterSchema, topoproto.TabletAliasString(alias), slaveSchema, er)
}

// ValidateSchemaShard will diff the schema from all the tablets in the shard.
func (wr *Wrangler) ValidateSchemaShard(ctx context.Context, keyspace, shard string, excludeTables []string, includeViews bool) error {
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// get schema from the master, or error
	if !si.HasMaster() {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shard)
	}
	log.Infof("Gathering schema for master %v", topoproto.TabletAliasString(si.MasterAlias))
	masterSchema, err := wr.GetSchema(ctx, si.MasterAlias, nil, excludeTables, includeViews)
	if err != nil {
		return err
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// then diff with all slaves
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if topoproto.TabletAliasEqual(alias, si.MasterAlias) {
			continue
		}

		wg.Add(1)
		go wr.diffSchema(ctx, masterSchema, si.MasterAlias, alias, excludeTables, includeViews, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

// ValidateSchemaKeyspace will diff the schema from all the tablets in
// the keyspace.
func (wr *Wrangler) ValidateSchemaKeyspace(ctx context.Context, keyspace string, excludeTables []string, includeViews bool) error {
	// find all the shards
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace %v", keyspace)
	}
	sort.Strings(shards)
	if len(shards) == 1 {
		return wr.ValidateSchemaShard(ctx, keyspace, shards[0], excludeTables, includeViews)
	}

	// find the reference schema using the first shard's master
	si, err := wr.ts.GetShard(ctx, keyspace, shards[0])
	if err != nil {
		return err
	}
	if !si.HasMaster() {
		return fmt.Errorf("No master in shard %v/%v", keyspace, shards[0])
	}
	referenceAlias := si.MasterAlias
	log.Infof("Gathering schema for reference master %v", topoproto.TabletAliasString(referenceAlias))
	referenceSchema, err := wr.GetSchema(ctx, referenceAlias, nil, excludeTables, includeViews)
	if err != nil {
		return err
	}

	// then diff with all other tablets everywhere
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// first diff the slaves in the reference shard 0
	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shards[0])
	if err != nil {
		return err
	}

	for _, alias := range aliases {
		if topoproto.TabletAliasEqual(alias, si.MasterAlias) {
			continue
		}

		wg.Add(1)
		go wr.diffSchema(ctx, referenceSchema, referenceAlias, alias, excludeTables, includeViews, &wg, &er)
	}

	// then diffs all tablets in the other shards
	for _, shard := range shards[1:] {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		if !si.HasMaster() {
			er.RecordError(fmt.Errorf("No master in shard %v/%v", keyspace, shard))
			continue
		}

		aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
		if err != nil {
			er.RecordError(err)
			continue
		}

		for _, alias := range aliases {
			wg.Add(1)
			go wr.diffSchema(ctx, referenceSchema, referenceAlias, alias, excludeTables, includeViews, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("Schema diffs:\n%v", er.Error().Error())
	}
	return nil
}

// PreflightSchema will try a schema change on the remote tablet.
func (wr *Wrangler) PreflightSchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias, change string) (*tmutils.SchemaChangeResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.PreflightSchema(ctx, ti, change)
}

// ApplySchema will apply a schema change on the remote tablet.
func (wr *Wrangler) ApplySchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias, sc *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return wr.tmc.ApplySchema(ctx, ti, sc)
}

// ApplySchemaShard applies a schema change on a shard.
func (wr *Wrangler) ApplySchemaShard(ctx context.Context, keyspace, shard, change string, newParentTabletAlias *topodatapb.TabletAlias, force bool, waitSlaveTimeout time.Duration) (*tmutils.SchemaChangeResult, error) {
	// read the shard
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
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
	preflight, err := wr.PreflightSchema(ctx, shardInfo.MasterAlias, change)
	if err != nil {
		return nil, err
	}

	return wr.lockAndApplySchemaShard(ctx, shardInfo, preflight, keyspace, shard, shardInfo.MasterAlias, change, newParentTabletAlias, force, waitSlaveTimeout)
}

func (wr *Wrangler) lockAndApplySchemaShard(ctx context.Context, shardInfo *topo.ShardInfo, preflight *tmutils.SchemaChangeResult, keyspace, shard string, masterTabletAlias *topodatapb.TabletAlias, change string, newParentTabletAlias *topodatapb.TabletAlias, force bool, waitSlaveTimeout time.Duration) (*tmutils.SchemaChangeResult, error) {
	// get a shard lock
	actionNode := actionnode.ApplySchemaShard(masterTabletAlias, change)
	lockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		return nil, err
	}

	scr, err := wr.applySchemaShard(ctx, shardInfo, preflight, masterTabletAlias, change, newParentTabletAlias, force, waitSlaveTimeout)
	return scr, wr.unlockShard(ctx, keyspace, shard, actionNode, lockPath, err)
}

// tabletStatus is a local structure used to keep track of what we're doing
type tabletStatus struct {
	ti           *topo.TabletInfo
	lastError    error
	beforeSchema *tabletmanagerdatapb.SchemaDefinition
}

func (wr *Wrangler) applySchemaShard(ctx context.Context, shardInfo *topo.ShardInfo, preflight *tmutils.SchemaChangeResult, masterTabletAlias *topodatapb.TabletAlias, change string, newParentTabletAlias *topodatapb.TabletAlias, force bool, waitSlaveTimeout time.Duration) (*tmutils.SchemaChangeResult, error) {

	// find all the shards we need to handle
	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, shardInfo.Keyspace(), shardInfo.ShardName())
	if err != nil {
		return nil, err
	}

	// build the array of tabletStatus we're going to use
	statusArray := make([]*tabletStatus, 0, len(aliases)-1)
	for _, alias := range aliases {
		if alias == masterTabletAlias {
			// we skip the master
			continue
		}

		ti, err := wr.ts.GetTablet(ctx, alias)
		if err != nil {
			return nil, err
		}
		statusArray = append(statusArray, &tabletStatus{ti: ti})
	}

	// get schema on all tablets.
	log.Infof("Getting schema on all tablets for shard %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())
	wg := &sync.WaitGroup{}
	for _, status := range statusArray {
		wg.Add(1)
		go func(status *tabletStatus) {
			status.beforeSchema, status.lastError = wr.tmc.GetSchema(ctx, status.ti, nil, nil, false)
			wg.Done()
		}(status)
	}
	wg.Wait()

	// quick check for errors
	for _, status := range statusArray {
		if status.lastError != nil {
			return nil, fmt.Errorf("Error getting schema on tablet %v: %v", status.ti.AliasString(), status.lastError)
		}
	}

	// check all tablets have the same schema as the master's
	// BeforeSchema. If not, we shouldn't proceed
	log.Infof("Checking schema on all tablets")
	for _, status := range statusArray {
		diffs := tmutils.DiffSchemaToArray("master", preflight.BeforeSchema, topoproto.TabletAliasString(status.ti.Alias), status.beforeSchema)
		if len(diffs) > 0 {
			if force {
				log.Warningf("Tablet %v has inconsistent schema, ignoring: %v", status.ti.AliasString(), strings.Join(diffs, "\n"))
			} else {
				return nil, fmt.Errorf("Tablet %v has inconsistent schema: %v", status.ti.AliasString(), strings.Join(diffs, "\n"))
			}
		}
	}

	// we're good, just send to the master
	log.Infof("Applying schema change to master")
	sc := &tmutils.SchemaChange{
		SQL:              change,
		Force:            force,
		AllowReplication: true,
		BeforeSchema:     preflight.BeforeSchema,
		AfterSchema:      preflight.AfterSchema,
	}
	return wr.ApplySchema(ctx, masterTabletAlias, sc)
}

// ApplySchemaKeyspace applies a schema change to an entire keyspace.
// take a keyspace lock to do this.
// first we will validate the Preflight works the same on all shard masters
// and fail if not (unless force is specified)
func (wr *Wrangler) ApplySchemaKeyspace(ctx context.Context, keyspace string, change string, force bool, waitSlaveTimeout time.Duration) (*tmutils.SchemaChangeResult, error) {
	actionNode := actionnode.ApplySchemaKeyspace(change)
	lockPath, err := wr.lockKeyspace(ctx, keyspace, actionNode)
	if err != nil {
		return nil, err
	}

	err = schemamanager.Run(
		ctx,
		schemamanager.NewPlainController(change, keyspace),
		schemamanager.NewTabletExecutor(wr.tmc, wr.ts),
	)

	return nil, wr.unlockKeyspace(ctx, keyspace, actionNode, lockPath, err)
}

// CopySchemaShardFromShard copies the schema from a source shard to the specified destination shard.
// For both source and destination it picks the master tablet. See also CopySchemaShard.
func (wr *Wrangler) CopySchemaShardFromShard(ctx context.Context, tables, excludeTables []string, includeViews bool, sourceKeyspace, sourceShard, destKeyspace, destShard string) error {
	sourceShardInfo, err := wr.ts.GetShard(ctx, sourceKeyspace, sourceShard)
	if err != nil {
		return err
	}

	return wr.CopySchemaShard(ctx, sourceShardInfo.MasterAlias, tables, excludeTables, includeViews, destKeyspace, destShard)
}

// CopySchemaShard copies the schema from a source tablet to the
// specified shard.  The schema is applied directly on the master of
// the destination shard, and is propogated to the replicas through
// binlogs.
func (wr *Wrangler) CopySchemaShard(ctx context.Context, sourceTabletAlias *topodatapb.TabletAlias, tables, excludeTables []string, includeViews bool, destKeyspace, destShard string) error {
	destShardInfo, err := wr.ts.GetShard(ctx, destKeyspace, destShard)
	if err != nil {
		return err
	}

	sourceSd, err := wr.GetSchema(ctx, sourceTabletAlias, tables, excludeTables, includeViews)
	if err != nil {
		return err
	}
	destSd, err := wr.GetSchema(ctx, destShardInfo.MasterAlias, tables, excludeTables, includeViews)
	if err != nil {
		destSd = nil
	}
	if destSd != nil {
		diffs := tmutils.DiffSchemaToArray("source", sourceSd, "dest", destSd)
		if diffs == nil {
			// Return early because dest has already the same schema as source.
			return nil
		}
	}

	createSQL := tmutils.SchemaDefinitionToSQLStrings(sourceSd)
	destTabletInfo, err := wr.ts.GetTablet(ctx, destShardInfo.MasterAlias)
	if err != nil {
		return err
	}
	for i, sqlLine := range createSQL {
		err = wr.applySQLShard(ctx, destTabletInfo, sqlLine, i == len(createSQL)-1)
		if err != nil {
			return err
		}
	}
	return nil
}

// applySQLShard applies a given SQL change on a given tablet alias. It allows executing arbitrary
// SQL statements, but doesn't return any results, so it's only useful for SQL statements
// that would be run for their effects (e.g., CREATE).
// It works by applying the SQL statement on the shard's master tablet with replication turned on.
// Thus it should be used only for changes that can be applied on a live instance without causing issues;
// it shouldn't be used for anything that will require a pivot.
// The SQL statement string is expected to have {{.DatabaseName}} in place of the actual db name.
func (wr *Wrangler) applySQLShard(ctx context.Context, tabletInfo *topo.TabletInfo, change string, reloadSchema bool) error {
	filledChange, err := fillStringTemplate(change, map[string]string{"DatabaseName": tabletInfo.DbName()})
	if err != nil {
		return fmt.Errorf("fillStringTemplate failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Need to make sure that we enable binlog, since we're only applying the statement on masters.
	_, err = wr.tmc.ExecuteFetchAsDba(ctx, tabletInfo, filledChange, 0, false, false, reloadSchema)
	return err
}

// fillStringTemplate returns the string template filled
func fillStringTemplate(tmpl string, vars interface{}) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}
