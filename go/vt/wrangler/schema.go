/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wrangler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// DefaultWaitReplicasTimeout is the default value for waitReplicasTimeout, which is used when calling method CopySchemaShardFromShard.
	DefaultWaitReplicasTimeout = 10 * time.Second
)

// GetSchema uses an RPC to get the schema from a remote tablet
func (wr *Wrangler) GetSchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, fmt.Errorf("GetTablet(%v) failed: %v", tabletAlias, err)
	}

	return wr.tmc.GetSchema(ctx, ti.Tablet, tables, excludeTables, includeViews)
}

// ReloadSchema forces the remote tablet to reload its schema.
func (wr *Wrangler) ReloadSchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %v", tabletAlias, err)
	}

	return wr.tmc.ReloadSchema(ctx, ti.Tablet, "")
}

// ReloadSchemaShard reloads the schema for all replica tablets in a shard,
// after they reach a given replication position (empty pos means immediate).
// In general, we don't always expect all replicas to be ready to reload,
// and the periodic schema reload makes them self-healing anyway.
// So we do this on a best-effort basis, and log warnings for any tablets
// that fail to reload within the context deadline.
func (wr *Wrangler) ReloadSchemaShard(ctx context.Context, keyspace, shard, replicationPos string, concurrency *sync2.Semaphore, includePrimary bool) {
	tablets, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	switch {
	case topo.IsErrType(err, topo.PartialResult):
		// We got a partial result. Do what we can, but warn
		// that some may be missed.
		wr.logger.Warningf("ReloadSchemaShard(%v/%v) got a partial tablet list. Some tablets may not have schema reloaded (use vtctl ReloadSchema to fix individual tablets)", keyspace, shard)
	case err == nil:
		// Good case, keep going too.
	default:
		// This is best-effort, so just log it and move on.
		wr.logger.Warningf("ReloadSchemaShard(%v/%v) failed to load tablet list, will not reload schema (use vtctl ReloadSchemaShard to try again): %v", keyspace, shard, err)
		return
	}

	var wg sync.WaitGroup
	for _, ti := range tablets {
		if !includePrimary && ti.Type == topodatapb.TabletType_PRIMARY {
			// We don't need to reload on the primary
			// because we assume ExecuteFetchAsDba()
			// already did that.
			continue
		}

		wg.Add(1)
		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			concurrency.Acquire()
			defer concurrency.Release()
			pos := replicationPos
			// Primary is always up-to-date. So, don't wait for position.
			if tablet.Type == topodatapb.TabletType_PRIMARY {
				pos = ""
			}
			if err := wr.tmc.ReloadSchema(ctx, tablet, pos); err != nil {
				wr.logger.Warningf(
					"Failed to reload schema on replica tablet %v in %v/%v (use vtctl ReloadSchema to try again): %v",
					topoproto.TabletAliasString(tablet.Alias), keyspace, shard, err)
			}
		}(ti.Tablet)
	}
	wg.Wait()
}

// ReloadSchemaKeyspace reloads the schema in all shards in a
// keyspace.  The concurrency is shared across all shards (only that
// many tablets will be reloaded at once).
func (wr *Wrangler) ReloadSchemaKeyspace(ctx context.Context, keyspace string, concurrency *sync2.Semaphore, includePrimary bool) error {
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("GetShardNames(%v) failed: %v", keyspace, err)
	}

	for _, shard := range shards {
		wr.ReloadSchemaShard(ctx, keyspace, shard, "" /* waitPosition */, concurrency, includePrimary)
	}
	return nil
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(ctx context.Context, primarySchema *tabletmanagerdatapb.SchemaDefinition, primaryTabletAlias, alias *topodatapb.TabletAlias, excludeTables []string, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering schema for %v", topoproto.TabletAliasString(alias))
	replicaSchema, err := wr.GetSchema(ctx, alias, nil, excludeTables, includeViews)
	if err != nil {
		er.RecordError(fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", alias, excludeTables, includeViews, err))
		return
	}

	log.Infof("Diffing schema for %v", topoproto.TabletAliasString(alias))
	tmutils.DiffSchema(topoproto.TabletAliasString(primaryTabletAlias), primarySchema, topoproto.TabletAliasString(alias), replicaSchema, er)
}

// ValidateSchemaShard will diff the schema from all the tablets in the shard.
func (wr *Wrangler) ValidateSchemaShard(ctx context.Context, keyspace, shard string, excludeTables []string, includeViews bool, includeVSchema bool) error {
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return fmt.Errorf("GetShard(%v, %v) failed: %v", keyspace, shard, err)
	}

	// get schema from the primary, or error
	if !si.HasPrimary() {
		return fmt.Errorf("no primary in shard %v/%v", keyspace, shard)
	}
	log.Infof("Gathering schema for primary %v", topoproto.TabletAliasString(si.PrimaryAlias))
	primarySchema, err := wr.GetSchema(ctx, si.PrimaryAlias, nil, excludeTables, includeViews)
	if err != nil {
		return fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", si.PrimaryAlias, excludeTables, includeViews, err)
	}

	if includeVSchema {
		err := wr.ValidateVSchema(ctx, keyspace, []string{shard}, excludeTables, includeViews)
		if err != nil {
			return err
		}
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the primary
	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		return fmt.Errorf("FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
	}

	// then diff with all replicas
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if topoproto.TabletAliasEqual(alias, si.PrimaryAlias) {
			continue
		}

		wg.Add(1)
		go wr.diffSchema(ctx, primarySchema, si.PrimaryAlias, alias, excludeTables, includeViews, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("schema diffs: %v", er.Error().Error())
	}
	return nil
}

// ValidateSchemaKeyspace will diff the schema from all the tablets in
// the keyspace.
func (wr *Wrangler) ValidateSchemaKeyspace(ctx context.Context, keyspace string, excludeTables []string, includeViews, skipNoPrimary bool, includeVSchema bool) error {
	// find all the shards
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("GetShardNames(%v) failed: %v", keyspace, err)
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("no shards in keyspace %v", keyspace)
	}
	sort.Strings(shards)
	if len(shards) == 1 {
		return wr.ValidateSchemaShard(ctx, keyspace, shards[0], excludeTables, includeViews, includeVSchema)
	}

	var referenceSchema *tabletmanagerdatapb.SchemaDefinition
	var referenceAlias *topodatapb.TabletAlias

	// then diff with all other tablets everywhere
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// If we are checking against the vschema then all shards
	// should just be validated individually against it
	if includeVSchema {
		err := wr.ValidateVSchema(ctx, keyspace, shards, excludeTables, includeViews)
		if err != nil {
			return err
		}
	}

	// then diffs all tablets in the other shards
	for _, shard := range shards[0:] {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			er.RecordError(fmt.Errorf("GetShard(%v, %v) failed: %v", keyspace, shard, err))
			continue
		}

		if !si.HasPrimary() {
			if !skipNoPrimary {
				er.RecordError(fmt.Errorf("no primary in shard %v/%v", keyspace, shard))
			}
			continue
		}

		if referenceSchema == nil {
			referenceAlias = si.PrimaryAlias
			log.Infof("Gathering schema for reference primary %v", topoproto.TabletAliasString(referenceAlias))
			referenceSchema, err = wr.GetSchema(ctx, referenceAlias, nil, excludeTables, includeViews)
			if err != nil {
				return fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", referenceAlias, excludeTables, includeViews, err)
			}
		}

		aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
		if err != nil {
			er.RecordError(fmt.Errorf("FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err))
			continue
		}

		for _, alias := range aliases {
			// Don't diff schemas for self
			if referenceAlias == alias {
				continue
			}
			wg.Add(1)
			go wr.diffSchema(ctx, referenceSchema, referenceAlias, alias, excludeTables, includeViews, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("schema diffs: %v", er.Error().Error())
	}
	return nil
}

// ValidateVSchema compares the schema of each primary tablet in "keyspace/shards..." to the vschema and errs if there are differences
func (wr *Wrangler) ValidateVSchema(ctx context.Context, keyspace string, shards []string, excludeTables []string, includeViews bool) error {
	vschm, err := wr.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("GetVSchema(%s) failed: %v", keyspace, err)
	}

	shardFailures := concurrency.AllErrorRecorder{}
	var wg sync.WaitGroup
	wg.Add(len(shards))

	for _, shard := range shards {
		go func(shard string) {
			defer wg.Done()
			notFoundTables := []string{}
			si, err := wr.ts.GetShard(ctx, keyspace, shard)
			if err != nil {
				shardFailures.RecordError(fmt.Errorf("GetShard(%v, %v) failed: %v", keyspace, shard, err))
				return
			}
			primarySchema, err := wr.GetSchema(ctx, si.PrimaryAlias, nil, excludeTables, includeViews)
			if err != nil {
				shardFailures.RecordError(fmt.Errorf("GetSchema(%s, nil, %v, %v) (%v/%v) failed: %v", si.PrimaryAlias.String(),
					excludeTables, includeViews, keyspace, shard, err,
				))
				return
			}
			for _, tableDef := range primarySchema.TableDefinitions {
				if _, ok := vschm.Tables[tableDef.Name]; !ok {
					notFoundTables = append(notFoundTables, tableDef.Name)
				}
			}
			if len(notFoundTables) > 0 {
				shardFailure := fmt.Errorf("%v/%v has tables that are not in the vschema: %v", keyspace, shard, notFoundTables)
				shardFailures.RecordError(shardFailure)
			}
		}(shard)
	}
	wg.Wait()
	if shardFailures.HasErrors() {
		return fmt.Errorf("ValidateVSchema(%v, %v, %v, %v) failed: %v", keyspace, shards, excludeTables, includeViews, shardFailures.Error().Error())
	}
	return nil
}

// PreflightSchema will try a schema change on the remote tablet.
func (wr *Wrangler) PreflightSchema(ctx context.Context, tabletAlias *topodatapb.TabletAlias, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, fmt.Errorf("GetTablet(%v) failed: %v", tabletAlias, err)
	}
	return wr.tmc.PreflightSchema(ctx, ti.Tablet, changes)
}

// CopySchemaShardFromShard copies the schema from a source shard to the specified destination shard.
// For both source and destination it picks the primary tablet. See also CopySchemaShard.
func (wr *Wrangler) CopySchemaShardFromShard(ctx context.Context, tables, excludeTables []string, includeViews bool, sourceKeyspace, sourceShard, destKeyspace, destShard string, waitReplicasTimeout time.Duration, skipVerify bool) error {
	sourceShardInfo, err := wr.ts.GetShard(ctx, sourceKeyspace, sourceShard)
	if err != nil {
		return fmt.Errorf("GetShard(%v, %v) failed: %v", sourceKeyspace, sourceShard, err)
	}
	if sourceShardInfo.PrimaryAlias == nil {
		return fmt.Errorf("no primary in shard record %v/%v. Consider running 'vtctl InitShardPrimary' in case of a new shard or reparenting the shard to fix the topology data, or providing a non-primary tablet alias", sourceKeyspace, sourceShard)
	}

	return grpcvtctldserver.NewVtctldServer(wr.ts).CopySchemaShard(ctx, sourceShardInfo.Shard.PrimaryAlias, tables, excludeTables, includeViews, destKeyspace, destShard, waitReplicasTimeout, skipVerify)
}
