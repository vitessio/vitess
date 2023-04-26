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
	"bytes"
	"context"
	"fmt"
	"html/template"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	// DefaultWaitReplicasTimeout is the default value for waitReplicasTimeout, which is used when calling method CopySchemaShardFromShard.
	DefaultWaitReplicasTimeout = 10 * time.Second
)

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(ctx context.Context, primarySchema *tabletmanagerdatapb.SchemaDefinition, primaryTabletAlias, alias *topodatapb.TabletAlias, excludeTables []string, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering schema for %v", topoproto.TabletAliasString(alias))
	req := &tabletmanagerdatapb.GetSchemaRequest{ExcludeTables: excludeTables, IncludeViews: includeViews}
	replicaSchema, err := schematools.GetSchema(ctx, wr.ts, wr.tmc, alias, req)
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
	req := &tabletmanagerdatapb.GetSchemaRequest{ExcludeTables: excludeTables, IncludeViews: includeViews}
	primarySchema, err := schematools.GetSchema(ctx, wr.ts, wr.tmc, si.PrimaryAlias, req)
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

// ValidateSchemaKeyspace will diff the schema from all the tablets in the keyspace.
func (wr *Wrangler) ValidateSchemaKeyspace(ctx context.Context, keyspace string, excludeTables []string, includeViews, skipNoPrimary bool, includeVSchema bool) error {
	res, err := wr.VtctldServer().ValidateSchemaKeyspace(ctx, &vtctldatapb.ValidateSchemaKeyspaceRequest{
		Keyspace:       keyspace,
		ExcludeTables:  excludeTables,
		IncludeViews:   includeViews,
		IncludeVschema: includeVSchema,
		SkipNoPrimary:  skipNoPrimary,
	})

	for _, result := range res.Results {
		wr.Logger().Printf("%s\n", result)
	}

	if len(res.Results) > 0 {
		return fmt.Errorf("schema diffs: %v", res.Results)
	}

	return err
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
			req := &tabletmanagerdatapb.GetSchemaRequest{ExcludeTables: excludeTables, IncludeViews: includeViews}
			primarySchema, err := schematools.GetSchema(ctx, wr.ts, wr.tmc, si.PrimaryAlias, req)
			if err != nil {
				shardFailures.RecordError(fmt.Errorf("GetSchema(%s, nil, %v, %v) (%v/%v) failed: %v", si.PrimaryAlias.String(),
					excludeTables, includeViews, keyspace, shard, err,
				))
				return
			}
			for _, tableDef := range primarySchema.TableDefinitions {
				if _, ok := vschm.Tables[tableDef.Name]; !ok {
					if !schema.IsInternalOperationTableName(tableDef.Name) {
						notFoundTables = append(notFoundTables, tableDef.Name)
					}
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

	return wr.CopySchemaShard(ctx, sourceShardInfo.PrimaryAlias, tables, excludeTables, includeViews, destKeyspace, destShard, waitReplicasTimeout, skipVerify)
}

// CopySchemaShard copies the schema from a source tablet to the
// specified shard.  The schema is applied directly on the primary of
// the destination shard, and is propagated to the replicas through
// binlogs.
func (wr *Wrangler) CopySchemaShard(ctx context.Context, sourceTabletAlias *topodatapb.TabletAlias, tables, excludeTables []string, includeViews bool, destKeyspace, destShard string, waitReplicasTimeout time.Duration, skipVerify bool) error {
	destShardInfo, err := wr.ts.GetShard(ctx, destKeyspace, destShard)
	if err != nil {
		return fmt.Errorf("GetShard(%v, %v) failed: %v", destKeyspace, destShard, err)
	}

	if destShardInfo.PrimaryAlias == nil {
		return fmt.Errorf("no primary in shard record %v/%v. Consider running 'vtctl InitShardPrimary' in case of a new shard or reparenting the shard to fix the topology data", destKeyspace, destShard)
	}

	err = schematools.CopyShardMetadata(ctx, wr.ts, wr.tmc, sourceTabletAlias, destShardInfo.PrimaryAlias)
	if err != nil {
		return fmt.Errorf("copyShardMetadata(%v, %v) failed: %v", sourceTabletAlias, destShardInfo.PrimaryAlias, err)
	}

	diffs, err := schematools.CompareSchemas(ctx, wr.ts, wr.tmc, sourceTabletAlias, destShardInfo.PrimaryAlias, tables, excludeTables, includeViews)
	if err != nil {
		return fmt.Errorf("CopySchemaShard failed because schemas could not be compared initially: %v", err)
	}
	if diffs == nil {
		// Return early because dest has already the same schema as source.
		return nil
	}

	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}
	sourceSd, err := schematools.GetSchema(ctx, wr.ts, wr.tmc, sourceTabletAlias, req)
	if err != nil {
		return fmt.Errorf("GetSchema(%v, %v, %v, %v) failed: %v", sourceTabletAlias, tables, excludeTables, includeViews, err)
	}

	createSQLstmts := tmutils.SchemaDefinitionToSQLStrings(sourceSd)

	destTabletInfo, err := wr.ts.GetTablet(ctx, destShardInfo.PrimaryAlias)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %v", destShardInfo.PrimaryAlias, err)
	}
	for _, createSQL := range createSQLstmts {
		err = wr.applySQLShard(ctx, destTabletInfo, createSQL)
		if err != nil {
			return fmt.Errorf("creating a table failed."+
				" Most likely some tables already exist on the destination and differ from the source."+
				" Please remove all to be copied tables from the destination manually and run this command again."+
				" Full error: %v", err)
		}
	}

	// Remember the replication position after all the above were applied.
	destPrimaryPos, err := wr.tmc.PrimaryPosition(ctx, destTabletInfo.Tablet)
	if err != nil {
		return fmt.Errorf("CopySchemaShard: can't get replication position after schema applied: %v", err)
	}

	// Although the copy was successful, we have to verify it to catch the case
	// where the database already existed on the destination, but with different
	// options e.g. a different character set.
	// In that case, MySQL would have skipped our CREATE DATABASE IF NOT EXISTS
	// statement.
	if !skipVerify {
		diffs, err = schematools.CompareSchemas(ctx, wr.ts, wr.tmc, sourceTabletAlias, destShardInfo.PrimaryAlias, tables, excludeTables, includeViews)
		if err != nil {
			return fmt.Errorf("CopySchemaShard failed because schemas could not be compared finally: %v", err)
		}
		if diffs != nil {
			return fmt.Errorf("CopySchemaShard was not successful because the schemas between the two tablets %v and %v differ: %v", sourceTabletAlias, destShardInfo.PrimaryAlias, diffs)
		}
	}

	// Notify Replicass to reload schema. This is best-effort.
	reloadCtx, cancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer cancel()
	resp, err := wr.VtctldServer().ReloadSchemaShard(reloadCtx, &vtctldatapb.ReloadSchemaShardRequest{
		Keyspace:       destKeyspace,
		Shard:          destShard,
		WaitPosition:   destPrimaryPos,
		Concurrency:    10,
		IncludePrimary: true,
	})
	if resp != nil {
		for _, e := range resp.Events {
			logutil.LogEvent(wr.Logger(), e)
		}
	}
	return err
}

// applySQLShard applies a given SQL change on a given tablet alias. It allows executing arbitrary
// SQL statements, but doesn't return any results, so it's only useful for SQL statements
// that would be run for their effects (e.g., CREATE).
// It works by applying the SQL statement on the shard's primary tablet with replication turned on.
// Thus it should be used only for changes that can be applied on a live instance without causing issues;
// it shouldn't be used for anything that will require a pivot.
// The SQL statement string is expected to have {{.DatabaseName}} in place of the actual db name.
func (wr *Wrangler) applySQLShard(ctx context.Context, tabletInfo *topo.TabletInfo, change string) error {
	filledChange, err := fillStringTemplate(change, map[string]string{"DatabaseName": tabletInfo.DbName()})
	if err != nil {
		return fmt.Errorf("fillStringTemplate failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Need to make sure that replication is enabled since we're only applying the statement on primaries
	_, err = wr.tmc.ApplySchema(ctx, tabletInfo.Tablet, &tmutils.SchemaChange{
		SQL:              filledChange,
		Force:            false,
		AllowReplication: true,
		SQLMode:          vreplication.SQLMode,
	})
	return err
}

// fillStringTemplate returns the string template filled
func fillStringTemplate(tmpl string, vars any) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}
