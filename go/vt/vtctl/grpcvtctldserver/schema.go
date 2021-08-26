package grpcvtctldserver

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// ValidateSchemaKeyspace will diff the schema from all the tablets in
// the keyspace.
func (s *VtctldServer) ValidateSchemaKeyspace(ctx context.Context, keyspace string, excludeTables []string, includeViews, skipNoPrimary bool, includeVSchema bool) error {
	// find all the shards
	shards, err := s.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("GetShardNames(%v) failed: %v", keyspace, err)
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("no shards in keyspace %v", keyspace)
	}
	sort.Strings(shards)
	if len(shards) == 1 {
		return s.ValidateSchemaShard(ctx, keyspace, shards[0], excludeTables, includeViews, includeVSchema)
	}

	var referenceSchema *tabletmanagerdatapb.SchemaDefinition
	var referenceAlias *topodatapb.TabletAlias

	// then diff with all other tablets everywhere
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	// If we are checking against the vschema then all shards
	// should just be validated individually against it
	if includeVSchema {
		err := s.ValidateVSchema(ctx, keyspace, shards, excludeTables, includeViews)
		if err != nil {
			return err
		}
	}

	// then diffs all tablets in the other shards
	for _, shard := range shards[0:] {
		si, err := s.ts.GetShard(ctx, keyspace, shard)
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
			referenceAlias = si.Shard.PrimaryAlias
			log.Infof("Gathering schema for reference primary %v", topoproto.TabletAliasString(referenceAlias))

			req := &vtctldata.GetTabletRequest{
				TabletAlias: referenceAlias,
			}

			getTabletResp, err := s.GetTablet(ctx, req)
			if err != nil {
				return fmt.Errorf("GetTablet(%v) failed: %v", referenceAlias, err)
			}

			referenceSchema, err = s.tmc.GetSchema(ctx, getTabletResp.GetTablet(), nil, excludeTables, includeViews)
			if err != nil {
				return fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", referenceAlias, excludeTables, includeViews, err)
			}
		}

		aliases, err := s.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
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
			go s.diffSchema(ctx, referenceSchema, referenceAlias, alias, excludeTables, includeViews, &wg, &er)
		}
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("schema diffs: %v", er.Error().Error())
	}
	return nil
}

// ValidateSchemaShard will diff the schema from all the tablets in the shard.
func (s *VtctldServer) ValidateSchemaShard(ctx context.Context, keyspace, shard string, excludeTables []string, includeViews bool, includeVSchema bool) error {
	si, err := s.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return fmt.Errorf("GetShard(%v, %v) failed: %v", keyspace, shard, err)
	}

	// get schema from the master, or error
	if !si.HasPrimary() {
		return fmt.Errorf("no primary in shard %v/%v", keyspace, shard)
	}
	log.Infof("Gathering schema for primary %v", topoproto.TabletAliasString(si.Shard.PrimaryAlias))

	req := &vtctldata.GetTabletRequest{
		TabletAlias: si.Shard.PrimaryAlias,
	}

	getTabletResponse, err := s.GetTablet(ctx, req)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %v", req.TabletAlias, err)
	}

	masterSchema, err := s.tmc.GetSchema(ctx, getTabletResponse.Tablet, nil, excludeTables, includeViews)
	if err != nil {
		return fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", si.Shard.PrimaryAlias, excludeTables, includeViews, err)
	}

	if includeVSchema {
		err := s.ValidateVSchema(ctx, keyspace, []string{shard}, excludeTables, includeViews)
		if err != nil {
			return err
		}
	}

	// read all the aliases in the shard, that is all tablets that are
	// replicating from the master
	aliases, err := s.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		return fmt.Errorf("FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
	}

	// then diff with all replicas
	er := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for _, alias := range aliases {
		if topoproto.TabletAliasEqual(alias, si.Shard.PrimaryAlias) {
			continue
		}

		wg.Add(1)
		go s.diffSchema(ctx, masterSchema, si.Shard.PrimaryAlias, alias, excludeTables, includeViews, &wg, &er)
	}
	wg.Wait()
	if er.HasErrors() {
		return fmt.Errorf("schema diffs: %v", er.Error().Error())
	}
	return nil
}

// ValidateVSchema compares the schema of each primary tablet in "keyspace/shards..." to the vschema and errs if there are differences
func (s *VtctldServer) ValidateVSchema(ctx context.Context, keyspace string, shards []string, excludeTables []string, includeViews bool) error {
	vschm, err := s.ts.GetVSchema(ctx, keyspace)
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
			si, err := s.ts.GetShard(ctx, keyspace, shard)
			if err != nil {
				shardFailures.RecordError(fmt.Errorf("GetShard(%v, %v) failed: %v", keyspace, shard, err))
				return
			}

			primaryTablet, err := s.ts.GetTablet(ctx, si.Shard.PrimaryAlias)
			if err != nil {
				shardFailures.RecordError(fmt.Errorf("GetTablet(%v) failed: %v", si.Shard.PrimaryAlias, err))
				return
			}

			masterSchema, err := s.tmc.GetSchema(ctx, primaryTablet.Tablet, nil, excludeTables, includeViews)
			if err != nil {
				shardFailures.RecordError(fmt.Errorf("GetSchema(%s, nil, %v, %v) (%v/%v) failed: %v", si.Shard.PrimaryAlias.String(),
					excludeTables, includeViews, keyspace, shard, err,
				))
				return
			}
			for _, tableDef := range masterSchema.TableDefinitions {
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

// helper method to asynchronously diff a schema
func (s *VtctldServer) diffSchema(ctx context.Context, masterSchema *tabletmanagerdatapb.SchemaDefinition, masterTabletAlias, alias *topodatapb.TabletAlias, excludeTables []string, includeViews bool, wg *sync.WaitGroup, er concurrency.ErrorRecorder) {
	defer wg.Done()
	log.Infof("Gathering schema for %v", topoproto.TabletAliasString(alias))

	req := &vtctldata.GetTabletRequest{
		TabletAlias: alias,
	}

	getTabletResp, err := s.GetTablet(ctx, req)
	if err != nil {
		er.RecordError(fmt.Errorf("GetTablet(%v) failed: %v", alias, err))
	}

	replicaSchema, err := s.tmc.GetSchema(ctx, getTabletResp.GetTablet(), nil, excludeTables, includeViews)
	if err != nil {
		er.RecordError(fmt.Errorf("GetSchema(%v, nil, %v, %v) failed: %v", alias, excludeTables, includeViews, err))
		return
	}

	log.Infof("Diffing schema for %v", topoproto.TabletAliasString(alias))
	tmutils.DiffSchema(topoproto.TabletAliasString(masterTabletAlias), masterSchema, topoproto.TabletAliasString(alias), replicaSchema, er)
}
