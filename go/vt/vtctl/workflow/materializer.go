/*
Copyright 2023 The Vitess Authors.

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

package workflow

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	createDDLAsCopy                = "copy"
	createDDLAsCopyDropConstraint  = "copy:drop_constraint"
	createDDLAsCopyDropForeignKeys = "copy:drop_foreign_keys"
)

type materializer struct {
	ctx      context.Context
	ts       *topo.Server
	sourceTs *topo.Server
	tmc      tmclient.TabletManagerClient

	ms                    *vtctldatapb.MaterializeSettings
	targetVSchema         *vindexes.KeyspaceSchema
	sourceShards          []*topo.ShardInfo
	targetShards          []*topo.ShardInfo
	isPartial             bool
	primaryVindexesDiffer bool
	workflowType          binlogdatapb.VReplicationWorkflowType

	env *vtenv.Environment
}

func (mz *materializer) getWorkflowType() binlogdatapb.VReplicationWorkflowType {
	var workflowType binlogdatapb.VReplicationWorkflowType
	switch mz.ms.MaterializationIntent {
	case vtctldatapb.MaterializationIntent_CUSTOM:
		workflowType = binlogdatapb.VReplicationWorkflowType_Materialize
	case vtctldatapb.MaterializationIntent_MOVETABLES:
		workflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	case vtctldatapb.MaterializationIntent_CREATELOOKUPINDEX:
		workflowType = binlogdatapb.VReplicationWorkflowType_CreateLookupIndex
	}
	return workflowType
}

func (mz *materializer) getWorkflowSubType() (binlogdatapb.VReplicationWorkflowSubType, error) {
	switch {
	case mz.isPartial && mz.ms.AtomicCopy:
		return binlogdatapb.VReplicationWorkflowSubType_None,
			fmt.Errorf("both atomic copy and partial mode cannot be specified for the same workflow")
	case mz.isPartial:
		return binlogdatapb.VReplicationWorkflowSubType_Partial, nil
	case mz.ms.AtomicCopy:
		return binlogdatapb.VReplicationWorkflowSubType_AtomicCopy, nil
	default:
		return binlogdatapb.VReplicationWorkflowSubType_None, nil
	}
}

func (mz *materializer) createWorkflowStreams(req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) error {
	if err := validateNewWorkflow(mz.ctx, mz.ts, mz.tmc, mz.ms.TargetKeyspace, mz.ms.Workflow); err != nil {
		return err
	}
	err := mz.buildMaterializer()
	if err != nil {
		return err
	}
	if err := mz.deploySchema(); err != nil {
		return err
	}

	var workflowSubType binlogdatapb.VReplicationWorkflowSubType
	workflowSubType, err = mz.getWorkflowSubType()
	if err != nil {
		return err
	}
	req.WorkflowSubType = workflowSubType

	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		targetPrimary, err := mz.ts.GetTablet(mz.ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}

		sourceShards := mz.filterSourceShards(target)
		// streamKeyRangesEqual allows us to optimize the stream for the cases
		// where while the target keyspace may be sharded, the target shard has
		// a single source shard to stream data from and the target and source
		// shard have equal key ranges. This can be done, for example, when doing
		// shard by shard migrations -- migrating a single shard at a time between
		// sharded source and sharded target keyspaces.
		streamKeyRangesEqual := false
		if len(sourceShards) == 1 && key.KeyRangeEqual(sourceShards[0].KeyRange, target.KeyRange) {
			streamKeyRangesEqual = true
		}
		// Each tablet needs its own copy of the request as it will have a unique
		// BinlogSource.
		tabletReq := req.CloneVT()
		tabletReq.BinlogSource, err = mz.generateBinlogSources(mz.ctx, target, sourceShards, streamKeyRangesEqual)
		if err != nil {
			return err
		}

		_, err = mz.tmc.CreateVReplicationWorkflow(mz.ctx, targetPrimary.Tablet, tabletReq)
		return err
	})
}

func (mz *materializer) generateBinlogSources(ctx context.Context, targetShard *topo.ShardInfo, sourceShards []*topo.ShardInfo, keyRangesEqual bool) ([]*binlogdatapb.BinlogSource, error) {
	blses := make([]*binlogdatapb.BinlogSource, 0, len(mz.sourceShards))
	for _, sourceShard := range sourceShards {
		bls := &binlogdatapb.BinlogSource{
			Keyspace:        mz.ms.SourceKeyspace,
			Shard:           sourceShard.ShardName(),
			Filter:          &binlogdatapb.Filter{},
			StopAfterCopy:   mz.ms.StopAfterCopy,
			ExternalCluster: mz.ms.ExternalCluster,
			SourceTimeZone:  mz.ms.SourceTimeZone,
			TargetTimeZone:  mz.ms.TargetTimeZone,
			OnDdl:           binlogdatapb.OnDDLAction(binlogdatapb.OnDDLAction_value[mz.ms.OnDdl]),
		}
		for _, ts := range mz.ms.TableSettings {
			rule := &binlogdatapb.Rule{
				Match: ts.TargetTable,
			}

			if ts.SourceExpression == "" {
				bls.Filter.Rules = append(bls.Filter.Rules, rule)
				continue
			}

			// Validate non-empty query.
			stmt, err := mz.env.Parser().Parse(ts.SourceExpression)
			if err != nil {
				return nil, err
			}
			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				return nil, fmt.Errorf("unrecognized statement: %s", ts.SourceExpression)
			}
			filter := ts.SourceExpression
			if !keyRangesEqual && mz.targetVSchema.Keyspace.Sharded && mz.targetVSchema.Tables[ts.TargetTable].Type != vindexes.TypeReference {
				cv, err := vindexes.FindBestColVindex(mz.targetVSchema.Tables[ts.TargetTable])
				if err != nil {
					return nil, err
				}
				mappedCols := make([]*sqlparser.ColName, 0, len(cv.Columns))
				for _, col := range cv.Columns {
					colName, err := matchColInSelect(col, sel)
					if err != nil {
						return nil, err
					}
					mappedCols = append(mappedCols, colName)
				}
				subExprs := make(sqlparser.Exprs, 0, len(mappedCols)+2)
				for _, mappedCol := range mappedCols {
					subExprs = append(subExprs, mappedCol)
				}
				vindexName := fmt.Sprintf("%s.%s", mz.ms.TargetKeyspace, cv.Name)
				subExprs = append(subExprs, sqlparser.NewStrLiteral(vindexName))
				subExprs = append(subExprs, sqlparser.NewStrLiteral(key.KeyRangeString(targetShard.KeyRange)))
				inKeyRange := &sqlparser.FuncExpr{
					Name:  sqlparser.NewIdentifierCI("in_keyrange"),
					Exprs: subExprs,
				}
				if sel.Where != nil {
					sel.Where = &sqlparser.Where{
						Type: sqlparser.WhereClause,
						Expr: &sqlparser.AndExpr{
							Left:  inKeyRange,
							Right: sel.Where.Expr,
						},
					}
				} else {
					sel.Where = &sqlparser.Where{
						Type: sqlparser.WhereClause,
						Expr: inKeyRange,
					}
				}

				filter = sqlparser.String(sel)
			}

			rule.Filter = filter
			bls.Filter.Rules = append(bls.Filter.Rules, rule)
		}
		blses = append(blses, bls)
	}
	return blses, nil
}

func (mz *materializer) deploySchema() error {
	var sourceDDLs map[string]string
	var mu sync.Mutex

	return forAllShards(mz.targetShards, func(target *topo.ShardInfo) error {
		allTables := []string{"/.*/"}

		hasTargetTable := map[string]bool{}
		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: allTables}
		targetSchema, err := schematools.GetSchema(mz.ctx, mz.ts, mz.tmc, target.PrimaryAlias, req)
		if err != nil {
			return err
		}

		for _, td := range targetSchema.TableDefinitions {
			hasTargetTable[td.Name] = true
		}

		targetTablet, err := mz.ts.GetTablet(mz.ctx, target.PrimaryAlias)
		if err != nil {
			return err
		}

		var applyDDLs []string
		for _, ts := range mz.ms.TableSettings {
			if hasTargetTable[ts.TargetTable] {
				// Table already exists.
				continue
			}
			if ts.CreateDdl == "" {
				return fmt.Errorf("target table %v does not exist and there is no create ddl defined", ts.TargetTable)
			}

			var err error
			mu.Lock()
			if len(sourceDDLs) == 0 {
				// Only get DDLs for tables once and lazily: if we need to copy the schema from source
				// to target then we copy schemas from primaries on the source keyspace; we have found
				// use cases where the user just has a replica (no primary) in the source keyspace.
				sourceDDLs, err = getSourceTableDDLs(mz.ctx, mz.sourceTs, mz.tmc, mz.sourceShards)
			}
			mu.Unlock()
			if err != nil {
				log.Errorf("Error getting DDLs of source tables: %s", err.Error())
				return err
			}

			createDDL := ts.CreateDdl
			if createDDL == createDDLAsCopy || createDDL == createDDLAsCopyDropConstraint || createDDL == createDDLAsCopyDropForeignKeys {
				if ts.SourceExpression != "" {
					// Check for table if non-empty SourceExpression.
					sourceTableName, err := mz.env.Parser().TableFromStatement(ts.SourceExpression)
					if err != nil {
						return err
					}
					if sourceTableName.Name.String() != ts.TargetTable {
						return fmt.Errorf("source and target table names must match for copying schema: %v vs %v", sqlparser.String(sourceTableName), ts.TargetTable)

					}
				}

				ddl, ok := sourceDDLs[ts.TargetTable]
				if !ok {
					return fmt.Errorf("source table %v does not exist", ts.TargetTable)
				}

				if createDDL == createDDLAsCopyDropConstraint {
					strippedDDL, err := stripTableConstraints(ddl, mz.env.Parser())
					if err != nil {
						return err
					}

					ddl = strippedDDL
				}

				if createDDL == createDDLAsCopyDropForeignKeys {
					strippedDDL, err := stripTableForeignKeys(ddl, mz.env.Parser())
					if err != nil {
						return err
					}

					ddl = strippedDDL
				}
				createDDL = ddl
			}

			applyDDLs = append(applyDDLs, createDDL)
		}

		if len(applyDDLs) > 0 {
			if mz.ms.AtomicCopy {
				// AtomicCopy suggests we may be interested in Foreign Key support. As such, we want to
				// normalize the source schema: ensure the order of table definitions is compatible with
				// the constraints graph. We want to first create the parents, then the children.
				// We use schemadiff to normalize the schema.
				// For now, and because this is could have wider implications, we ignore any errors in
				// reading the source schema.
				env := schemadiff.NewEnv(mz.env, mz.env.CollationEnv().DefaultConnectionCharset())
				schema, err := schemadiff.NewSchemaFromQueries(env, applyDDLs)
				if err != nil {
					log.Error(vterrors.Wrapf(err, "AtomicCopy: failed to normalize schema via schemadiff"))
				} else {
					applyDDLs = schema.ToQueries()
					log.Infof("AtomicCopy used, and schema was normalized via schemadiff. %v queries normalized", len(applyDDLs))
				}
			}
			sql := strings.Join(applyDDLs, ";\n")

			_, err = mz.tmc.ApplySchema(mz.ctx, targetTablet.Tablet, &tmutils.SchemaChange{
				SQL:                     sql,
				Force:                   false,
				AllowReplication:        true,
				SQLMode:                 vreplication.SQLMode,
				DisableForeignKeyChecks: true,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (mz *materializer) buildMaterializer() error {
	ctx := mz.ctx
	ms := mz.ms
	vschema, err := mz.ts.GetVSchema(ctx, ms.TargetKeyspace)
	if err != nil {
		return err
	}
	targetVSchema, err := vindexes.BuildKeyspaceSchema(vschema, ms.TargetKeyspace, mz.env.Parser())
	if err != nil {
		return err
	}
	if targetVSchema.Keyspace.Sharded {
		for _, ts := range ms.TableSettings {
			if targetVSchema.Tables[ts.TargetTable] == nil {
				return fmt.Errorf("table %s not found in vschema for keyspace %s", ts.TargetTable, ms.TargetKeyspace)
			}
		}
	}
	isPartial := false
	sourceShards, err := mz.sourceTs.GetServingShards(ctx, ms.SourceKeyspace)
	if err != nil {
		return err
	}
	if len(ms.SourceShards) > 0 {
		isPartial = true
		var sourceShards2 []*topo.ShardInfo
		for _, shard := range sourceShards {
			for _, shard2 := range ms.SourceShards {
				if shard.ShardName() == shard2 {
					sourceShards2 = append(sourceShards2, shard)
					break
				}
			}
		}
		sourceShards = sourceShards2
	}
	if len(sourceShards) == 0 {
		return fmt.Errorf("no source shards specified for workflow %s ", ms.Workflow)
	}

	targetShards, err := mz.ts.GetServingShards(ctx, ms.TargetKeyspace)
	if err != nil {
		return err
	}
	if len(ms.SourceShards) > 0 {
		var targetShards2 []*topo.ShardInfo
		for _, shard := range targetShards {
			for _, shard2 := range ms.SourceShards {
				if shard.ShardName() == shard2 {
					targetShards2 = append(targetShards2, shard)
					break
				}
			}
		}
		targetShards = targetShards2
	}
	if len(targetShards) == 0 {
		return fmt.Errorf("no target shards specified for workflow %s ", ms.Workflow)
	}

	sourceTs := mz.ts
	if ms.ExternalCluster != "" { // when the source is an external mysql cluster mounted using the Mount command
		externalTopo, err := mz.ts.OpenExternalVitessClusterServer(ctx, ms.ExternalCluster)
		if err != nil {
			return fmt.Errorf("failed to open external topo: %v", err)
		}
		sourceTs = externalTopo
	}
	differentPVs := false
	sourceVSchema, err := sourceTs.GetVSchema(ctx, ms.SourceKeyspace)
	if err != nil {
		return fmt.Errorf("failed to get source keyspace vschema: %v", err)
	}
	differentPVs = primaryVindexesDiffer(ms, sourceVSchema, vschema)

	mz.targetVSchema = targetVSchema
	mz.sourceShards = sourceShards
	mz.targetShards = targetShards
	mz.isPartial = isPartial
	mz.primaryVindexesDiffer = differentPVs
	return nil
}

func (mz *materializer) startStreams(ctx context.Context) error {
	return forAllShards(mz.targetShards, func(target *topo.ShardInfo) error {
		targetPrimary, err := mz.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}
		if _, err := mz.tmc.UpdateVReplicationWorkflow(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow: mz.ms.Workflow,
			State:    binlogdatapb.VReplicationWorkflowState_Running,
			// Don't change anything else, so pass simulated NULLs.
			Cells: textutil.SimulatedNullStringSlice,
			TabletTypes: []topodatapb.TabletType{
				topodatapb.TabletType(textutil.SimulatedNullInt),
			},
			OnDdl: binlogdatapb.OnDDLAction(textutil.SimulatedNullInt),
		}); err != nil {
			return vterrors.Wrap(err, "failed to update workflow")
		}
		return nil
	})
}

func (mz *materializer) forAllTargets(f func(*topo.ShardInfo) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range mz.targetShards {
		wg.Add(1)
		go func(target *topo.ShardInfo) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

// checkTZConversion is a light-weight consistency check to validate that, if a source time zone is specified to MoveTables,
// that the current primary has the time zone loaded in order to run the convert_tz() function used by VReplication to do the
// datetime conversions. We only check the current primaries on each shard and note here that it is possible a new primary
// gets elected: in this case user will either see errors during vreplication or vdiff will report mismatches.
func (mz *materializer) checkTZConversion(ctx context.Context, tz string) error {
	err := mz.forAllTargets(func(target *topo.ShardInfo) error {
		targetPrimary, err := mz.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}
		testDateTime := "2006-01-02 15:04:05"
		query := fmt.Sprintf("select convert_tz(%s, %s, 'UTC')", encodeString(testDateTime), encodeString(tz))
		qrproto, err := mz.tmc.ExecuteFetchAsApp(ctx, targetPrimary.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
			Query:   []byte(query),
			MaxRows: 1,
		})
		if err != nil {
			return vterrors.Wrapf(err, "ExecuteFetchAsApp(%v, %s)", targetPrimary.Tablet, query)
		}
		qr := sqltypes.Proto3ToResult(qrproto)
		if gotDate, err := time.Parse(testDateTime, qr.Rows[0][0].ToString()); err != nil {
			return fmt.Errorf("unable to perform time_zone conversions from %s to UTC — value from DB was: %+v and the result of the attempt was: %s. Either the specified source time zone is invalid or the time zone tables have not been loaded on the %s tablet",
				tz, qr.Rows, gotDate, targetPrimary.Alias)
		}
		return nil
	})
	return err
}

// filterSourceShards filters out source shards that do not overlap with the
// provided target shard. This is an optimization to avoid copying unnecessary
// data between the shards. This optimization is only applied for MoveTables
// when the source and target shard have the same primary vindexes.
func (mz *materializer) filterSourceShards(targetShard *topo.ShardInfo) []*topo.ShardInfo {
	if mz.primaryVindexesDiffer || mz.ms.MaterializationIntent != vtctldatapb.MaterializationIntent_MOVETABLES {
		// Use all source shards.
		return mz.sourceShards
	}
	// Use intersecting source shards.
	var filteredSourceShards []*topo.ShardInfo
	for _, sourceShard := range mz.sourceShards {
		if !key.KeyRangeIntersect(sourceShard.KeyRange, targetShard.KeyRange) {
			continue
		}
		filteredSourceShards = append(filteredSourceShards, sourceShard)
	}
	return filteredSourceShards
}

// primaryVindexesDiffer returns true if, for any tables defined in the provided
// materialize settings, the source and target vschema definitions for those
// tables have different primary vindexes.
//
// The result of this function is used to determine whether to apply a source
// shard selection optimization in MoveTables.
func primaryVindexesDiffer(ms *vtctldatapb.MaterializeSettings, source, target *vschemapb.Keyspace) bool {
	// Unless both keyspaces are sharded, treat the answer to the question as
	// trivially false.
	if source.Sharded != target.Sharded {
		return false
	}

	// For source and target keyspaces that are sharded, we can optimize source
	// shard selection if source and target tables' primary vindexes are equal.
	//
	// To determine this, iterate over all target tables, looking for primary
	// vindexes that differ from the corresponding source table.
	for _, ts := range ms.TableSettings {
		sColumnVindexes := []*vschemapb.ColumnVindex{}
		tColumnVindexes := []*vschemapb.ColumnVindex{}
		if tt, ok := source.Tables[ts.TargetTable]; ok {
			sColumnVindexes = tt.ColumnVindexes
		}
		if tt, ok := target.Tables[ts.TargetTable]; ok {
			tColumnVindexes = tt.ColumnVindexes
		}

		// If source does not have a primary vindex, but the target does, then
		// the primary vindexes differ.
		if len(sColumnVindexes) == 0 && len(tColumnVindexes) > 0 {
			return true
		}
		// If source has a primary vindex, but the target does not, then the
		// primary vindexes differ.
		if len(sColumnVindexes) > 0 && len(tColumnVindexes) == 0 {
			return true
		}
		// If neither source nor target have any vindexes, treat the answer to
		// the question as trivially false.
		if len(sColumnVindexes) == 0 && len(tColumnVindexes) == 0 {
			return true
		}

		sPrimaryVindex := sColumnVindexes[0]
		tPrimaryVindex := tColumnVindexes[0]

		// Compare source and target primary vindex columns.
		var sColumns, tColumns []string
		if sPrimaryVindex.Column != "" {
			sColumns = []string{sPrimaryVindex.Column}
		} else {
			sColumns = sPrimaryVindex.Columns
		}
		if tPrimaryVindex.Column != "" {
			tColumns = []string{tPrimaryVindex.Column}
		} else {
			tColumns = tPrimaryVindex.Columns
		}
		if len(sColumns) != len(tColumns) {
			return true
		}
		for i := 0; i < len(sColumns); i++ {
			if !strings.EqualFold(sColumns[i], tColumns[i]) {
				return true
			}
		}

		// Get source and target vindex definitions.
		spv := source.Vindexes[sColumnVindexes[0].Name]
		tpv := target.Vindexes[tColumnVindexes[0].Name]
		// If the source has vindex definition, but target does not, then the
		// target vschema is invalid. Assume the primary vindexes differ.
		if spv != nil && tpv == nil {
			return true
		}
		// If the target has vindex definition, but source does not, then the
		// source vschema is invalid. Assume the primary vindexes differ.
		if spv == nil && tpv != nil {
			return true
		}
		// If both target and source are missing vindex definitions, then both
		// are equally invalid.
		if spv == nil && tpv == nil {
			continue
		}
		// Compare source and target vindex type.
		if !strings.EqualFold(spv.Type, tpv.Type) {
			return true
		}
	}
	return false
}
