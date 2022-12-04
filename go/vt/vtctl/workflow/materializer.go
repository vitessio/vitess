package workflow

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
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

	ms            *vtctldatapb.MaterializeSettings
	targetVSchema *vindexes.KeyspaceSchema
	sourceShards  []*topo.ShardInfo
	targetShards  []*topo.ShardInfo
	isPartial     bool
}

func (mz *materializer) prepareMaterializerStreams() error {
	if err := validateNewWorkflow(mz.ctx, mz.ts, mz.tmc, mz.ms.TargetKeyspace, mz.ms.Workflow); err != nil {
		return err
	}
	err := mz.buildMaterializer()
	if err != nil {
		return err
	}
	if mz.isPartial {
		if err := createDefaultShardRoutingRules(mz.ctx, mz.ms, mz.ts, mz.ts); err != nil {
			return err
		}
	}
	if err := mz.deploySchema(); err != nil {
		return err
	}
	insertMap := make(map[string]string, len(mz.targetShards))
	for _, targetShard := range mz.targetShards {
		inserts, err := mz.generateInserts(mz.ctx, targetShard)
		if err != nil {
			return err
		}
		insertMap[targetShard.ShardName()] = inserts
	}
	if err := mz.createStreams(mz.ctx, insertMap); err != nil {
		return err
	}
	return nil

}

func (mz *materializer) generateInserts(ctx context.Context, targetShard *topo.ShardInfo) (string, error) {
	ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, "{{.dbname}}")

	for _, sourceShard := range mz.sourceShards {
		// Don't create streams from sources which won't contain data for the target shard.
		// We only do it for MoveTables for now since this doesn't hold for materialize flows
		// where the target's sharding key might differ from that of the source
		if mz.ms.MaterializationIntent == vtctldatapb.MaterializationIntent_MOVETABLES &&
			!key.KeyRangesIntersect(sourceShard.KeyRange, targetShard.KeyRange) {
			continue
		}
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
			stmt, err := sqlparser.Parse(ts.SourceExpression)
			if err != nil {
				return "", err
			}
			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				return "", fmt.Errorf("unrecognized statement: %s", ts.SourceExpression)
			}
			filter := ts.SourceExpression
			if mz.targetVSchema.Keyspace.Sharded && mz.targetVSchema.Tables[ts.TargetTable].Type != vindexes.TypeReference {
				cv, err := vindexes.FindBestColVindex(mz.targetVSchema.Tables[ts.TargetTable])
				if err != nil {
					return "", err
				}
				mappedCols := make([]*sqlparser.ColName, 0, len(cv.Columns))
				for _, col := range cv.Columns {
					colName, err := matchColInSelect(col, sel)
					if err != nil {
						return "", err
					}
					mappedCols = append(mappedCols, colName)
				}
				subExprs := make(sqlparser.SelectExprs, 0, len(mappedCols)+2)
				for _, mappedCol := range mappedCols {
					subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: mappedCol})
				}
				vindexName := fmt.Sprintf("%s.%s", mz.ms.TargetKeyspace, cv.Name)
				subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral(vindexName)})
				subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("{{.keyrange}}")})
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
		workflowSubType := binlogdatapb.VReplicationWorkflowSubType_None
		if mz.isPartial {
			workflowSubType = binlogdatapb.VReplicationWorkflowSubType_Partial
		}
		ig.AddRow(mz.ms.Workflow, bls, "", mz.ms.Cell, mz.ms.TabletTypes,
			int64(mz.ms.MaterializationIntent),
			int64(workflowSubType))
	}
	return ig.String(), nil
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
				//only get ddls for tables, once and lazily: if we need to copy the schema from source to target
				//we copy schemas from primaries on the source keyspace
				//and we have found use cases where user just has a replica (no primary) in the source keyspace
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
					sourceTableName, err := sqlparser.TableFromStatement(ts.SourceExpression)
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
					strippedDDL, err := stripTableConstraints(ddl)
					if err != nil {
						return err
					}

					ddl = strippedDDL
				}

				if createDDL == createDDLAsCopyDropForeignKeys {
					strippedDDL, err := stripTableForeignKeys(ddl)
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
			sql := strings.Join(applyDDLs, ";\n")

			_, err = mz.tmc.ApplySchema(mz.ctx, targetTablet.Tablet, &tmutils.SchemaChange{
				SQL:              sql,
				Force:            false,
				AllowReplication: true,
				SQLMode:          vreplication.SQLMode,
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
	targetVSchema, err := vindexes.BuildKeyspaceSchema(vschema, ms.TargetKeyspace)
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
	mz.targetVSchema = targetVSchema
	mz.sourceShards = sourceShards
	mz.targetShards = targetShards
	mz.isPartial = isPartial
	return nil
}

func (mz *materializer) createStreams(ctx context.Context, insertsMap map[string]string) error {
	return forAllShards(mz.targetShards, func(target *topo.ShardInfo) error {
		inserts := insertsMap[target.ShardName()]
		targetPrimary, err := mz.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}
		buf := &strings.Builder{}
		t := template.Must(template.New("").Parse(inserts))
		input := map[string]string{
			"keyrange": key.KeyRangeString(target.KeyRange),
			"dbname":   targetPrimary.DbName(),
		}
		if err := t.Execute(buf, input); err != nil {
			return err
		}
		if _, err := mz.tmc.VReplicationExec(ctx, targetPrimary.Tablet, buf.String()); err != nil {
			return err
		}
		return nil
	})
}

func (mz *materializer) startStreams(ctx context.Context) error {
	return forAllShards(mz.targetShards, func(target *topo.ShardInfo) error {
		targetPrimary, err := mz.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow=%s", encodeString(targetPrimary.DbName()), encodeString(mz.ms.Workflow))
		if _, err := mz.tmc.VReplicationExec(ctx, targetPrimary.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetPrimary.Tablet, query)
		}
		return nil
	})
}

func Materialize(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, ms *vtctldatapb.MaterializeSettings) error {
	mz := &materializer{
		ctx:      ctx,
		ts:       ts,
		sourceTs: ts,
		tmc:      tmc,
		ms:       ms,
	}

	err := mz.prepareMaterializerStreams()
	if err != nil {
		return err
	}
	return mz.startStreams(ctx)
}
