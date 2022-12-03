package workflow

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// validateNewWorkflow ensures that the specified workflow doesn't already exist
// in the keyspace.
func validateNewWorkflow(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, keyspace, workflow string) error {
	allshards, err := ts.FindAllShardsInKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, si := range allshards {
		if si.PrimaryAlias == nil {
			allErrors.RecordError(fmt.Errorf("shard has no primary: %v", si.ShardName()))
			continue
		}
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()

			primary, err := ts.GetTablet(ctx, si.PrimaryAlias)
			if err != nil {
				allErrors.RecordError(vterrors.Wrap(err, "validateWorkflowName.GetTablet"))
				return
			}
			validations := []struct {
				query string
				msg   string
			}{{
				fmt.Sprintf("select 1 from _vt.vreplication where db_name=%s and workflow=%s", encodeString(primary.DbName()), encodeString(workflow)),
				fmt.Sprintf("workflow %s already exists in keyspace %s on tablet %d", workflow, keyspace, primary.Alias.Uid),
			}, {
				fmt.Sprintf("select 1 from _vt.vreplication where db_name=%s and message='FROZEN' and workflow_sub_type != %d", encodeString(primary.DbName()), binlogdata.VReplicationWorkflowSubType_Partial),
				fmt.Sprintf("found previous frozen workflow on tablet %d, please review and delete it first before creating a new workflow",
					primary.Alias.Uid),
			}}
			for _, validation := range validations {
				p3qr, err := tmc.VReplicationExec(ctx, primary.Tablet, validation.query)
				if err != nil {
					allErrors.RecordError(vterrors.Wrap(err, "validateWorkflowName.VReplicationExec"))
					return
				}
				if p3qr != nil && len(p3qr.Rows) != 0 {
					allErrors.RecordError(vterrors.Wrap(fmt.Errorf(validation.msg), "validateWorkflowName.VReplicationExec"))
					return
				}
			}
		}(si)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

// createDefaultShardRoutingRules creates a reverse routing rule for
// each shard in a new partial keyspace migration workflow that does
// not already have an existing routing rule in place.
func createDefaultShardRoutingRules(ctx context.Context, ms *vtctldatapb.MaterializeSettings, ts, sourceTs *topo.Server) error {
	srr, err := topotools.GetShardRoutingRules(ctx, ts)
	if err != nil {
		return err
	}
	allShards, err := sourceTs.GetServingShards(ctx, ms.SourceKeyspace)
	if err != nil {
		return err
	}
	changed := false
	for _, si := range allShards {
		fromSource := fmt.Sprintf("%s.%s", ms.SourceKeyspace, si.ShardName())
		fromTarget := fmt.Sprintf("%s.%s", ms.TargetKeyspace, si.ShardName())
		if srr[fromSource] == "" && srr[fromTarget] == "" {
			srr[fromTarget] = ms.SourceKeyspace
			changed = true
			log.Infof("Added default shard routing rule from %q to %q", fromTarget, fromSource)
		}
	}
	if changed {
		if err := topotools.SaveShardRoutingRules(ctx, ts, srr); err != nil {
			return err
		}
		if err := ts.RebuildSrvVSchema(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

func stripTableConstraints(ddl string) (string, error) {
	ast, err := sqlparser.ParseStrictDDL(ddl)
	if err != nil {
		return "", err
	}

	stripConstraints := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.DDLStatement:
			if node.GetTableSpec() != nil {
				node.GetTableSpec().Constraints = nil
			}
		}
		return true
	}

	noConstraintAST := sqlparser.Rewrite(ast, stripConstraints, nil)
	newDDL := sqlparser.String(noConstraintAST)

	return newDDL, nil
}

func stripTableForeignKeys(ddl string) (string, error) {

	ast, err := sqlparser.ParseStrictDDL(ddl)
	if err != nil {
		return "", err
	}

	stripFKConstraints := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.DDLStatement:
			if node.GetTableSpec() != nil {
				var noFKConstraints []*sqlparser.ConstraintDefinition
				for _, constraint := range node.GetTableSpec().Constraints {
					if constraint.Details != nil {
						if _, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition); !ok {
							noFKConstraints = append(noFKConstraints, constraint)
						}
					}
				}
				node.GetTableSpec().Constraints = noFKConstraints
			}
		}
		return true
	}

	noFKConstraintAST := sqlparser.Rewrite(ast, stripFKConstraints, nil)
	newDDL := sqlparser.String(noFKConstraintAST)
	return newDDL, nil
}

func getSourceTableDDLs(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, shards []*topo.ShardInfo) (map[string]string, error) {
	sourceDDLs := make(map[string]string)
	allTables := []string{"/.*/"}

	sourcePrimary := shards[0].PrimaryAlias
	if sourcePrimary == nil {
		return nil, fmt.Errorf("shard must have a primary for copying schema: %v", shards[0].ShardName())
	}

	ti, err := ts.GetTablet(ctx, sourcePrimary)
	if err != nil {
		return nil, err
	}
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: allTables}
	sourceSchema, err := tmc.GetSchema(ctx, ti.Tablet, req)
	if err != nil {
		return nil, err
	}

	for _, td := range sourceSchema.TableDefinitions {
		sourceDDLs[td.Name] = td.Schema
	}
	return sourceDDLs, nil
}

func forAllShards(shards []*topo.ShardInfo, f func(*topo.ShardInfo) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range shards {
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

func matchColInSelect(col sqlparser.IdentifierCI, sel *sqlparser.Select) (*sqlparser.ColName, error) {
	for _, selExpr := range sel.SelectExprs {
		switch selExpr := selExpr.(type) {
		case *sqlparser.StarExpr:
			return &sqlparser.ColName{Name: col}, nil
		case *sqlparser.AliasedExpr:
			match := selExpr.As
			if match.IsEmpty() {
				if colExpr, ok := selExpr.Expr.(*sqlparser.ColName); ok {
					match = colExpr.Name
				} else {
					// Cannot match against a complex expression.
					continue
				}
			}
			if match.Equal(col) {
				colExpr, ok := selExpr.Expr.(*sqlparser.ColName)
				if !ok {
					return nil, fmt.Errorf("vindex column cannot be a complex expression: %v", sqlparser.String(selExpr))
				}
				return colExpr, nil
			}
		default:
			return nil, fmt.Errorf("unsupported select expression: %v", sqlparser.String(selExpr))
		}
	}
	return nil, fmt.Errorf("could not find vindex column %v", sqlparser.String(col))
}
