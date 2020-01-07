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
	"strings"
	"sync"
	"text/template"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type materializer struct {
	wr            *Wrangler
	ms            *vtctldatapb.MaterializeSettings
	targetVSchema *vindexes.KeyspaceSchema
	sourceShards  []*topo.ShardInfo
	targetShards  []*topo.ShardInfo
}

// Materialize performs the steps needed to materialize a list of tables based on the materialization specs.
func (wr *Wrangler) Materialize(ctx context.Context, ms *vtctldatapb.MaterializeSettings) error {
	if err := wr.validateNewWorkflow(ctx, ms.TargetKeyspace, ms.Workflow); err != nil {
		return err
	}
	mz, err := wr.buildMaterializer(ctx, ms)
	if err != nil {
		return err
	}
	if err := mz.deploySchema(ctx); err != nil {
		return err
	}
	inserts, err := mz.generateInserts(ctx)
	if err != nil {
		return err
	}
	if err := mz.createStreams(ctx, inserts); err != nil {
		return err
	}
	return mz.startStreams(ctx)
}

func (wr *Wrangler) buildMaterializer(ctx context.Context, ms *vtctldatapb.MaterializeSettings) (*materializer, error) {
	vschema, err := wr.ts.GetVSchema(ctx, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	targetVSchema, err := vindexes.BuildKeyspaceSchema(vschema, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	if targetVSchema.Keyspace.Sharded {
		for _, ts := range ms.TableSettings {
			if targetVSchema.Tables[ts.TargetTable] == nil {
				return nil, fmt.Errorf("table %s not found in vschema for keyspace %s", ts.TargetTable, ms.TargetKeyspace)
			}
		}
	}

	sourceShards, err := wr.ts.GetServingShards(ctx, ms.SourceKeyspace)
	if err != nil {
		return nil, err
	}
	targetShards, err := wr.ts.GetServingShards(ctx, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	return &materializer{
		wr:            wr,
		ms:            ms,
		targetVSchema: targetVSchema,
		sourceShards:  sourceShards,
		targetShards:  targetShards,
	}, nil
}

func (mz *materializer) deploySchema(ctx context.Context) error {
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		for _, ts := range mz.ms.TableSettings {
			tableSchema, err := mz.wr.GetSchema(ctx, target.MasterAlias, []string{ts.TargetTable}, nil, false)
			if err != nil {
				return err
			}
			if len(tableSchema.TableDefinitions) != 0 {
				// Table already exists.
				continue
			}
			if ts.CreateDdl == "" {
				return fmt.Errorf("target table %v does not exist and there is no create ddl defined", ts.TargetTable)
			}
			createddl := ts.CreateDdl
			if createddl == "copy" {
				sourceTableName, err := sqlparser.TableFromStatement(ts.SourceExpression)
				if err != nil {
					return err
				}
				if sourceTableName.Name.String() != ts.TargetTable {
					return fmt.Errorf("source and target table names must match for copying schema: %v vs %v", sqlparser.String(sourceTableName), ts.TargetTable)
				}
				sourceMaster := mz.sourceShards[0].MasterAlias
				if sourceMaster == nil {
					return fmt.Errorf("source shard must have a master for copying schema: %v", mz.sourceShards[0].ShardName())
				}
				sourceSchema, err := mz.wr.GetSchema(ctx, sourceMaster, []string{ts.TargetTable}, nil, false)
				if err != nil {
					return err
				}
				if len(sourceSchema.TableDefinitions) == 0 {
					return fmt.Errorf("source table %v does not exist", ts.TargetTable)
				}
				createddl = sourceSchema.TableDefinitions[0].Schema
			}
			targetTablet, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
			if err != nil {
				return err
			}
			if _, err := mz.wr.tmc.ExecuteFetchAsDba(ctx, targetTablet.Tablet, false, []byte(createddl), 0, false, true); err != nil {
				return err
			}
		}
		return nil
	})
}

func (mz *materializer) generateInserts(ctx context.Context) (string, error) {
	ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, "{{.dbname}}")

	for _, source := range mz.sourceShards {
		bls := &binlogdatapb.BinlogSource{
			Keyspace: mz.ms.SourceKeyspace,
			Shard:    source.ShardName(),
			Filter:   &binlogdatapb.Filter{},
		}
		for _, ts := range mz.ms.TableSettings {
			rule := &binlogdatapb.Rule{
				Match: ts.TargetTable,
			}
			// Validate the query.
			stmt, err := sqlparser.Parse(ts.SourceExpression)
			if err != nil {
				return "", err
			}
			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				return "", fmt.Errorf("unrecognized statement: %s", ts.SourceExpression)
			}
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
				subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte(vindexName))})
				subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("'{{.keyrange}}'"))})
				sel.Where = &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: &sqlparser.FuncExpr{
						Name:  sqlparser.NewColIdent("in_keyrange"),
						Exprs: subExprs,
					},
				}
				rule.Filter = sqlparser.String(sel)
			} else {
				rule.Filter = ts.SourceExpression
			}
			bls.Filter.Rules = append(bls.Filter.Rules, rule)
		}
		ig.AddRow(mz.ms.Workflow, bls, "", mz.ms.Cell, mz.ms.TabletTypes)
	}
	return ig.String(), nil
}

func matchColInSelect(col sqlparser.ColIdent, sel *sqlparser.Select) (*sqlparser.ColName, error) {
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

func (mz *materializer) createStreams(ctx context.Context, inserts string) error {
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		targetMaster, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.MasterAlias)
		}
		buf := &strings.Builder{}
		t := template.Must(template.New("").Parse(inserts))
		input := map[string]string{
			"keyrange": key.KeyRangeString(target.KeyRange),
			"dbname":   targetMaster.DbName(),
		}
		if err := t.Execute(buf, input); err != nil {
			return err
		}
		if _, err := mz.wr.TabletManagerClient().VReplicationExec(ctx, targetMaster.Tablet, buf.String()); err != nil {
			return err
		}
		return nil
	})
}

func (mz *materializer) startStreams(ctx context.Context) error {
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		targetMaster, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.MasterAlias)
		}
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow=%s", encodeString(targetMaster.DbName()), encodeString(mz.ms.Workflow))
		if _, err := mz.wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetMaster.Tablet, query)
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
