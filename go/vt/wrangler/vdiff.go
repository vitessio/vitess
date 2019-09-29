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
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type tableDiffer struct {
	targetTable      string
	targetExpression string
	sourceExpression string
	compareCols      []int
	orderBy          []engine.OrderbyParams
}

// VDiff reports differences between the sources and targets of a vreplication workflow.
func (wr *Wrangler) VDiff(ctx context.Context, targetKeyspace, workflow string, filteredReplicationWaitTime time.Duration) error {
	mi, err := wr.buildMigrater(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildMigrater failed: %v", err)
		return err
	}
	if err := mi.validate(ctx, false /* isWrite */); err != nil {
		mi.wr.Logger().Errorf("validate failed: %v", err)
		return err
	}
	var oneTarget *miTarget
	for _, target := range mi.targets {
		oneTarget = target
		break
	}
	var oneFilter *binlogdatapb.Filter
	for _, bls := range oneTarget.sources {
		oneFilter = bls.Filter
		break
	}
	schm, err := wr.GetSchema(ctx, oneTarget.master.Alias, nil, nil, false)
	if err != nil {
		return err
	}
	_, err = buildVDiffPlan(ctx, oneFilter, schm)
	return err
}

func buildVDiffPlan(ctx context.Context, filter *binlogdatapb.Filter, schm *tabletmanagerdatapb.SchemaDefinition) (map[string]*tableDiffer, error) {
	tableDiffers := make(map[string]*tableDiffer)
	for _, table := range schm.TableDefinitions {
		rule, err := vreplication.MatchTable(table.Name, filter)
		if err != nil {
			return nil, err
		}
		if rule == nil {
			continue
		}
		query := rule.Filter
		if rule.Filter == "" || key.IsKeyRange(rule.Filter) {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("select * from %v", sqlparser.NewTableIdent(table.Name))
			query = buf.String()
		}
		tableDiffers[table.Name], err = buildDifferPlan(table, query)
		if err != nil {
			return nil, err
		}
	}
	return tableDiffers, nil
}

func buildDifferPlan(table *tabletmanagerdatapb.TableDefinition, query string) (*tableDiffer, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}
	td := &tableDiffer{
		targetTable: table.Name,
	}
	sourceSelect := &sqlparser.Select{}
	targetSelect := &sqlparser.Select{}
	for _, selExpr := range sel.SelectExprs {
		switch selExpr := selExpr.(type) {
		case *sqlparser.StarExpr:
			for _, fld := range table.Fields {
				aliased := &sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(fld.Name)}}
				sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, aliased)
				targetSelect.SelectExprs = append(targetSelect.SelectExprs, aliased)
			}
		case *sqlparser.AliasedExpr:
			var targetCol *sqlparser.ColName
			if !selExpr.As.IsEmpty() {
				targetCol = &sqlparser.ColName{Name: selExpr.As}
			} else {
				if colAs, ok := selExpr.Expr.(*sqlparser.ColName); ok {
					targetCol = colAs
				} else {
					return nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(selExpr))
				}
			}
			sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, selExpr)
			targetSelect.SelectExprs = append(targetSelect.SelectExprs, &sqlparser.AliasedExpr{Expr: targetCol})
		}
	}
	fields := make(map[string]querypb.Type)
	for _, field := range table.Fields {
		fields[strings.ToLower(field.Name)] = field.Type
	}

	td.compareCols = make([]int, len(sourceSelect.SelectExprs))
	for i := range td.compareCols {
		colname := sourceSelect.SelectExprs[i].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.Lowered()
		typ, ok := fields[colname]
		if !ok {
			return nil, fmt.Errorf("column %v not found in table %v", colname, table.Name)
		}
		td.compareCols[i] = i
		if sqltypes.IsText(typ) {
			sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, wrapWeightString(sourceSelect.SelectExprs[i]))
			targetSelect.SelectExprs = append(targetSelect.SelectExprs, wrapWeightString(targetSelect.SelectExprs[i]))
			td.compareCols[i] = len(sourceSelect.SelectExprs) - 1
		}
	}

	sourceSelect.From = sel.From
	targetSelect.From = sqlparser.TableExprs{
		&sqlparser.AliasedTableExpr{
			Expr: &sqlparser.TableName{
				Name: sqlparser.NewTableIdent(table.Name),
			},
		},
	}

	var orderby sqlparser.OrderBy
	for _, pk := range table.PrimaryKeyColumns {
		found := false
		for i, selExpr := range targetSelect.SelectExprs {
			colname := selExpr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.Lowered()
			if pk == colname {
				td.orderBy = append(td.orderBy, engine.OrderbyParams{Col: td.compareCols[i]})
				found = true
				break
			}
		}
		if !found {
			// Unreachable.
			return nil, fmt.Errorf("column %v not found in table %v", pk, table.Name)
		}
		orderby = append(orderby, &sqlparser.Order{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(pk)}})
	}
	targetSelect.OrderBy = orderby

	sourceSelect.Where = removeKeyrange(sel.Where)
	sourceSelect.GroupBy = sel.GroupBy
	sourceSelect.OrderBy = orderby

	td.sourceExpression = sqlparser.String(sourceSelect)
	td.targetExpression = sqlparser.String(targetSelect)
	return td, nil
}

func removeKeyrange(where *sqlparser.Where) *sqlparser.Where {
	if where == nil {
		return nil
	}
	if isFuncKeyrange(where.Expr) {
		return nil
	}
	where.Expr = removeExprKeyrange(where.Expr)
	return where
}

func removeExprKeyrange(node sqlparser.Expr) sqlparser.Expr {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		if isFuncKeyrange(node.Left) {
			return removeExprKeyrange(node.Right)
		}
		if isFuncKeyrange(node.Right) {
			return removeExprKeyrange(node.Left)
		}
		return &sqlparser.AndExpr{
			Left:  removeExprKeyrange(node.Left),
			Right: removeExprKeyrange(node.Right),
		}
	case *sqlparser.ParenExpr:
		return &sqlparser.ParenExpr{
			Expr: removeExprKeyrange(node.Expr),
		}
	}
	return node
}

func isFuncKeyrange(expr sqlparser.Expr) bool {
	funcExpr, ok := expr.(*sqlparser.FuncExpr)
	return ok && funcExpr.Name.EqualString("in_keyrange")
}

func wrapWeightString(expr sqlparser.SelectExpr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name: sqlparser.NewColIdent("weight_string"),
			Exprs: []sqlparser.SelectExpr{
				&sqlparser.AliasedExpr{
					Expr: expr.(*sqlparser.AliasedExpr).Expr,
				},
			},
		},
	}
}
