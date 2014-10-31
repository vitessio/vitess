// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

func buildSelectPlan(sel *sqlparser.Select, schema *VTGateSchema) *Plan {
	// TODO(sougou): handle joins & unions.
	tablename, _ := analyzeFrom(sel.From)
	plan := getTableRouting(tablename, schema)
	if plan != nil {
		plan.Query = generateQuery(sel)
		return plan
	}
	plan = getWhereRouting(sel.Where, schema.Tables[tablename].Indexes)
	if plan.ID.IsMulti() {
		if hasAggregates(sel.SelectExprs) || sel.Distinct != "" || sel.GroupBy != nil || sel.Having != nil || sel.OrderBy != nil || sel.Limit != nil {
			return &Plan{
				ID:        NoPlan,
				Reason:    "too complex",
				TableName: tablename,
				Query:     generateQuery(sel),
			}
		}
	}

	plan.TableName = tablename
	plan.Query = generateQuery(sel)
	return plan
}

// TODO(sougou): Copied from tabletserver. Reuse.
func analyzeFrom(tableExprs sqlparser.TableExprs) (tablename string, hasHints bool) {
	if len(tableExprs) > 1 {
		return "", false
	}
	node, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", false
	}
	return sqlparser.GetTableName(node.Expr), node.Hints != nil
}

func hasAggregates(node sqlparser.SelectExprs) bool {
	for _, node := range node {
		switch node := node.(type) {
		case *sqlparser.NonStarExpr:
			if exprHasAggregates(node.Expr) {
				return true
			}
		}
	}
	return false
}

func exprHasAggregates(node sqlparser.Expr) bool {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		return exprHasAggregates(node.Left) || exprHasAggregates(node.Right)
	case *sqlparser.OrExpr:
		return false
	case *sqlparser.NotExpr:
		return exprHasAggregates(node.Expr)
	case *sqlparser.ParenBoolExpr:
		return exprHasAggregates(node.Expr)
	case *sqlparser.ComparisonExpr:
		return exprHasAggregates(node.Left) || exprHasAggregates(node.Right)
	case *sqlparser.RangeCond:
		return exprHasAggregates(node.Left) || exprHasAggregates(node.From) || exprHasAggregates(node.To)
	case *sqlparser.NullCheck:
		return exprHasAggregates(node.Expr)
	case *sqlparser.ExistsExpr:
		return false
	case sqlparser.StrVal, sqlparser.NumVal, sqlparser.ValArg,
		*sqlparser.NullVal, *sqlparser.ColName, sqlparser.ValTuple,
		sqlparser.ListArg:
		return false
	case *sqlparser.Subquery:
		return false
	case *sqlparser.BinaryExpr:
		return exprHasAggregates(node.Left) || exprHasAggregates(node.Right)
	case *sqlparser.UnaryExpr:
		return exprHasAggregates(node.Expr)
	case *sqlparser.FuncExpr:
		return node.IsAggregate()
	case *sqlparser.CaseExpr:
		return false
	default:
		panic("unexpected")
	}
}
