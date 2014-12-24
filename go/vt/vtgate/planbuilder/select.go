// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

func buildSelectPlan(sel *sqlparser.Select, schema *Schema) *Plan {
	plan := &Plan{ID: NoPlan}
	tablename, _ := analyzeFrom(sel.From)
	plan.Table, plan.Reason = schema.FindTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if !plan.Table.Keyspace.Sharded {
		plan.ID = SelectUnsharded
		return plan
	}

	getWhereRouting(sel.Where, plan, false)
	if plan.IsMulti() {
		if hasPostProcessing(sel) {
			plan.ID = NoPlan
			plan.Reason = "multi-shard query has post-processing constructs"
			return plan
		}
	}
	// The where clause might have changed.
	plan.Rewritten = generateQuery(sel)
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
		return exprHasAggregates(node.Left) || exprHasAggregates(node.Right)
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
		if node.IsAggregate() {
			return true
		}
		for _, expr := range node.Exprs {
			switch expr := expr.(type) {
			case *sqlparser.NonStarExpr:
				if exprHasAggregates(expr.Expr) {
					return true
				}
			}
		}
		return false
	case *sqlparser.CaseExpr:
		if exprHasAggregates(node.Expr) || exprHasAggregates(node.Else) {
			return true
		}
		for _, expr := range node.Whens {
			if exprHasAggregates(expr.Cond) || exprHasAggregates(expr.Val) {
				return true
			}
		}
		return false
	default:
		panic("unexpected")
	}
}

func hasPostProcessing(sel *sqlparser.Select) bool {
	return hasAggregates(sel.SelectExprs) || sel.Distinct != "" || sel.GroupBy != nil || sel.Having != nil || sel.OrderBy != nil || sel.Limit != nil
}
