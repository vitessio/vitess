// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

func buildSelectPlan(sel *sqlparser.Select, query string) *Plan {
	tablename, _ := analyzeFrom(sel.From)
	// TODO(sougou): handle joins & unions.
	if tablename == "" {
		return &Plan{
			ID:     NoPlan,
			Reason: "complex table expression",
			Query:  generateQuery(sel),
		}
	}
	table := gateSchema[tablename]
	if table == nil {
		return &Plan{
			ID:     NoPlan,
			Reason: "table not found",
			Query:  generateQuery(sel),
		}
	}
	if table.Keyspace.ShardingScheme == Unsharded {
		return &Plan{
			ID:    SelectUnsharded,
			Query: generateQuery(sel),
		}
	}
	if sel.Where == nil || !isRoutable(sel.Where.Expr) {
		return &Plan{
			ID:     SelectScatter,
			Reason: "unwieldy where clause",
			Query:  generateQuery(sel),
		}
	}
	plan := getRouting(sel.Where.Expr, table.Indexes)
	if plan.ID == SelectMultiPrimary || plan.ID == SelectMultiLookup {
		if hasAggregates(sel.SelectExprs) || sel.Distinct != "" || sel.GroupBy != nil || sel.Having != nil || sel.OrderBy != nil || sel.Limit != nil {
			return &Plan{
				ID:     NoPlan,
				Reason: "query too complex",
				Query:  generateQuery(sel),
			}
		}
	}

	plan.Query = generateQuery(sel)
	return plan
}

func generateQuery(sel *sqlparser.Select) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	sel.Format(buf)
	return buf.String()
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

func isRoutable(node sqlparser.Expr) bool {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		return isRoutable(node.Left) && isRoutable(node.Right)
	case *sqlparser.OrExpr:
		return false
	case *sqlparser.NotExpr:
		return isRoutable(node.Expr)
	case *sqlparser.ParenBoolExpr:
		return isRoutable(node.Expr)
	case *sqlparser.ComparisonExpr:
		return isRoutable(node.Left) && isRoutable(node.Right)
	case *sqlparser.RangeCond:
		return isRoutable(node.Left) && isRoutable(node.From) && isRoutable(node.To)
	case *sqlparser.NullCheck:
		return isRoutable(node.Expr)
	case *sqlparser.ExistsExpr:
		return false
	case sqlparser.StrVal, sqlparser.NumVal, sqlparser.ValArg,
		*sqlparser.NullVal, *sqlparser.ColName, sqlparser.ValTuple,
		sqlparser.ListArg:
		return true
	case *sqlparser.Subquery:
		return false
	case *sqlparser.BinaryExpr:
		return isRoutable(node.Left) && isRoutable(node.Right)
	case *sqlparser.UnaryExpr:
		return isRoutable(node.Expr)
	case *sqlparser.FuncExpr, *sqlparser.CaseExpr:
		return true
	default:
		panic("unexpected")
	}
}

func getRouting(node sqlparser.BoolExpr, indexes []*GateIndex) (plan *Plan) {
	for _, index := range indexes {
		if planID, values := getMatch(node, index); planID != SelectScatter {
			return &Plan{
				ID:     planID,
				Index:  index,
				Values: values,
			}
		}
	}
	return &Plan{
		ID:     SelectScatter,
		Reason: "no index match",
	}
}

func getMatch(node sqlparser.BoolExpr, index *GateIndex) (planID PlanID, values []interface{}) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		if planID, values = getMatch(node.Left, index); planID != SelectScatter {
			return planID, values
		}
		if planID, values = getMatch(node.Right, index); planID != SelectScatter {
			return planID, values
		}
	case *sqlparser.ParenBoolExpr:
		return getMatch(node.Expr, index)
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case "=":
			if !nameMatch(node.Left, index.Column) {
				return SelectScatter, nil
			}
			if !sqlparser.IsValue(node.Right) {
				return SelectScatter, nil
			}
			val, err := sqlparser.AsInterface(node.Right)
			if err != nil {
				return SelectScatter, nil
			}
			if index.Type == Primary {
				planID = SelectSinglePrimary
			} else {
				planID = SelectSingleLookup
			}
			return planID, []interface{}{val}
		case "in":
			if !nameMatch(node.Left, index.Column) {
				return SelectScatter, nil
			}
			if !sqlparser.IsSimpleTuple(node.Right) {
				return SelectScatter, nil
			}
			val, err := sqlparser.AsInterface(node.Right)
			if err != nil {
				return SelectScatter, nil
			}
			node.Right = sqlparser.ListArg("::_vals")
			if index.Type == Primary {
				planID = SelectMultiPrimary
			} else {
				planID = SelectMultiLookup
			}
			values, ok := val.([]interface{})
			if !ok {
				values = []interface{}{val}
			}
			return planID, values
		}
	}
	return SelectScatter, nil
}

func nameMatch(node sqlparser.ValExpr, col string) bool {
	colname, ok := node.(*sqlparser.ColName)
	if !ok {
		return false
	}
	if string(colname.Name) != col {
		return false
	}
	return true
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
