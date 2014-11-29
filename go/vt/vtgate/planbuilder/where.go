// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

/*
import "github.com/youtube/vitess/go/vt/sqlparser"

// getWhereRouting fills the plan fields for the where clause of a SELECT
// statement. It gets reused for DML planning also, where the select plan is
// replaced with the appropriate DML plan after the fact.
func getWhereRouting(where *sqlparser.Where, plan *Plan) {
	if where == nil {
		plan.ID = SelectScatter
		plan.Reason = "no where clause"
		return
	}
	if hasSubquery(where.Expr) {
		plan.ID = NoPlan
		plan.Reason = "has subquery"
		return
	}
	for _, index := range plan.Table.Indexes {
		if planID, values := getMatch(where.Expr, index); planID != SelectScatter {
			plan.ID = planID
			plan.Index = index
			plan.Values = values
			return
		}
	}
	plan.ID = SelectScatter
	plan.Reason = "no index match"
}

func hasSubquery(node sqlparser.Expr) bool {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		return hasSubquery(node.Left) || hasSubquery(node.Right)
	case *sqlparser.OrExpr:
		return hasSubquery(node.Left) || hasSubquery(node.Right)
	case *sqlparser.NotExpr:
		return hasSubquery(node.Expr)
	case *sqlparser.ParenBoolExpr:
		return hasSubquery(node.Expr)
	case *sqlparser.ComparisonExpr:
		return hasSubquery(node.Left) || hasSubquery(node.Right)
	case *sqlparser.RangeCond:
		return hasSubquery(node.Left) || hasSubquery(node.From) || hasSubquery(node.To)
	case *sqlparser.NullCheck:
		return hasSubquery(node.Expr)
	case *sqlparser.ExistsExpr:
		return true
	case sqlparser.StrVal, sqlparser.NumVal, sqlparser.ValArg,
		*sqlparser.NullVal, *sqlparser.ColName, sqlparser.ValTuple,
		sqlparser.ListArg:
		return false
	case *sqlparser.Subquery:
		return true
	case *sqlparser.BinaryExpr:
		return hasSubquery(node.Left) || hasSubquery(node.Right)
	case *sqlparser.UnaryExpr:
		return hasSubquery(node.Expr)
	case *sqlparser.FuncExpr:
		for _, expr := range node.Exprs {
			switch expr := expr.(type) {
			case *sqlparser.NonStarExpr:
				if hasSubquery(expr.Expr) {
					return true
				}
			}
		}
		return false
	case *sqlparser.CaseExpr:
		if hasSubquery(node.Expr) || hasSubquery(node.Else) {
			return true
		}
		for _, expr := range node.Whens {
			if hasSubquery(expr.Cond) || hasSubquery(expr.Val) {
				return true
			}
		}
		return false
	case nil:
		return false
	default:
		panic("unexpected")
	}
}

func getMatch(node sqlparser.BoolExpr, index *Index) (planID PlanID, values interface{}) {
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
			if index.Type == ShardKey {
				planID = SelectSingleShardKey
			} else {
				planID = SelectSingleLookup
			}
			return planID, val
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
			if index.Type == ShardKey {
				planID = SelectMultiShardKey
			} else {
				planID = SelectMultiLookup
			}
			return planID, val
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
*/
