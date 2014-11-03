// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

func getWhereRouting(where *sqlparser.Where, indexes []*VTGateIndex) (plan *Plan) {
	if where == nil {
		return &Plan{
			ID:     SelectScatter,
			Reason: "no where clause",
		}
	}
	if hasSubquery(where.Expr) {
		return &Plan{
			ID:     NoPlan,
			Reason: "has subquery",
		}
	}
	for _, index := range indexes {
		if planID, values := getMatch(where.Expr, index); planID != SelectScatter {
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
	case *sqlparser.FuncExpr, *sqlparser.CaseExpr:
		return false
	default:
		panic("unexpected")
	}
}

func getMatch(node sqlparser.BoolExpr, index *VTGateIndex) (planID PlanID, values interface{}) {
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
