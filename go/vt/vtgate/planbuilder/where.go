// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// ListVarName is the bind var name used for plans
// that require VTGate to compute custom list values,
// like for IN clauses.
const ListVarName = "_vals"

// getWhereRouting is only used for DMLs now.
// TODO(sougou): revisit after refactor.
func getWhereRouting(where *sqlparser.Where, route *Route) error {
	if where == nil {
		return errors.New("DML has multi-shard where clause")
	}
	if hasSubquery(where.Expr) {
		return errors.New("DML has subquery")
	}
	for _, index := range route.Table.Ordered {
		if !IsUnique(index.Vindex) {
			continue
		}
		if values := getMatch(where.Expr, index.Col); values != nil {
			route.Vindex = index.Vindex
			route.Values = values
			return nil
		}
	}
	return errors.New("DML has multi-shard where clause")
}

// TODO(sougou): rewrite.
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
	case *sqlparser.IsExpr:
		return hasSubquery(node.Expr)
	case *sqlparser.ExistsExpr:
		return true
	case sqlparser.StrVal, sqlparser.NumVal, sqlparser.ValArg,
		*sqlparser.NullVal, sqlparser.BoolVal, *sqlparser.ColName,
		sqlparser.ValTuple, sqlparser.ListArg, *sqlparser.KeyrangeExpr:
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
		panic(fmt.Errorf("unexpected type: %T", node))
	}
}

func getMatch(node sqlparser.BoolExpr, col string) interface{} {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		if values := getMatch(node.Left, col); values != nil {
			return values
		}
		if values := getMatch(node.Right, col); values != nil {
			return values
		}
	case *sqlparser.ParenBoolExpr:
		return getMatch(node.Expr, col)
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case "=":
			if !nameMatch(node.Left, col) {
				return nil
			}
			if !sqlparser.IsValue(node.Right) {
				return nil
			}
			val, err := asInterface(node.Right)
			if err != nil {
				return nil
			}
			return val
		}
		return nil
	}
	return nil
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

// asInterface is similar to sqlparser.AsInterface, but it converts
// numeric and string types to native go types.
func asInterface(node sqlparser.ValExpr) (interface{}, error) {
	switch node := node.(type) {
	case sqlparser.ValTuple:
		vals := make([]interface{}, 0, len(node))
		for _, val := range node {
			v, err := asInterface(val)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return vals, nil
	case sqlparser.ValArg:
		return string(node), nil
	case sqlparser.ListArg:
		return string(node), nil
	case sqlparser.StrVal:
		return []byte(node), nil
	case sqlparser.NumVal:
		val := string(node)
		signed, err := strconv.ParseInt(val, 0, 64)
		if err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(val, 0, 64)
		if err == nil {
			return unsigned, nil
		}
		return nil, err
	case *sqlparser.NullVal:
		return nil, nil
	}
	return nil, fmt.Errorf("%v is not a value", sqlparser.String(node))
}
