// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client2

import (
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

const (
	EID_NODE = iota
	VALUE_NODE
	LIST_NODE
	OTHER_NODE
)

type RoutingPlan struct {
	criteria sqlparser.SQLNode
}

func GetShardList(sql string, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (shardlist []int, err error) {
	plan, err := buildPlan(sql)
	if err != nil {
		return nil, err
	}
	return shardListFromPlan(plan, bindVariables, tabletKeys)
}

func buildPlan(sql string) (plan *RoutingPlan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return getRoutingPlan(statement)
}

func shardListFromPlan(plan *RoutingPlan, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (shardList []int, err error) {
	if plan.criteria == nil {
		return makeList(0, len(tabletKeys)), nil
	}

	switch criteria := plan.criteria.(type) {
	case sqlparser.Values:
		index, err := findInsertShard(criteria, bindVariables, tabletKeys)
		if err != nil {
			return nil, err
		}
		return []int{index}, nil
	case *sqlparser.ComparisonExpr:
		switch criteria.Operator {
		case "=", "<=>":
			index, err := findShard(criteria.Right, bindVariables, tabletKeys)
			if err != nil {
				return nil, err
			}
			return []int{index}, nil
		case "<", "<=":
			index, err := findShard(criteria.Right, bindVariables, tabletKeys)
			if err != nil {
				return nil, err
			}
			return makeList(0, index+1), nil
		case ">", ">=":
			index, err := findShard(criteria.Right, bindVariables, tabletKeys)
			if err != nil {
				return nil, err
			}
			return makeList(index, len(tabletKeys)), nil
		case "in":
			return findShardList(criteria.Right, bindVariables, tabletKeys)
		}
	case *sqlparser.RangeCond:
		if criteria.Operator == "between" {
			start, err := findShard(criteria.From, bindVariables, tabletKeys)
			if err != nil {
				return nil, err
			}
			last, err := findShard(criteria.To, bindVariables, tabletKeys)
			if err != nil {
				return nil, err
			}
			if last < start {
				start, last = last, start
			}
			return makeList(start, last+1), nil
		}
	}
	return makeList(0, len(tabletKeys)), nil
}

func getRoutingPlan(statement sqlparser.Statement) (plan *RoutingPlan, err error) {
	plan = &RoutingPlan{}
	if ins, ok := statement.(*sqlparser.Insert); ok {
		if sel, ok := ins.Rows.(sqlparser.SelectStatement); ok {
			return getRoutingPlan(sel)
		}
		plan.criteria, err = routingAnalyzeValues(ins.Rows.(sqlparser.Values))
		if err != nil {
			return nil, err
		}
		return plan, nil
	}
	var where *sqlparser.Where
	switch stmt := statement.(type) {
	case *sqlparser.Select:
		where = stmt.Where
	case *sqlparser.Update:
		where = stmt.Where
	case *sqlparser.Delete:
		where = stmt.Where
	}
	if where != nil {
		plan.criteria = routingAnalyzeBoolean(where.Expr)
	}
	return plan, nil
}

func routingAnalyzeValues(vals sqlparser.Values) (sqlparser.Values, error) {
	// Analyze first value of every item in the list
	for i := 0; i < len(vals); i++ {
		switch tuple := vals[i].(type) {
		case sqlparser.ValTuple:
			result := routingAnalyzeValue(tuple[0])
			if result != VALUE_NODE {
				return nil, fmt.Errorf("insert is too complex")
			}
		default:
			return nil, fmt.Errorf("insert is too complex")
		}
	}
	return vals, nil
}

func routingAnalyzeBoolean(node sqlparser.BoolExpr) sqlparser.BoolExpr {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left := routingAnalyzeBoolean(node.Left)
		right := routingAnalyzeBoolean(node.Right)
		if left != nil && right != nil {
			return nil
		} else if left != nil {
			return left
		} else {
			return right
		}
	case *sqlparser.ParenBoolExpr:
		return routingAnalyzeBoolean(node.Expr)
	case *sqlparser.ComparisonExpr:
		switch {
		case sqlparser.StringIn(node.Operator, "=", "<", ">", "<=", ">=", "<=>"):
			left := routingAnalyzeValue(node.Left)
			right := routingAnalyzeValue(node.Right)
			if (left == EID_NODE && right == VALUE_NODE) || (left == VALUE_NODE && right == EID_NODE) {
				return node
			}
		case node.Operator == "in":
			left := routingAnalyzeValue(node.Left)
			right := routingAnalyzeValue(node.Right)
			if left == EID_NODE && right == LIST_NODE {
				return node
			}
		}
	case *sqlparser.RangeCond:
		if node.Operator != "between" {
			return nil
		}
		left := routingAnalyzeValue(node.Left)
		from := routingAnalyzeValue(node.From)
		to := routingAnalyzeValue(node.To)
		if left == EID_NODE && from == VALUE_NODE && to == VALUE_NODE {
			return node
		}
	}
	return nil
}

func routingAnalyzeValue(valExpr sqlparser.ValExpr) int {
	switch node := valExpr.(type) {
	case *sqlparser.ColName:
		if string(node.Name) == "entity_id" {
			return EID_NODE
		}
	case sqlparser.ValTuple:
		for _, n := range node {
			if routingAnalyzeValue(n) != VALUE_NODE {
				return OTHER_NODE
			}
		}
		return LIST_NODE
	case sqlparser.StrVal, sqlparser.NumVal, sqlparser.ValArg:
		return VALUE_NODE
	}
	return OTHER_NODE
}

func findShardList(valExpr sqlparser.ValExpr, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) ([]int, error) {
	shardset := make(map[int]bool)
	switch node := valExpr.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			index, err := findShard(n, bindVariables, tabletKeys)
			if err != nil {
				return nil, err
			}
			shardset[index] = true
		}
	}
	shardlist := make([]int, len(shardset))
	index := 0
	for k := range shardset {
		shardlist[index] = k
		index++
	}
	return shardlist, nil
}

func findInsertShard(vals sqlparser.Values, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (int, error) {
	index := -1
	for i := 0; i < len(vals); i++ {
		first_value_expression := vals[i].(sqlparser.ValTuple)[0]
		newIndex, err := findShard(first_value_expression, bindVariables, tabletKeys)
		if err != nil {
			return -1, err
		}
		if index == -1 {
			index = newIndex
		} else if index != newIndex {
			return -1, fmt.Errorf("insert has multiple shard targets")
		}
	}
	return index, nil
}

func findShard(valExpr sqlparser.ValExpr, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (int, error) {
	value, err := getBoundValue(valExpr, bindVariables)
	if err != nil {
		return -1, err
	}
	return key.FindShardForValue(value, tabletKeys), nil
}

func getBoundValue(valExpr sqlparser.ValExpr, bindVariables map[string]interface{}) (string, error) {
	switch node := valExpr.(type) {
	case sqlparser.ValTuple:
		if len(node) != 1 {
			return "", fmt.Errorf("tuples not allowed as insert values")
		}
		// TODO: Change parser to create single value tuples into non-tuples.
		return getBoundValue(node[0], bindVariables)
	case sqlparser.StrVal:
		return string(node), nil
	case sqlparser.NumVal:
		val, err := strconv.ParseInt(string(node), 10, 64)
		if err != nil {
			return "", err
		}
		return key.Uint64Key(val).String(), nil
	case sqlparser.ValArg:
		value, err := findBindValue(node, bindVariables)
		if err != nil {
			return "", err
		}
		return key.EncodeValue(value), nil
	}
	panic("Unexpected token")
}

func findBindValue(valArg sqlparser.ValArg, bindVariables map[string]interface{}) (interface{}, error) {
	if bindVariables == nil {
		return nil, fmt.Errorf("No bind variable for " + string(valArg))
	}
	value, ok := bindVariables[string(valArg[1:])]
	if !ok {
		return nil, fmt.Errorf("No bind variable for " + string(valArg))
	}
	return value, nil
}

func makeList(start, end int) []int {
	list := make([]int, end-start)
	for i := start; i < end; i++ {
		list[i-start] = i
	}
	return list
}
