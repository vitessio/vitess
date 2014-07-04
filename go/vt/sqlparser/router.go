// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"strconv"

	"github.com/youtube/vitess/go/vt/key"
)

const (
	ROUTE_BY_CONDITION = iota
	ROUTE_BY_VALUE
)

const (
	EID_NODE = iota
	VALUE_NODE
	LIST_NODE
	OTHER_NODE
)

type RoutingPlan struct {
	routingType int
	criteria    SQLNode
}

func GetShardList(sql string, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (shardlist []int, err error) {
	defer handleError(&err)

	plan := buildPlan(sql)
	return shardListFromPlan(plan, bindVariables, tabletKeys), nil
}

func buildPlan(sql string) (plan *RoutingPlan) {
	statement, err := Parse(sql)
	if err != nil {
		panic(err)
	}
	return getRoutingPlan(statement)
}

func shardListFromPlan(plan *RoutingPlan, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) (shardList []int) {
	if plan.routingType == ROUTE_BY_VALUE {
		index := findInsertShard(plan.criteria.(Values), bindVariables, tabletKeys)
		return []int{index}
	}

	if plan.criteria == nil {
		return makeList(0, len(tabletKeys))
	}

	switch criteria := plan.criteria.(type) {
	case *ComparisonExpr:
		switch criteria.Operator {
		case "=", "<=>":
			index := findShard(criteria.Right, bindVariables, tabletKeys)
			return []int{index}
		case "<", "<=":
			index := findShard(criteria.Right, bindVariables, tabletKeys)
			return makeList(0, index+1)
		case ">", ">=":
			index := findShard(criteria.Right, bindVariables, tabletKeys)
			return makeList(index, len(tabletKeys))
		case "in":
			return findShardList(criteria.Right, bindVariables, tabletKeys)
		}
	case *RangeCond:
		if criteria.Operator == "between" {
			start := findShard(criteria.From, bindVariables, tabletKeys)
			last := findShard(criteria.To, bindVariables, tabletKeys)
			if last < start {
				start, last = last, start
			}
			return makeList(start, last+1)
		}
	}
	return makeList(0, len(tabletKeys))
}

func getRoutingPlan(statement Statement) (plan *RoutingPlan) {
	plan = &RoutingPlan{}
	if ins, ok := statement.(*Insert); ok {
		if sel, ok := ins.Rows.(SelectStatement); ok {
			return getRoutingPlan(sel)
		}
		plan.routingType = ROUTE_BY_VALUE
		plan.criteria = routingAnalyzeValues(ins.Rows.(Values))
		return plan
	}
	var where *Where
	plan.routingType = ROUTE_BY_CONDITION
	switch stmt := statement.(type) {
	case *Select:
		where = stmt.Where
	case *Update:
		where = stmt.Where
	case *Delete:
		where = stmt.Where
	}
	if where != nil {
		plan.criteria = routingAnalyzeBoolean(where.Expr)
	}
	return plan
}

func routingAnalyzeValues(vals Values) Values {
	// Analyze first value of every item in the list
	for i := 0; i < len(vals); i++ {
		switch tuple := vals[i].(type) {
		case ValTuple:
			result := routingAnalyzeValue(tuple[0])
			if result != VALUE_NODE {
				panic(NewParserError("insert is too complex"))
			}
		default:
			panic(NewParserError("insert is too complex"))
		}
	}
	return vals
}

func routingAnalyzeBoolean(node BoolExpr) BoolExpr {
	switch node := node.(type) {
	case *AndExpr:
		left := routingAnalyzeBoolean(node.Left)
		right := routingAnalyzeBoolean(node.Right)
		if left != nil && right != nil {
			return nil
		} else if left != nil {
			return left
		} else {
			return right
		}
	case *ParenBoolExpr:
		return routingAnalyzeBoolean(node.Expr)
	case *ComparisonExpr:
		switch {
		case stringIn(node.Operator, "=", "<", ">", "<=", ">=", "<=>"):
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
	case *RangeCond:
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

func routingAnalyzeValue(valExpr ValExpr) int {
	switch node := valExpr.(type) {
	case *ColName:
		if string(node.Name) == "entity_id" {
			return EID_NODE
		}
	case ValTuple:
		for _, n := range node {
			if routingAnalyzeValue(n) != VALUE_NODE {
				return OTHER_NODE
			}
		}
		return LIST_NODE
	case StrVal, NumVal, ValArg:
		return VALUE_NODE
	}
	return OTHER_NODE
}

func findShardList(valExpr ValExpr, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) []int {
	shardset := make(map[int]bool)
	switch node := valExpr.(type) {
	case ValTuple:
		for _, n := range node {
			index := findShard(n, bindVariables, tabletKeys)
			shardset[index] = true
		}
	}
	shardlist := make([]int, len(shardset))
	index := 0
	for k := range shardset {
		shardlist[index] = k
		index++
	}
	return shardlist
}

func findInsertShard(vals Values, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) int {
	index := -1
	for i := 0; i < len(vals); i++ {
		first_value_expression := vals[i].(ValTuple)[0]
		newIndex := findShard(first_value_expression, bindVariables, tabletKeys)
		if index == -1 {
			index = newIndex
		} else if index != newIndex {
			panic(NewParserError("insert has multiple shard targets"))
		}
	}
	return index
}

func findShard(valExpr ValExpr, bindVariables map[string]interface{}, tabletKeys []key.KeyspaceId) int {
	value := getBoundValue(valExpr, bindVariables)
	return key.FindShardForValue(value, tabletKeys)
}

func getBoundValue(valExpr ValExpr, bindVariables map[string]interface{}) string {
	switch node := valExpr.(type) {
	case ValTuple:
		if len(node) != 1 {
			panic(NewParserError("tuples not allowed as insert values"))
		}
		// TODO: Change parser to create single value tuples into non-tuples.
		return getBoundValue(node[0], bindVariables)
	case StrVal:
		return string(node)
	case NumVal:
		val, err := strconv.ParseInt(string(node), 10, 64)
		if err != nil {
			panic(NewParserError("%s", err.Error()))
		}
		return key.Uint64Key(val).String()
	case ValArg:
		value := findBindValue(node, bindVariables)
		return key.EncodeValue(value)
	}
	panic("Unexpected token")
}

func findBindValue(valArg ValArg, bindVariables map[string]interface{}) interface{} {
	if bindVariables == nil {
		panic(NewParserError("No bind variable for " + string(valArg)))
	}
	value, ok := bindVariables[string(valArg[1:])]
	if !ok {
		panic(NewParserError("No bind variable for " + string(valArg)))
	}
	return value
}

func makeList(start, end int) []int {
	list := make([]int, end-start)
	for i := start; i < end; i++ {
		list[i-start] = i
	}
	return list
}
