// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"encoding/binary"
	"strconv"
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
	criteria    *Node
}

func GetShardList(sql string, bindVariables map[string]interface{}, tabletKeys []string) (shardlist []int, err error) {
	defer handleError(&err)

	plan := buildPlan(sql)
	return shardListFromPlan(plan, bindVariables, tabletKeys), nil
}

func buildPlan(sql string) (plan *RoutingPlan) {
	tree, err := Parse(sql)
	if err != nil {
		panic(err)
	}
	return tree.getRoutingPlan()
}

func shardListFromPlan(plan *RoutingPlan, bindVariables map[string]interface{}, tabletKeys []string) (shardList []int) {
	if plan.routingType == ROUTE_BY_VALUE {
		index := plan.criteria.findInsertShard(bindVariables, tabletKeys)
		return []int{index}
	}

	if plan.criteria == nil {
		return makeList(0, len(tabletKeys))
	}

	switch plan.criteria.Type {
	case '=', NULL_SAFE_EQUAL:
		index := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		return []int{index}
	case '<', LE:
		index := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		return makeList(0, index+1)
	case '>', GE:
		index := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		return makeList(index, len(tabletKeys))
	case IN:
		return plan.criteria.At(1).findShardList(bindVariables, tabletKeys)
	case BETWEEN:
		start := plan.criteria.At(1).findShard(bindVariables, tabletKeys)
		last := plan.criteria.At(2).findShard(bindVariables, tabletKeys)
		if last < start {
			start, last = last, start
		}
		return makeList(start, last+1)
	}
	return makeList(0, len(tabletKeys))
}

func (self *Node) getRoutingPlan() (plan *RoutingPlan) {
	plan = &RoutingPlan{}
	if self.Type == INSERT {
		if self.At(INSERT_VALUES_OFFSET).Type == VALUES {
			plan.routingType = ROUTE_BY_VALUE
			plan.criteria = self.At(INSERT_VALUES_OFFSET).At(0).routingAnalyzeValues()
			return plan
		} else { // SELECT, let us recurse
			return self.At(INSERT_VALUES_OFFSET).getRoutingPlan()
		}
	}
	var where *Node
	plan.routingType = ROUTE_BY_CONDITION
	switch self.Type {
	case SELECT:
		where = self.At(SELECT_WHERE_OFFSET)
	case UPDATE:
		where = self.At(UPDATE_WHERE_OFFSET)
	case DELETE:
		where = self.At(DELETE_WHERE_OFFSET)
	}
	if where != nil && where.Len() > 0 {
		plan.criteria = where.At(0).routingAnalyzeBoolean()
	}
	return plan
}

func (self *Node) routingAnalyzeValues() *Node {
	// Analyze first value of every item in the list
	for i := 0; i < self.Len(); i++ {
		value_expression_list := self.At(i).At(0)
		result := value_expression_list.At(0).routingAnalyzeValue()
		if result != VALUE_NODE {
			panic(NewParserError("insert is too complex"))
		}
	}
	return self
}

func (self *Node) routingAnalyzeBoolean() *Node {
	switch self.Type {
	case AND:
		left := self.At(0).routingAnalyzeBoolean()
		right := self.At(1).routingAnalyzeBoolean()
		if left != nil && right != nil {
			return nil
		} else if left != nil {
			return left
		} else {
			return right
		}
	case '(':
		return self.At(0).routingAnalyzeBoolean()
	case '=', '<', '>', LE, GE, NULL_SAFE_EQUAL:
		left := self.At(0).routingAnalyzeValue()
		right := self.At(1).routingAnalyzeValue()
		if (left == EID_NODE && right == VALUE_NODE) || (left == VALUE_NODE && right == EID_NODE) {
			return self
		}
	case IN:
		left := self.At(0).routingAnalyzeValue()
		right := self.At(1).routingAnalyzeValue()
		if left == EID_NODE && right == LIST_NODE {
			return self
		}
	case BETWEEN:
		left := self.At(0).routingAnalyzeValue()
		right1 := self.At(1).routingAnalyzeValue()
		right2 := self.At(2).routingAnalyzeValue()
		if left == EID_NODE && right1 == VALUE_NODE && right2 == VALUE_NODE {
			return self
		}
	}
	return nil
}

func (self *Node) routingAnalyzeValue() int {
	switch self.Type {
	case ID:
		if string(self.Value) == "entity_id" {
			return EID_NODE
		}
	case '.':
		return self.At(1).routingAnalyzeValue()
	case '(':
		return self.At(0).routingAnalyzeValue()
	case NODE_LIST:
		for i := 0; i < self.Len(); i++ {
			if self.At(i).routingAnalyzeValue() != VALUE_NODE {
				return OTHER_NODE
			}
		}
		return LIST_NODE
	case STRING, NUMBER, VALUE_ARG:
		return VALUE_NODE
	}
	return OTHER_NODE
}

func (self *Node) findShardList(bindVariables map[string]interface{}, tabletKeys []string) []int {
	shardset := make(map[int]bool)
	switch self.Type {
	case '(':
		return self.At(0).findShardList(bindVariables, tabletKeys)
	case NODE_LIST:
		for i := 0; i < self.Len(); i++ {
			index := self.At(i).findShard(bindVariables, tabletKeys)
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

func (self *Node) findInsertShard(bindVariables map[string]interface{}, tabletKeys []string) int {
	index := -1
	for i := 0; i < self.Len(); i++ {
		first_value_expression := self.At(i).At(0).At(0) // '('->value_expression_list->first_value
		newIndex := first_value_expression.findShard(bindVariables, tabletKeys)
		if index == -1 {
			index = newIndex
		} else if index != newIndex {
			panic(NewParserError("insert has multiple shard targets"))
		}
	}
	return index
}

func (self *Node) findShard(bindVariables map[string]interface{}, tabletKeys []string) int {
	value := self.getBoundValue(bindVariables)
	return findShardForValue(value, tabletKeys)
}

func (self *Node) getBoundValue(bindVariables map[string]interface{}) string {
	switch self.Type {
	case '(':
		return self.At(0).getBoundValue(bindVariables)
	case STRING:
		return string(self.Value)
	case NUMBER:
		val, err := strconv.ParseInt(string(self.Value), 10, 64)
		if err != nil {
			panic(NewParserError("%s", err.Error()))
		}
		return binaryEncode(uint64(val))
	case VALUE_ARG:
		value := self.findBindValue(bindVariables)
		return encodeValue(value)
	}
	panic("Unexpected token")
}

func (self *Node) findBindValue(bindVariables map[string]interface{}) interface{} {
	if bindVariables == nil {
		panic(NewParserError("No bind variable for " + string(self.Value)))
	}
	value, ok := bindVariables[string(self.Value[1:])]
	if !ok {
		panic(NewParserError("No bind variable for " + string(self.Value)))
	}
	return value
}

// this function will not check the value is under the last shard's max
// (as we assume it will be empty, as checked by RebuildKeyspace)
func findShardForValue(value string, tabletKeys []string) int {
	var index int
	for index < len(tabletKeys)-1 && value >= tabletKeys[index] {
		index++
	}
	return index
}

// Given a list of upper bounds for shards, returns the shard index
// for the given value (between 0 and len(tabletKeys)-1)
func FindShardForKey(key interface{}, tabletKeys []string) (i int, err error) {
	defer handleError(&err)
	return findShardForValue(encodeValue(key), tabletKeys), nil
}

func makeList(start, end int) []int {
	list := make([]int, end-start)
	for i := start; i < end; i++ {
		list[i-start] = i
	}
	return list
}

func encodeValue(value interface{}) string {
	switch bindVal := value.(type) {
	case int:
		return binaryEncode(uint64(bindVal))
	case uint64:
		return binaryEncode(bindVal)
	case int64:
		return binaryEncode(uint64(bindVal))
	case string:
		return bindVal
	case []byte:
		return string(bindVal)
	}
	panic(NewParserError("Unexpected bind variable type"))
}

func binaryEncode(val uint64) string {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return string(bytes)
}
