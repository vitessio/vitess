/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package sqlparser

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/schema"
	"fmt"
	"strconv"
)

type PlanType int

const (
	PLAN_PASS_SELECT PlanType = iota
	PLAN_PASS_DML
	PLAN_SELECT_CACHE_RESULT
	PLAN_SELECT_PK
	PLAN_SELECT_SUBQUERY
	PLAN_DML_PK
	PLAN_DML_SUBQUERY
	PLAN_INSERT_PK
	PLAN_INSERT_SUBQUERY
	PLAN_SET
	PLAN_DDL
)

func (self PlanType) IsSelect() bool {
	return self == PLAN_PASS_SELECT || self == PLAN_SELECT_CACHE_RESULT || self == PLAN_SELECT_PK || self == PLAN_SELECT_SUBQUERY
}

const (
	REASON_DEFAULT = iota
	REASON_SELECT
	REASON_TABLE
	REASON_NOCACHE
	REASON_SELECT_LIST
	REASON_FOR_UPDATE
	REASON_WHERE
	REASON_ORDER
	REASON_NOINDEX_MATCH
	REASON_TABLE_NOINDEX
	REASON_PK_CHANGE
)

type ExecPlan struct {
	PlanId    PlanType
	Reason    int
	TableName string

	// PLAN_PASS_*
	FullQuery *ParsedQuery

	// For anything that's not PLAN_PASS_*
	OuterQuery *ParsedQuery
	Subquery   *ParsedQuery

	// For selects, columns to be returned
	// For PLAN_INSERT_SUBQUERY, columns to be inserted
	ColumnNumbers []int

	// PLAN_*_PK
	// For select, update & delete: where clause
	// For insert: values clause
	// For PLAN_INSERT_SUBQUERY: Location of pk values in subquery
	PKValues []interface{}

	// For update: set clause
	// For insert: on duplicate key clause
	SecondaryPKValues []interface{}

	// For PLAN_INSERT_SUBQUERY: pk columns in the subquery result
	SubqueryPKColumns []int

	// PLAN_SET
	SetKey   string
	SetValue interface{}
}

func (self *ExecPlan) Size() int {
	return 1
}

type DDLPlan struct {
	Action    int
	TableName string
	NewName   string
}

type TableGetter func(tableName string) (*schema.Table, bool)

func ExecParse(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	defer handleError(&err)

	tree, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	plan = tree.execAnalyzeSql(getTable)
	if plan.PlanId == PLAN_PASS_DML {
		relog.Warning("PASS_DML: %s", sql)
	}
	return plan, nil
}

func DDLParse(sql string) (plan *DDLPlan) {
	rootNode, err := Parse(sql)
	if err != nil {
		return &DDLPlan{Action: 0}
	}
	switch rootNode.Type {
	case CREATE, ALTER, DROP:
		return &DDLPlan{
			Action:    rootNode.Type,
			TableName: string(rootNode.At(0).Value),
			NewName:   string(rootNode.At(0).Value),
		}
	case RENAME:
		return &DDLPlan{
			Action:    rootNode.Type,
			TableName: string(rootNode.At(0).Value),
			NewName:   string(rootNode.At(1).Value),
		}
	}
	return &DDLPlan{Action: 0}
}

//-----------------------------------------------
// Implementation

func (self *Node) execAnalyzeSql(getTable TableGetter) (plan *ExecPlan) {
	switch self.Type {
	case SELECT, UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT:
		return self.execAnalyzeSelect(getTable)
	case INSERT:
		return self.execAnalyzeInsert(getTable)
	case UPDATE:
		return self.execAnalyzeUpdate(getTable)
	case DELETE:
		return self.execAnalyzeDelete(getTable)
	case SET:
		return self.execAnalyzeSet()
	case CREATE, ALTER, DROP, RENAME:
		return &ExecPlan{PlanId: PLAN_DDL}
	}
	panic(NewParserError("Invalid SQL"))
}

func (self *Node) execAnalyzeSelect(getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{PlanId: PLAN_PASS_SELECT, FullQuery: self.GenerateSelectLimitQuery()}

	if !self.execAnalyzeSelectStructure() {
		plan.Reason = REASON_SELECT
		return plan
	}

	// from
	tableName := self.At(SELECT_FROM_OFFSET).execAnalyzeFrom()
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan
	}
	tableInfo := plan.setTableInfo(tableName, getTable)

	// Don't improve the plan if the select is for update
	if self.At(SELECT_FOR_UPDATE_OFFSET).Type == FOR_UPDATE {
		plan.Reason = REASON_FOR_UPDATE
		return plan
	}

	// Further improvements possible only if table is row-cached
	if tableInfo.CacheType == 0 {
		plan.Reason = REASON_NOCACHE
		return plan
	}

	// Select expressions
	selects := self.At(SELECT_EXPR_OFFSET).execAnalyzeSelectExpressions(tableInfo)
	if selects == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan
	}
	// The plan has improved
	plan.PlanId = PLAN_SELECT_CACHE_RESULT
	plan.ColumnNumbers = selects
	plan.OuterQuery = self.GenerateDefaultQuery(tableInfo)

	// where
	conditions := self.At(SELECT_WHERE_OFFSET).execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}

	if pkValues := getPKValues(conditions, tableInfo.Indexes[0]); pkValues != nil {
		plan.PlanId = PLAN_SELECT_PK
		plan.OuterQuery = self.GenerateSelectOuterQuery(tableInfo)
		plan.PKValues = pkValues
		return plan
	}

	// order
	orders := self.At(SELECT_ORDER_OFFSET).execAnalyzeOrder()
	if orders == nil {
		plan.Reason = REASON_ORDER
		return plan
	}

	if getIndexMatch(conditions, orders, tableInfo.Indexes) > 0 {
		// TODO: We can further optimize. Change this to pass-through if select list matches all columns in index
		plan.PlanId = PLAN_SELECT_SUBQUERY
		plan.OuterQuery = self.GenerateSelectOuterQuery(tableInfo)
		plan.Subquery = self.GenerateSelectSubquery(tableInfo)
		return plan
	}

	plan.Reason = REASON_NOINDEX_MATCH
	return plan
}

func (self *Node) execAnalyzeInsert(getTable TableGetter) (plan *ExecPlan) {
	plan = &ExecPlan{PlanId: PLAN_PASS_DML, FullQuery: self.GenerateFullQuery()}
	tableName := string(self.At(INSERT_TABLE_OFFSET).Value)
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		relog.Warning("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	columns := self.At(INSERT_COLUMN_LIST_OFFSET)
	if columns.Len() == 0 {
		relog.Warning("insert column list not specified for table %s", tableName)
		return plan
	}

	if self.At(INSERT_ON_DUP_OFFSET).Len() != 0 {
		var ok bool
		if plan.SecondaryPKValues, ok = self.At(INSERT_ON_DUP_OFFSET).At(0).execAnalyzeUpdateExpressions(tableInfo.Indexes[0]); !ok {
			plan.Reason = REASON_PK_CHANGE
			return plan
		}
	}

	rowValues := self.At(INSERT_VALUES_OFFSET) // VALUES/SELECT
	if rowValues.Type == SELECT {
		plan.PlanId = PLAN_INSERT_SUBQUERY
		plan.OuterQuery = self.GenerateInsertOuterQuery()
		plan.Subquery = rowValues.GenerateSelectLimitQuery()
		// Column list syntax is a subset of select expressions
		plan.ColumnNumbers = columns.execAnalyzeSelectExpressions(tableInfo)
		plan.SubqueryPKColumns = getPKColumnsFromColumns(columns, tableInfo.Indexes[0])
		return plan
	}

	rowList := rowValues.At(0) // VALUES->NODE_LIST
	if pkValues := getInsertPKValues(columns, rowList, tableInfo.Indexes[0]); pkValues != nil {
		plan.PlanId = PLAN_INSERT_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
	}
	return plan
}

func (self *Node) execAnalyzeUpdate(getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{PlanId: PLAN_PASS_DML, FullQuery: self.GenerateFullQuery()}

	tableName := string(self.At(UPDATE_TABLE_OFFSET).Value)
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		relog.Warning("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	var ok bool
	if plan.SecondaryPKValues, ok = self.At(UPDATE_LIST_OFFSET).execAnalyzeUpdateExpressions(tableInfo.Indexes[0]); !ok {
		plan.Reason = REASON_PK_CHANGE
		return plan
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = self.GenerateUpdateOuterQuery(tableInfo.Indexes[0])
	plan.Subquery = self.GenerateUpdateSubquery(tableInfo)

	conditions := self.At(UPDATE_WHERE_OFFSET).execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}

	if pkValues := getPKValues(conditions, tableInfo.Indexes[0]); pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan
	}

	return plan
}

func (self *Node) execAnalyzeDelete(getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{PlanId: PLAN_PASS_DML, FullQuery: self.GenerateFullQuery()}

	tableName := string(self.At(DELETE_TABLE_OFFSET).Value)
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		relog.Warning("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = self.GenerateDeleteOuterQuery(tableInfo.Indexes[0])
	plan.Subquery = self.GenerateDeleteSubquery(tableInfo)

	conditions := self.At(DELETE_WHERE_OFFSET).execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}

	if pkValues := getPKValues(conditions, tableInfo.Indexes[0]); pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan
	}

	return plan
}

func (self *Node) execAnalyzeSet() (plan *ExecPlan) {
	plan = &ExecPlan{PlanId: PLAN_SET, FullQuery: self.GenerateFullQuery()}
	update_list := self.At(1)  // NODE_LIST
	if update_list.Len() > 1 { // Multiple set values
		return
	}
	update_expression := update_list.At(0)              // '='
	plan.SetKey = string(update_expression.At(0).Value) // ID
	expression := update_expression.At(1)
	if expression.Type == NUMBER {
		if val, err := strconv.ParseFloat(string(expression.Value), 64); err == nil {
			plan.SetValue = val
		}
	}
	return plan
}

func (self *ExecPlan) setTableInfo(tableName string, getTable TableGetter) *schema.Table {
	tableInfo, ok := getTable(tableName)
	if !ok {
		panic(NewParserError("Table %s not found in schema", tableName))
	}
	self.TableName = tableInfo.Name
	return tableInfo
}

//-----------------------------------------------
// Select

func (self *Node) execAnalyzeSelectStructure() bool {
	switch self.Type {
	case UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT:
		return false
	}
	if self.At(SELECT_DISTINCT_OFFSET).Type == DISTINCT {
		return false
	}
	if self.At(SELECT_GROUP_OFFSET).Len() > 0 {
		return false
	}
	if self.At(SELECT_HAVING_OFFSET).Len() > 0 {
		return false
	}
	return true
}

//-----------------------------------------------
// Select Expressions

func (self *Node) execAnalyzeSelectExpressions(table *schema.Table) (selects []int) {
	selects = make([]int, 0, self.Len())
	for i := 0; i < self.Len(); i++ {
		if name := self.At(i).execAnalyzeSelectExpression(); name != "" {
			if name == "*" {
				for colIndex := range table.Columns {
					selects = append(selects, colIndex)
				}
			} else if colIndex := table.FindColumn(name); colIndex != -1 {
				selects = append(selects, colIndex)
			} else {
				panic(NewParserError("Column %s not found in table %s", name, table.Name))
			}
		} else {
			// Complex expression
			return nil
		}
	}
	return selects
}

func (self *Node) execAnalyzeSelectExpression() (name string) {
	switch self.Type {
	case ID, SELECT_STAR:
		return string(self.Value)
	case '.':
		return self.At(1).execAnalyzeSelectExpression()
	case AS:
		return self.At(0).execAnalyzeSelectExpression()
	}
	return ""
}

//-----------------------------------------------
// From

type TableInfo struct {
	Name  string
	Alias string
}

func (self TableInfo) String() string {
	if self.Alias == "" {
		return fmt.Sprintf("%s", self.Name)
	}
	return fmt.Sprintf("%s as %s", self.Name, self.Alias)
}

func (self *Node) execAnalyzeFrom() (tablename string) {
	if self.Len() > 1 {
		return ""
	}

	list := make([]TableInfo, 0, 8)
	list = self.At(0).collectTableName(list)
	if len(list) > 1 {
		return ""
	}

	return list[0].Name
}

func (self *Node) collectTableName(list []TableInfo) []TableInfo {
	switch self.Type {
	case ID:
		list = append(list, TableInfo{string(self.Value), ""})
	case '.':
		list = append(list, TableInfo{string(self.At(1).Value), ""})
	case '(':
		list = append(list, TableInfo{"", ""})
	case JOIN, LEFT, RIGHT, CROSS, NATURAL:
		list = self.At(0).collectTableName(list)
		list = self.At(1).collectTableName(list)
	case AS:
		list = self.At(0).collectTableName(list)
		list[len(list)-1].Alias = string(self.At(1).Value)
	}
	return list
}

//-----------------------------------------------
// Where

func (self *Node) execAnalyzeWhere() (conditions []*Node) {
	if self.Len() == 0 {
		return nil
	}
	return self.At(0).execAnalyzeBoolean()
}

func (self *Node) execAnalyzeBoolean() (conditions []*Node) {
	switch self.Type {
	case AND:
		left := self.At(0).execAnalyzeBoolean()
		right := self.At(1).execAnalyzeBoolean()
		if left == nil || right == nil {
			return nil
		}
		if hasINClause(left) && hasINClause(right) {
			return nil
		}
		return append(left, right...)
	case '(':
		return self.At(0).execAnalyzeBoolean()
	case '=', '<', '>', LE, GE, NULL_SAFE_EQUAL, LIKE:
		left := self.At(0).execAnalyzeID()
		right := self.At(1).execAnalyzeValue()
		if left == nil || right == nil {
			return nil
		}
		node := NewParseNode(self.Type, self.Value)
		node.PushTwo(left, right)
		return []*Node{node}
	case IN:
		return self.execAnalyzeIN()
	case BETWEEN:
		left := self.At(0).execAnalyzeID()
		right1 := self.At(1).execAnalyzeValue()
		right2 := self.At(2).execAnalyzeValue()
		if left == nil || right1 == nil || right2 == nil {
			return nil
		}
		return []*Node{self}
	}
	return nil
}

func (self *Node) execAnalyzeIN() []*Node {
	// simple
	if self.At(0).Type != '(' { // IN->ID
		left := self.At(0).execAnalyzeID()
		right := self.At(1).execAnalyzeSimpleINList() // IN->'('
		if left == nil || right == nil {
			return nil
		}
		node := NewParseNode(self.Type, self.Value)
		node.PushTwo(left, right)
		return []*Node{node}
	}

	// composite
	idList := self.At(0).At(0) // IN->'('->NODE_LIST
	conditions := make([]*Node, idList.Len())
	for i := 0; i < idList.Len(); i++ {
		left := idList.At(i).execAnalyzeID()
		right := self.execBuildINList(i)
		if left == nil || right == nil {
			return nil
		}
		node := NewParseNode(self.Type, self.Value)
		node.PushTwo(left, right)
		conditions[i] = node
	}
	return conditions
}

func (self *Node) execBuildINList(index int) *Node {
	valuesList := self.At(1).At(0) // IN->'('->NODE_LIST
	newList := NewSimpleParseNode(NODE_LIST, "node_list")
	for i := 0; i < valuesList.Len(); i++ {
		if valuesList.At(i).Type != '(' { // NODE_LIST->'('
			return nil
		}
		innerList := valuesList.At(i).At(0) // NODE_LIST->'('->NODE_LIST
		if innerList.Type != NODE_LIST || index >= innerList.Len() {
			return nil
		}
		innerValue := innerList.At(index).execAnalyzeValue()
		if innerValue == nil {
			return nil
		}
		newList.Push(innerValue)
	}
	INList := NewSimpleParseNode('(', "(")
	INList.Push(newList)
	return INList
}

func (self *Node) execAnalyzeSimpleINList() *Node {
	list := self.At(0) // '('->NODE_LIST
	for i := 0; i < list.Len(); i++ {
		if node := list.At(i).execAnalyzeValue(); node == nil {
			return nil
		}
	}
	return self
}

func (self *Node) execAnalyzeID() *Node {
	switch self.Type {
	case ID:
		return self
	case '.':
		return self.At(1).execAnalyzeID()
	}
	return nil
}

func (self *Node) execAnalyzeValue() *Node {
	switch self.Type {
	case STRING, NUMBER, VALUE_ARG:
		return self
	}
	return nil
}

func hasINClause(conditions []*Node) bool {
	for _, node := range conditions {
		if node.Type == IN {
			return true
		}
	}
	return false
}

func (self *Node) parseList() (values interface{}, isList bool) {
	vals := make([]interface{}, self.Len())
	for i := 0; i < self.Len(); i++ {
		vals[i] = asInterface(self.At(i))
	}
	return vals, true
}

//-----------------------------------------------
// Update expressions

func (self *Node) execAnalyzeUpdateExpressions(pkIndex *schema.Index) (pkValues []interface{}, ok bool) {
	for i := 0; i < self.Len(); i++ {
		columnName := string(self.At(i).At(0).Value)
		if index := pkIndex.FindColumn(columnName); index != -1 {
			value := self.At(i).At(1).execAnalyzeValue()
			if value == nil {
				relog.Warning("expression is too complex %v", self.At(i).At(0))
				return nil, false
			}
			if pkValues == nil {
				pkValues = make([]interface{}, len(pkIndex.Columns))
			}
			if pkValues[index] != nil {
				relog.Warning("ambiguous update expression %v", self.At(i).At(0))
				return nil, false
			}
			pkValues[index] = asInterface(value)
		}
	}
	return pkValues, true
}

//-----------------------------------------------
// Order

func (self *Node) execAnalyzeOrder() (orders []*Node) {
	orders = make([]*Node, 0, 8)
	if self.Len() == 0 {
		return orders
	}
	orderList := self.At(0)
	for i := 0; i < orderList.Len(); i++ {
		if order := orderList.At(i).execAnalyzeOrderExpression(); order != nil {
			orders = append(orders, order)
		} else {
			return nil
		}
	}
	return orders
}

func (self *Node) execAnalyzeOrderExpression() (order *Node) {
	switch self.Type {
	case ID:
		return self
	case '.':
		return self.At(1).execAnalyzeOrderExpression()
	case '(', ASC, DESC:
		return self.At(0).execAnalyzeOrderExpression()
	}
	return nil
}

//-----------------------------------------------
// Index Analysis

type IndexScore struct {
	Index       *schema.Index
	ColumnMatch []bool
	MatchFailed bool
}

func NewIndexScore(index *schema.Index) *IndexScore {
	return &IndexScore{index, make([]bool, len(index.Columns)), false}
}

func (self *IndexScore) FindMatch(columnName string) int {
	if self.MatchFailed {
		return -1
	}
	if index := self.Index.FindColumn(columnName); index != -1 {
		self.ColumnMatch[index] = true
		return index
	} else {
		self.MatchFailed = true
	}
	return -1
}

func (self *IndexScore) GetScore() int {
	if self.MatchFailed {
		return 0
	}
	score := 0
	for _, indexColumn := range self.ColumnMatch {
		if indexColumn {
			score++
			continue
		}
		break
	}
	if score == len(self.ColumnMatch) {
		// All columns matched
		return 1000
	}
	return score
}

func NewIndexScoreList(indexes []*schema.Index) []*IndexScore {
	scoreList := make([]*IndexScore, len(indexes))
	for i, v := range indexes {
		scoreList[i] = NewIndexScore(v)
	}
	return scoreList
}

func getPKValues(conditions []*Node, pkIndex *schema.Index) (pkValues []interface{}) {
	if pkIndex.Name != "PRIMARY" {
		relog.Warning("Table has no primary key")
		return nil
	}
	pkIndexScore := NewIndexScore(pkIndex)
	pkValues = make([]interface{}, len(pkIndexScore.ColumnMatch))
	for _, condition := range conditions {
		if condition.Type != '=' && condition.Type != IN {
			return nil
		}
		index := pkIndexScore.FindMatch(string(condition.At(0).Value))
		if index == -1 {
			return nil
		}
		switch condition.Type {
		case '=':
			pkValues[index] = asInterface(condition.At(1))
		case IN:
			pkValues[index], _ = condition.At(1).At(0).parseList()
		}
	}
	if pkIndexScore.GetScore() == 1000 {
		return pkValues
	}
	return nil
}

func getIndexMatch(conditions []*Node, orders []*Node, indexes []*schema.Index) (indexId int) {
	indexScores := NewIndexScoreList(indexes)
	for _, condition := range conditions {
		matchFound := false
		for _, index := range indexScores {
			if index.FindMatch(string(condition.At(0).Value)) != -1 {
				matchFound = true
			}
		}
		if !matchFound {
			return -1
		}
	}
	for _, order := range orders {
		matchFound := false
		for _, index := range indexScores {
			if index.FindMatch(string(order.Value)) != -1 {
				matchFound = true
			}
		}
		if !matchFound {
			return -1
		}
	}
	highScore := 0
	highScorer := -1
	for i, index := range indexScores {
		curScore := index.GetScore()
		if curScore > highScore {
			highScore = curScore
			highScorer = i
		}
	}
	return highScorer
}

func getPKColumnsFromColumns(columns *Node, pkIndex *schema.Index) (columnNumbers []int) {
	columnNumbers = make([]int, len(pkIndex.Columns))
	for i, _ := range columnNumbers {
		columnNumbers[i] = -1
	}
	for i, column := range columns.Sub {
		index := pkIndex.FindColumn(string(column.Value))
		if index == -1 {
			continue
		}
		columnNumbers[index] = i
	}
	return columnNumbers
}

func getInsertPKValues(columns *Node, rowList *Node, pkIndex *schema.Index) (pkValues []interface{}) {
	for i := 0; i < rowList.Len(); i++ {
		if columns.Len() != rowList.At(i).At(0).Len() { // NODE_LIST->'('->NODE_LIST
			panic(NewParserError("number of columns does not match number of values"))
		}
	}

	pkValues = make([]interface{}, len(pkIndex.Columns))
	for i := 0; i < columns.Len(); i++ {
		index := pkIndex.FindColumn(string(columns.At(i).Value))
		if index == -1 {
			continue
		}
		if rowList.Len() == 1 { // simple
			node := rowList.At(0).At(0).At(i) // NODE_LIST->'('->NODE_LIST->Value
			value := node.execAnalyzeValue()
			if value == nil {
				relog.Warning("insert is too complex %v", node)
				return nil
			}
			pkValues[index] = asInterface(value)
		} else { // composite
			values := make([]interface{}, rowList.Len())
			for j := 0; j < rowList.Len(); j++ {
				node := rowList.At(j).At(0).At(i) // NODE_LIST->'('->NODE_LIST->Value
				value := node.execAnalyzeValue()
				if value == nil {
					relog.Warning("insert is too complex %v", node)
					return nil
				}
				values[j] = asInterface(value)
			}
			pkValues[index] = values
		}
	}
	return pkValues
}

//-----------------------------------------------
// Query Generation

func (self *Node) GenerateFullQuery() *ParsedQuery {
	buf := NewTrackedBuffer()
	self.Format(buf)
	return NewParsedQuery(buf)
}

func (self *Node) GenerateSelectLimitQuery() *ParsedQuery {
	buf := NewTrackedBuffer()
	if self.Type == SELECT {
		limit := self.At(SELECT_LIMIT_OFFSET)
		if limit.Len() == 0 {
			limit.PushLimit()
			defer limit.Pop()
		}
	}
	self.Format(buf)
	return NewParsedQuery(buf)
}

func (self *Node) GenerateDefaultQuery(tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer()
	limit := self.At(SELECT_LIMIT_OFFSET)
	if limit.Len() == 0 {
		limit.PushLimit()
		defer limit.Pop()
	}
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	Fprintf(buf, " from %v%v%v%v",
		self.At(SELECT_FROM_OFFSET),
		self.At(SELECT_WHERE_OFFSET),
		self.At(SELECT_ORDER_OFFSET),
		limit)
	return NewParsedQuery(buf)
}

func (self *Node) GenerateSelectOuterQuery(tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer()
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	Fprintf(buf, " from %v where ", self.At(SELECT_FROM_OFFSET))
	generatePKWhere(buf, tableInfo.Indexes[0])
	return NewParsedQuery(buf)
}

func (self *Node) GenerateInsertOuterQuery() *ParsedQuery {
	buf := NewTrackedBuffer()
	Fprintf(buf, "insert %vinto %v%v values ",
		self.At(INSERT_COMMENT_OFFSET), self.At(INSERT_TABLE_OFFSET), self.At(INSERT_COLUMN_LIST_OFFSET))
	writeArg(buf, "_rowValues")
	Fprintf(buf, "%v", self.At(INSERT_ON_DUP_OFFSET))
	return NewParsedQuery(buf)
}

func (self *Node) GenerateUpdateOuterQuery(pkIndex *schema.Index) *ParsedQuery {
	buf := NewTrackedBuffer()
	Fprintf(buf, "update %v%v set %v where ",
		self.At(UPDATE_COMMENT_OFFSET), self.At(UPDATE_TABLE_OFFSET), self.At(UPDATE_LIST_OFFSET))
	generatePKWhere(buf, pkIndex)
	return NewParsedQuery(buf)
}

func (self *Node) GenerateDeleteOuterQuery(pkIndex *schema.Index) *ParsedQuery {
	buf := NewTrackedBuffer()
	Fprintf(buf, "delete %vfrom %v where ", self.At(DELETE_COMMENT_OFFSET), self.At(DELETE_TABLE_OFFSET))
	generatePKWhere(buf, pkIndex)
	return NewParsedQuery(buf)
}

func generatePKWhere(buf *TrackedBuffer, pkIndex *schema.Index) {
	for i := 0; i < len(pkIndex.Columns); i++ {
		if i != 0 {
			buf.WriteString(" and ")
		}
		buf.WriteString(pkIndex.Columns[i])
		buf.WriteString(" = ")
		writeArg(buf, strconv.FormatInt(int64(i), 10))
	}
}

func writeArg(buf *TrackedBuffer, arg string) {
	start := buf.Len()
	buf.WriteString(":")
	buf.WriteString(arg)
	end := buf.Len()
	buf.bind_locations = append(buf.bind_locations, BindLocation{start, end - start})
}

func (self *Node) GenerateSelectSubquery(tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		self.At(SELECT_FROM_OFFSET),
		self.At(SELECT_WHERE_OFFSET),
		self.At(SELECT_ORDER_OFFSET),
		self.At(SELECT_LIMIT_OFFSET),
		false,
	)
}

func (self *Node) GenerateUpdateSubquery(tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		self.At(UPDATE_TABLE_OFFSET),
		self.At(UPDATE_WHERE_OFFSET),
		self.At(UPDATE_ORDER_OFFSET),
		self.At(UPDATE_LIMIT_OFFSET),
		true,
	)
}

func (self *Node) GenerateDeleteSubquery(tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		self.At(DELETE_TABLE_OFFSET),
		self.At(DELETE_WHERE_OFFSET),
		self.At(DELETE_ORDER_OFFSET),
		self.At(DELETE_LIMIT_OFFSET),
		true,
	)
}

func (self *Node) PushLimit() {
	self.Push(NewSimpleParseNode(VALUE_ARG, ":_vtMaxResultSize"))
}

func GenerateSubquery(columns []string, table *Node, where *Node, order *Node, limit *Node, for_update bool) *ParsedQuery {
	buf := NewTrackedBuffer()
	if limit.Len() == 0 {
		limit.PushLimit()
		defer limit.Pop()
	}
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, columns)
	Fprintf(buf, " from %v%v%v%v", table, where, order, limit)
	if for_update {
		Fprintf(buf, " for update")
	}
	return NewParsedQuery(buf)
}

func writeColumnList(buf *TrackedBuffer, columns []string) {
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i])
	}
	fmt.Fprintf(buf, "%s", columns[i])
}

func asInterface(node *Node) interface{} {
	switch node.Type {
	case STRING, VALUE_ARG:
		return string(node.Value)
	case NUMBER:
		return tonumber(node.Value)
	}
	panic(NewParserError("Unexpected node %v", node))
}

// duplicated in vt/tabletserver/codex.go
func tonumber(val []byte) (number interface{}) {
	var err error
	if val[0] == '-' {
		number, err = strconv.ParseInt(string(val), 0, 64)
	} else {
		number, err = strconv.ParseUint(string(val), 0, 64)
	}
	if err != nil {
		panic(NewParserError("%s", err))
	}
	return number
}
