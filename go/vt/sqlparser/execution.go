// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"fmt"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
)

type PlanType int

const (
	PLAN_PASS_SELECT PlanType = iota
	PLAN_PASS_DML
	PLAN_PK_EQUAL
	PLAN_PK_IN
	PLAN_SELECT_SUBQUERY
	PLAN_DML_PK
	PLAN_DML_SUBQUERY
	PLAN_INSERT_PK
	PLAN_INSERT_SUBQUERY
	PLAN_SET
	PLAN_DDL
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
	"PASS_SELECT",
	"PASS_DML",
	"PK_EQUAL",
	"PK_IN",
	"SELECT_SUBQUERY",
	"DML_PK",
	"DML_SUBQUERY",
	"INSERT_PK",
	"INSERT_SUBQUERY",
	"SET",
	"DDL",
}

func (pt PlanType) String() string {
	if pt < 0 || pt >= NumPlans {
		return ""
	}
	return planName[pt]
}

func PlanByName(s string) (pt PlanType, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanType(i), true
		}
	}
	return NumPlans, false
}

func (pt PlanType) IsSelect() bool {
	return pt == PLAN_PASS_SELECT || pt == PLAN_PK_EQUAL || pt == PLAN_PK_IN || pt == PLAN_SELECT_SUBQUERY
}

func (pt PlanType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", pt.String())), nil
}

type ReasonType int

const (
	REASON_DEFAULT ReasonType = iota
	REASON_SELECT
	REASON_TABLE
	REASON_NOCACHE
	REASON_SELECT_LIST
	REASON_LOCK
	REASON_WHERE
	REASON_ORDER
	REASON_PKINDEX
	REASON_NOINDEX_MATCH
	REASON_TABLE_NOINDEX
	REASON_PK_CHANGE
	REASON_COMPOSITE_PK
	REASON_HAS_HINTS
	REASON_UPSERT
)

// Must exactly match order of reason constants.
var reasonName = []string{
	"DEFAULT",
	"SELECT",
	"TABLE",
	"NOCACHE",
	"SELECT_LIST",
	"LOCK",
	"WHERE",
	"ORDER",
	"PKINDEX",
	"NOINDEX_MATCH",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"COMPOSITE_PK",
	"HAS_HINTS",
	"UPSERT",
}

func (rt ReasonType) String() string {
	return reasonName[rt]
}

func (rt ReasonType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", rt.String())), nil
}

// ExecPlan is built for selects and DMLs.
// PK Values values within ExecPlan can be:
// sqltypes.Value: sourced form the query, or
// string: bind variable name starting with ':', or
// nil if no value was specified
type ExecPlan struct {
	PlanId    PlanType
	Reason    ReasonType
	TableName string

	// DisplayQuery is the displayable version of the
	// original query. Depending on the mode, it may be
	// the original query, or an anonymized version.
	DisplayQuery string

	// FieldQuery is used to fetch field info
	FieldQuery *ParsedQuery

	// FullQuery will be set for all plans.
	FullQuery *ParsedQuery

	// For PK plans, only OuterQuery is set.
	// For SUBQUERY plans, Subquery is also set.
	// IndexUsed is set only for PLAN_SELECT_SUBQUERY
	OuterQuery *ParsedQuery
	Subquery   *ParsedQuery
	IndexUsed  string

	// For selects, columns to be returned
	// For PLAN_INSERT_SUBQUERY, columns to be inserted
	ColumnNumbers []int

	// PLAN_PK_EQUAL, PLAN_DML_PK: where clause values
	// PLAN_PK_IN: IN clause values
	// PLAN_INSERT_PK: values clause
	PKValues []interface{}

	// For update: set clause if pk is changing
	SecondaryPKValues []interface{}

	// For PLAN_INSERT_SUBQUERY: pk columns in the subquery result
	SubqueryPKColumns []int

	// PLAN_SET
	SetKey   string
	SetValue interface{}
}

type DDLPlan struct {
	Action    int
	TableName string
	NewName   string
}

type StreamExecPlan struct {
	DisplayQuery string
	FullQuery    *ParsedQuery
}

type TableGetter func(tableName string) (*schema.Table, bool)

func ExecParse(sql string, getTable TableGetter, sensitiveMode bool) (plan *ExecPlan, err error) {
	defer handleError(&err)

	statement, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	plan = execAnalyzeSql(statement, getTable)
	if plan.PlanId == PLAN_PASS_DML {
		log.Warningf("PASS_DML: %s", sql)
	}
	if sensitiveMode {
		plan.DisplayQuery = GenerateAnonymizedQuery(statement)
	} else {
		plan.DisplayQuery = sql
	}
	return plan, nil
}

func StreamExecParse(sql string, sensitiveMode bool) (plan *StreamExecPlan, err error) {
	defer handleError(&err)

	statement, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	switch stmt := statement.(type) {
	case *Select:
		if stmt.Lock.Type != NO_LOCK {
			return nil, NewParserError("select with lock disallowed with streaming")
		}
	case *Union:
		// pass
	default:
		return nil, NewParserError("'%v' not allowed for streaming", String(stmt))
	}
	plan = &StreamExecPlan{FullQuery: GenerateFullQuery(statement)}

	if sensitiveMode {
		plan.DisplayQuery = GenerateAnonymizedQuery(statement)
	} else {
		plan.DisplayQuery = sql
	}

	return plan, nil
}

func DDLParse(sql string) (plan *DDLPlan) {
	statement, err := Parse(sql)
	if err != nil {
		return &DDLPlan{Action: 0}
	}
	switch stmt := statement.(type) {
	case *DDLSimple:
		return &DDLPlan{
			Action:    stmt.Action,
			TableName: string(stmt.Table.Value),
			NewName:   string(stmt.Table.Value),
		}
	case *Rename:
		return &DDLPlan{
			Action:    RENAME,
			TableName: string(stmt.OldName.Value),
			NewName:   string(stmt.NewName.Value),
		}
	}
	return &DDLPlan{Action: 0}
}

//-----------------------------------------------
// Implementation

func execAnalyzeSql(statement Statement, getTable TableGetter) (plan *ExecPlan) {
	switch stmt := statement.(type) {
	case *Union:
		return &ExecPlan{
			PlanId:     PLAN_PASS_SELECT,
			FieldQuery: GenerateFieldQuery(stmt),
			FullQuery:  GenerateFullQuery(stmt),
			Reason:     REASON_SELECT,
		}
	case *Select:
		return execAnalyzeSelect(stmt, getTable)
	case *Insert:
		return execAnalyzeInsert(stmt, getTable)
	case *Update:
		return execAnalyzeUpdate(stmt, getTable)
	case *Delete:
		return execAnalyzeDelete(stmt, getTable)
	case *Set:
		return execAnalyzeSet(stmt)
	case *DDLSimple, *Rename:
		return &ExecPlan{PlanId: PLAN_DDL}
	}
	panic(NewParserError("invalid SQL"))
}

func execAnalyzeSelect(sel *Select, getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{
		PlanId:     PLAN_PASS_SELECT,
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateSelectLimitQuery(sel),
	}

	// There are bind variables in the SELECT list
	if plan.FieldQuery == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan
	}

	if !execAnalyzeSelectStructure(sel) {
		plan.Reason = REASON_SELECT
		return plan
	}

	// from
	tableName, hasHints := execAnalyzeFrom(sel.From)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan
	}
	tableInfo := plan.setTableInfo(tableName, getTable)

	// Don't improve the plan if the select is locking the row
	if sel.Lock.Type != NO_LOCK {
		plan.Reason = REASON_LOCK
		return plan
	}

	// Further improvements possible only if table is row-cached
	if tableInfo.CacheType == schema.CACHE_NONE || tableInfo.CacheType == schema.CACHE_W {
		plan.Reason = REASON_NOCACHE
		return plan
	}

	// Select expressions
	selects := execAnalyzeSelectExprs(sel.SelectExprs, tableInfo)
	if selects == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan
	}
	plan.ColumnNumbers = selects

	// where
	conditions := sel.Where.execAnalyzeWhere()
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan
	}

	// order
	if sel.OrderBy.Len() != 0 {
		plan.Reason = REASON_ORDER
		return plan
	}

	// This check should never fail because we only cache tables with primary keys.
	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		panic("unexpected")
	}

	// Attempt PK match only if there's no limit clause
	if sel.Limit.Len() == 0 {
		planId, pkValues := getSelectPKValues(conditions, tableInfo.Indexes[0])
		switch planId {
		case PLAN_PK_EQUAL:
			plan.PlanId = PLAN_PK_EQUAL
			plan.OuterQuery = GenerateEqualOuterQuery(sel, tableInfo)
			plan.PKValues = pkValues
			return plan
		case PLAN_PK_IN:
			plan.PlanId = PLAN_PK_IN
			plan.OuterQuery = GenerateInOuterQuery(sel, tableInfo)
			plan.PKValues = pkValues
			return plan
		}
	}

	if len(tableInfo.Indexes[0].Columns) != 1 {
		plan.Reason = REASON_COMPOSITE_PK
		return plan
	}

	// TODO: Analyze hints to improve plan.
	if hasHints {
		plan.Reason = REASON_HAS_HINTS
		return plan
	}

	plan.IndexUsed = getIndexMatch(conditions, tableInfo.Indexes)
	if plan.IndexUsed == "" {
		plan.Reason = REASON_NOINDEX_MATCH
		return plan
	}
	if plan.IndexUsed == "PRIMARY" {
		plan.Reason = REASON_PKINDEX
		return plan
	}
	// TODO: We can further optimize. Change this to pass-through if select list matches all columns in index.
	plan.PlanId = PLAN_SELECT_SUBQUERY
	plan.OuterQuery = GenerateInOuterQuery(sel, tableInfo)
	plan.Subquery = GenerateSelectSubquery(sel, tableInfo, plan.IndexUsed)
	return plan
}

func execAnalyzeInsert(ins *Insert, getTable TableGetter) (plan *ExecPlan) {
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(ins),
	}
	tableName := ins.Table.collectTableName()
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan
	}
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	pkColumnNumbers := getInsertPKColumns(ins.Columns, tableInfo)

	if ins.OnDup.Len() != 0 {
		// Upserts are not safe for statement based replication:
		// http://bugs.mysql.com/bug.php?id=58637
		plan.Reason = REASON_UPSERT
		return plan
	}

	if sel, ok := ins.Values.(SelectStatement); ok {
		plan.PlanId = PLAN_INSERT_SUBQUERY
		plan.OuterQuery = GenerateInsertOuterQuery(ins)
		plan.Subquery = GenerateSelectLimitQuery(sel)
		if len(ins.Columns) != 0 {
			plan.ColumnNumbers = execAnalyzeSelectExprs(SelectExprs(ins.Columns), tableInfo)
		} else {
			// StarExpr node will expand into all columns
			n := SelectExprs{&StarExpr{}}
			plan.ColumnNumbers = execAnalyzeSelectExprs(n, tableInfo)
		}
		plan.SubqueryPKColumns = pkColumnNumbers
		return plan
	}

	// If it's not a SelectStatement, it's a Node.
	rowList := ins.Values.(*Node).NodeAt(0) // VALUES->NODE_LIST
	if pkValues := getInsertPKValues(pkColumnNumbers, rowList, tableInfo); pkValues != nil {
		plan.PlanId = PLAN_INSERT_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
	}
	return plan
}

func execAnalyzeUpdate(upd *Update, getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(upd),
	}

	tableName := upd.Table.collectTableName()
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan
	}
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	var ok bool
	if plan.SecondaryPKValues, ok = upd.List.execAnalyzeUpdateExpressions(tableInfo.Indexes[0]); !ok {
		plan.Reason = REASON_PK_CHANGE
		return plan
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = GenerateUpdateOuterQuery(upd, tableInfo.Indexes[0])
	plan.Subquery = GenerateUpdateSubquery(upd, tableInfo)

	conditions := upd.Where.execAnalyzeWhere()
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

func execAnalyzeDelete(del *Delete, getTable TableGetter) (plan *ExecPlan) {
	// Default plan
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(del),
	}

	tableName := del.Table.collectTableName()
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan
	}
	tableInfo := plan.setTableInfo(tableName, getTable)

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = GenerateDeleteOuterQuery(del, tableInfo.Indexes[0])
	plan.Subquery = GenerateDeleteSubquery(del, tableInfo)

	conditions := del.Where.execAnalyzeWhere()
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

func execAnalyzeSet(set *Set) (plan *ExecPlan) {
	plan = &ExecPlan{
		PlanId:    PLAN_SET,
		FullQuery: GenerateFullQuery(set),
	}
	if set.Updates.Len() > 1 { // Multiple set values
		return
	}
	update_expression := set.Updates.NodeAt(0)              // '='
	plan.SetKey = string(update_expression.NodeAt(0).Value) // ID
	expression := update_expression.NodeAt(1)
	valstr := string(expression.Value)
	if expression.Type == NUMBER {
		if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
			plan.SetValue = ival
		} else if fval, err := strconv.ParseFloat(valstr, 64); err == nil {
			plan.SetValue = fval
		}
	}
	return plan
}

func (node *ExecPlan) setTableInfo(tableName string, getTable TableGetter) *schema.Table {
	tableInfo, ok := getTable(tableName)
	if !ok {
		panic(NewParserError("table %s not found in schema", tableName))
	}
	node.TableName = tableInfo.Name
	return tableInfo
}

//-----------------------------------------------
// Select

func execAnalyzeSelectStructure(sel *Select) bool {
	if sel.Distinct {
		return false
	}
	if sel.GroupBy.Len() > 0 {
		return false
	}
	if sel.Having.Len() > 0 {
		return false
	}
	return true
}

//-----------------------------------------------
// Select Expressions

func execAnalyzeSelectExprs(exprs SelectExprs, table *schema.Table) (selects []int) {
	selects = make([]int, 0, len(exprs))
	for _, expr := range exprs {
		if name := execAnalyzeSelectExpr(expr); name != "" {
			if name == "*" {
				for colIndex := range table.Columns {
					selects = append(selects, colIndex)
				}
			} else if colIndex := table.FindColumn(name); colIndex != -1 {
				selects = append(selects, colIndex)
			} else {
				panic(NewParserError("column %s not found in table %s", name, table.Name))
			}
		} else {
			// Complex expression
			return nil
		}
	}
	return selects
}

func execAnalyzeSelectExpr(expr SelectExpr) string {
	switch expr := expr.(type) {
	case *StarExpr:
		return "*"
	case *NonStarExpr:
		return execGetColumnName(expr.Expr)
	}
	panic("unreachable")
}

func execGetColumnName(node *Node) string {
	switch node.Type {
	case ID:
		return string(node.Value)
	case '.':
		if node.NodeAt(1).Type == ID {
			return string(node.NodeAt(1).Value)
		}
	}
	return ""
}

//-----------------------------------------------
// From

func execAnalyzeFrom(tableExprs TableExprs) (tablename string, hasHints bool) {
	if len(tableExprs) > 1 {
		return "", false
	}
	node := tableExprs[0]
	for node.Type == '(' {
		node = node.NodeAt(0)
	}
	if node.Type != TABLE_EXPR {
		return "", false
	}
	hasHints = (node.NodeAt(2).Len() > 0)
	return node.NodeAt(0).collectTableName(), hasHints
}

func (node *Node) collectTableName() string {
	if node.Type == ID {
		return string(node.Value)
	}
	// sub-select or '.' expression
	return ""
}

//-----------------------------------------------
// Where

func (node *Node) execAnalyzeWhere() (conditions []*Node) {
	if node.Len() == 0 {
		return nil
	}
	return node.NodeAt(0).execAnalyzeBoolean()
}

func (node *Node) execAnalyzeBoolean() (conditions []*Node) {
	switch node.Type {
	case AND:
		left := node.NodeAt(0).execAnalyzeBoolean()
		right := node.NodeAt(1).execAnalyzeBoolean()
		if left == nil || right == nil {
			return nil
		}
		if hasINClause(left) && hasINClause(right) {
			return nil
		}
		return append(left, right...)
	case '(':
		node, ok := node.At(0).(*Node)
		if !ok {
			return nil
		}
		return node.execAnalyzeBoolean()
	case '=', '<', '>', LE, GE, NULL_SAFE_EQUAL, LIKE:
		left := node.NodeAt(0).execAnalyzeID()
		right := node.NodeAt(1).execAnalyzeValue()
		if left == nil || right == nil {
			return nil
		}
		n := NewParseNode(node.Type, node.Value)
		n.PushTwo(left, right)
		return []*Node{n}
	case IN:
		left := node.NodeAt(0).execAnalyzeID()
		right := node.NodeAt(1).execAnalyzeSimpleINList()
		if left == nil || right == nil {
			return nil
		}
		n := NewParseNode(node.Type, node.Value)
		n.PushTwo(left, right)
		return []*Node{n}
	case BETWEEN:
		left := node.NodeAt(0).execAnalyzeID()
		right1 := node.NodeAt(1).execAnalyzeValue()
		right2 := node.NodeAt(2).execAnalyzeValue()
		if left == nil || right1 == nil || right2 == nil {
			return nil
		}
		return []*Node{node}
	}
	return nil
}

func (node *Node) execAnalyzeSimpleINList() *Node {
	list, ok := node.At(0).(*Node) // '('->NODE_LIST
	if !ok {
		// It's a subquery.
		return nil
	}
	for i := 0; i < list.Len(); i++ {
		if n := list.NodeAt(i).execAnalyzeValue(); n == nil {
			return nil
		}
	}
	return node
}

func (node *Node) execAnalyzeID() *Node {
	switch node.Type {
	case ID:
		return node
	case '.':
		return node.NodeAt(1).execAnalyzeID()
	}
	return nil
}

func (node *Node) execAnalyzeValue() *Node {
	switch node.Type {
	case STRING, NUMBER, VALUE_ARG:
		return node
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

//-----------------------------------------------
// Update expressions

func (node *Node) execAnalyzeUpdateExpressions(pkIndex *schema.Index) (pkValues []interface{}, ok bool) {
	for i := 0; i < node.Len(); i++ {
		columnName := string(execGetColumnName(node.NodeAt(i).NodeAt(0)))
		index := pkIndex.FindColumn(columnName)
		if index == -1 {
			continue
		}
		value := node.NodeAt(i).NodeAt(1).execAnalyzeValue()
		if value == nil {
			log.Warningf("expression is too complex %v", node.NodeAt(i).At(0))
			return nil, false
		}
		if pkValues == nil {
			pkValues = make([]interface{}, len(pkIndex.Columns))
		}
		pkValues[index] = asInterface(value)
	}
	return pkValues, true
}

//-----------------------------------------------
// Insert

func getInsertPKColumns(columns Columns, tableInfo *schema.Table) (pkColumnNumbers []int) {
	if len(columns) == 0 {
		return tableInfo.PKColumns
	}
	pkIndex := tableInfo.Indexes[0]
	pkColumnNumbers = make([]int, len(pkIndex.Columns))
	for i := range pkColumnNumbers {
		pkColumnNumbers[i] = -1
	}
	for i, column := range columns {
		index := pkIndex.FindColumn(string(execGetColumnName(column.(*NonStarExpr).Expr)))
		if index == -1 {
			continue
		}
		pkColumnNumbers[index] = i
	}
	return pkColumnNumbers
}

func getInsertPKValues(pkColumnNumbers []int, rowList *Node, tableInfo *schema.Table) (pkValues []interface{}) {
	pkValues = make([]interface{}, len(pkColumnNumbers))
	for index, columnNumber := range pkColumnNumbers {
		if columnNumber == -1 {
			pkValues[index] = tableInfo.GetPKColumn(index).Default
			continue
		}
		values := make([]interface{}, rowList.Len())
		for j := 0; j < rowList.Len(); j++ {
			if columnNumber >= rowList.NodeAt(j).NodeAt(0).Len() { // NODE_LIST->'('->NODE_LIST
				panic(NewParserError("column count doesn't match value count"))
			}
			node := rowList.NodeAt(j).NodeAt(0).NodeAt(columnNumber) // NODE_LIST->'('->NODE_LIST->Value
			value := node.execAnalyzeValue()
			if value == nil {
				log.Warningf("insert is too complex %v", node)
				return nil
			}
			values[j] = asInterface(value)
		}
		if len(values) == 1 {
			pkValues[index] = values[0]
		} else {
			pkValues[index] = values
		}
	}
	return pkValues
}

//-----------------------------------------------
// Index Analysis

type IndexScore struct {
	Index       *schema.Index
	ColumnMatch []bool
	MatchFailed bool
}

type scoreValue int64

const (
	NO_MATCH      = scoreValue(-1)
	PERFECT_SCORE = scoreValue(0)
)

func NewIndexScore(index *schema.Index) *IndexScore {
	return &IndexScore{index, make([]bool, len(index.Columns)), false}
}

func (is *IndexScore) FindMatch(columnName string) int {
	if is.MatchFailed {
		return -1
	}
	if index := is.Index.FindColumn(columnName); index != -1 {
		is.ColumnMatch[index] = true
		return index
	}
	// If the column is among the data columns, we can still use
	// the index without going to the main table
	if index := is.Index.FindDataColumn(columnName); index == -1 {
		is.MatchFailed = true
	}
	return -1
}

func (is *IndexScore) GetScore() scoreValue {
	if is.MatchFailed {
		return NO_MATCH
	}
	score := NO_MATCH
	for i, indexColumn := range is.ColumnMatch {
		if indexColumn {
			score = scoreValue(is.Index.Cardinality[i])
			continue
		}
		return score
	}
	return PERFECT_SCORE
}

func NewIndexScoreList(indexes []*schema.Index) []*IndexScore {
	scoreList := make([]*IndexScore, len(indexes))
	for i, v := range indexes {
		scoreList[i] = NewIndexScore(v)
	}
	return scoreList
}

func getSelectPKValues(conditions []*Node, pkIndex *schema.Index) (planId PlanType, pkValues []interface{}) {
	pkValues = getPKValues(conditions, pkIndex)
	if pkValues == nil {
		return PLAN_PASS_SELECT, nil
	}
	for _, pkValue := range pkValues {
		inList, ok := pkValue.([]interface{})
		if !ok {
			continue
		}
		if len(pkValues) == 1 {
			return PLAN_PK_IN, inList
		}
		return PLAN_PASS_SELECT, nil
	}
	return PLAN_PK_EQUAL, pkValues
}

func getPKValues(conditions []*Node, pkIndex *schema.Index) (pkValues []interface{}) {
	pkIndexScore := NewIndexScore(pkIndex)
	pkValues = make([]interface{}, len(pkIndexScore.ColumnMatch))
	for _, condition := range conditions {
		if condition.Type != '=' && condition.Type != IN {
			return nil
		}
		index := pkIndexScore.FindMatch(string(condition.NodeAt(0).Value))
		if index == -1 {
			return nil
		}
		switch condition.Type {
		case '=':
			pkValues[index] = asInterface(condition.NodeAt(1))
		case IN:
			pkValues[index] = condition.NodeAt(1).NodeAt(0).parseList()
		}
	}
	if pkIndexScore.GetScore() == PERFECT_SCORE {
		return pkValues
	}
	return nil
}

func (node *Node) parseList() (values interface{}) {
	vals := make([]interface{}, node.Len())
	for i := 0; i < node.Len(); i++ {
		vals[i] = asInterface(node.NodeAt(i))
	}
	return vals
}

func getIndexMatch(conditions []*Node, indexes []*schema.Index) string {
	indexScores := NewIndexScoreList(indexes)
	for _, condition := range conditions {
		for _, index := range indexScores {
			index.FindMatch(string(condition.NodeAt(0).Value))
		}
	}
	highScore := NO_MATCH
	highScorer := -1
	for i, index := range indexScores {
		curScore := index.GetScore()
		if curScore == NO_MATCH {
			continue
		}
		if curScore == PERFECT_SCORE {
			highScorer = i
			break
		}
		// Prefer secondary index over primary key
		if curScore >= highScore {
			highScore = curScore
			highScorer = i
		}
	}
	if highScorer == -1 {
		return ""
	}
	return indexes[highScorer].Name
}

//-----------------------------------------------
// Query Generation
func GenerateFullQuery(statement Statement) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.ParsedQuery()
}

func GenerateAnonymizedQuery(statement Statement) string {
	buf := NewTrackedBuffer(AnonymizedFormatter)
	buf.Fprintf("%v", statement)
	return buf.ParsedQuery().Query
}

func GenerateFieldQuery(statement Statement) *ParsedQuery {
	buf := NewTrackedBuffer(FormatImpossible)
	buf.Fprintf("%v", statement)
	if len(buf.bindLocations) != 0 {
		return nil
	}
	return buf.ParsedQuery()
}

// FormatImpossible is a callback function used by TrackedBuffer
// to generate a modified version of the query where all selects
// have impossible where clauses. It overrides a few node types
// and passes the rest down to the default FormatNode.
func FormatImpossible(buf *TrackedBuffer, node SQLNode) {
	switch node := node.(type) {
	case *Select:
		buf.Fprintf("select %v from %v where 1 != 1", node.SelectExprs, node.From)
	case *Node:
		switch node.Type {
		case JOIN, STRAIGHT_JOIN, CROSS, NATURAL:
			// We skip ON clauses (if any)
			buf.Fprintf("%v %s %v", node.At(0), node.Value, node.At(1))
		case LEFT, RIGHT:
			// ON clause is requried
			buf.Fprintf("%v %s %v on 1 != 1", node.At(0), node.Value, node.At(1))
		default:
			node.Format(buf)
		}
	default:
		node.Format(buf)
	}
}

func GenerateSelectLimitQuery(selStmt SelectStatement) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	sel, ok := selStmt.(*Select)
	if ok {
		limit := sel.Limit
		if limit.Len() == 0 {
			limit.PushLimit()
			defer limit.Pop()
		}
	}
	buf.Fprintf("%v", selStmt)
	return buf.ParsedQuery()
}

func GenerateEqualOuterQuery(sel *Select, tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	buf.Fprintf(" from %v where ", sel.From)
	generatePKWhere(buf, tableInfo.Indexes[0])
	return buf.ParsedQuery()
}

func GenerateInOuterQuery(sel *Select, tableInfo *schema.Table) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	// We assume there is one and only one PK column.
	// A '*' argument name means all variables of the list.
	buf.Fprintf(" from %v where %s in (%a)", sel.From, tableInfo.Indexes[0].Columns[0], "*")
	return buf.ParsedQuery()
}

func GenerateInsertOuterQuery(ins *Insert) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("insert %vinto %v%v values %a%v",
		ins.Comments,
		ins.Table,
		ins.Columns,
		"_rowValues",
		ins.OnDup,
	)
	return buf.ParsedQuery()
}

func GenerateUpdateOuterQuery(upd *Update, pkIndex *schema.Index) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("update %v%v set %v where ", upd.Comments, upd.Table, upd.List)
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func GenerateDeleteOuterQuery(del *Delete, pkIndex *schema.Index) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("delete %vfrom %v where ", del.Comments, del.Table)
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func generatePKWhere(buf *TrackedBuffer, pkIndex *schema.Index) {
	for i := 0; i < len(pkIndex.Columns); i++ {
		if i != 0 {
			buf.WriteString(" and ")
		}
		buf.Fprintf("%s = %a", pkIndex.Columns[i], strconv.FormatInt(int64(i), 10))
	}
}

func GenerateSelectSubquery(sel *Select, tableInfo *schema.Table, index string) *ParsedQuery {
	hint := NewSimpleParseNode(USE, "use")
	hint.Push(NewSimpleParseNode(INDEX_LIST, ""))
	hint.NodeAt(0).Push(NewSimpleParseNode(ID, index))
	table_expr := sel.From[0]
	savedHint := table_expr.NodeAt(2)
	table_expr.Sub[2] = hint
	defer func() {
		table_expr.Sub[2] = savedHint
	}()
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		sel.From[0],
		sel.Where,
		sel.OrderBy,
		sel.Limit,
		false,
	)
}

func GenerateUpdateSubquery(upd *Update, tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		upd.Table,
		upd.Where,
		upd.OrderBy,
		upd.Limit,
		true,
	)
}

func GenerateDeleteSubquery(del *Delete, tableInfo *schema.Table) *ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		del.Table,
		del.Where,
		del.OrderBy,
		del.Limit,
		true,
	)
}

func (node *Node) PushLimit() {
	node.Push(NewSimpleParseNode(VALUE_ARG, ":_vtMaxResultSize"))
}

func GenerateSubquery(columns []string, table *Node, where *Node, order *Node, limit *Node, for_update bool) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	if limit.Len() == 0 {
		limit.PushLimit()
		defer limit.Pop()
	}
	fmt.Fprintf(buf, "select ")
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i])
	}
	fmt.Fprintf(buf, "%s", columns[i])
	buf.Fprintf(" from %v%v%v%v", table, where, order, limit)
	if for_update {
		buf.Fprintf(" for update")
	}
	return buf.ParsedQuery()
}

func writeColumnList(buf *TrackedBuffer, columns []schema.TableColumn) {
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i].Name)
	}
	fmt.Fprintf(buf, "%s", columns[i].Name)
}

func asInterface(node *Node) interface{} {
	switch node.Type {
	case VALUE_ARG:
		return string(node.Value)
	case STRING:
		return sqltypes.MakeString(node.Value)
	case NUMBER:
		n, err := sqltypes.BuildNumeric(string(node.Value))
		if err != nil {
			panic(NewParserError("type mismatch: %s", err))
		}
		return n
	}
	panic(NewParserError("unexpected node %v", node))
}
