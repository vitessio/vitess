// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
)

var (
	TooComplex = errors.New("Complex")
	execLimit  = &sqlparser.Limit{Rowcount: sqlparser.ValArg(":_vtMaxResultSize")}
)

type PlanType int

const (
	// PLAN_PASS_SELECT is pass through select statements. This is the
	// default plan for select statements.
	PLAN_PASS_SELECT PlanType = iota
	// PLAN_PASS_DML is pass through update & delete statements. This is
	// the default plan for update and delete statements.
	PLAN_PASS_DML
	// PLAN_PK_EQUAL is select statement which has an equality where clause
	// on primary key
	PLAN_PK_EQUAL
	// PLAN_PK_IN is select statement with a single IN clause on primary key
	PLAN_PK_IN
	// PLAN_SELECT_SUBQUERY is select statement with a subselect statement
	PLAN_SELECT_SUBQUERY
	// PLAN_DML_PK is an update or delete with an equality where clause(s)
	// on primary key(s)
	PLAN_DML_PK
	// PLAN_DML_SUBQUERY is an update or delete with a subselect statement
	PLAN_DML_SUBQUERY
	// PLAN_INSERT_PK is insert statement where the PK value is
	// supplied with the query
	PLAN_INSERT_PK
	// PLAN_INSERT_SUBQUERY is same as PLAN_DML_SUBQUERY but for inserts
	PLAN_INSERT_SUBQUERY
	// PLAN_SET is for SET statements
	PLAN_SET
	// PLAN_DDL is for DDL statements
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

// MinRole is the minimum Role required to execute this PlanType
func (pt PlanType) MinRole() tableacl.Role {
	return tableAclRoles[pt]
}

var tableAclRoles = map[PlanType]tableacl.Role{
	PLAN_PASS_SELECT:     tableacl.READER,
	PLAN_PK_EQUAL:        tableacl.READER,
	PLAN_PK_IN:           tableacl.READER,
	PLAN_SELECT_SUBQUERY: tableacl.READER,
	PLAN_SET:             tableacl.READER,
	PLAN_PASS_DML:        tableacl.WRITER,
	PLAN_DML_PK:          tableacl.WRITER,
	PLAN_DML_SUBQUERY:    tableacl.WRITER,
	PLAN_INSERT_PK:       tableacl.WRITER,
	PLAN_INSERT_SUBQUERY: tableacl.WRITER,
	PLAN_DDL:             tableacl.ADMIN,
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

	// FieldQuery is used to fetch field info
	FieldQuery *sqlparser.ParsedQuery

	// FullQuery will be set for all plans.
	FullQuery *sqlparser.ParsedQuery

	// For PK plans, only OuterQuery is set.
	// For SUBQUERY plans, Subquery is also set.
	// IndexUsed is set only for PLAN_SELECT_SUBQUERY
	OuterQuery *sqlparser.ParsedQuery
	Subquery   *sqlparser.ParsedQuery
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
	Action    string
	TableName string
	NewName   string
}

type TableGetter func(tableName string) (*schema.Table, bool)

func ExecParse(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	plan, err = execAnalyzeSql(statement, getTable)
	if err != nil {
		return nil, err
	}
	if plan.PlanId == PLAN_PASS_DML {
		log.Warningf("PASS_DML: %s", sql)
	}
	return plan, nil
}

func StreamExecParse(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plan = &ExecPlan{
		PlanId:    PLAN_PASS_SELECT,
		FullQuery: GenerateFullQuery(statement),
	}

	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if stmt.Lock != "" {
			return nil, errors.New("select with lock disallowed with streaming")
		}
		tableName, _ := execAnalyzeFrom(stmt.From)
		if tableName != "" {
			plan.setTableInfo(tableName, getTable)
		}

	case *sqlparser.Union:
		// pass
	default:
		return nil, fmt.Errorf("'%v' not allowed for streaming", sqlparser.String(stmt))
	}

	return plan, nil
}

func DDLParse(sql string) (plan *DDLPlan) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return &DDLPlan{Action: ""}
	}
	stmt, ok := statement.(*sqlparser.DDL)
	if !ok {
		return &DDLPlan{Action: ""}
	}
	return &DDLPlan{
		Action:    stmt.Action,
		TableName: string(stmt.Table),
		NewName:   string(stmt.NewName),
	}
}

//-----------------------------------------------
// Implementation

func execAnalyzeSql(statement sqlparser.Statement, getTable TableGetter) (plan *ExecPlan, err error) {
	switch stmt := statement.(type) {
	case *sqlparser.Union:
		return &ExecPlan{
			PlanId:     PLAN_PASS_SELECT,
			FieldQuery: GenerateFieldQuery(stmt),
			FullQuery:  GenerateFullQuery(stmt),
			Reason:     REASON_SELECT,
		}, nil
	case *sqlparser.Select:
		return execAnalyzeSelect(stmt, getTable)
	case *sqlparser.Insert:
		return execAnalyzeInsert(stmt, getTable)
	case *sqlparser.Update:
		return execAnalyzeUpdate(stmt, getTable)
	case *sqlparser.Delete:
		return execAnalyzeDelete(stmt, getTable)
	case *sqlparser.Set:
		return execAnalyzeSet(stmt), nil
	case *sqlparser.DDL:
		return execAnalyzeDDL(stmt, getTable), nil
	}
	return nil, errors.New("invalid SQL")
}

func execAnalyzeSelect(sel *sqlparser.Select, getTable TableGetter) (plan *ExecPlan, err error) {
	// Default plan
	plan = &ExecPlan{
		PlanId:     PLAN_PASS_SELECT,
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateSelectLimitQuery(sel),
	}

	// There are bind variables in the SELECT list
	if plan.FieldQuery == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan, nil
	}

	if sel.Distinct != "" || sel.GroupBy != nil || sel.Having != nil {
		plan.Reason = REASON_SELECT
		return plan, nil
	}

	// from
	tableName, hasHints := execAnalyzeFrom(sel.From)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	// Don't improve the plan if the select is locking the row
	if sel.Lock != "" {
		plan.Reason = REASON_LOCK
		return plan, nil
	}

	// Further improvements possible only if table is row-cached
	if tableInfo.CacheType == schema.CACHE_NONE || tableInfo.CacheType == schema.CACHE_W {
		plan.Reason = REASON_NOCACHE
		return plan, nil
	}

	// Select expressions
	selects, err := execAnalyzeSelectExprs(sel.SelectExprs, tableInfo)
	if err != nil {
		return nil, err
	}
	if selects == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan, nil
	}
	plan.ColumnNumbers = selects

	// where
	conditions := execAnalyzeWhere(sel.Where)
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan, nil
	}

	// order
	if sel.OrderBy != nil {
		plan.Reason = REASON_ORDER
		return plan, nil
	}

	// This check should never fail because we only cache tables with primary keys.
	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		panic("unexpected")
	}

	// Attempt PK match only if there's no limit clause
	if sel.Limit == nil {
		planId, pkValues, err := getSelectPKValues(conditions, tableInfo.Indexes[0])
		if err != nil {
			return nil, err
		}
		switch planId {
		case PLAN_PK_EQUAL:
			plan.PlanId = PLAN_PK_EQUAL
			plan.OuterQuery = GenerateEqualOuterQuery(sel, tableInfo)
			plan.PKValues = pkValues
			return plan, nil
		case PLAN_PK_IN:
			plan.PlanId = PLAN_PK_IN
			plan.OuterQuery = GenerateInOuterQuery(sel, tableInfo)
			plan.PKValues = pkValues
			return plan, nil
		}
	}

	if len(tableInfo.Indexes[0].Columns) != 1 {
		plan.Reason = REASON_COMPOSITE_PK
		return plan, nil
	}

	// TODO: Analyze hints to improve plan.
	if hasHints {
		plan.Reason = REASON_HAS_HINTS
		return plan, nil
	}

	plan.IndexUsed = getIndexMatch(conditions, tableInfo.Indexes)
	if plan.IndexUsed == "" {
		plan.Reason = REASON_NOINDEX_MATCH
		return plan, nil
	}
	if plan.IndexUsed == "PRIMARY" {
		plan.Reason = REASON_PKINDEX
		return plan, nil
	}
	// TODO: We can further optimize. Change this to pass-through if select list matches all columns in index.
	plan.PlanId = PLAN_SELECT_SUBQUERY
	plan.OuterQuery = GenerateInOuterQuery(sel, tableInfo)
	plan.Subquery = GenerateSelectSubquery(sel, tableInfo, plan.IndexUsed)
	return plan, nil
}

func execAnalyzeInsert(ins *sqlparser.Insert, getTable TableGetter) (plan *ExecPlan, err error) {
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(ins),
	}
	tableName := sqlparser.GetTableName(ins.Table)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan, nil
	}

	pkColumnNumbers := getInsertPKColumns(ins.Columns, tableInfo)

	if ins.OnDup != nil {
		// Upserts are not safe for statement based replication:
		// http://bugs.mysql.com/bug.php?id=58637
		plan.Reason = REASON_UPSERT
		return plan, nil
	}

	if sel, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		plan.PlanId = PLAN_INSERT_SUBQUERY
		plan.OuterQuery = GenerateInsertOuterQuery(ins)
		plan.Subquery = GenerateSelectLimitQuery(sel)
		if len(ins.Columns) != 0 {
			plan.ColumnNumbers, err = execAnalyzeSelectExprs(sqlparser.SelectExprs(ins.Columns), tableInfo)
			if err != nil {
				return nil, err
			}
		} else {
			// StarExpr node will expand into all columns
			n := sqlparser.SelectExprs{&sqlparser.StarExpr{}}
			plan.ColumnNumbers, err = execAnalyzeSelectExprs(n, tableInfo)
			if err != nil {
				return nil, err
			}
		}
		plan.SubqueryPKColumns = pkColumnNumbers
		return plan, nil
	}

	// If it's not a sqlparser.SelectStatement, it's Values.
	rowList := ins.Rows.(sqlparser.Values)
	pkValues, err := getInsertPKValues(pkColumnNumbers, rowList, tableInfo)
	if err != nil {
		return nil, err
	}
	if pkValues != nil {
		plan.PlanId = PLAN_INSERT_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
	}
	return plan, nil
}

func execAnalyzeUpdate(upd *sqlparser.Update, getTable TableGetter) (plan *ExecPlan, err error) {
	// Default plan
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(upd),
	}

	tableName := sqlparser.GetTableName(upd.Table)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan, nil
	}

	plan.SecondaryPKValues, err = execAnalyzeUpdateExpressions(upd.Exprs, tableInfo.Indexes[0])
	if err != nil {
		if err == TooComplex {
			plan.Reason = REASON_PK_CHANGE
			return plan, nil
		}
		return nil, err
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = GenerateUpdateOuterQuery(upd, tableInfo.Indexes[0])
	plan.Subquery = GenerateUpdateSubquery(upd, tableInfo)

	conditions := execAnalyzeWhere(upd.Where)
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan, nil
	}

	pkValues, err := getPKValues(conditions, tableInfo.Indexes[0])
	if err != nil {
		return nil, err
	}
	if pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan, nil
	}

	return plan, nil
}

func execAnalyzeDelete(del *sqlparser.Delete, getTable TableGetter) (plan *ExecPlan, err error) {
	// Default plan
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(del),
	}

	tableName := sqlparser.GetTableName(del.Table)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan, nil
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = GenerateDeleteOuterQuery(del, tableInfo.Indexes[0])
	plan.Subquery = GenerateDeleteSubquery(del, tableInfo)

	conditions := execAnalyzeWhere(del.Where)
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan, nil
	}

	pkValues, err := getPKValues(conditions, tableInfo.Indexes[0])
	if err != nil {
		return nil, err
	}
	if pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan, nil
	}

	return plan, nil
}

func execAnalyzeSet(set *sqlparser.Set) (plan *ExecPlan) {
	plan = &ExecPlan{
		PlanId:    PLAN_SET,
		FullQuery: GenerateFullQuery(set),
	}
	if len(set.Exprs) > 1 { // Multiple set values
		return plan
	}
	update_expression := set.Exprs[0]
	plan.SetKey = string(update_expression.Name.Name)
	numExpr, ok := update_expression.Expr.(sqlparser.NumVal)
	if !ok {
		return plan
	}
	val := string(numExpr)
	if ival, err := strconv.ParseInt(val, 0, 64); err == nil {
		plan.SetValue = ival
	} else if fval, err := strconv.ParseFloat(val, 64); err == nil {
		plan.SetValue = fval
	}
	return plan
}

func execAnalyzeDDL(ddl *sqlparser.DDL, getTable TableGetter) *ExecPlan {
	plan := &ExecPlan{PlanId: PLAN_DDL}
	tableName := string(ddl.Table)
	// Skip TableName if table is empty (create statements) or not found in schema
	if tableName != "" {
		tableInfo, ok := getTable(tableName)
		if ok {
			plan.TableName = tableInfo.Name
		}
	}
	return plan
}

func (node *ExecPlan) setTableInfo(tableName string, getTable TableGetter) (*schema.Table, error) {
	tableInfo, ok := getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found in schema", tableName)
	}
	node.TableName = tableInfo.Name
	return tableInfo, nil
}

//-----------------------------------------------
// Select Expressions

func execAnalyzeSelectExprs(exprs sqlparser.SelectExprs, table *schema.Table) (selects []int, err error) {
	selects = make([]int, 0, len(exprs))
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.StarExpr:
			// Append all columns.
			for colIndex := range table.Columns {
				selects = append(selects, colIndex)
			}
		case *sqlparser.NonStarExpr:
			name := sqlparser.GetColName(expr.Expr)
			if name == "" {
				// Not a simple column name.
				return nil, nil
			}
			colIndex := table.FindColumn(name)
			if colIndex == -1 {
				return nil, fmt.Errorf("column %s not found in table %s", name, table.Name)
			}
			selects = append(selects, colIndex)
		default:
			panic("unreachable")
		}
	}
	return selects, nil
}

//-----------------------------------------------
// From

func execAnalyzeFrom(tableExprs sqlparser.TableExprs) (tablename string, hasHints bool) {
	if len(tableExprs) > 1 {
		return "", false
	}
	node, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", false
	}
	return sqlparser.GetTableName(node.Expr), node.Hints != nil
}

//-----------------------------------------------
// Where

func execAnalyzeWhere(node *sqlparser.Where) (conditions []sqlparser.BoolExpr) {
	if node == nil {
		return nil
	}
	return execAnalyzeBoolean(node.Expr)
}

func execAnalyzeBoolean(node sqlparser.BoolExpr) (conditions []sqlparser.BoolExpr) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left := execAnalyzeBoolean(node.Left)
		right := execAnalyzeBoolean(node.Right)
		if left == nil || right == nil {
			return nil
		}
		if sqlparser.HasINClause(left) && sqlparser.HasINClause(right) {
			return nil
		}
		return append(left, right...)
	case *sqlparser.ParenBoolExpr:
		return execAnalyzeBoolean(node.Expr)
	case *sqlparser.ComparisonExpr:
		switch {
		case sqlparser.StringIn(
			node.Operator,
			sqlparser.AST_EQ,
			sqlparser.AST_LT,
			sqlparser.AST_GT,
			sqlparser.AST_LE,
			sqlparser.AST_GE,
			sqlparser.AST_NSE,
			sqlparser.AST_LIKE):
			if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.Right) {
				return []sqlparser.BoolExpr{node}
			}
		case node.Operator == sqlparser.AST_IN:
			if sqlparser.IsColName(node.Left) && sqlparser.IsSimpleTuple(node.Right) {
				return []sqlparser.BoolExpr{node}
			}
		}
	case *sqlparser.RangeCond:
		if node.Operator != sqlparser.AST_BETWEEN {
			return nil
		}
		if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.From) && sqlparser.IsValue(node.To) {
			return []sqlparser.BoolExpr{node}
		}
	}
	return nil
}

//-----------------------------------------------
// Update expressions

func execAnalyzeUpdateExpressions(exprs sqlparser.UpdateExprs, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	for _, expr := range exprs {
		index := pkIndex.FindColumn(sqlparser.GetColName(expr.Name))
		if index == -1 {
			continue
		}
		if !sqlparser.IsValue(expr.Expr) {
			log.Warningf("expression is too complex %v", expr)
			return nil, TooComplex
		}
		if pkValues == nil {
			pkValues = make([]interface{}, len(pkIndex.Columns))
		}
		var err error
		pkValues[index], err = sqlparser.AsInterface(expr.Expr)
		if err != nil {
			return nil, err
		}
	}
	return pkValues, nil
}

//-----------------------------------------------
// Insert

func getInsertPKColumns(columns sqlparser.Columns, tableInfo *schema.Table) (pkColumnNumbers []int) {
	if len(columns) == 0 {
		return tableInfo.PKColumns
	}
	pkIndex := tableInfo.Indexes[0]
	pkColumnNumbers = make([]int, len(pkIndex.Columns))
	for i := range pkColumnNumbers {
		pkColumnNumbers[i] = -1
	}
	for i, column := range columns {
		index := pkIndex.FindColumn(sqlparser.GetColName(column.(*sqlparser.NonStarExpr).Expr))
		if index == -1 {
			continue
		}
		pkColumnNumbers[index] = i
	}
	return pkColumnNumbers
}

func getInsertPKValues(pkColumnNumbers []int, rowList sqlparser.Values, tableInfo *schema.Table) (pkValues []interface{}, err error) {
	pkValues = make([]interface{}, len(pkColumnNumbers))
	for index, columnNumber := range pkColumnNumbers {
		if columnNumber == -1 {
			pkValues[index] = tableInfo.GetPKColumn(index).Default
			continue
		}
		values := make([]interface{}, len(rowList))
		for j := 0; j < len(rowList); j++ {
			if _, ok := rowList[j].(*sqlparser.Subquery); ok {
				return nil, errors.New("row subquery not supported for inserts")
			}
			row := rowList[j].(sqlparser.ValTuple)
			if columnNumber >= len(row) {
				return nil, errors.New("column count doesn't match value count")
			}
			node := row[columnNumber]
			if !sqlparser.IsValue(node) {
				log.Warningf("insert is too complex %v", node)
				return nil, nil
			}
			var err error
			values[j], err = sqlparser.AsInterface(node)
			if err != nil {
				return nil, err
			}
		}
		if len(values) == 1 {
			pkValues[index] = values[0]
		} else {
			pkValues[index] = values
		}
	}
	return pkValues, nil
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

func getSelectPKValues(conditions []sqlparser.BoolExpr, pkIndex *schema.Index) (planId PlanType, pkValues []interface{}, err error) {
	pkValues, err = getPKValues(conditions, pkIndex)
	if err != nil {
		return 0, nil, err
	}
	if pkValues == nil {
		return PLAN_PASS_SELECT, nil, nil
	}
	for _, pkValue := range pkValues {
		inList, ok := pkValue.([]interface{})
		if !ok {
			continue
		}
		if len(pkValues) == 1 {
			return PLAN_PK_IN, inList, nil
		}
		return PLAN_PASS_SELECT, nil, nil
	}
	return PLAN_PK_EQUAL, pkValues, nil
}

func getPKValues(conditions []sqlparser.BoolExpr, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	pkIndexScore := NewIndexScore(pkIndex)
	pkValues = make([]interface{}, len(pkIndexScore.ColumnMatch))
	for _, condition := range conditions {
		condition, ok := condition.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, nil
		}
		if !sqlparser.StringIn(condition.Operator, sqlparser.AST_EQ, sqlparser.AST_IN) {
			return nil, nil
		}
		index := pkIndexScore.FindMatch(string(condition.Left.(*sqlparser.ColName).Name))
		if index == -1 {
			return nil, nil
		}
		switch condition.Operator {
		case sqlparser.AST_EQ, sqlparser.AST_IN:
			var err error
			pkValues[index], err = sqlparser.AsInterface(condition.Right)
			if err != nil {
				return nil, err
			}
		default:
			panic("unreachable")
		}
	}
	if pkIndexScore.GetScore() == PERFECT_SCORE {
		return pkValues, nil
	}
	return nil, nil
}

func getIndexMatch(conditions []sqlparser.BoolExpr, indexes []*schema.Index) string {
	indexScores := NewIndexScoreList(indexes)
	for _, condition := range conditions {
		var col string
		switch condition := condition.(type) {
		case *sqlparser.ComparisonExpr:
			col = string(condition.Left.(*sqlparser.ColName).Name)
		case *sqlparser.RangeCond:
			col = string(condition.Left.(*sqlparser.ColName).Name)
		default:
			panic("unreachaable")
		}
		for _, index := range indexScores {
			index.FindMatch(col)
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
func GenerateFullQuery(statement sqlparser.Statement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.ParsedQuery()
}

func GenerateFieldQuery(statement sqlparser.Statement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(FormatImpossible)
	buf.Fprintf("%v", statement)
	if buf.HasBindVars() {
		return nil
	}
	return buf.ParsedQuery()
}

// FormatImpossible is a callback function used by TrackedBuffer
// to generate a modified version of the query where all selects
// have impossible where clauses. It overrides a few node types
// and passes the rest down to the default FormatNode.
func FormatImpossible(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case *sqlparser.Select:
		buf.Fprintf("select %v from %v where 1 != 1", node.SelectExprs, node.From)
	case *sqlparser.JoinTableExpr:
		if node.Join == sqlparser.AST_LEFT_JOIN || node.Join == sqlparser.AST_RIGHT_JOIN {
			// ON clause is requried
			buf.Fprintf("%v %s %v on 1 != 1", node.LeftExpr, node.Join, node.RightExpr)
		} else {
			buf.Fprintf("%v %s %v", node.LeftExpr, node.Join, node.RightExpr)
		}
	default:
		node.Format(buf)
	}
}

func GenerateSelectLimitQuery(selStmt sqlparser.SelectStatement) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	sel, ok := selStmt.(*sqlparser.Select)
	if ok {
		limit := sel.Limit
		if limit == nil {
			sel.Limit = execLimit
			defer func() {
				sel.Limit = nil
			}()
		}
	}
	buf.Fprintf("%v", selStmt)
	return buf.ParsedQuery()
}

func GenerateEqualOuterQuery(sel *sqlparser.Select, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	buf.Fprintf(" from %v where ", sel.From)
	generatePKWhere(buf, tableInfo.Indexes[0])
	return buf.ParsedQuery()
}

func GenerateInOuterQuery(sel *sqlparser.Select, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "select ")
	writeColumnList(buf, tableInfo.Columns)
	// We assume there is one and only one PK column.
	// A '*' argument name means all variables of the list.
	buf.Fprintf(" from %v where %s in (%a)", sel.From, tableInfo.Indexes[0].Columns[0], "*")
	return buf.ParsedQuery()
}

func GenerateInsertOuterQuery(ins *sqlparser.Insert) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Fprintf("insert %vinto %v%v values %a%v",
		ins.Comments,
		ins.Table,
		ins.Columns,
		"_rowValues",
		ins.OnDup,
	)
	return buf.ParsedQuery()
}

func GenerateUpdateOuterQuery(upd *sqlparser.Update, pkIndex *schema.Index) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Fprintf("update %v%v set %v where ", upd.Comments, upd.Table, upd.Exprs)
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func GenerateDeleteOuterQuery(del *sqlparser.Delete, pkIndex *schema.Index) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Fprintf("delete %vfrom %v where ", del.Comments, del.Table)
	generatePKWhere(buf, pkIndex)
	return buf.ParsedQuery()
}

func generatePKWhere(buf *sqlparser.TrackedBuffer, pkIndex *schema.Index) {
	for i := 0; i < len(pkIndex.Columns); i++ {
		if i != 0 {
			buf.WriteString(" and ")
		}
		buf.Fprintf("%s = %a", pkIndex.Columns[i], strconv.FormatInt(int64(i), 10))
	}
}

func GenerateSelectSubquery(sel *sqlparser.Select, tableInfo *schema.Table, index string) *sqlparser.ParsedQuery {
	hint := &sqlparser.IndexHints{Type: sqlparser.AST_USE, Indexes: [][]byte{[]byte(index)}}
	table_expr := sel.From[0].(*sqlparser.AliasedTableExpr)
	savedHint := table_expr.Hints
	table_expr.Hints = hint
	defer func() {
		table_expr.Hints = savedHint
	}()
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		table_expr,
		sel.Where,
		sel.OrderBy,
		sel.Limit,
		false,
	)
}

func GenerateUpdateSubquery(upd *sqlparser.Update, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		&sqlparser.AliasedTableExpr{Expr: upd.Table},
		upd.Where,
		upd.OrderBy,
		upd.Limit,
		true,
	)
}

func GenerateDeleteSubquery(del *sqlparser.Delete, tableInfo *schema.Table) *sqlparser.ParsedQuery {
	return GenerateSubquery(
		tableInfo.Indexes[0].Columns,
		&sqlparser.AliasedTableExpr{Expr: del.Table},
		del.Where,
		del.OrderBy,
		del.Limit,
		true,
	)
}

func GenerateSubquery(columns []string, table *sqlparser.AliasedTableExpr, where *sqlparser.Where, order sqlparser.OrderBy, limit *sqlparser.Limit, for_update bool) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	if limit == nil {
		limit = execLimit
	}
	fmt.Fprintf(buf, "select ")
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i])
	}
	fmt.Fprintf(buf, "%s", columns[i])
	buf.Fprintf(" from %v%v%v%v", table, where, order, limit)
	if for_update {
		buf.Fprintf(sqlparser.AST_FOR_UPDATE)
	}
	return buf.ParsedQuery()
}

func writeColumnList(buf *sqlparser.TrackedBuffer, columns []schema.TableColumn) {
	i := 0
	for i = 0; i < len(columns)-1; i++ {
		fmt.Fprintf(buf, "%s, ", columns[i].Name)
	}
	fmt.Fprintf(buf, "%s", columns[i].Name)
}
