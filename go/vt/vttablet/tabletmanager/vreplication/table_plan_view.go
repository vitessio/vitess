/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
 This file contains all the specific table plan and associated code related to materialized views.
*/

package vreplication

import (
	"fmt"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet"
)

// ReplicatorJoinPlan is for a materialized view that joins multiple tables.
type ReplicatorJoinPlan struct {
	Tables        []string // all tables in the join
	ViewTableName string   // the view table
	BaseTableName string   // the base table for which we run the copy phase
}

// ViewColumnMap maps the source column names to the view column names, since they can be different.
type ViewColumnMap struct {
	SourceColumnName string
	ViewColumnName   string
}

type TableJoinPlan struct {
	Insert        *sqlparser.ParsedQuery            // insert query for the view table
	Updates       map[string]*sqlparser.ParsedQuery // update queries for each participating table
	Deletes       map[string]*sqlparser.ParsedQuery // delete queries for each participating table
	ColumnNameMap *map[string][]*ViewColumnMap      // columns for each participating table
	BaseTableName string                            // the base table for which we run the copy phase
}

func (tjp *TableJoinPlan) String() string {
	s := "TableJoinPlan: \n"
	s += fmt.Sprintf("BaseTableName: %s\n", tjp.BaseTableName)
	s += fmt.Sprintf("Insert: %s\n", tjp.Insert.Query)
	for table, upd := range tjp.Updates {
		s += fmt.Sprintf("Update for %s: %s\n", table, upd.Query)
	}
	for table, del := range tjp.Deletes {
		s += fmt.Sprintf("Delete for %s: %s\n", table, del.Query)
	}
	for table, cols := range *tjp.ColumnNameMap {
		s += fmt.Sprintf("Columns for %s: %v\n", table, cols)
	}
	return s
}

func buildReplicatorPlanForJoin(source *binlogdatapb.BinlogSource, colInfoMap map[string][]*ColumnInfo,
	copyState map[string]*sqltypes.Result, stats *binlogplayer.Stats, collationEnv *collations.Environment,
	parser *sqlparser.Parser, dbClient *vdbClient, copy bool) (*ReplicatorPlan, error) {

	joinTables, err := vttablet.GetJoinedTables(parser, source.Filter.Rules[0].Filter)
	if err != nil {
		return nil, err
	}
	if len(joinTables) == 0 {
		return buildReplicatorPlan(source, colInfoMap, copyState, stats, collationEnv, parser)
	}
	log.Infof("In buildReplicatorPlanForJoin with join tables %v", joinTables)

	filter := source.Filter
	joinPlan := &ReplicatorJoinPlan{
		Tables: joinTables,
	}
	plan := &ReplicatorPlan{
		VStreamFilter: &binlogdatapb.Filter{FieldEventMode: filter.FieldEventMode},
		TargetTables:  make(map[string]*TablePlan),
		TablePlans:    make(map[string]*TablePlan),
		ColInfoMap:    colInfoMap,
		stats:         stats,
		Source:        source,
		collationEnv:  collationEnv,
		joinPlan:      joinPlan,
	}
	plan.VStreamFilter = source.Filter.CloneVT()
	// Note the view corresponds to a mysql table and it is the table into which data is being replicated.
	view := source.Filter.Rules[0].Match
	joinPlan.ViewTableName = view
	joinPlan.BaseTableName = joinTables[0]
	query := source.Filter.Rules[0].Filter
	tablePlan := &TablePlan{
		TargetName: view,
		SendRule:   source.Filter.Rules[0],
	}
	colInfos, ok := colInfoMap[view]
	if !ok {
		return nil, fmt.Errorf("table %s not found in schema", view)
	}

	statement, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unsupported non-select statement")
	}

	tpb := &tablePlanBuilder{
		name: sqlparser.NewIdentifierCS(view),
		sendSelect: &sqlparser.Select{
			From:  sel.From,
			Where: sel.Where,
		},
		lastpk:       nil,
		colInfos:     colInfos,
		stats:        stats,
		source:       source,
		collationEnv: collationEnv,
		viewMode:     true,
	}
	err = tpb.analyzeExprs(sel.SelectExprs)
	if err != nil {
		return nil, err
	}
	tpb.pkCols = append(tpb.pkCols, tpb.colExprs[0])
	tablePlan.PKReferences = []string{tpb.colExprs[0].colName.String()}
	columnNameMap := make(map[string][]*ViewColumnMap)

	for _, selExpr := range sel.SelectExprs {
		aliasExpr, ok := selExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported expression %s", sqlparser.String(selExpr))
		}
		colName, ok := aliasExpr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("unsupported expression %s", sqlparser.String(aliasExpr.Expr))
		}
		if colName.Qualifier.IsEmpty() {
			return nil, fmt.Errorf("for a join plan, every column needs to have a table qualifier %s", colName)
		}
		tableName := colName.Qualifier.Name.String()
		columnName := colName.Name.String()
		aliasName := columnName
		if !aliasExpr.As.IsEmpty() {
			aliasName = aliasExpr.As.String()
		}
		columnNameMap[tableName] = append(columnNameMap[tableName], &ViewColumnMap{
			SourceColumnName: columnName,
			ViewColumnName:   aliasName,
		})
	}

	if view == "" {
		return nil, fmt.Errorf("view name is empty")
	}
	qr, err := dbClient.ExecuteFetch(fmt.Sprintf("select * from %s limit 1", view), 1)
	if err != nil {
		return nil, err
	}
	tablePlan.Fields = qr.Fields

	baseTableColInfos := colInfoMap[joinTables[0]]
	for _, colInfo := range baseTableColInfos {
		log.Infof("ColInfo: %v, isPK %t", colInfo.Name, colInfo.IsPK)
	}
	var pkCol *ColumnInfo
	for _, colInfo := range baseTableColInfos {
		if colInfo.IsPK {
			pkCol = colInfo
			break
		}
	}
	tablePlan.JoinPlan = &TableJoinPlan{
		BaseTableName: joinTables[0],
		ColumnNameMap: &columnNameMap,
	}
	tablePlan.JoinPlan.Insert = generateInsertForJoin(tablePlan.Fields, tablePlan.JoinPlan, tablePlan.TargetName, query, pkCol)
	tablePlan.JoinPlan.Updates = generateUpdatesForJoin(view, columnNameMap)
	tablePlan.JoinPlan.Deletes = generateDeletesForJoin(view, columnNameMap)
	log.Infof("Table Join plan is %v", tablePlan.JoinPlan)

	if copy {
		// In copy phase we stream the view ordered by the primary key of the base table.
		plan.VStreamFilter.Rules[0].Filter = fmt.Sprintf("/*vt+ view=%s */ %s", view, plan.VStreamFilter.Rules[0].Filter)
	} else {
		// In replicate phase we stream all the participating tables.
		plan.VStreamFilter.Rules[0].Match = fmt.Sprintf("/\\b(%s)\\b", strings.Join(plan.joinPlan.Tables, "|"))
	}
	plan.TablePlans[view] = tablePlan
	plan.TargetTables[view] = tablePlan
	for _, tableName := range joinTables {
		rule := &binlogdatapb.Rule{
			Match: tableName,
		}
		tablePlan, err := buildTablePlan(tableName, rule, colInfos, nil, stats, source, collationEnv, parser)
		if err != nil {
			return nil, err
		}
		plan.TablePlans[tableName] = tablePlan
	}
	return plan, nil
}

func generateInsertForJoin(fields []*querypb.Field, joinPlan *TableJoinPlan, viewName, viewQuery string, pkColInfo *ColumnInfo) *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	bvf.mode = bvAfter
	buf.Myprintf("insert into %v (", sqlparser.NewIdentifierCS(viewName))
	separator := ""
	for _, field := range fields {
		buf.Myprintf("%s%v", separator, sqlparser.NewIdentifierCI(field.Name))
		separator = ","
	}
	buf.Myprintf(") ")
	buf.Myprintf("%s", viewQuery)
	buf.Myprintf(" where %s.%s = ", joinPlan.BaseTableName, sqlparser.NewColName(pkColInfo.Name).CompliantName())
	buf.Myprintf("%v", sqlparser.NewColName(pkColInfo.Name))
	return buf.ParsedQuery()
}

func generateUpdatesForJoin(view string, viewColumns map[string][]*ViewColumnMap) map[string]*sqlparser.ParsedQuery {
	updates := map[string]*sqlparser.ParsedQuery{}
	for table, cols := range viewColumns {
		bvf := &bindvarFormatter{}
		buf := sqlparser.NewTrackedBuffer(bvf.formatter)
		bvf.mode = bvAfter
		buf.Myprintf("update %v set ", sqlparser.NewIdentifierCS(view))
		separator := ""
		for i, col := range cols {
			if i == 0 {
				continue
			}
			buf.Myprintf("%s%s = ", separator, sqlparser.NewIdentifierCI(col.ViewColumnName).CompliantName())
			buf.Myprintf("%v", sqlparser.NewColName(col.ViewColumnName))
			separator = ", "
		}
		col := cols[0]
		bvf.mode = bvBefore
		buf.Myprintf(" where %s = ", sqlparser.NewIdentifierCI(col.ViewColumnName).String())
		buf.Myprintf("%v", sqlparser.NewColName(col.ViewColumnName))
		updates[table] = buf.ParsedQuery()
		log.Infof("Update for table %s is %s, bindLocations %d", table, buf.String(), len(updates[table].BindLocations()))
	}
	return updates
}

func generateDeletesForJoin(view string, viewColumns map[string][]*ViewColumnMap) map[string]*sqlparser.ParsedQuery {
	deletes := map[string]*sqlparser.ParsedQuery{}
	for table, cols := range viewColumns {
		bvf := &bindvarFormatter{}
		buf := sqlparser.NewTrackedBuffer(bvf.formatter)
		bvf.mode = bvBefore
		buf.Myprintf("delete from %v where ", sqlparser.NewIdentifierCS(view))
		col := cols[0]
		buf.Myprintf("%s = ", sqlparser.NewIdentifierCI(col.ViewColumnName).CompliantName())
		buf.Myprintf("%v", sqlparser.NewColName(col.ViewColumnName))
		deletes[table] = buf.ParsedQuery()
		log.Infof("Delete for table %s is %s, bindLocations %d", table, buf.String(), len(deletes[table].BindLocations()))
	}
	return deletes
}

func (vc *ViewColumnMap) String() string {
	return fmt.Sprintf("SourceColumnName %s, ViewColumnName %s", vc.SourceColumnName, vc.ViewColumnName)
}

type DMLType int

// Enum values for DMLType
const (
	NONE DMLType = iota
	INSERT
	DELETE
	UPDATE
)

/*
applyChangeForJoin applies the DML change to the view table in case of a materialized view. Depending on the table
involved the query plan used will differ. We only insert into the base table if the source has an insert. However for
updates and deletes, we need to update the base table differently. For the base table, we rerun
*/
func (tp *TablePlan) applyChangeForJoin(eventTableName string, eventTablePlan *TablePlan, rowChange *binlogdatapb.RowChange, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	// mapColumnName maps a field name from the source table to the view table, if names are different
	// since the applied DML is on the view table, we need to map the field names to the view table
	cols := (*tp.JoinPlan.ColumnNameMap)[eventTableName]
	if cols == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "column name map not found for %v", eventTableName)
	}
	mapColumnName := func(colName string) string {
		for _, col := range cols {
			if col.SourceColumnName == colName {
				return col.ViewColumnName
			}
		}
		return colName
	}

	var dmlType DMLType
	switch {
	case rowChange.Before == nil && rowChange.After != nil:
		dmlType = INSERT
	case rowChange.Before != nil && rowChange.After == nil:
		dmlType = DELETE
	case rowChange.Before != nil && rowChange.After != nil:
		dmlType = UPDATE
	}

	bindvars := make(map[string]*querypb.BindVariable, len(eventTablePlan.Fields))
	switch dmlType {
	case INSERT:
		vals := sqltypes.MakeRowTrusted(eventTablePlan.Fields, rowChange.After)
		for i, field := range eventTablePlan.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[i])
			if err != nil {
				return nil, err
			}
			// for inserts, we need to bind the values to the base table
			bindvars["a_"+field.Name] = bindVar
		}
	case DELETE:
		vals := sqltypes.MakeRowTrusted(eventTablePlan.Fields, rowChange.Before)
		for i, field := range eventTablePlan.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[i])
			if err != nil {
				return nil, err
			}
			bindvars["b_"+mapColumnName(field.Name)] = bindVar
		}
	case UPDATE:
		vals := sqltypes.MakeRowTrusted(eventTablePlan.Fields, rowChange.Before)
		for i, field := range eventTablePlan.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[i])
			if err != nil {
				return nil, err
			}
			bindvars["b_"+mapColumnName(field.Name)] = bindVar
		}
		vals = sqltypes.MakeRowTrusted(eventTablePlan.Fields, rowChange.After)
		for i, field := range eventTablePlan.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[i])
			if err != nil {
				return nil, err
			}
			bindvars["a_"+mapColumnName(field.Name)] = bindVar
		}
	}
	switch dmlType {
	case INSERT:
		if eventTableName != tp.JoinPlan.BaseTableName {
			// We expect that any insert into a lookup table is not resulting in a new row in the base table.
			// This means that cases where the view had lookup entries that didn't exist earlier and were added later
			// will not work with the current implementation.
			return nil, nil
		}
		log.Infof("Inserting into base table %v: %s, bindvars %+q", tp.JoinPlan.BaseTableName, tp.JoinPlan.Insert.Query, bindvars)
		return execParsedQuery(tp.JoinPlan.Insert, bindvars, executor)
	case UPDATE:
		upd := tp.JoinPlan.Updates[eventTableName]
		if upd == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "update query not found for %v", eventTableName)
		}
		log.Infof("Updating %v: %s with bindvars %+q", eventTableName, upd.Query, bindvars)
		return execParsedQuery(upd, bindvars, executor)
	case DELETE:
		del := tp.JoinPlan.Deletes[eventTableName]
		if del == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "delete query not found for %v", eventTableName)
		}
		log.Infof("Deleting from %v: %s with bindvars %+q", eventTableName, del.Query, bindvars)
		return execParsedQuery(del, bindvars, executor)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "applyChangeForJoin called with before or without after")
	}
}
