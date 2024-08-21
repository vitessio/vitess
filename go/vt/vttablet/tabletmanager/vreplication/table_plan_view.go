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

package vreplication

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet"
)

// ReplicatorJoinPlan is for a materialized view that joins multiple tables.
type ReplicatorJoinPlan struct {
	Tables        []string // all tables in the join
	ViewTableName string   // the view table
	BaseTableName string   // the base table for which we run the copy phase
}

// The materialized view can have different column names than the source. Hence we need to map the source column names
// to the view column names.
type ViewColumnMap struct {
	SourceColumnName string
	ViewColumnName   string
}

type TableJoinPlan struct {
	Insert        *sqlparser.ParsedQuery            // insert query for the view table
	Updates       map[string]*sqlparser.ParsedQuery // update queries for each participating table
	Deletes       map[string]*sqlparser.ParsedQuery // delete queries for each participating table
	TableColumns  *map[string][]*ViewColumnMap      // columns for each participating table
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
	for table, cols := range *tjp.TableColumns {
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
	tableColumns := make(map[string][]*ViewColumnMap)

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
		tableColumns[tableName] = append(tableColumns[tableName], &ViewColumnMap{
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

	insert := generateInsertForJoin(view, query, tablePlan.Fields)
	updates := generateUpdatesForJoin(view, tableColumns)
	deletes := generateDeletesForJoin(view, tableColumns)
	tablePlan.JoinPlan = &TableJoinPlan{
		BaseTableName: joinTables[0],
		TableColumns:  &tableColumns,
		Insert:        insert,
		Updates:       updates,
		Deletes:       deletes,
	}
	log.Info(tablePlan.JoinPlan)

	if copy {
		// In copy phase we stream the view ordered by the primary key of the base table.
		plan.VStreamFilter.Rules[0].Filter = fmt.Sprintf("/*vt+ view=%s */ %s", view, plan.VStreamFilter.Rules[0].Filter)
	} else {
		// In replicate phase we stream all the participating tables.
		plan.VStreamFilter.Rules[0].Match = fmt.Sprintf("/\\b(%s)\\b", strings.Join(plan.joinPlan.Tables, "|"))
	}
	plan.TablePlans[view] = tablePlan
	plan.TargetTables[view] = tablePlan
	log.Infof("Added table plan for view %s: %v, insert %v, updates %+v, deletes %+v", view, tablePlan, insert, updates, deletes)
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

func generateInsertForJoin(view string, viewQuery string, fields []*querypb.Field) *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)
	bvf.mode = bvAfter
	buf.Myprintf("insert into %v (", sqlparser.NewIdentifierCS(view))
	separator := ""
	for _, field := range fields {
		buf.Myprintf("%s%v", separator, sqlparser.NewIdentifierCI(field.Name))
		separator = ","
	}
	buf.Myprintf(") ")
	buf.Myprintf("%s", viewQuery)
	buf.Myprintf(" where oid = ")
	buf.Myprintf("%v", sqlparser.NewColName("oid"))
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
		separator := ""
		for i, col := range cols {
			if i == 0 {
				continue
			}
			buf.Myprintf("%s%s = ", separator, sqlparser.NewIdentifierCI(col.ViewColumnName).CompliantName())
			buf.Myprintf("%v", sqlparser.NewColName(col.ViewColumnName))
			separator = " and "
		}
		deletes[table] = buf.ParsedQuery()
		log.Infof("Delete for table %s is %s, bindLocations %d", table, buf.String(), len(deletes[table].BindLocations()))
	}
	return deletes
}

func (vc *ViewColumnMap) String() string {
	return fmt.Sprintf("SourceColumnName %s, ViewColumnName %s", vc.SourceColumnName, vc.ViewColumnName)
}
