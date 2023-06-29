/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

const sqlSelectColumnCollations = "select column_name as column_name, collation_name as collation_name from information_schema.columns where table_schema=%a and table_name=%a and column_name in %a"

type tablePlan struct {
	// sourceQuery and targetQuery are select queries.
	sourceQuery string
	targetQuery string

	// compareCols is the list of non-pk columns to compare.
	// If the value is -1, it's a pk column and should not be
	// compared.
	compareCols []compareColInfo
	// comparePKs is the list of pk columns to compare. The logic
	// for comparing pk columns is different from compareCols
	comparePKs []compareColInfo
	// pkCols has the indices of PK cols in the select list
	pkCols []int

	// selectPks is the list of pk columns as they appear in the select clause for the diff.
	selectPks  []int
	dbName     string
	table      *tabletmanagerdatapb.TableDefinition
	orderBy    sqlparser.OrderBy
	aggregates []*engine.AggregateParams
}

func (td *tableDiffer) buildTablePlan(dbClient binlogplayer.DBClient, dbName string) (*tablePlan, error) {
	tp := &tablePlan{
		table:  td.table,
		dbName: dbName,
	}
	statement, err := sqlparser.Parse(td.sourceQuery)
	if err != nil {
		return nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}

	sourceSelect := &sqlparser.Select{}
	targetSelect := &sqlparser.Select{}
	// aggregates is the list of Aggregate functions, if any.
	var aggregates []*engine.AggregateParams
	for _, selExpr := range sel.SelectExprs {
		switch selExpr := selExpr.(type) {
		case *sqlparser.StarExpr:
			// If it's a '*' expression, expand column list from the schema.
			for _, fld := range tp.table.Fields {
				aliased := &sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(fld.Name)}}
				sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, aliased)
				targetSelect.SelectExprs = append(targetSelect.SelectExprs, aliased)
			}
		case *sqlparser.AliasedExpr:
			var targetCol *sqlparser.ColName
			if !selExpr.As.IsEmpty() {
				targetCol = &sqlparser.ColName{Name: selExpr.As}
			} else {
				if colAs, ok := selExpr.Expr.(*sqlparser.ColName); ok {
					targetCol = colAs
				} else {
					return nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(selExpr))
				}
			}
			// If the input was "select a as b", then source will use "a" and target will use "b".
			sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, selExpr)
			targetSelect.SelectExprs = append(targetSelect.SelectExprs, &sqlparser.AliasedExpr{Expr: targetCol})

			// Check if it's an aggregate expression
			if expr, ok := selExpr.Expr.(sqlparser.AggrFunc); ok {
				switch fname := expr.AggrName(); fname {
				case "count", "sum":
					// this will only work as long as aggregates can be pushed down to tablets
					// this won't work: "select count(*) from (select id from t limit 1)"
					// since vreplication only handles simple tables (no joins/derived tables) this is fine for now
					// but will need to be revisited when we add such support to vreplication
					aggregates = append(aggregates, engine.NewAggregateParam(
						/*opcode*/ opcode.AggregateSum,
						/*offset*/ len(sourceSelect.SelectExprs)-1,
						/*alias*/ ""))
				}
			}
		default:
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
		}
	}
	fields := make(map[string]querypb.Type)
	for _, field := range tp.table.Fields {
		fields[strings.ToLower(field.Name)] = field.Type
	}

	targetSelect.SelectExprs = td.adjustForSourceTimeZone(targetSelect.SelectExprs, fields)
	// Start with adding all columns for comparison.
	tp.compareCols = make([]compareColInfo, len(sourceSelect.SelectExprs))
	for i := range tp.compareCols {
		tp.compareCols[i].colIndex = i
		colname, err := getColumnNameForSelectExpr(targetSelect.SelectExprs[i])
		if err != nil {
			return nil, err
		}
		_, ok := fields[colname]
		if !ok {
			return nil, fmt.Errorf("column %v not found in table %v on tablet %v",
				colname, tp.table.Name, td.wd.ct.vde.thisTablet.Alias)
		}
		tp.compareCols[i].colName = colname
	}

	sourceSelect.From = sel.From
	// The target table name should the one that matched the rule.
	// It can be different from the source table.
	targetSelect.From = sqlparser.TableExprs{
		&sqlparser.AliasedTableExpr{
			Expr: &sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS(tp.table.Name),
			},
		},
	}

	err = tp.findPKs(dbClient, targetSelect)
	if err != nil {
		return nil, err
	}
	// Remove in_keyrange. It's not understood by mysql.
	sourceSelect.Where = sel.Where //removeKeyrange(sel.Where)
	// The source should also perform the group by.
	sourceSelect.GroupBy = sel.GroupBy
	sourceSelect.OrderBy = tp.orderBy

	// The target should perform the order by, but not the group by.
	targetSelect.OrderBy = tp.orderBy

	tp.sourceQuery = sqlparser.String(sourceSelect)
	tp.targetQuery = sqlparser.String(targetSelect)
	log.Info("VDiff query on source: %v", tp.sourceQuery)
	log.Info("VDiff query on target: %v", tp.targetQuery)

	tp.aggregates = aggregates
	td.tablePlan = tp
	return tp, err
}

// findPKs identifies PKs and removes them from the columns to do data comparison.
func (tp *tablePlan) findPKs(dbClient binlogplayer.DBClient, targetSelect *sqlparser.Select) error {
	var orderby sqlparser.OrderBy
	for _, pk := range tp.table.PrimaryKeyColumns {
		found := false
		for i, selExpr := range targetSelect.SelectExprs {
			expr := selExpr.(*sqlparser.AliasedExpr).Expr
			colname := ""
			switch ct := expr.(type) {
			case *sqlparser.ColName:
				colname = ct.Name.String()
			case *sqlparser.FuncExpr: //eg. weight_string()
				//no-op
			default:
				log.Warningf("Not considering column %v for PK, type %v not handled", selExpr, ct)
			}
			if strings.EqualFold(pk, colname) {
				tp.compareCols[i].isPK = true
				tp.comparePKs = append(tp.comparePKs, tp.compareCols[i])
				tp.selectPks = append(tp.selectPks, i)
				// We'll be comparing pks separately. So, remove them from compareCols.
				tp.pkCols = append(tp.pkCols, i)
				found = true
				break
			}
		}
		if !found {
			// Unreachable.
			return fmt.Errorf("column %v not found in table %v", pk, tp.table.Name)
		}
		orderby = append(orderby, &sqlparser.Order{
			Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(pk)},
			Direction: sqlparser.AscOrder,
		})
	}
	if err := tp.getPKColumnCollations(dbClient); err != nil {
		return vterrors.Wrapf(err, "error getting PK column collations for table %s", tp.table.Name)
	}
	tp.orderBy = orderby
	return nil
}

// getPKColumnCollations queries the database to find the collation
// to use for the each PK column used in the query to ensure proper
// sorting when we do the merge sort and for the comparisons. It then
// saves the collations in the tablePlan's comparePKs column info
// structs for those subsequent operations.
func (tp *tablePlan) getPKColumnCollations(dbClient binlogplayer.DBClient) error {
	columnList := make([]string, len(tp.comparePKs))
	for i := range tp.comparePKs {
		columnList[i] = tp.comparePKs[i].colName
	}
	columnsBV, err := sqltypes.BuildBindVariable(columnList)
	if err != nil {
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlSelectColumnCollations,
		sqltypes.StringBindVariable(tp.dbName),
		sqltypes.StringBindVariable(tp.table.Name),
		columnsBV,
	)
	if err != nil {
		return err
	}
	qr, err := dbClient.ExecuteFetch(query, len(tp.comparePKs))
	if err != nil {
		return err
	}
	if qr == nil || len(qr.Rows) != len(tp.comparePKs) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected result for query %s: %+v", query, qr)
	}
	collationEnv := collations.Local()
	for _, row := range qr.Named().Rows {
		columnName := row["column_name"].ToString()
		collateName := strings.ToLower(row["collation_name"].ToString())
		for i := range tp.comparePKs {
			if strings.EqualFold(tp.comparePKs[i].colName, columnName) {
				tp.comparePKs[i].collation = collationEnv.LookupByName(collateName)
				break
			}
		}
	}
	return nil
}
