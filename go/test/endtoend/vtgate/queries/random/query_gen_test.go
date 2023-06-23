/*
Copyright 2023 The Vitess Authors.

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

package random

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// this file contains the structs and functions to generate random queries

type (
	column struct {
		tableName string
		name      string
		typ       string
	}
	tableT struct {
		// name will be a tableName object if it is used, with name: alias or name if no alias is provided
		// name will only be a DerivedTable for moving its data around
		name sqlparser.SimpleTableExpr
		cols []column
	}
)

var _ sqlparser.ExprGenerator = (*tableT)(nil)

func (t *tableT) typeExpr(typ string) sqlparser.Expr {
	tableCopy := t.clone()

	for len(tableCopy.cols) > 0 {
		idx := rand.Intn(len(tableCopy.cols))
		randCol := tableCopy.cols[idx]
		if randCol.typ == typ {
			newTableName := ""
			if tName, ok := tableCopy.name.(sqlparser.TableName); ok {
				newTableName = sqlparser.String(tName.Name)
			}
			return sqlparser.NewColNameWithQualifier(randCol.name, sqlparser.NewTableName(newTableName))
		} else {
			// delete randCol from table.columns
			tableCopy.cols[idx] = tableCopy.cols[len(tableCopy.cols)-1]
			tableCopy.cols = tableCopy.cols[:len(tableCopy.cols)-1]
		}
	}

	return nil
}

func (t *tableT) IntExpr() sqlparser.Expr {
	// better way to check if int type?
	return t.typeExpr("bigint")
}

func (t *tableT) StringExpr() sqlparser.Expr {
	return t.typeExpr("varchar")
}

// setName sets the alias for t, as well as setting the tableName for all columns in cols
func (t *tableT) setName(newName string) {
	t.name = sqlparser.NewTableName(newName)
	for i := range t.cols {
		t.cols[i].tableName = newName
	}
}

// setColumns sets the columns of t, and automatically assigns tableName
// this makes it unnatural (but still possible as cols is exportable) to modify tableName
func (t *tableT) setColumns(col ...column) {
	t.cols = nil
	t.addColumns(col...)
}

// addColumns adds columns to t, and automatically assigns tableName
// this makes it unnatural (but still possible as cols is exportable) to modify tableName
func (t *tableT) addColumns(col ...column) {
	for i := range col {
		// only change the Col's tableName if t is of type tableName
		if tName, ok := t.name.(sqlparser.TableName); ok {
			col[i].tableName = sqlparser.String(tName.Name)
		}

		t.cols = append(t.cols, col[i])
	}
}

// clone returns a deep copy of t
func (t *tableT) clone() *tableT {
	return &tableT{
		name: t.name,
		cols: slices.Clone(t.cols),
	}
}

// getColumnName returns tableName.name
func (c *column) getColumnName() string {
	return fmt.Sprintf("%s.%s", c.tableName, c.name)
}

func TestRandomQuery(t *testing.T) {
	schemaTables := []tableT{
		{name: sqlparser.NewTableName("emp")},
		{name: sqlparser.NewTableName("dept")},
	}
	schemaTables[0].addColumns([]column{
		{name: "empno", typ: "bigint"},
		{name: "ename", typ: "varchar"},
		{name: "job", typ: "varchar"},
		{name: "mgr", typ: "bigint"},
		{name: "hiredate", typ: "date"},
		{name: "sal", typ: "bigint"},
		{name: "comm", typ: "bigint"},
		{name: "deptno", typ: "bigint"},
	}...)
	schemaTables[1].addColumns([]column{
		{name: "deptno", typ: "bigint"},
		{name: "dname", typ: "varchar"},
		{name: "loc", typ: "varchar"},
	}...)

	fmt.Println(sqlparser.String(randomQuery(schemaTables, 3, 3)))
}

func randomQuery(schemaTables []tableT, maxAggrs, maxGroupBy int) *sqlparser.Select {
	sel := &sqlparser.Select{}
	sel.SetComments(sqlparser.Comments{"/*vt+ PLANNER=Gen4 */"})

	// select distinct (fails with group by bigint)
	isDistinct := rand.Intn(2) < 1
	if isDistinct {
		sel.MakeDistinct()
	}

	// create both tables and join at the same time since both occupy the from clause
	tables, isJoin := createTablesAndJoin(schemaTables, sel)

	groupExprs, groupSelectExprs, grouping := createGroupBy(tables, maxGroupBy)
	sel.AddSelectExprs(groupSelectExprs)
	sel.GroupBy = groupExprs
	aggrExprs, aggregates := createAggregations(tables, maxAggrs)
	sel.AddSelectExprs(aggrExprs)

	// can add both aggregate and grouping columns to order by
	isOrdered := rand.Intn(2) < 1
	if isOrdered && (!isDistinct || TestFailingQueries) && (!isJoin || TestFailingQueries) {
		addOrderBy(sel)
	}

	// where
	sel.AddWhere(sqlparser.AndExpressions(createWherePredicates(tables, false)...))

	// random predicate expression
	if rand.Intn(2) < 1 {
		predRandomExpr, _ := getRandomExpr(tables)
		sel.AddWhere(predRandomExpr)
	}

	// having
	sel.AddHaving(sqlparser.AndExpressions(createHavingPredicates(tables)...))
	if rand.Intn(2) < 1 && TestFailingQueries {
		// TODO: having can only contain aggregate or grouping columns in mysql, works fine in vitess
		sel.AddHaving(sqlparser.AndExpressions(createWherePredicates(tables, false)...))
	}

	// only add a limit if the grouping columns are ordered
	if rand.Intn(2) < 1 && (isOrdered || len(grouping) == 0) {
		sel.Limit = createLimit()
	}

	var newTable tableT
	// add random expression to select
	isRandomExpr := rand.Intn(2) < 1
	randomExpr, typ := getRandomExpr(tables)
	if isRandomExpr && (!isDistinct || TestFailingQueries) && (!isJoin || TestFailingQueries) {
		sel.SelectExprs = append(sel.SelectExprs, sqlparser.NewAliasedExpr(randomExpr, "crandom0"))
		newTable.addColumns(column{
			name: "crandom0",
			typ:  typ,
		})
	}

	// add them to newTable
	newTable.addColumns(grouping...)
	newTable.addColumns(aggregates...)

	// add new table to schemaTables
	newTable.name = sqlparser.NewDerivedTable(false, sel)
	schemaTables = append(schemaTables, newTable)

	// derived tables (partially unsupported)
	if rand.Intn(10) < 1 && TestFailingQueries {
		sel = randomQuery(schemaTables, 3, 3)
	}

	return sel
}

func createTablesAndJoin(schemaTables []tableT, sel *sqlparser.Select) ([]tableT, bool) {
	var tables []tableT
	// add at least one of original emp/dept tables for now because derived tables have nil columns
	tables = append(tables, schemaTables[rand.Intn(2)])

	sel.From = append(sel.From, newAliasedTable(tables[0], "tbl0"))
	tables[0].setName("tbl0")

	numTables := rand.Intn(len(schemaTables))
	for i := 0; i < numTables; i++ {
		tables = append(tables, randomEl(schemaTables))
		sel.From = append(sel.From, newAliasedTable(tables[i+1], fmt.Sprintf("tbl%d", i+1)))
		tables[i+1].setName(fmt.Sprintf("tbl%d", i+1))
	}

	isJoin := rand.Intn(2) < 1
	if isJoin {
		newTable := randomEl(schemaTables)
		tables = append(tables, newTable)

		// create the join before aliasing
		newJoinTableExpr := createJoin(tables, sel)

		// alias
		tables[numTables+1].setName(fmt.Sprintf("tbl%d", numTables+1))

		// create the condition after aliasing
		newJoinTableExpr.Condition = sqlparser.NewJoinCondition(sqlparser.AndExpressions(createWherePredicates(tables, true)...), nil)
		sel.From[numTables] = newJoinTableExpr
	}

	return tables, isJoin
}

// creates a left join (without the condition) between the last table in sel and newTable
// tables should have one more table than sel
func createJoin(tables []tableT, sel *sqlparser.Select) *sqlparser.JoinTableExpr {
	n := len(sel.From)
	if len(tables) != n+1 {
		log.Fatalf("sel has %d tables and tables has %d tables", len(sel.From), n)
	}

	return sqlparser.NewJoinTableExpr(sel.From[n-1], sqlparser.LeftJoinType, newAliasedTable(tables[n], fmt.Sprintf("tbl%d", n)), nil)
}

// returns the grouping columns as three types: sqlparser.GroupBy, sqlparser.SelectExprs, []column
func createGroupBy(tables []tableT, maxGB int) (groupBy sqlparser.GroupBy, groupSelectExprs sqlparser.SelectExprs, grouping []column) {
	numGBs := rand.Intn(maxGB)
	for i := 0; i < numGBs; i++ {
		tblIdx := rand.Intn(len(tables))
		col := randomEl(tables[tblIdx].cols)
		groupBy = append(groupBy, newColumn(col))

		// add to select
		if rand.Intn(2) < 1 {
			groupSelectExprs = append(groupSelectExprs, newAliasedColumn(col, fmt.Sprintf("cgroup%d", i)))
			col.name = fmt.Sprintf("cgroup%d", i)
			grouping = append(grouping, col)
		}
	}

	return groupBy, groupSelectExprs, grouping
}

// returns the aggregation columns as three types: sqlparser.SelectExprs, []column
func createAggregations(tables []tableT, maxAggrs int) (aggrExprs sqlparser.SelectExprs, aggregates []column) {
	aggregations := []func(col column) sqlparser.Expr{
		func(_ column) sqlparser.Expr { return &sqlparser.CountStar{} },
		func(col column) sqlparser.Expr { return &sqlparser.Count{Args: sqlparser.Exprs{newColumn(col)}} },
		func(col column) sqlparser.Expr { return &sqlparser.Sum{Arg: newColumn(col)} },
		// func(col column) sqlparser.Expr { return &sqlparser.Avg{Arg: newAggregateExpr(col)} },
		func(col column) sqlparser.Expr { return &sqlparser.Min{Arg: newColumn(col)} },
		func(col column) sqlparser.Expr { return &sqlparser.Max{Arg: newColumn(col)} },
	}

	numAggrs := rand.Intn(maxAggrs) + 1
	for i := 0; i < numAggrs; i++ {
		tblIdx, aggrIdx := rand.Intn(len(tables)), rand.Intn(len(aggregations))
		col := randomEl(tables[tblIdx].cols)
		newAggregate := aggregations[aggrIdx](col)
		aggrExprs = append(aggrExprs, sqlparser.NewAliasedExpr(newAggregate, fmt.Sprintf("caggr%d", i)))

		if aggrIdx <= 1 /* CountStar and Count */ {
			col.typ = "bigint"
		} else if _, ok := newAggregate.(*sqlparser.Avg); ok && col.getColumnName() == "bigint" {
			col.typ = "decimal"
		}

		col.name = sqlparser.String(newAggregate)
		col.name = fmt.Sprintf("caggr%d", i)
		aggregates = append(aggregates, col)
	}
	return aggrExprs, aggregates
}

// orders on all non-aggregate SelectExprs and independently at random on all aggregate SelectExprs of sel
func addOrderBy(sel *sqlparser.Select) {
	for _, selExpr := range sel.SelectExprs {
		if aliasedExpr, ok := selExpr.(*sqlparser.AliasedExpr); ok {
			// if the SelectExpr is non-aggregate (the AliasedExpr has Expr of type ColName)
			// then add to the order by
			if colName, ok1 := aliasedExpr.Expr.(*sqlparser.ColName); ok1 {
				sel.AddOrder(sqlparser.NewOrder(colName, getRandomOrderDirection()))
			} else if rand.Intn(2) < 1 {
				sel.AddOrder(sqlparser.NewOrder(aliasedExpr.Expr, getRandomOrderDirection()))
			}
		}
	}
}

// compares two random columns (usually of the same type)
// returns a random expression if there are no other predicates and isJoin is true
// returns the predicates as a sqlparser.Exprs (slice of sqlparser.Expr's)
func createWherePredicates(tables []tableT, isJoin bool) (predicates sqlparser.Exprs) {
	// if creating predicates for a join,
	// then make sure predicates are created for the last two tables (which are being joined)
	incr := 0
	if isJoin && len(tables) > 2 {
		incr += len(tables) - 2
	}

	for idx1 := range tables {
		for idx2 := range tables {
			// fmt.Printf("predicate tables:\n%v\n idx1: %d idx2: %d, incr: %d", tables, idx1, idx2, incr)
			if idx1 >= idx2 || idx1 < incr || idx2 < incr {
				continue
			}
			noOfPredicates := rand.Intn(2)
			if isJoin {
				noOfPredicates++
			}

			for i := 0; noOfPredicates > 0; i++ {
				col1 := randomEl(tables[idx1].cols)
				col2 := randomEl(tables[idx2].cols)

				// prevent infinite loops
				if i > 50 {
					predicates = append(predicates, sqlparser.NewComparisonExpr(getRandomComparisonExprOperator(), newColumn(col1), newColumn(col2), nil))
					break
				}

				if col1.typ != col2.typ {
					continue
				}

				predicates = append(predicates, sqlparser.NewComparisonExpr(getRandomComparisonExprOperator(), newColumn(col1), newColumn(col2), nil))
				noOfPredicates--
			}
		}
	}

	// make sure the join predicate is never empty
	if len(predicates) == 0 && isJoin {
		predRandomExpr, _ := getRandomExpr(tables)
		predicates = append(predicates, predRandomExpr)
	}

	return predicates
}

// creates predicates for the having clause comparing a column to a random expression
func createHavingPredicates(tables []tableT) (havingPredicates sqlparser.Exprs) {
	aggrSelectExprs, _ := createAggregations(tables, 2)
	for i := range aggrSelectExprs {
		if aliasedExpr, ok := aggrSelectExprs[i].(*sqlparser.AliasedExpr); ok {
			predRandomExpr, _ := getRandomExpr(tables)
			havingPredicates = append(havingPredicates, sqlparser.NewComparisonExpr(getRandomComparisonExprOperator(), aliasedExpr.Expr, predRandomExpr, nil))
		}
	}
	return havingPredicates
}

// creates sel.Limit
func createLimit() *sqlparser.Limit {
	limitNum := rand.Intn(10)
	if rand.Intn(2) < 1 {
		offset := rand.Intn(10)
		return sqlparser.NewLimit(offset, limitNum)
	} else {
		return sqlparser.NewLimitWithoutOffset(limitNum)
	}

}

// returns a random expression and its type
func getRandomExpr(tables []tableT) (sqlparser.Expr, string) {
	seed := time.Now().UnixNano()
	g := sqlparser.NewGenerator(seed, 2, slices2.Map(tables, func(t tableT) sqlparser.ExprGenerator { return &t })...)
	return g.Expression()
}

func newAliasedTable(tbl tableT, alias string) *sqlparser.AliasedTableExpr {
	return sqlparser.NewAliasedTableExpr(tbl.name, alias)
}

func newAliasedColumn(col column, alias string) *sqlparser.AliasedExpr {
	return sqlparser.NewAliasedExpr(newColumn(col), alias)
}

func newColumn(col column) *sqlparser.ColName {
	return sqlparser.NewColNameWithQualifier(col.name, sqlparser.NewTableName(col.tableName))
}

func getRandomComparisonExprOperator() sqlparser.ComparisonExprOperator {
	// =, <, >, <=, >=, !=, <=>
	return randomEl([]sqlparser.ComparisonExprOperator{0, 1, 2, 3, 4, 5, 6})
}

func getRandomOrderDirection() sqlparser.OrderDirection {
	// asc, desc
	return randomEl([]sqlparser.OrderDirection{0, 1})
}

func randomEl[K any](in []K) K {
	return in[rand.Intn(len(in))]
}
