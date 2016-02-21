// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func buildUpdatePlan(upd *sqlparser.Update, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(upd),
	}
	tablename := sqlparser.GetTableName(upd.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if err := checkSubquery(upd); err != nil {
		return nil, err
	}
	if !route.Keyspace.Sharded {
		route.PlanID = UpdateUnsharded
		return route, nil
	}

	err = getDMLRouting(upd.Where, route)
	if err != nil {
		return nil, err
	}
	route.PlanID = UpdateEqual
	if isIndexChanging(upd.Exprs, route.Table.ColVindexes) {
		return nil, errors.New("unsupported: DML cannot change vindex column")
	}
	return route, nil
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.String()
}

func isIndexChanging(setClauses sqlparser.UpdateExprs, colVindexes []*ColVindex) bool {
	vindexCols := make([]string, len(colVindexes))
	for i, index := range colVindexes {
		vindexCols[i] = index.Col
	}
	for _, assignment := range setClauses {
		if sqlparser.StringIn(string(assignment.Name.Name), vindexCols...) {
			return true
		}
	}
	return false
}

func buildDeletePlan(del *sqlparser.Delete, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(del),
	}
	tablename := sqlparser.GetTableName(del.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if err := checkSubquery(del); err != nil {
		return nil, err
	}
	if !route.Keyspace.Sharded {
		route.PlanID = DeleteUnsharded
		return route, nil
	}

	err = getDMLRouting(del.Where, route)
	if err != nil {
		return nil, err
	}
	route.PlanID = DeleteEqual
	route.Subquery = generateDeleteSubquery(del, route.Table)
	return route, nil
}

func generateDeleteSubquery(del *sqlparser.Delete, table *Table) string {
	if len(table.Owned) == 0 {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	buf.WriteString("select ")
	prefix := ""
	for _, cv := range table.Owned {
		buf.WriteString(prefix)
		buf.WriteString(cv.Col)
		prefix = ", "
	}
	fmt.Fprintf(buf, " from %s", table.Name)
	buf.WriteString(sqlparser.String(del.Where))
	buf.WriteString(" for update")
	return buf.String()
}

func checkSubquery(node sqlparser.SQLNode) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			return false, errors.New("unsupported: subqueries in DML")
		}
		return true, nil
	}, node)
}

func getDMLRouting(where *sqlparser.Where, route *Route) error {
	if where == nil {
		return errors.New("unsupported: multi-shard where clause in DML")
	}
	for _, index := range route.Table.Ordered {
		if !IsUnique(index.Vindex) {
			continue
		}
		if values := getMatch(where.Expr, index.Col); values != nil {
			route.Vindex = index.Vindex
			route.Values = values
			return nil
		}
	}
	return errors.New("unsupported: multi-shard where clause in DML")
}

func getMatch(node sqlparser.BoolExpr, col string) interface{} {
	filters := splitAndExpression(nil, node)
	for _, filter := range filters {
		comparison, ok := filter.(*sqlparser.ComparisonExpr)
		if !ok {
			continue
		}
		if comparison.Operator != sqlparser.EqualStr {
			continue
		}
		if !nameMatch(comparison.Left, col) {
			continue
		}
		if !sqlparser.IsValue(comparison.Right) {
			continue
		}
		val, err := valConvert(comparison.Right)
		if err != nil {
			continue
		}
		return val
	}
	return nil
}

func nameMatch(node sqlparser.ValExpr, col string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && string(colname.Name) == col
}
