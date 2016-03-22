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

// buildUpdatePlan builds the instructions for an UPDATE statement.
func buildUpdatePlan(upd *sqlparser.Update, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(upd),
	}
	// We allow only one table in an update.
	tablename := sqlparser.GetTableName(upd.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if hasSubquery(upd) {
		return nil, errors.New("unsupported: subqueries in DML")
	}
	if !route.Keyspace.Sharded {
		route.Opcode = UpdateUnsharded
		return route, nil
	}

	err = getDMLRouting(upd.Where, route)
	if err != nil {
		return nil, err
	}
	route.Opcode = UpdateEqual
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

// isIndexChanging returns true if any of the update
// expressions modify a vindex column.
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

// buildUpdatePlan builds the instructions for a DELETE statement.
func buildDeletePlan(del *sqlparser.Delete, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(del),
	}
	// We allow only one table in a delete.
	tablename := sqlparser.GetTableName(del.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if hasSubquery(del) {
		return nil, errors.New("unsupported: subqueries in DML")
	}
	if !route.Keyspace.Sharded {
		route.Opcode = DeleteUnsharded
		return route, nil
	}

	err = getDMLRouting(del.Where, route)
	if err != nil {
		return nil, err
	}
	route.Opcode = DeleteEqual
	route.Subquery = generateDeleteSubquery(del, route.Table)
	return route, nil
}

// generateDeleteSubquery generates the query to fetch the rows
// that will be deleted. This allows VTGate to clean up any
// owned vindexes as needed.
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

// getDMLRouting updates the route with the necessary routing
// info. If it cannot find a unique route, then it returns an error.
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

// getMatch returns the matched value if there is an equality
// constraint on the specified column that can be used to
// decide on a route.
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
