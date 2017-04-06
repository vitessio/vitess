// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// dmlFormatter strips out keyspace name from dmls.
func dmlFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case *sqlparser.TableName:
		node.Name.Format(buf)
		return
	}
	node.Format(buf)
}

// buildUpdatePlan builds the instructions for an UPDATE statement.
func buildUpdatePlan(upd *sqlparser.Update, vschema VSchema) (*engine.Route, error) {
	route := &engine.Route{
		Query: generateQuery(upd),
	}
	updateTable, _ := upd.Table.Expr.(*sqlparser.TableName)

	var err error
	route.Table, err = vschema.Find(updateTable.Qualifier, updateTable.Name)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if hasSubquery(upd) {
		return nil, errors.New("unsupported: subqueries in DML")
	}
	if !route.Keyspace.Sharded {
		route.Opcode = engine.UpdateUnsharded
		return route, nil
	}

	err = getDMLRouting(upd.Where, route)
	if err != nil {
		return nil, err
	}
	route.Opcode = engine.UpdateEqual
	if isIndexChanging(upd.Exprs, route.Table.ColumnVindexes) {
		return nil, errors.New("unsupported: DML cannot change vindex column")
	}
	return route, nil
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}

// isIndexChanging returns true if any of the update
// expressions modify a vindex column.
func isIndexChanging(setClauses sqlparser.UpdateExprs, colVindexes []*vindexes.ColumnVindex) bool {
	for _, assignment := range setClauses {
		for _, vcol := range colVindexes {
			if vcol.Column.Equal(assignment.Name.Name) {
				return true
			}
		}
	}
	return false
}

// buildUpdatePlan builds the instructions for a DELETE statement.
func buildDeletePlan(del *sqlparser.Delete, vschema VSchema) (*engine.Route, error) {
	route := &engine.Route{
		Query: generateQuery(del),
	}
	var err error
	route.Table, err = vschema.Find(del.Table.Qualifier, del.Table.Name)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if hasSubquery(del) {
		return nil, errors.New("unsupported: subqueries in DML")
	}
	if !route.Keyspace.Sharded {
		route.Opcode = engine.DeleteUnsharded
		return route, nil
	}

	err = getDMLRouting(del.Where, route)
	if err != nil {
		return nil, err
	}
	route.Opcode = engine.DeleteEqual
	route.Subquery = generateDeleteSubquery(del, route.Table)
	return route, nil
}

// generateDeleteSubquery generates the query to fetch the rows
// that will be deleted. This allows VTGate to clean up any
// owned vindexes as needed.
func generateDeleteSubquery(del *sqlparser.Delete, table *vindexes.Table) string {
	if len(table.Owned) == 0 {
		return ""
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.WriteString("select ")
	prefix := ""
	for _, cv := range table.Owned {
		buf.Myprintf("%s%v", prefix, cv.Column)
		prefix = ", "
	}
	buf.Myprintf(" from %v%v for update", table.Name, del.Where)
	return buf.String()
}

// getDMLRouting updates the route with the necessary routing
// info. If it cannot find a unique route, then it returns an error.
func getDMLRouting(where *sqlparser.Where, route *engine.Route) error {
	if where == nil {
		return errors.New("unsupported: multi-shard where clause in DML")
	}
	for _, index := range route.Table.Ordered {
		if !vindexes.IsUnique(index.Vindex) {
			continue
		}
		if values := getMatch(where.Expr, index.Column); values != nil {
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
func getMatch(node sqlparser.Expr, col sqlparser.ColIdent) interface{} {
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

func nameMatch(node sqlparser.Expr, col sqlparser.ColIdent) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && colname.Name.Equal(col)
}
