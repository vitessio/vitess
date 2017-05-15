/*
Copyright 2017 Google Inc.

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
	case sqlparser.TableName:
		node.Name.Format(buf)
		return
	}
	node.Format(buf)
}

// buildUpdatePlan builds the instructions for an UPDATE statement.
func buildUpdatePlan(upd *sqlparser.Update, vschema VSchema) (*engine.Route, error) {
	bldr, err := processTableExprs(upd.TableExprs, vschema)
	rb, ok := bldr.(*route)
	if !ok {
		return nil, errors.New("unsupported: update statement spans multiple shards")
	}
	if hasSubquery(upd) {
		return nil, errors.New("unsupported: subqueries in DML")
	}
	rb.ERoute.Query = generateQuery(upd)
	if !rb.ERoute.Keyspace.Sharded {
		rb.ERoute.Opcode = engine.UpdateUnsharded
		return rb.ERoute, nil
	}
	if rb.ERoute.Opcode != engine.SelectEqualUnique {
		return nil, errors.New("unsupported: update statement spans multiple shards")
	}
	err = getDMLRouting(upd.Where, rb.ERoute)
	if err != nil {
		return nil, err
	}
	rb.ERoute.Opcode = engine.UpdateEqual
	if isIndexChanging(upd.Exprs, rb.ERoute.Table.ColumnVindexes) {
		return nil, errors.New("unsupported: DML cannot change vindex column")
	}
	return rb.ERoute, nil
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
	bldr, err := processTableExprs(del.TableExprs, vschema)
	rb, ok := bldr.(*route)
	if !ok {
		return nil, errors.New("unsupported: update statement spans multiple shards")
	}
	if hasSubquery(del) {
		return nil, errors.New("unsupported: subqueries in DML")
	}
	rb.ERoute.Query = generateQuery(del)
	if !rb.ERoute.Keyspace.Sharded {
		rb.ERoute.Opcode = engine.DeleteUnsharded
		return rb.ERoute, nil
	}
	if rb.ERoute.Opcode != engine.SelectEqualUnique {
		return nil, errors.New("unsupported: update statement spans multiple shards")
	}
	err = getDMLRouting(del.Where, rb.ERoute)
	if err != nil {
		return nil, err
	}
	rb.ERoute.Opcode = engine.DeleteEqual
	rb.ERoute.Subquery = generateDeleteSubquery(del, rb.ERoute.Table)
	return rb.ERoute, nil
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
