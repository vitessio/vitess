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

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
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
	er := &engine.Route{
		Query:               generateQuery(upd),
		ChangedVindexValues: make(map[string]sqltypes.PlanValue),
	}
	bldr, err := processTableExprs(upd.TableExprs, vschema)
	if err != nil {
		return nil, err
	}
	rb, ok := bldr.(*route)
	if !ok {
		return nil, errors.New("unsupported: multi-table update statement in sharded keyspace")
	}

	er.Keyspace = rb.ERoute.Keyspace
	if !er.Keyspace.Sharded {
		if !validateSubquerySamePlan(upd, rb.ERoute, vschema) {
			return nil, errors.New("unsupported: sharded subqueries in DML")
		}
		er.Opcode = engine.UpdateUnsharded
		return er, nil
	}

	if hasSubquery(upd) {
		return nil, errors.New("unsupported: subqueries in sharded DML")
	}
	if len(rb.Symtab().tables) != 1 {
		return nil, errors.New("unsupported: multi-table update statement in sharded keyspace")
	}

	var tableName sqlparser.TableName
	for t := range rb.Symtab().tables {
		tableName = t
	}
	er.Table, err = vschema.Find(tableName)
	if err != nil {
		return nil, err
	}
	err = getDMLRouting(upd.Where, er)
	if err != nil {
		return nil, err
	}
	er.Opcode = engine.UpdateEqual

	if isSharedVindexChanging(upd.Exprs, er.Table.ColumnVindexes) {
		return nil, errors.New("unsupported: DML cannot change vindex column that is shared by other tables")

	}

	err = addChangedVindexesValues(er, upd.Exprs, er.Table.Owned)

	if err != nil {
		return nil, err
	}

	if len(er.ChangedVindexValues) != 0 {
		er.Subquery = generateUpdateSubquery(upd, er.Table)
	}
	return er, nil
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}

// isSharedVindexChanging returns true if any of the update
// expressions modifies a vindex column that is shared by other tables.
func isSharedVindexChanging(setClauses sqlparser.UpdateExprs, colVindexes []*vindexes.ColumnVindex) bool {
	for _, assignment := range setClauses {
		for _, vcol := range colVindexes {
			if _, ok := vcol.Vindex.(vindexes.Lookup); ok && vcol.Column.Equal(assignment.Name.Name) && !vcol.Exclusive {
				return true
			}
		}
	}
	return false
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

func addChangedVindexesValues(route *engine.Route, setClauses sqlparser.UpdateExprs, ownColVindexes []*vindexes.ColumnVindex) error {
	for _, assignment := range setClauses {
		for _, vcol := range ownColVindexes {
			if vcol.Column.Equal(assignment.Name.Name) {
				pv, ok := extractValueFromUpdate(assignment, vcol.Column)
				if !ok {
					return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Only values are supported. Invalid update on column: %v", assignment.Name.Name)
				}
				route.ChangedVindexValues[vcol.Name] = pv
			}
		}
	}
	return nil
}

// buildUpdatePlan builds the instructions for a DELETE statement.
func buildDeletePlan(del *sqlparser.Delete, vschema VSchema) (*engine.Route, error) {
	er := &engine.Route{
		Query: generateQuery(del),
	}
	bldr, err := processTableExprs(del.TableExprs, vschema)
	if err != nil {
		return nil, err
	}
	rb, ok := bldr.(*route)
	if !ok {
		return nil, errors.New("unsupported: multi-table delete statement in sharded keyspace")
	}
	er.Keyspace = rb.ERoute.Keyspace
	if !er.Keyspace.Sharded {
		if !validateSubquerySamePlan(del, rb.ERoute, vschema) {
			return nil, errors.New("unsupported: sharded subqueries in DML")
		}
		er.Opcode = engine.DeleteUnsharded
		return er, nil
	}
	if del.Targets != nil || len(rb.Symtab().tables) != 1 {
		return nil, errors.New("unsupported: multi-table delete statement in sharded keyspace")
	}
	if hasSubquery(del) {
		return nil, errors.New("unsupported: subqueries in sharded DML")
	}
	var tableName sqlparser.TableName
	for t := range rb.Symtab().tables {
		tableName = t
	}
	er.Table, err = vschema.Find(tableName)
	if err != nil {
		return nil, err
	}
	err = getDMLRouting(del.Where, er)
	if err != nil {
		return nil, err
	}
	er.Opcode = engine.DeleteEqual
	er.Subquery = generateDeleteSubquery(del, er.Table)
	return er, nil
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

func generateUpdateSubquery(upd *sqlparser.Update, table *vindexes.Table) string {
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
	buf.Myprintf(" from %v%v for update", table.Name, upd.Where)
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
		if pv, ok := getMatch(where.Expr, index.Column); ok {
			route.Vindex = index.Vindex
			route.Values = []sqltypes.PlanValue{pv}
			return nil
		}
	}
	return errors.New("unsupported: multi-shard where clause in DML")
}

func extractValueFromUpdate(upd *sqlparser.UpdateExpr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, ok bool) {
	if !sqlparser.IsValue(upd.Expr) {
		return sqltypes.PlanValue{}, false
	}
	pv, err := sqlparser.NewPlanValue(upd.Expr)

	if err != nil {
		return sqltypes.PlanValue{}, false
	}
	return pv, true
}

// getMatch returns the matched value if there is an equality
// constraint on the specified column that can be used to
// decide on a route.
func getMatch(node sqlparser.Expr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, ok bool) {
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
		pv, err := sqlparser.NewPlanValue(comparison.Right)
		if err != nil {
			continue
		}
		return pv, true
	}
	return sqltypes.PlanValue{}, false
}

func nameMatch(node sqlparser.Expr, col sqlparser.ColIdent) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && colname.Name.Equal(col)
}
