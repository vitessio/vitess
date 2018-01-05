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
		ChangedVindexValues: make(map[string][]sqltypes.PlanValue),
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
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		if !validateSubquerySamePlan(rb.ERoute, vschema, upd.Exprs, upd.Where, upd.OrderBy, upd.Limit) {
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
	er.Table, err = vschema.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	err = getDMLRouting(upd.Where, er)
	if err != nil {
		return nil, err
	}
	er.Opcode = engine.UpdateEqual

	if er.ChangedVindexValues, err = buildChangedVindexesValues(er, upd, er.Table.ColumnVindexes); err != nil {
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

// buildChangedVindexesValues adds to the plan all the lookup vindexes that are changing.
// Updates can only be performed to secondary lookup vindexes with no complex expressions
// in the set clause.
func buildChangedVindexesValues(route *engine.Route, update *sqlparser.Update, colVindexes []*vindexes.ColumnVindex) (map[string][]sqltypes.PlanValue, error) {
	changedVindexes := make(map[string][]sqltypes.PlanValue)
	for i, vindex := range colVindexes {
		for _, assignment := range update.Exprs {
			for _, vcol := range vindex.Columns {
				if vcol.Equal(assignment.Name.Name) {
					pv, err := extractValueFromUpdate(assignment, vcol)
					if err != nil {
						return changedVindexes, err
					}
					changedVindexes[vindex.Name] = append(changedVindexes[vindex.Name], pv)
				}
			}
		}
		if len(changedVindexes[vindex.Name]) == 0 {
			// Vindex not changing, continue
			continue
		}
		if len(changedVindexes[vindex.Name]) != len(vindex.Columns) {
			return changedVindexes, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: update does not have values for all the columns in vindex (%s)", vindex.Name)
		}

		if update.Limit != nil && len(update.OrderBy) == 0 {
			return changedVindexes, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Need to provide order by clause when using limit. Invalid update on vindex: %v", vindex.Name)
		}
		if i == 0 {
			return changedVindexes, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can't update primary vindex columns. Invalid update on vindex: %v", vindex.Name)
		}
		if _, ok := vindex.Vindex.(vindexes.Lookup); !ok {
			return changedVindexes, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can only update lookup vindexes. Invalid update on vindex: %v", vindex.Name)
		}
		if !vindex.Owned {
			return changedVindexes, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can only update owned vindexes. Invalid update on vindex: %v", vindex.Name)
		}
	}

	return changedVindexes, nil
}

// buildDeletePlan builds the instructions for a DELETE statement.
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
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		if !validateSubquerySamePlan(rb.ERoute, vschema, del.Targets, del.Where, del.OrderBy, del.Limit) {
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
	er.Table, err = vschema.FindTable(tableName)
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
	for vIdx, cv := range table.Owned {
		for cIdx, column := range cv.Columns {
			if cIdx == 0 && vIdx == 0 {
				buf.Myprintf("%v", column)
			} else {
				buf.Myprintf(", %v", column)
			}
		}
	}
	buf.Myprintf(" from %v%v for update", table.Name, del.Where)
	return buf.String()
}

func generateUpdateSubquery(upd *sqlparser.Update, table *vindexes.Table) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.WriteString("select ")
	for vIdx, cv := range table.Owned {
		for cIdx, column := range cv.Columns {
			if cIdx == 0 && vIdx == 0 {
				buf.Myprintf("%v", column)
			} else {
				buf.Myprintf(", %v", column)
			}
		}
	}
	buf.Myprintf(" from %v%v%v%v for update", table.Name, upd.Where, upd.OrderBy, upd.Limit)
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
		if pv, ok := getMatch(where.Expr, index.Columns[0]); ok {
			route.Vindex = index.Vindex
			route.Values = []sqltypes.PlanValue{pv}
			return nil
		}
	}
	return errors.New("unsupported: multi-shard where clause in DML")
}

// extractValueFromUpdate given an UpdateExpr attempts to extracts the Value
// it's holding. At the moment it only supports: StrVal, HexVal, IntVal, ValArg.
// If a complex expression is provided (e.g set name = name + 1), the update will be rejected.
func extractValueFromUpdate(upd *sqlparser.UpdateExpr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, err error) {
	if !sqlparser.IsValue(upd.Expr) {
		err := vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Only values are supported. Invalid update on column: %v", upd.Name.Name)
		return sqltypes.PlanValue{}, err
	}
	return sqlparser.NewPlanValue(upd.Expr)
}

// getMatch returns the matched value if there is an equality
// constraint on the specified column that can be used to
// decide on a route.
func getMatch(node sqlparser.Expr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, ok bool) {
	filters := splitAndExpression(nil, node)
	for _, filter := range filters {
		filter = skipParenthesis(filter)
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
