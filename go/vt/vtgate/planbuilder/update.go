/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// buildUpdatePlan builds the instructions for an UPDATE statement.
func buildUpdatePlan(upd *sqlparser.Update, vschema ContextVSchema) (_ *engine.Update, needsLastInsertID bool, needDbName bool, _ error) {
	eupd := &engine.Update{
		ChangedVindexValues: make(map[string][]sqltypes.PlanValue),
	}
	pb := newPrimitiveBuilder(vschema, newJointab(sqlparser.GetBindvars(upd)))
	err := rewriteExpressions(pb, upd.Exprs)
	if err != nil {
		return nil, false, false, err
	}
	ro, err := pb.processDMLTable(upd.TableExprs)
	if err != nil {
		return nil, false, false, err
	}
	eupd.Keyspace = ro.eroute.Keyspace
	if !eupd.Keyspace.Sharded {
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		if !pb.finalizeUnshardedDMLSubqueries(upd.Exprs, upd.Where, upd.OrderBy, upd.Limit) {
			return nil, false, false, errors.New("unsupported: sharded subqueries in DML")
		}
		eupd.Opcode = engine.UpdateUnsharded
		// Generate query after all the analysis. Otherwise table name substitutions for
		// routed tables won't happen.
		eupd.Query = generateQuery(upd)
		return eupd, pb.needsLastInsertID, pb.needsDbName, nil
	}

	if hasSubquery(upd) {
		return nil, false, false, errors.New("unsupported: subqueries in sharded DML")
	}
	if len(pb.st.tables) != 1 {
		return nil, false, false, errors.New("unsupported: multi-table update statement in sharded keyspace")
	}

	// Generate query after all the analysis. Otherwise table name substitutions for
	// routed tables won't happen.
	eupd.Query = generateQuery(upd)

	directives := sqlparser.ExtractCommentDirectives(upd.Comments)
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		eupd.MultiShardAutocommit = true
	}

	eupd.QueryTimeout = queryTimeout(directives)
	eupd.Table = ro.vschemaTable
	if eupd.Table == nil {
		return nil, false, false, errors.New("internal error: table.vindexTable is mysteriously nil")
	}

	if ro.eroute.TargetDestination != nil {
		if ro.eroute.TargetTabletType != topodatapb.TabletType_MASTER {
			return nil, false, false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported: UPDATE statement with a replica target")
		}
		eupd.Opcode = engine.UpdateByDestination
		eupd.TargetDestination = ro.eroute.TargetDestination
		return eupd, pb.needsLastInsertID, pb.needsDbName, nil
	}

	eupd.Vindex, eupd.Values, err = getDMLRouting(upd.Where, eupd.Table)
	if err != nil {
		eupd.Opcode = engine.UpdateScatter
	} else {
		eupd.Opcode = engine.UpdateEqual
	}

	if eupd.Opcode == engine.UpdateScatter {
		if len(eupd.Table.Owned) != 0 {
			return eupd, false, false, errors.New("unsupported: multi shard update on a table with owned lookup vindexes")
		}
		if upd.Limit != nil {
			return eupd, false, false, errors.New("unsupported: multi shard update with limit")
		}
	}

	if eupd.ChangedVindexValues, err = buildChangedVindexesValues(eupd, upd, eupd.Table.ColumnVindexes); err != nil {
		return nil, false, false, err
	}
	if len(eupd.ChangedVindexValues) != 0 {
		eupd.OwnedVindexQuery = generateUpdateSubquery(upd, eupd.Table)
	}
	return eupd, pb.needsLastInsertID, pb.needsDbName, nil
}

func rewriteExpressions(pb *primitiveBuilder, exprs sqlparser.UpdateExprs) error {
	for _, e := range exprs {
		rewritten, err := RewriteAndUpdateBuilder(e.Expr, pb)
		if err != nil {
			return err
		}
		e.Expr = rewritten
	}
	return nil
}

// buildChangedVindexesValues adds to the plan all the lookup vindexes that are changing.
// Updates can only be performed to secondary lookup vindexes with no complex expressions
// in the set clause.
func buildChangedVindexesValues(eupd *engine.Update, update *sqlparser.Update, colVindexes []*vindexes.ColumnVindex) (map[string][]sqltypes.PlanValue, error) {
	changedVindexes := make(map[string][]sqltypes.PlanValue)
	for i, vindex := range colVindexes {
		var vindexValues []sqltypes.PlanValue
		for _, vcol := range vindex.Columns {
			// Searching in order of columns in colvindex.
			found := false
			for _, assignment := range update.Exprs {
				if !vcol.Equal(assignment.Name.Name) {
					continue
				}
				if found {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column has duplicate set values: '%v'", assignment.Name.Name)
				}
				found = true
				pv, err := extractValueFromUpdate(assignment)
				if err != nil {
					return nil, err
				}
				vindexValues = append(vindexValues, pv)
			}
		}
		if len(vindexValues) == 0 {
			// Vindex not changing, continue
			continue
		}
		if len(vindexValues) != len(vindex.Columns) {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: update does not have values for all the columns in vindex (%s)", vindex.Name)
		}

		if update.Limit != nil && len(update.OrderBy) == 0 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Need to provide order by clause when using limit. Invalid update on vindex: %v", vindex.Name)
		}
		if i == 0 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can't update primary vindex columns. Invalid update on vindex: %v", vindex.Name)
		}
		if _, ok := vindex.Vindex.(vindexes.Lookup); !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can only update lookup vindexes. Invalid update on vindex: %v", vindex.Name)
		}
		if !vindex.Owned {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can only update owned vindexes. Invalid update on vindex: %v", vindex.Name)
		}
		changedVindexes[vindex.Name] = vindexValues
	}

	return changedVindexes, nil
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

// extractValueFromUpdate given an UpdateExpr attempts to extracts the Value
// it's holding. At the moment it only supports: StrVal, HexVal, IntVal, ValArg.
// If a complex expression is provided (e.g set name = name + 1), the update will be rejected.
func extractValueFromUpdate(upd *sqlparser.UpdateExpr) (pv sqltypes.PlanValue, err error) {
	if !sqlparser.IsValue(upd.Expr) && !sqlparser.IsNull(upd.Expr) {
		err := vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Only values are supported. Invalid update on column: %v", upd.Name.Name)
		return sqltypes.PlanValue{}, err
	}
	return sqlparser.NewPlanValue(upd.Expr)
}

// dmlFormatter strips out keyspace name from dmls.
func dmlFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case sqlparser.TableName:
		node.Name.Format(buf)
		return
	}
	node.Format(buf)
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}

// getDMLRouting returns the vindex and values for the DML,
// If it cannot find a unique vindex match, it returns an error.
func getDMLRouting(where *sqlparser.Where, table *vindexes.Table) (vindexes.SingleColumn, []sqltypes.PlanValue, error) {
	if where == nil {
		return nil, nil, errors.New("unsupported: multi-shard where clause in DML")
	}
	for _, index := range table.Ordered {
		if !index.Vindex.IsUnique() {
			continue
		}
		single, ok := index.Vindex.(vindexes.SingleColumn)
		if !ok {
			continue
		}
		if pv, ok := getMatch(where.Expr, index.Columns[0]); ok {
			return single, []sqltypes.PlanValue{pv}, nil
		}
	}
	return nil, nil, errors.New("unsupported: multi-shard where clause in DML")
}

// getMatch returns the matched value if there is an equality
// constraint on the specified column that can be used to
// decide on a route.
func getMatch(node sqlparser.Expr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, ok bool) {
	filters := splitAndExpression(nil, node)
	for _, filter := range filters {
		filter = skipParenthesis(filter)
		if parenthesis, ok := node.(*sqlparser.ParenExpr); ok {
			if pv, ok := getMatch(parenthesis.Expr, col); ok {
				return pv, ok
			}
			continue
		}
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
