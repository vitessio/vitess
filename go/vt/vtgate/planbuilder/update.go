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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// buildUpdatePlan builds the instructions for an UPDATE statement.
func buildUpdatePlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	upd := stmt.(*sqlparser.Update)
	if upd.With != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in update statement")
	}
	if updateColsHasSubquery(upd) {
		return updateSelectSubqueryPlan(upd, reservedVars, vschema)
	}
	dml, ksidVindex, err := buildDMLPlan(vschema, "update", stmt, reservedVars, upd.TableExprs, upd.Where, upd.OrderBy, upd.Limit, upd.Comments, upd.Exprs)
	if err != nil {
		return nil, err
	}
	eupd := &engine.Update{DML: dml}

	if dml.Opcode == engine.Unsharded {
		return eupd, nil
	}

	cvv, ovq, err := buildChangedVindexesValues(upd, eupd.Table, ksidVindex.Columns)
	if err != nil {
		return nil, err
	}
	eupd.ChangedVindexValues = cvv
	eupd.OwnedVindexQuery = ovq
	if len(eupd.ChangedVindexValues) != 0 {
		eupd.KsidVindex = ksidVindex.Vindex
		eupd.KsidLength = len(ksidVindex.Columns)
	}
	return eupd, nil
}

func updateColsHasSubquery(upd *sqlparser.Update) bool {
	for _, updExpr := range upd.Exprs {
		if _, ok := updExpr.Expr.(*sqlparser.Subquery); ok {
			return true
		}
	}
	return false
}

func updateSelectSubqueryPlan(upd *sqlparser.Update, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	edml := &engine.Update{DML: engine.NewDML()}
	pb := newPrimitiveBuilder(vschema, newJointab(reservedVars))
	rb, err := pb.processDMLTable(upd.TableExprs, reservedVars, nil)
	if err != nil {
		return nil, err
	}
	edml.Keyspace = rb.eroute.Keyspace
	if !edml.Keyspace.Sharded {
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		var subqueryArgs []sqlparser.SQLNode
		subqueryArgs = append(subqueryArgs, upd.Where, upd.OrderBy, upd.Limit)
		if pb.finalizeUnshardedDMLSubqueries(reservedVars, subqueryArgs...) {
			vschema.WarnUnshardedOnly("subqueries can't be sharded in DML")
		} else {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: sharded subqueries in DML")
		}
		edml.Opcode = engine.Unsharded
		// Generate query after all the analysis. Otherwise table name substitutions for
		// routed tables won't happen.
		edml.Query = generateQuery(upd)
		return edml, nil
	}

	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.UpdateExprs:
			return false, nil
		case *sqlparser.DerivedTable, *sqlparser.Subquery:
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	for _, updExpr := range upd.Exprs {
		subq, ok := updExpr.Expr.(*sqlparser.Subquery)
		if !ok {
			continue
		}
		// select plan will be taken as input to insert rows into the table.
		plan, err := dmlSubquerySelectPlan(subq.Select, vschema, reservedVars)
		if err != nil {
			return nil, err
		}
		edml.Input = append(edml.Input, plan)
	}

	return edml, nil
}

// buildChangedVindexesValues adds to the plan all the lookup vindexes that are changing.
// Updates can only be performed to secondary lookup vindexes with no complex expressions
// in the set clause.
func buildChangedVindexesValues(update *sqlparser.Update, table *vindexes.Table, ksidCols []sqlparser.ColIdent) (map[string]*engine.VindexValues, string, error) {
	changedVindexes := make(map[string]*engine.VindexValues)
	buf, offset := initialQuery(ksidCols, table)
	for i, vindex := range table.ColumnVindexes {
		vindexValueMap := make(map[string]evalengine.Expr)
		first := true
		for _, vcol := range vindex.Columns {
			// Searching in order of columns in colvindex.
			found := false
			for _, assignment := range update.Exprs {
				if !vcol.Equal(assignment.Name.Name) {
					continue
				}
				if found {
					return nil, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column has duplicate set values: '%v'", assignment.Name.Name)
				}
				found = true
				pv, err := extractValueFromUpdate(assignment)
				if err != nil {
					return nil, "", err
				}
				vindexValueMap[vcol.String()] = pv
				if first {
					buf.Myprintf(", %v", assignment)
					first = false
				} else {
					buf.Myprintf(" and %v", assignment)
				}
			}
		}
		if len(vindexValueMap) == 0 {
			// Vindex not changing, continue
			continue
		}

		if update.Limit != nil && len(update.OrderBy) == 0 {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Need to provide order by clause when using limit. Invalid update on vindex: %v", vindex.Name)
		}
		if i == 0 {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can't update primary vindex columns. Invalid update on vindex: %v", vindex.Name)
		}
		if _, ok := vindex.Vindex.(vindexes.Lookup); !ok {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can only update lookup vindexes. Invalid update on vindex: %v", vindex.Name)
		}
		if !vindex.Owned {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: You can only update owned vindexes. Invalid update on vindex: %v", vindex.Name)
		}
		changedVindexes[vindex.Name] = &engine.VindexValues{
			PvMap:  vindexValueMap,
			Offset: offset,
		}
		offset++
	}
	if len(changedVindexes) == 0 {
		return nil, "", nil
	}
	// generate rest of the owned vindex query.
	aTblExpr, ok := update.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, "", vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: update on complex table expression")
	}
	tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: table.Name}, As: aTblExpr.As}
	buf.Myprintf(" from %v%v%v%v for update", tblExpr, update.Where, update.OrderBy, update.Limit)
	return changedVindexes, buf.String(), nil
}

func initialQuery(ksidCols []sqlparser.ColIdent, table *vindexes.Table) (*sqlparser.TrackedBuffer, int) {
	buf := sqlparser.NewTrackedBuffer(nil)
	offset := 0
	for _, col := range ksidCols {
		if offset == 0 {
			buf.Myprintf("select %v", col)
		} else {
			buf.Myprintf(", %v", col)
		}
		offset++
	}
	for _, cv := range table.Owned {
		for _, column := range cv.Columns {
			buf.Myprintf(", %v", column)
			offset++
		}
	}
	return buf, offset
}

// extractValueFromUpdate given an UpdateExpr attempts to extracts the Value
// it's holding. At the moment it only supports: StrVal, HexVal, IntVal, ValArg.
// If a complex expression is provided (e.g set name = name + 1), the update will be rejected.
func extractValueFromUpdate(upd *sqlparser.UpdateExpr) (evalengine.Expr, error) {
	pv, err := evalengine.Translate(upd.Expr, semantics.EmptySemTable())
	if err != nil || sqlparser.IsSimpleTuple(upd.Expr) {
		err := vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: Only values are supported. Invalid update on column: `%s` with expr: [%s]", upd.Name.Name.String(), sqlparser.String(upd.Expr))
		return nil, err
	}
	return pv, nil
}
