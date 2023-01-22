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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// buildUpdatePlan returns a stmtPlanner that builds the instructions for an UPDATE statement.
func buildUpdatePlan(string) stmtPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
		upd := stmt.(*sqlparser.Update)
		if upd.With != nil {
			return nil, vterrors.VT12001("WITH expression in UPDATE statement")
		}
		dml, tables, ksidVindex, err := buildDMLPlan(vschema, "update", stmt, reservedVars, upd.TableExprs, upd.Where, upd.OrderBy, upd.Limit, upd.Comments, upd.Exprs)
		if err != nil {
			return nil, err
		}
		eupd := &engine.Update{DML: dml}

		if dml.Opcode == engine.Unsharded {
			return newPlanResult(eupd, tables...), nil
		}
		eupdTable, err := eupd.GetSingleTable()
		if err != nil {
			return nil, err
		}
		cvv, ovq, err := buildChangedVindexesValues(upd, eupdTable, ksidVindex.Columns)
		if err != nil {
			return nil, err
		}
		eupd.ChangedVindexValues = cvv
		eupd.OwnedVindexQuery = ovq
		if len(eupd.ChangedVindexValues) != 0 {
			eupd.KsidVindex = ksidVindex.Vindex
			eupd.KsidLength = len(ksidVindex.Columns)
		}
		return newPlanResult(eupd, tables...), nil
	}
}

// buildChangedVindexesValues adds to the plan all the lookup vindexes that are changing.
// Updates can only be performed to secondary lookup vindexes with no complex expressions
// in the set clause.
func buildChangedVindexesValues(update *sqlparser.Update, table *vindexes.Table, ksidCols []sqlparser.IdentifierCI) (map[string]*engine.VindexValues, string, error) {
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
					return nil, "", vterrors.VT03015(assignment.Name.Name)
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
			return nil, "", vterrors.VT12001(fmt.Sprintf("need to provide ORDER BY clause when using LIMIT; invalid update on vindex: %v", vindex.Name))
		}
		if i == 0 {
			return nil, "", vterrors.VT12001(fmt.Sprintf("you cannot update primary vindex columns; invalid update on vindex: %v", vindex.Name))
		}
		if _, ok := vindex.Vindex.(vindexes.Lookup); !ok {
			return nil, "", vterrors.VT12001(fmt.Sprintf("you can only update lookup vindexes; invalid update on vindex: %v", vindex.Name))
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
		return nil, "", vterrors.VT12001("UPDATE on complex table expression")
	}
	tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: table.Name}, As: aTblExpr.As}
	buf.Myprintf(" from %v%v%v%v for update", tblExpr, update.Where, update.OrderBy, update.Limit)
	return changedVindexes, buf.String(), nil
}

func initialQuery(ksidCols []sqlparser.IdentifierCI, table *vindexes.Table) (*sqlparser.TrackedBuffer, int) {
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
		err := vterrors.VT12001(fmt.Sprintf("only values are supported: invalid update on column: `%s` with expr: [%s]", upd.Name.Name.String(), sqlparser.String(upd.Expr)))
		return nil, err
	}
	return pv, nil
}
