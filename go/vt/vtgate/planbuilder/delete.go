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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// buildDeletePlan builds the instructions for a DELETE statement.
func buildDeletePlan(string) stmtPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
		del := stmt.(*sqlparser.Delete)
		if del.With != nil {
			return nil, vterrors.VT12001("WITH expression in DELETE statement")
		}
		err := checkUnsupportedExpressions(del)
		if err != nil {
			return nil, err
		}
		if len(del.TableExprs) == 1 && len(del.Targets) == 1 {
			del, err = rewriteSingleTbl(del)
			if err != nil {
				return nil, err
			}
		}
		dml, tables, ksidVindex, err := buildDMLPlan(vschema, "delete", del, reservedVars, del.TableExprs, del.Where, del.OrderBy, del.Limit, del.Comments, del.Targets)
		if err != nil {
			return nil, err
		}
		edel := &engine.Delete{DML: dml}
		if dml.Opcode == engine.Unsharded {
			return newPlanResult(edel, tables...), nil
		}

		if len(del.Targets) > 1 {
			return nil, vterrors.VT12001("multi-table DELETE statement in a sharded keyspace")
		}

		edelTable, err := edel.GetSingleTable()
		if err != nil {
			return nil, err
		}
		if len(del.Targets) == 1 && del.Targets[0].Name != edelTable.Name {
			return nil, vterrors.VT03003(del.Targets[0].Name.String())
		}

		if len(edelTable.Owned) > 0 {
			aTblExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
			if !ok {
				return nil, vterrors.VT12001("deleting from a complex table expression")
			}
			tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: edelTable.Name}, As: aTblExpr.As}
			edel.OwnedVindexQuery = generateDMLSubquery(tblExpr, del.Where, del.OrderBy, del.Limit, edelTable, ksidVindex.Columns)
			edel.KsidVindex = ksidVindex.Vindex
			edel.KsidLength = len(ksidVindex.Columns)
		}

		return newPlanResult(edel, tables...), nil
	}
}

func rewriteSingleTbl(del *sqlparser.Delete) (*sqlparser.Delete, error) {
	atExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return del, nil
	}
	if !atExpr.As.IsEmpty() && !sqlparser.Equals.IdentifierCS(del.Targets[0].Name, atExpr.As) {
		// Unknown table in MULTI DELETE
		return nil, vterrors.VT03003(del.Targets[0].Name.String())
	}

	tbl, ok := atExpr.Expr.(sqlparser.TableName)
	if !ok {
		// derived table
		return nil, vterrors.VT03004(atExpr.As.String())
	}
	if atExpr.As.IsEmpty() && !sqlparser.Equals.IdentifierCS(del.Targets[0].Name, tbl.Name) {
		// Unknown table in MULTI DELETE
		return nil, vterrors.VT03003(del.Targets[0].Name.String())
	}

	del.TableExprs = sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: tbl}}
	del.Targets = nil
	if del.Where != nil {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			col, ok := node.(*sqlparser.ColName)
			if !ok {
				return true, nil
			}
			if !col.Qualifier.IsEmpty() {
				col.Qualifier = tbl
			}
			return true, nil
		}, del.Where)
	}
	return del, nil
}
