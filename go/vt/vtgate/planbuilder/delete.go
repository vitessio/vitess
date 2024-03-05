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
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func gen4DeleteStmtPlanner(
	version querypb.ExecuteOptions_PlannerVersion,
	deleteStmt *sqlparser.Delete,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	if deleteStmt.With != nil {
		return nil, vterrors.VT12001("WITH expression in DELETE statement")
	}

	var err error
	if len(deleteStmt.TableExprs) == 1 && len(deleteStmt.Targets) == 1 {
		deleteStmt, err = rewriteSingleTbl(deleteStmt)
		if err != nil {
			return nil, err
		}
	}

	ctx, err := plancontext.CreatePlanningContext(deleteStmt, reservedVars, vschema, version)
	if err != nil {
		return nil, err
	}

	err = queryRewrite(ctx.SemTable, reservedVars, deleteStmt)
	if err != nil {
		return nil, err
	}

	// Remove all the foreign keys that don't require any handling.
	err = ctx.SemTable.RemoveNonRequiredForeignKeys(ctx.VerifyAllFKs, vindexes.DeleteAction)
	if err != nil {
		return nil, err
	}

	if ks, tables := ctx.SemTable.SingleUnshardedKeyspace(); ks != nil {
		if !ctx.SemTable.ForeignKeysPresent() {
			plan := deleteUnshardedShortcut(deleteStmt, ks, tables)
			return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
		}
	}

	// error out here if delete query cannot bypass the planner and
	// planner cannot plan such query due to different reason like missing full information, etc.
	if ctx.SemTable.NotUnshardedErr != nil {
		return nil, ctx.SemTable.NotUnshardedErr
	}

	op, err := operators.PlanQuery(ctx, deleteStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op)
	if err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
}

func rewriteSingleTbl(del *sqlparser.Delete) (*sqlparser.Delete, error) {
	atExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return del, nil
	}
	if atExpr.As.NotEmpty() && !sqlparser.Equals.IdentifierCS(del.Targets[0].Name, atExpr.As) {
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

func deleteUnshardedShortcut(stmt *sqlparser.Delete, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	edml := engine.NewDML()
	edml.Keyspace = ks
	edml.Opcode = engine.Unsharded
	edml.Query = generateQuery(stmt)
	for _, tbl := range tables {
		edml.TableNames = append(edml.TableNames, tbl.Name.String())
	}
	return &primitiveWrapper{prim: &engine.Delete{DML: edml}}
}
