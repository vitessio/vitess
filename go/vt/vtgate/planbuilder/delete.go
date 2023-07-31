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
	"vitess.io/vitess/go/vt/vtgate/semantics"
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

	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(deleteStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}

	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)
	err = rewriteRoutedTables(deleteStmt, vschema)
	if err != nil {
		return nil, err
	}

	if ks, tables := semTable.SingleUnshardedKeyspace(); ks != nil {
		plan := deleteUnshardedShortcut(deleteStmt, ks, tables)
		plan = pushCommentDirectivesOnPlan(plan, deleteStmt)
		return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
	}

	if err := checkIfDeleteSupported(deleteStmt, semTable); err != nil {
		return nil, err
	}

	err = queryRewrite(semTable, reservedVars, deleteStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)
	op, err := operators.PlanQuery(ctx, deleteStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, err
	}

	plan = pushCommentDirectivesOnPlan(plan, deleteStmt)

	setLockOnAllSelect(plan)

	if err := plan.Wireup(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
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

func deleteUnshardedShortcut(stmt *sqlparser.Delete, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	edml := engine.NewDML()
	edml.Keyspace = ks
	edml.Table = tables
	edml.Opcode = engine.Unsharded
	edml.Query = generateQuery(stmt)
	return &primitiveWrapper{prim: &engine.Delete{DML: edml}}
}

// checkIfDeleteSupported checks if the delete query is supported or we must return an error.
func checkIfDeleteSupported(del *sqlparser.Delete, semTable *semantics.SemTable) error {
	if semTable.NotUnshardedErr != nil {
		return semTable.NotUnshardedErr
	}

	// Delete is only supported for a single TableExpr which is supposed to be an aliased expression
	multiShardErr := vterrors.VT12001("multi-shard or vindex write statement")
	if len(del.TableExprs) != 1 {
		return multiShardErr
	}
	_, isAliasedExpr := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !isAliasedExpr {
		return multiShardErr
	}

	if len(del.Targets) > 1 {
		return vterrors.VT12001("multi-table DELETE statement in a sharded keyspace")
	}

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.Subquery, *sqlparser.DerivedTable:
			// We have a subquery, so we must fail the planning.
			// If this subquery and the table expression were all belonging to the same unsharded keyspace,
			// we would have already created a plan for them before doing these checks.
			return false, vterrors.VT12001("subqueries in DML")
		}
		return true, nil
	}, del)
	if err != nil {
		return err
	}

	return nil
}
