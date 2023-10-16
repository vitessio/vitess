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

func gen4UpdateStmtPlanner(
	version querypb.ExecuteOptions_PlannerVersion,
	updStmt *sqlparser.Update,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	if updStmt.With != nil {
		return nil, vterrors.VT12001("WITH expression in UPDATE statement")
	}

	ctx, err := plancontext.CreatePlanningContext(updStmt, reservedVars, vschema, version)
	if err != nil {
		return nil, err
	}

	err = rewriteRoutedTables(updStmt, vschema)
	if err != nil {
		return nil, err
	}

	// Remove all the foreign keys that don't require any handling.
	err = ctx.SemTable.RemoveNonRequiredForeignKeys(ctx.VerifyAllFKs, vindexes.UpdateAction)
	if err != nil {
		return nil, err
	}
	if ks, tables := ctx.SemTable.SingleUnshardedKeyspace(); ks != nil {
		if !ctx.SemTable.ForeignKeysPresent() {
			plan := updateUnshardedShortcut(updStmt, ks, tables)
			plan = pushCommentDirectivesOnPlan(plan, updStmt)
			return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
		}
	}

	if ctx.SemTable.NotUnshardedErr != nil {
		return nil, ctx.SemTable.NotUnshardedErr
	}

	err = queryRewrite(ctx.SemTable, reservedVars, updStmt)
	if err != nil {
		return nil, err
	}

	op, err := operators.PlanQuery(ctx, updStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op)
	if err != nil {
		return nil, err
	}

	plan = pushCommentDirectivesOnPlan(plan, updStmt)

	setLockOnAllSelect(plan)

	if err := plan.Wireup(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
}

func updateUnshardedShortcut(stmt *sqlparser.Update, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	edml := engine.NewDML()
	edml.Keyspace = ks
	edml.Opcode = engine.Unsharded
	edml.Query = generateQuery(stmt)
	for _, tbl := range tables {
		edml.TableNames = append(edml.TableNames, tbl.Name.String())
	}
	return &primitiveWrapper{prim: &engine.Update{DML: edml}}
}
