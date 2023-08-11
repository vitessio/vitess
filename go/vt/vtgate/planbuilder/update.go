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
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
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

	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(updStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	err = rewriteRoutedTables(updStmt, vschema)
	if err != nil {
		return nil, err
	}

	if ks, tables := semTable.SingleUnshardedKeyspace(); ks != nil {
		if fkManagementNotRequiredForUpdate(semTable, vschema, tables, updStmt.Exprs) {
			plan := updateUnshardedShortcut(updStmt, ks, tables)
			plan = pushCommentDirectivesOnPlan(plan, updStmt)
			return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
		}
	}

	if semTable.NotUnshardedErr != nil {
		return nil, semTable.NotUnshardedErr
	}

	err = queryRewrite(semTable, reservedVars, updStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	op, err := operators.PlanQuery(ctx, updStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op, true)
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

// TODO: Handle all this in semantic analysis.
func fkManagementNotRequiredForUpdate(semTable *semantics.SemTable, vschema plancontext.VSchema, vTables []*vindexes.Table, updateExprs sqlparser.UpdateExprs) bool {
	childFkMap := make(map[string][]vindexes.ChildFKInfo)

	// Find the foreign key mode and check for any managed child foreign keys.
	for _, vTable := range vTables {
		ksMode, err := vschema.ForeignKeyMode(vTable.Keyspace.Name)
		if err != nil {
			return false
		}
		if ksMode != vschemapb.Keyspace_FK_MANAGED {
			continue
		}
		childFks := vTable.ChildFKsNeedsHandling(vindexes.UpdateAction)
		if len(childFks) > 0 {
			childFkMap[vTable.String()] = childFks
		}
	}

	getChildFKInfo := func(expr *sqlparser.UpdateExpr) []vindexes.ChildFKInfo {
		tblInfo, err := semTable.TableInfoForExpr(expr.Name)
		if err != nil {
			return nil
		}
		vTable := tblInfo.GetVindexTable()
		return childFkMap[vTable.String()]
	}

	// Check if any column in the parent table is being updated which has a child foreign key.
	return !operators.ColumnModified(updateExprs, getChildFKInfo)
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
