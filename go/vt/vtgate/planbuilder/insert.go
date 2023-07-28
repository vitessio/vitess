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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func gen4InsertStmtPlanner(version querypb.ExecuteOptions_PlannerVersion, insStmt *sqlparser.Insert, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(insStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	err = rewriteRoutedTables(insStmt, vschema)
	if err != nil {
		return nil, err
	}
	// remove any alias added from routing table.
	// insert query does not support table alias.
	insStmt.Table.As = sqlparser.NewIdentifierCS("")

	// Check single unsharded. Even if the table is for single unsharded but sequence table is used.
	// We cannot shortcut here as sequence column needs additional planning.
	ks, tables := semTable.SingleUnshardedKeyspace()
	if ks != nil && tables[0].AutoIncrement == nil {
		plan := insertUnshardedShortcut(insStmt, ks, tables)
		plan = pushCommentDirectivesOnPlan(plan, insStmt)
		return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
	}

	tblInfo, err := semTable.TableInfoFor(semTable.TableSetFor(insStmt.Table))
	if err != nil {
		return nil, err
	}
	if tblInfo.GetVindexTable().Keyspace.Sharded && semTable.NotUnshardedErr != nil {
		return nil, semTable.NotUnshardedErr
	}

	err = queryRewrite(semTable, reservedVars, insStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	op, err := operators.PlanQuery(ctx, insStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, err
	}

	plan = pushCommentDirectivesOnPlan(plan, insStmt)

	setLockOnAllSelect(plan)

	if err := plan.Wireup(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
}

func insertUnshardedShortcut(stmt *sqlparser.Insert, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	eIns := &engine.Insert{}
	eIns.Keyspace = ks
	eIns.Table = tables[0]
	eIns.Opcode = engine.InsertUnsharded
	eIns.Query = generateQuery(stmt)
	return &insert{eInsert: eIns}
}

type insert struct {
	eInsert *engine.Insert
	source  logicalPlan
}

var _ logicalPlan = (*insert)(nil)

func (i *insert) Wireup(ctx *plancontext.PlanningContext) error {
	if i.source == nil {
		return nil
	}
	return i.source.Wireup(ctx)
}

func (i *insert) Primitive() engine.Primitive {
	if i.source != nil {
		i.eInsert.Input = i.source.Primitive()
	}
	return i.eInsert
}

func (i *insert) Inputs() []logicalPlan {
	if i.source == nil {
		return nil
	}
	return []logicalPlan{i.source}
}

func (i *insert) Rewrite(inputs ...logicalPlan) error {
	panic("does not expect insert to get rewrite call")
}

func (i *insert) ContainsTables() semantics.TableSet {
	panic("does not expect insert to get contains tables call")
}

func (i *insert) OutputColumns() []sqlparser.SelectExpr {
	panic("does not expect insert to get output columns call")
}
