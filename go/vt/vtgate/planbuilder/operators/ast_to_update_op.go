/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func createOperatorFromUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update) (ops.Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, updStmt.TableExprs[0], updStmt.Where)
	if err != nil {
		return nil, err
	}

	vindexTable, routing, err := buildVindexTableForDML(ctx, tableInfo, qt, "update")
	if err != nil {
		return nil, err
	}

	updClone := sqlparser.CloneRefOfUpdate(updStmt)
	updOp, err := createUpdateOperator(ctx, updStmt, vindexTable, qt, routing)
	if err != nil {
		return nil, err
	}

	ksMode, err := ctx.VSchema.ForeignKeyMode(vindexTable.Keyspace.Name)
	if err != nil {
		return nil, err
	}
	// Unmanaged foreign-key-mode, we don't need to do anything.
	if ksMode != vschemapb.Keyspace_FK_MANAGED {
		return updOp, nil
	}

	parentFks, childFks := getFKRequirementsForUpdate(updStmt.Exprs, vindexTable)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return updOp, nil
	}

	// If the delete statement has a limit, we don't support it yet.
	if updStmt.Limit != nil {
		return nil, vterrors.VT12001("foreign keys management at vitess with limit")
	}

	return buildFkOperator(ctx, updOp, updClone, parentFks, childFks)
}

func createUpdateOperator(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update, vindexTable *vindexes.Table, qt *QueryTable, routing Routing) (ops.Operator, error) {
	assignments := make(map[string]sqlparser.Expr)
	for _, set := range updStmt.Exprs {
		assignments[set.Name.Name.String()] = set.Expr
	}

	vp, cvv, ovq, err := getUpdateVindexInformation(updStmt, vindexTable, qt.ID, qt.Predicates)
	if err != nil {
		return nil, err
	}

	tr, ok := routing.(*ShardedRouting)
	if ok {
		tr.VindexPreds = vp
	}

	for _, predicate := range qt.Predicates {
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && updStmt.Limit != nil {
		// TODO systay: we should probably check for other op code types - IN could also hit multiple shards (2022-04-07)
		return nil, vterrors.VT12001("multi shard UPDATE with LIMIT")
	}

	r := &Route{
		Source: &Update{
			QTable:              qt,
			VTable:              vindexTable,
			Assignments:         assignments,
			ChangedVindexValues: cvv,
			OwnedVindexQuery:    ovq,
			AST:                 updStmt,
		},
		Routing: routing,
	}

	subq, err := createSubqueryFromStatement(ctx, updStmt)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return r, nil
	}
	subq.Outer = r
	return subq, nil
}

// getFKRequirementsForUpdate analyzes update expressions to determine which foreign key constraints needs management at the VTGate.
// It identifies parent and child foreign keys that require verification or cascade operations due to column updates.
func getFKRequirementsForUpdate(updateExprs sqlparser.UpdateExprs, vindexTable *vindexes.Table) ([]vindexes.ParentFKInfo, []vindexes.ChildFKInfo) {
	parentFks := vindexTable.ParentFKsNeedsHandling()
	childFks := vindexTable.ChildFKsNeedsHandling(vindexes.UpdateAction)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return nil, nil
	}

	pFksRequired := make([]bool, len(parentFks))
	cFksRequired := make([]bool, len(childFks))
	// Go over all the update expressions
	for _, updateExpr := range updateExprs {
		// Any foreign key to a child table for a column that has been updated
		// will require the cascade operations to happen, so we include all such foreign keys.
		for idx, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				cFksRequired[idx] = true
			}
		}
		// If we are setting a column to NULL, then we don't need to verify the existance of an
		// equivalent row in the parent table, even if this column was part of a foreign key to a parent table.
		if sqlparser.IsNull(updateExpr.Expr) {
			continue
		}
		// We add all the possible parent foreign key constraints that need verification that an equivalent row
		// exists, given that this column has changed.
		for idx, parentFk := range parentFks {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[idx] = true
			}
		}
	}
	// For the parent foreign keys, if any of the columns part of the fk is set to NULL,
	// then, we don't care for the existance of an equivalent row in the parent table.
	for idx, parentFk := range parentFks {
		for _, updateExpr := range updateExprs {
			if !sqlparser.IsNull(updateExpr.Expr) {
				continue
			}
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[idx] = false
			}
		}
	}
	// Get the filtered lists and return them.
	var pFksNeedsHandling []vindexes.ParentFKInfo
	var cFksNeedsHandling []vindexes.ChildFKInfo
	for idx, parentFk := range parentFks {
		if pFksRequired[idx] {
			pFksNeedsHandling = append(pFksNeedsHandling, parentFk)
		}
	}
	for idx, childFk := range childFks {
		if cFksRequired[idx] {
			cFksNeedsHandling = append(cFksNeedsHandling, childFk)
		}
	}
	return pFksNeedsHandling, cFksNeedsHandling
}

func buildFkOperator(ctx *plancontext.PlanningContext, updOp ops.Operator, updClone *sqlparser.Update, parentFks []vindexes.ParentFKInfo, childFks []vindexes.ChildFKInfo) (ops.Operator, error) {
	op, err := createFKCascadeOp(ctx, updOp, updClone, childFks)
	if err != nil {
		return nil, err
	}

	return createFKVerifyOp(ctx, op, updClone, parentFks)
}

func createFKCascadeOp(ctx *plancontext.PlanningContext, parentOp ops.Operator, updStmt *sqlparser.Update, childFks []vindexes.ChildFKInfo) (ops.Operator, error) {
	if len(childFks) == 0 {
		return parentOp, nil
	}

	// We only support simple expressions in update queries with cascade.
	for _, updateExpr := range updStmt.Exprs {
		switch updateExpr.Expr.(type) {
		case *sqlparser.Argument, *sqlparser.NullVal, sqlparser.BoolVal, *sqlparser.Literal:
		default:
			return nil, vterrors.VT12001("foreign keys management at vitess with non-literal values")
		}
	}

	var fkChildren []*FkChild
	var selectExprs []sqlparser.SelectExpr

	for _, fk := range childFks {
		// Any RESTRICT type foreign keys that arrive here,
		// are cross-shard/cross-keyspace RESTRICT cases, which we don't currently support.
		if isRestrict(fk.OnUpdate) {
			return nil, vterrors.VT12002()
		}

		// We need to select all the parent columns for the foreign key constraint, to use in the update of the child table.
		cols, exprs := selectParentColumns(fk, len(selectExprs))
		selectExprs = append(selectExprs, exprs...)

		fkChild, err := createFkChildForUpdate(ctx, fk, updStmt, cols)
		if err != nil {
			return nil, err
		}
		fkChildren = append(fkChildren, fkChild)
	}

	selectionOp, err := createSelectionOp(ctx, selectExprs, updStmt.TableExprs, updStmt.Where)
	if err != nil {
		return nil, err
	}

	return &FkCascade{
		Selection: selectionOp,
		Children:  fkChildren,
		Parent:    parentOp,
	}, nil
}

// createFkChildForUpdate creates the update query operator for the child table based on the foreign key constraints.
func createFkChildForUpdate(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, updStmt *sqlparser.Update, cols []int) (*FkChild, error) {
	// Reserve a bind variable name
	bvName := ctx.ReservedVars.ReserveVariable(foriegnKeyContraintValues)

	// Create child update operator
	// Create a ValTuple of child column names
	var valTuple sqlparser.ValTuple
	for _, column := range fk.ChildColumns {
		valTuple = append(valTuple, sqlparser.NewColName(column.String()))
	}

	// Create a comparison expression for WHERE clause
	compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, valTuple, sqlparser.NewListArg(bvName), nil)

	// Populate the update expressions and the where clause for the child update query based on the foreign key constraint type.
	var childWhereExpr sqlparser.Expr = compExpr
	var childUpdateExprs sqlparser.UpdateExprs

	switch fk.OnUpdate {
	case sqlparser.Cascade:
		// For CASCADE type constraint, the query looks like this -
		//	`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query> WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`

		// The update expressions are the same as the update expressions in the parent update query
		// with the column names replaced with the child column names.
		for _, updateExpr := range updStmt.Exprs {
			colIdx := fk.ParentColumns.FindColumn(updateExpr.Name.Name)
			if colIdx == -1 {
				continue
			}

			// The where condition is the same as the comparison expression above
			// with the column names replaced with the child column names.
			childUpdateExprs = append(childUpdateExprs, &sqlparser.UpdateExpr{
				Name: sqlparser.NewColName(fk.ChildColumns[colIdx].String()),
				Expr: updateExpr.Expr,
			})
		}
	case sqlparser.SetNull:
		// For SET NULL type constraint, the query looks like this -
		//		`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query>
		//		WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)
		//		[AND <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>)]`

		// For the SET NULL type constraint, we need to set all the child columns to NULL.
		for _, column := range fk.ChildColumns {
			childUpdateExprs = append(childUpdateExprs, &sqlparser.UpdateExpr{
				Name: sqlparser.NewColName(column.String()),
				Expr: &sqlparser.NullVal{},
			})
		}

		// SET NULL cascade should be avoided for the case where the parent columns remains unchanged on the update.
		// We need to add a condition to the where clause to handle this case.
		// The additional condition looks like [AND <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>)].
		// If any of the parent columns is being set to NULL, then we don't need this condition.
		var updateValues sqlparser.ValTuple
		colSetToNull := false
		for _, updateExpr := range updStmt.Exprs {
			colIdx := fk.ParentColumns.FindColumn(updateExpr.Name.Name)
			if colIdx >= 0 {
				if sqlparser.IsNull(updateExpr.Expr) {
					colSetToNull = true
					break
				}
				updateValues = append(updateValues, updateExpr.Expr)
			}
		}
		if !colSetToNull {
			childWhereExpr = &sqlparser.AndExpr{
				Left:  compExpr,
				Right: sqlparser.NewComparisonExpr(sqlparser.NotInOp, valTuple, updateValues, nil),
			}
		}
	case sqlparser.SetDefault:
		return nil, vterrors.VT09016()
	}

	childStmt := &sqlparser.Update{
		Exprs:      childUpdateExprs,
		TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
		Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: childWhereExpr},
	}

	childOp, err := createOpFromStmt(ctx, childStmt)
	if err != nil {
		return nil, err
	}

	return &FkChild{
		BVName: bvName,
		Cols:   cols,
		Op:     childOp,
	}, nil
}

func createFKVerifyOp(ctx *plancontext.PlanningContext, childOp ops.Operator, updStmt *sqlparser.Update, parentFks []vindexes.ParentFKInfo) (ops.Operator, error) {
	if len(parentFks) == 0 {
		return childOp, nil
	}

	childTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	childTbl, err := childTblExpr.TableName()
	if err != nil {
		return nil, err
	}
	var FkParents []ops.Operator
	for _, fk := range parentFks {
		// SELECT 1 from child left join parent on <parent_child_columns with new value expressions> where <child_columns_not_null> and <parent_columns_are_null> limit 1
		var whereExpr sqlparser.Expr
		var joinCond sqlparser.Expr
		found := false
		for idx, column := range fk.ChildColumns {
			added := false
			var cmpExpr sqlparser.Expr
			for _, updateExpr := range updStmt.Exprs {
				if column.Equal(updateExpr.Name.Name) {
					added = true
					found = true
					cmpExpr = &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualOp,
						Left:     sqlparser.NewColNameWithQualifier(fk.ParentColumns[idx].String(), fk.Table.GetTableName()),
						Right:    updateExpr.Expr, // TODO: rewrite with table qualifier
					}
				}
			}
			pwExpr := &sqlparser.IsExpr{
				Left:  sqlparser.NewColNameWithQualifier(fk.ParentColumns[idx].String(), fk.Table.GetTableName()),
				Right: sqlparser.IsNullOp,
			}
			var rightExpr sqlparser.Expr = pwExpr
			if !added {
				rightExpr = &sqlparser.AndExpr{
					Left: pwExpr,
					Right: &sqlparser.IsExpr{
						Left:  sqlparser.NewColNameWithQualifier(fk.ChildColumns[idx].String(), childTbl),
						Right: sqlparser.IsNotNullOp,
					},
				}
				cmpExpr = &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualOp,
					Left:     sqlparser.NewColNameWithQualifier(fk.ParentColumns[idx].String(), fk.Table.GetTableName()),
					Right:    sqlparser.NewColNameWithQualifier(fk.ChildColumns[idx].String(), childTbl),
				}
			}
			if joinCond == nil {
				joinCond = cmpExpr
			} else {
				joinCond = &sqlparser.AndExpr{
					Left:  joinCond,
					Right: cmpExpr,
				}
			}

			if whereExpr == nil {
				whereExpr = rightExpr
			} else {
				whereExpr = &sqlparser.AndExpr{
					Left:  whereExpr,
					Right: rightExpr,
				}
			}
		}
		if !found {
			return nil, vterrors.VT12001("foreign keys management at vitess with partial foreign key columns update")
		}
		selStmt := &sqlparser.Select{
			SelectExprs: sqlparser.SelectExprs{sqlparser.NewAliasedExpr(sqlparser.NewIntLiteral("1"), "")},
			From: []sqlparser.TableExpr{
				sqlparser.NewJoinTableExpr(
					updStmt.TableExprs[0],
					sqlparser.LeftJoinType,
					sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), ""),
					sqlparser.NewJoinCondition(joinCond, nil)),
			},
			Where: sqlparser.NewWhere(sqlparser.WhereClause, whereExpr),
			Limit: sqlparser.NewLimitWithoutOffset(1),
		}
		selOp, err := createOpFromStmt(ctx, selStmt)
		if err != nil {
			return nil, err
		}
		FkParents = append(FkParents, selOp)
	}

	return &FkVerify{
		Verify: FkParents,
		Input:  childOp,
	}, nil
}
