/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"maps"
	"slices"
	"strings"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Update struct {
		QTable              *QueryTable
		VTable              *vindexes.Table
		Assignments         []SetExpr
		ChangedVindexValues map[string]*engine.VindexValues
		OwnedVindexQuery    string
		Ignore              sqlparser.Ignore
		OrderBy             sqlparser.OrderBy
		Limit               *sqlparser.Limit

		// these subqueries cannot be merged as they are part of the changed vindex values
		// these values are needed to be sent over to lookup vindex for update.
		// On merging this information will be lost, so subquery merge is blocked.
		SubQueriesArgOnChangedVindex []string

		noInputs
		noColumns
		noPredicates
	}

	SetExpr struct {
		Name *sqlparser.ColName
		Expr *ProjExpr
	}
)

func (se SetExpr) String() string {
	return fmt.Sprintf("%s = %s", sqlparser.String(se.Name), sqlparser.String(se.Expr.EvalExpr))
}

// Introduces implements the PhysicalOperator interface
func (u *Update) introducesTableID() semantics.TableSet {
	return u.QTable.ID
}

// Clone implements the Operator interface
func (u *Update) Clone([]ops.Operator) ops.Operator {
	upd := *u
	upd.Assignments = slices.Clone(u.Assignments)
	upd.ChangedVindexValues = maps.Clone(u.ChangedVindexValues)
	return &upd
}

func (u *Update) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

func (u *Update) TablesUsed() []string {
	if u.VTable != nil {
		return SingleQualifiedIdentifier(u.VTable.Keyspace, u.VTable.Name)
	}
	return nil
}

func (u *Update) ShortDescription() string {
	s := []string{u.VTable.String()}
	if u.Limit != nil {
		s = append(s, sqlparser.String(u.Limit))
	}
	if len(u.OrderBy) > 0 {
		s = append(s, sqlparser.String(u.OrderBy))
	}
	return strings.Join(s, " ")
}

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
	if ksMode != vschemapb.Keyspace_managed {
		return updOp, nil
	}

	parentFks, childFks := getFKRequirementsForUpdate(ctx, updStmt.Exprs, vindexTable)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return updOp, nil
	}

	// If the delete statement has a limit, we don't support it yet.
	if updStmt.Limit != nil {
		return nil, vterrors.VT12001("update with limit with foreign key constraints")
	}

	return buildFkOperator(ctx, updOp, updClone, parentFks, childFks, vindexTable)
}

func createUpdateOperator(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update, vindexTable *vindexes.Table, qt *QueryTable, routing Routing) (ops.Operator, error) {
	sqc := &SubQueryBuilder{}
	assignments := make([]SetExpr, len(updStmt.Exprs))
	for idx, updExpr := range updStmt.Exprs {
		expr, subqs, err := sqc.pullOutValueSubqueries(ctx, updExpr.Expr, qt.ID, true)
		if err != nil {
			return nil, err
		}
		if len(subqs) == 0 {
			expr = updExpr.Expr
		}
		proj := newProjExpr(aeWrap(expr))
		if len(subqs) != 0 {
			proj.Info = SubQueryExpression(subqs)
		}
		assignments[idx] = SetExpr{
			Name: updExpr.Name,
			Expr: proj,
		}
	}

	vp, cvv, ovq, subQueriesArgOnChangedVindex, err := getUpdateVindexInformation(updStmt, vindexTable, qt.ID, assignments)
	if err != nil {
		return nil, err
	}

	tr, ok := routing.(*ShardedRouting)
	if ok {
		tr.VindexPreds = vp
	}

	for _, predicate := range qt.Predicates {
		if subq, err := sqc.handleSubquery(ctx, predicate, qt.ID); err != nil {
			return nil, err
		} else if subq != nil {
			continue
		}
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && updStmt.Limit != nil {
		// TODO systay: we should probably check for other op code types - IN could also hit multiple shards (2022-04-07)
		return nil, vterrors.VT12001("multi shard UPDATE with LIMIT")
	}

	route := &Route{
		Source: &Update{
			QTable:                       qt,
			VTable:                       vindexTable,
			Assignments:                  assignments,
			ChangedVindexValues:          cvv,
			OwnedVindexQuery:             ovq,
			Ignore:                       updStmt.Ignore,
			Limit:                        updStmt.Limit,
			OrderBy:                      updStmt.OrderBy,
			SubQueriesArgOnChangedVindex: subQueriesArgOnChangedVindex,
		},
		Routing:  routing,
		Comments: updStmt.Comments,
	}

	return sqc.getRootOperator(route), nil
}

// getFKRequirementsForUpdate analyzes update expressions to determine which foreign key constraints needs management at the VTGate.
// It identifies parent and child foreign keys that require verification or cascade operations due to column updates.
func getFKRequirementsForUpdate(ctx *plancontext.PlanningContext, updateExprs sqlparser.UpdateExprs, vindexTable *vindexes.Table) ([]vindexes.ParentFKInfo, []vindexes.ChildFKInfo) {
	parentFks := vindexTable.ParentFKsNeedsHandling(ctx.VerifyAllFKs, ctx.ParentFKToIgnore)
	childFks := vindexTable.ChildFKsNeedsHandling(ctx.VerifyAllFKs, vindexes.UpdateAction)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return nil, nil
	}

	pFksRequired := make([]bool, len(parentFks))
	cFksRequired := make([]bool, len(childFks))
	// Go over all the update expressions
	for _, updateExpr := range updateExprs {
		// Any foreign key to a child table for a column that has been updated
		// will require the cascade operations or restrict verification to happen, so we include all such foreign keys.
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

func buildFkOperator(ctx *plancontext.PlanningContext, updOp ops.Operator, updClone *sqlparser.Update, parentFks []vindexes.ParentFKInfo, childFks []vindexes.ChildFKInfo, updatedTable *vindexes.Table) (ops.Operator, error) {
	// We only support simple expressions in update queries for foreign key handling.
	if isNonLiteral(updClone.Exprs, parentFks, childFks) {
		return nil, vterrors.VT12001("update expression with non-literal values with foreign key constraints")
	}

	restrictChildFks, cascadeChildFks := splitChildFks(childFks)

	op, err := createFKCascadeOp(ctx, updOp, updClone, cascadeChildFks, updatedTable)
	if err != nil {
		return nil, err
	}

	return createFKVerifyOp(ctx, op, updClone, parentFks, restrictChildFks)
}

func isNonLiteral(updExprs sqlparser.UpdateExprs, parentFks []vindexes.ParentFKInfo, childFks []vindexes.ChildFKInfo) bool {
	for _, updateExpr := range updExprs {
		if sqlparser.IsLiteral(updateExpr.Expr) {
			continue
		}
		for _, parentFk := range parentFks {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				return true
			}
		}
		for _, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				return true
			}
		}
	}
	return false
}

// splitChildFks splits the child foreign keys into restrict and cascade list as restrict is handled through Verify operator and cascade is handled through Cascade operator.
func splitChildFks(fks []vindexes.ChildFKInfo) (restrictChildFks, cascadeChildFks []vindexes.ChildFKInfo) {
	for _, fk := range fks {
		// Any RESTRICT type foreign keys that arrive here for 2 reasons—
		//	1. cross-shard/cross-keyspace RESTRICT cases.
		//	2. shard-scoped/unsharded RESTRICT cases arising because we have to validate all the foreign keys on VTGate.
		if fk.OnUpdate.IsRestrict() {
			// For RESTRICT foreign keys, we need to verify that there are no child rows corresponding to the rows being updated.
			// This is done using a FkVerify Operator.
			restrictChildFks = append(restrictChildFks, fk)
		} else {
			// For all the other foreign keys like CASCADE, SET NULL, we have to cascade the update to the children,
			// This is done by using a FkCascade Operator.
			cascadeChildFks = append(cascadeChildFks, fk)
		}
	}
	return
}

func createFKCascadeOp(ctx *plancontext.PlanningContext, parentOp ops.Operator, updStmt *sqlparser.Update, childFks []vindexes.ChildFKInfo, updatedTable *vindexes.Table) (ops.Operator, error) {
	if len(childFks) == 0 {
		return parentOp, nil
	}

	var fkChildren []*FkChild
	var selectExprs []sqlparser.SelectExpr

	for _, fk := range childFks {
		// We should have already filtered out update restrict foreign keys.
		if fk.OnUpdate.IsRestrict() {
			return nil, vterrors.VT13001("ON UPDATE RESTRICT foreign keys should already be filtered")
		}

		// We need to select all the parent columns for the foreign key constraint, to use in the update of the child table.
		cols, exprs := selectParentColumns(fk, len(selectExprs))
		selectExprs = append(selectExprs, exprs...)

		fkChild, err := createFkChildForUpdate(ctx, fk, updStmt, cols, updatedTable)
		if err != nil {
			return nil, err
		}
		fkChildren = append(fkChildren, fkChild)
	}

	selectionOp, err := createSelectionOp(ctx, selectExprs, updStmt.TableExprs, updStmt.Where, nil, sqlparser.ForUpdateLock)
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
func createFkChildForUpdate(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, updStmt *sqlparser.Update, cols []int, updatedTable *vindexes.Table) (*FkChild, error) {
	// Create a ValTuple of child column names
	var valTuple sqlparser.ValTuple
	for _, column := range fk.ChildColumns {
		valTuple = append(valTuple, sqlparser.NewColName(column.String()))
	}

	// Reserve a bind variable name
	bvName := ctx.ReservedVars.ReserveVariable(foreignKeyConstraintValues)
	// Create a comparison expression for WHERE clause
	compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, valTuple, sqlparser.NewListArg(bvName), nil)
	var childWhereExpr sqlparser.Expr = compExpr

	var childOp ops.Operator
	var err error
	switch fk.OnUpdate {
	case sqlparser.Cascade:
		childOp, err = buildChildUpdOpForCascade(ctx, fk, updStmt, childWhereExpr, updatedTable)
	case sqlparser.SetNull:
		childOp, err = buildChildUpdOpForSetNull(ctx, fk, updStmt, childWhereExpr)
	case sqlparser.SetDefault:
		return nil, vterrors.VT09016()
	}
	if err != nil {
		return nil, err
	}

	return &FkChild{
		BVName: bvName,
		Cols:   cols,
		Op:     childOp,
	}, nil
}

// buildChildUpdOpForCascade builds the child update statement operator for the CASCADE type foreign key constraint.
// The query looks like this -
//
//	`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query> WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`
func buildChildUpdOpForCascade(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, updStmt *sqlparser.Update, childWhereExpr sqlparser.Expr, updatedTable *vindexes.Table) (ops.Operator, error) {
	// The update expressions are the same as the update expressions in the parent update query
	// with the column names replaced with the child column names.
	var childUpdateExprs sqlparser.UpdateExprs
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
	// Because we could be updating the child to a non-null value,
	// We have to run with foreign key checks OFF because the parent isn't guaranteed to have
	// the data being updated to.
	parsedComments := sqlparser.Comments{
		"/*+ SET_VAR(foreign_key_checks=OFF) */",
	}.Parsed()
	childUpdStmt := &sqlparser.Update{
		Comments:   parsedComments,
		Exprs:      childUpdateExprs,
		TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
		Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: childWhereExpr},
	}
	// Since we are running the child update with foreign key checks turned off,
	// we need to verify the validity of the remaining foreign keys on VTGate,
	// while specifically ignoring the parent foreign key in question.
	return createOpFromStmt(ctx, childUpdStmt, true, fk.String(updatedTable))
}

// buildChildUpdOpForSetNull builds the child update statement operator for the SET NULL type foreign key constraint.
// The query looks like this -
//
//	`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query>
//	WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)
//	[AND ({<bind variables in the SET clause of the original update> IS NULL OR}... <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>))]`
func buildChildUpdOpForSetNull(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, updStmt *sqlparser.Update, childWhereExpr sqlparser.Expr) (ops.Operator, error) {
	// For the SET NULL type constraint, we need to set all the child columns to NULL.
	var childUpdateExprs sqlparser.UpdateExprs
	for _, column := range fk.ChildColumns {
		childUpdateExprs = append(childUpdateExprs, &sqlparser.UpdateExpr{
			Name: sqlparser.NewColName(column.String()),
			Expr: &sqlparser.NullVal{},
		})
	}

	// SET NULL cascade should be avoided for the case where the parent columns remains unchanged on the update.
	// We need to add a condition to the where clause to handle this case.
	// The additional condition looks like [AND ({<bind variables in the SET clause of the original update> IS NULL OR}... <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>))].
	// If any of the parent columns is being set to NULL, then we don't need this condition.
	// However, we don't necessarily know on Plan time if the Expr being updated to is NULL or not. Specifically, bindVariables in Prepared statements can be NULL on runtime.
	// Therefore, in the condition we create, we also need to make it resilient to NULL values. Therefore we check if each individual value is NULL or not and OR it with the main condition.
	// For example, if we are setting `update parent cola = :v1 and colb = :v2`, then on the child, the where condition would look something like this -
	// `:v1 IS NULL OR :v2 IS NULL OR (child_cola, child_colb) NOT IN ((:v1,:v2))`
	// So, if either of :v1 or :v2 is NULL, then the entire condition is true (which is the same as not having the condition when :v1 or :v2 is NULL).
	compExpr := nullSafeNotInComparison(updStmt.Exprs, fk)
	if compExpr != nil {
		childWhereExpr = &sqlparser.AndExpr{
			Left:  childWhereExpr,
			Right: compExpr,
		}
	}
	childUpdStmt := &sqlparser.Update{
		Exprs:      childUpdateExprs,
		TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
		Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: childWhereExpr},
	}
	return createOpFromStmt(ctx, childUpdStmt, false, "")
}

// createFKVerifyOp creates the verify operator for the parent foreign key constraints.
func createFKVerifyOp(ctx *plancontext.PlanningContext, childOp ops.Operator, updStmt *sqlparser.Update, parentFks []vindexes.ParentFKInfo, restrictChildFks []vindexes.ChildFKInfo) (ops.Operator, error) {
	if len(parentFks) == 0 && len(restrictChildFks) == 0 {
		return childOp, nil
	}

	var Verify []*VerifyOp
	// This validates that new values exists on the parent table.
	for _, fk := range parentFks {
		op, err := createFkVerifyOpForParentFKForUpdate(ctx, updStmt, fk)
		if err != nil {
			return nil, err
		}
		Verify = append(Verify, &VerifyOp{
			Op:  op,
			Typ: engine.ParentVerify,
		})
	}
	// This validates that the old values don't exist on the child table.
	for _, fk := range restrictChildFks {
		op, err := createFkVerifyOpForChildFKForUpdate(ctx, updStmt, fk)
		if err != nil {
			return nil, err
		}
		Verify = append(Verify, &VerifyOp{
			Op:  op,
			Typ: engine.ChildVerify,
		})
	}

	return &FkVerify{
		Verify: Verify,
		Input:  childOp,
	}, nil
}

// Each parent foreign key constraint is verified by an anti join query of the form:
// select 1 from child_tbl left join parent_tbl on <parent_child_columns with new value expressions, remaining fk columns join>
// where <parent columns are null> and <unchanged child columns not null> and <clause same as original update> limit 1
// E.g:
// Child (c1, c2) references Parent (p1, p2)
// update Child set c1 = 1 where id = 1
// verify query:
// select 1 from Child left join Parent on Parent.p1 = 1 and Parent.p2 = Child.c2
// where Parent.p1 is null and Parent.p2 is null and Child.id = 1
// and Child.c2 is not null
// limit 1
func createFkVerifyOpForParentFKForUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update, pFK vindexes.ParentFKInfo) (ops.Operator, error) {
	childTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	childTbl, err := childTblExpr.TableName()
	if err != nil {
		return nil, err
	}
	parentTbl := pFK.Table.GetTableName()
	var whereCond sqlparser.Expr
	var joinCond sqlparser.Expr
	for idx, column := range pFK.ChildColumns {
		var matchedExpr *sqlparser.UpdateExpr
		for _, updateExpr := range updStmt.Exprs {
			if column.Equal(updateExpr.Name.Name) {
				matchedExpr = updateExpr
				break
			}
		}
		parentIsNullExpr := &sqlparser.IsExpr{
			Left:  sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
			Right: sqlparser.IsNullOp,
		}
		var predicate sqlparser.Expr = parentIsNullExpr
		var joinExpr sqlparser.Expr
		if matchedExpr == nil {
			predicate = &sqlparser.AndExpr{
				Left: parentIsNullExpr,
				Right: &sqlparser.IsExpr{
					Left:  sqlparser.NewColNameWithQualifier(pFK.ChildColumns[idx].String(), childTbl),
					Right: sqlparser.IsNotNullOp,
				},
			}
			joinExpr = &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
				Right:    sqlparser.NewColNameWithQualifier(pFK.ChildColumns[idx].String(), childTbl),
			}
		} else {
			joinExpr = &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
				Right:    prefixColNames(childTbl, matchedExpr.Expr),
			}
		}

		if idx == 0 {
			joinCond, whereCond = joinExpr, predicate
			continue
		}
		joinCond = &sqlparser.AndExpr{Left: joinCond, Right: joinExpr}
		whereCond = &sqlparser.AndExpr{Left: whereCond, Right: predicate}
	}
	// add existing where condition on the update statement
	if updStmt.Where != nil {
		whereCond = &sqlparser.AndExpr{Left: whereCond, Right: prefixColNames(childTbl, updStmt.Where.Expr)}
	}
	return createSelectionOp(ctx,
		sqlparser.SelectExprs{sqlparser.NewAliasedExpr(sqlparser.NewIntLiteral("1"), "")},
		[]sqlparser.TableExpr{
			sqlparser.NewJoinTableExpr(
				childTblExpr,
				sqlparser.LeftJoinType,
				sqlparser.NewAliasedTableExpr(parentTbl, ""),
				sqlparser.NewJoinCondition(joinCond, nil)),
		},
		sqlparser.NewWhere(sqlparser.WhereClause, whereCond),
		sqlparser.NewLimitWithoutOffset(1),
		sqlparser.ShareModeLock)
}

// Each child foreign key constraint is verified by a join query of the form:
// select 1 from child_tbl join parent_tbl on <columns in fk> where <clause same as original update> [AND ({<bind variables in the SET clause of the original update> IS NULL OR}... <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>))] limit 1
// E.g:
// Child (c1, c2) references Parent (p1, p2)
// update Parent set p1 = 1 where id = 1
// verify query:
// select 1 from Child join Parent on Parent.p1 = Child.c1 and Parent.p2 = Child.c2
// where Parent.id = 1 and (1 IS NULL OR (child.c1) NOT IN ((1))) limit 1
func createFkVerifyOpForChildFKForUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update, cFk vindexes.ChildFKInfo) (ops.Operator, error) {
	// ON UPDATE RESTRICT foreign keys that require validation, should only be allowed in the case where we
	// are verifying all the FKs on vtgate level.
	if !ctx.VerifyAllFKs {
		return nil, vterrors.VT12002()
	}
	parentTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	parentTbl, err := parentTblExpr.TableName()
	if err != nil {
		return nil, err
	}
	childTbl := cFk.Table.GetTableName()
	var joinCond sqlparser.Expr
	for idx := range cFk.ParentColumns {
		joinExpr := &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     sqlparser.NewColNameWithQualifier(cFk.ParentColumns[idx].String(), parentTbl),
			Right:    sqlparser.NewColNameWithQualifier(cFk.ChildColumns[idx].String(), childTbl),
		}

		if idx == 0 {
			joinCond = joinExpr
			continue
		}
		joinCond = &sqlparser.AndExpr{Left: joinCond, Right: joinExpr}
	}

	var whereCond sqlparser.Expr
	// add existing where condition on the update statement
	if updStmt.Where != nil {
		whereCond = prefixColNames(parentTbl, updStmt.Where.Expr)
	}

	// We don't want to fail the RESTRICT for the case where the parent columns remains unchanged on the update.
	// We need to add a condition to the where clause to handle this case.
	// The additional condition looks like [AND ({<bind variables in the SET clause of the original update> IS NULL OR}... <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>))].
	// If any of the parent columns is being set to NULL, then we don't need this condition.
	// However, we don't necessarily know on Plan time if the Expr being updated to is NULL or not. Specifically, bindVariables in Prepared statements can be NULL on runtime.
	// Therefore, in the condition we create, we also need to make it resilient to NULL values. Therefore we check if each individual value is NULL or not and OR it with the main condition.
	// For example, if we are setting `update child cola = :v1 and colb = :v2`, then on the parent, the where condition would look something like this -
	// `:v1 IS NULL OR :v2 IS NULL OR (cola, colb) NOT IN ((:v1,:v2))`
	// So, if either of :v1 or :v2 is NULL, then the entire condition is true (which is the same as not having the condition when :v1 or :v2 is NULL).
	compExpr := nullSafeNotInComparison(updStmt.Exprs, cFk)
	if compExpr != nil {
		whereCond = sqlparser.AndExpressions(whereCond, compExpr)
	}

	return createSelectionOp(ctx,
		sqlparser.SelectExprs{sqlparser.NewAliasedExpr(sqlparser.NewIntLiteral("1"), "")},
		[]sqlparser.TableExpr{
			sqlparser.NewJoinTableExpr(
				parentTblExpr,
				sqlparser.NormalJoinType,
				sqlparser.NewAliasedTableExpr(childTbl, ""),
				sqlparser.NewJoinCondition(joinCond, nil)),
		},
		sqlparser.NewWhere(sqlparser.WhereClause, whereCond),
		sqlparser.NewLimitWithoutOffset(1),
		sqlparser.ShareModeLock)
}

// nullSafeNotInComparison is used to compare the child columns in the foreign key constraint aren't the same as the updateExpressions exactly.
// This comparison has to be null safe so we create an expression which looks like the following for a query like `update child cola = :v1 and colb = :v2` -
// `:v1 IS NULL OR :v2 IS NULL OR (cola, colb) NOT IN ((:v1,:v2))`
// So, if either of :v1 or :v2 is NULL, then the entire condition is true (which is the same as not having the condition when :v1 or :v2 is NULL)
// This expression is used in cascading SET NULLs and in verifying whether an update should be restricted.
func nullSafeNotInComparison(updateExprs sqlparser.UpdateExprs, cFk vindexes.ChildFKInfo) sqlparser.Expr {
	var updateValues sqlparser.ValTuple
	for _, updateExpr := range updateExprs {
		colIdx := cFk.ParentColumns.FindColumn(updateExpr.Name.Name)
		if colIdx >= 0 {
			if sqlparser.IsNull(updateExpr.Expr) {
				return nil
			}
			updateValues = append(updateValues, updateExpr.Expr)
		}
	}
	// Create a ValTuple of child column names
	var valTuple sqlparser.ValTuple
	for _, column := range cFk.ChildColumns {
		valTuple = append(valTuple, sqlparser.NewColNameWithQualifier(column.String(), cFk.Table.GetTableName()))
	}
	var finalExpr sqlparser.Expr = sqlparser.NewComparisonExpr(sqlparser.NotInOp, valTuple, sqlparser.ValTuple{updateValues}, nil)
	for _, value := range updateValues {
		finalExpr = &sqlparser.OrExpr{
			Left: &sqlparser.IsExpr{
				Left:  value,
				Right: sqlparser.IsNullOp,
			},
			Right: finalExpr,
		}
	}

	return finalExpr
}
