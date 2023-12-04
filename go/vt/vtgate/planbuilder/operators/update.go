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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/sysvars"
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

func (u *Update) GetOrdering(*plancontext.PlanningContext) []ops.OrderBy {
	return nil
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

	parentFks := ctx.SemTable.GetParentForeignKeysList()
	childFks := ctx.SemTable.GetChildForeignKeysList()
	if len(childFks) == 0 && len(parentFks) == 0 {
		return updOp, nil
	}

	// If the delete statement has a limit, we don't support it yet.
	if updStmt.Limit != nil {
		return nil, vterrors.VT12001("update with limit with foreign key constraints")
	}

	// Now we check if any of the foreign key columns that are being udpated have dependencies on other updated columns.
	// This is unsafe, and we currently don't support this in Vitess.
	if err = ctx.SemTable.ErrIfFkDependentColumnUpdated(updStmt.Exprs); err != nil {
		return nil, err
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

	vp, cvv, ovq, subQueriesArgOnChangedVindex, err := getUpdateVindexInformation(ctx, updStmt, vindexTable, qt.ID, assignments)
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

	decorator := func(op ops.Operator) ops.Operator {
		return &LockAndComment{
			Source: op,
			Lock:   sqlparser.ShareModeLock,
		}
	}

	return sqc.getRootOperator(route, decorator), nil
}

func buildFkOperator(ctx *plancontext.PlanningContext, updOp ops.Operator, updClone *sqlparser.Update, parentFks []vindexes.ParentFKInfo, childFks []vindexes.ChildFKInfo, updatedTable *vindexes.Table) (ops.Operator, error) {
	restrictChildFks, cascadeChildFks := splitChildFks(childFks)

	op, err := createFKCascadeOp(ctx, updOp, updClone, cascadeChildFks, updatedTable)
	if err != nil {
		return nil, err
	}

	return createFKVerifyOp(ctx, op, updClone, parentFks, restrictChildFks, updatedTable)
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
		var selectOffsets []int
		selectOffsets, selectExprs = addColumns(ctx, fk.ParentColumns, selectExprs)

		// If we are updating a foreign key column to a non-literal value then, need information about
		// 1. whether the new value is different from the old value
		// 2. the new value itself.
		// 3. the bind variable to assign to this value.
		var nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo
		ue := ctx.SemTable.GetUpdateExpressionsForFk(fk.String(updatedTable))
		// We only need to store these offsets and add these expressions to SELECT when there are non-literal updates present.
		if hasNonLiteralUpdate(ue) {
			for _, updExpr := range ue {
				// We add the expression and a comparison expression to the SELECT exprssion while storing their offsets.
				var info engine.NonLiteralUpdateInfo
				info, selectExprs = addNonLiteralUpdExprToSelect(ctx, updatedTable, updExpr, selectExprs)
				nonLiteralUpdateInfo = append(nonLiteralUpdateInfo, info)
			}
		}

		fkChild, err := createFkChildForUpdate(ctx, fk, selectOffsets, nonLiteralUpdateInfo, updatedTable)
		if err != nil {
			return nil, err
		}
		fkChildren = append(fkChildren, fkChild)
	}

	selectionOp, err := createSelectionOp(ctx, selectExprs, updStmt.TableExprs, updStmt.Where, updStmt.OrderBy, nil, sqlparser.ForUpdateLockNoWait)
	if err != nil {
		return nil, err
	}

	return &FkCascade{
		Selection: selectionOp,
		Children:  fkChildren,
		Parent:    parentOp,
	}, nil
}

// hasNonLiteralUpdate checks if any of the update expressions have a non-literal update.
func hasNonLiteralUpdate(exprs sqlparser.UpdateExprs) bool {
	for _, expr := range exprs {
		if !sqlparser.IsLiteral(expr.Expr) {
			return true
		}
	}
	return false
}

// addColumns adds the given set of columns to the select expressions provided. It tries to reuse the columns if already present in it.
// It returns the list of offsets for the columns and the updated select expressions.
func addColumns(ctx *plancontext.PlanningContext, columns sqlparser.Columns, exprs []sqlparser.SelectExpr) ([]int, []sqlparser.SelectExpr) {
	var offsets []int
	selectExprs := exprs
	for _, column := range columns {
		ae := aeWrap(sqlparser.NewColName(column.String()))
		exists := false
		for idx, expr := range exprs {
			if ctx.SemTable.EqualsExpr(expr.(*sqlparser.AliasedExpr).Expr, ae.Expr) {
				offsets = append(offsets, idx)
				exists = true
				break
			}
		}
		if !exists {
			offsets = append(offsets, len(selectExprs))
			selectExprs = append(selectExprs, ae)

		}
	}
	return offsets, selectExprs
}

// For an update query having non-literal updates, we add the updated expression and a comparison expression to the select query.
// For example, for a query like `update fk_table set col = id * 100 + 1`
// We would add the expression `id * 100 + 1` and the comparison expression `col <=> id * 100 + 1` to the select query.
func addNonLiteralUpdExprToSelect(ctx *plancontext.PlanningContext, updatedTable *vindexes.Table, updExpr *sqlparser.UpdateExpr, exprs []sqlparser.SelectExpr) (engine.NonLiteralUpdateInfo, []sqlparser.SelectExpr) {
	// Create the comparison expression.
	castedExpr := getCastedUpdateExpression(updatedTable, updExpr)
	compExpr := sqlparser.NewComparisonExpr(sqlparser.NullSafeEqualOp, updExpr.Name, castedExpr, nil)
	info := engine.NonLiteralUpdateInfo{
		CompExprCol:   -1,
		UpdateExprCol: -1,
	}
	// Add the expressions to the select expressions. We make sure to reuse the offset if it has already been added once.
	for idx, selectExpr := range exprs {
		if ctx.SemTable.EqualsExpr(selectExpr.(*sqlparser.AliasedExpr).Expr, compExpr) {
			info.CompExprCol = idx
		}
		if ctx.SemTable.EqualsExpr(selectExpr.(*sqlparser.AliasedExpr).Expr, castedExpr) {
			info.UpdateExprCol = idx
		}
	}
	// If the expression doesn't exist, then we add the expression and store the offset.
	if info.CompExprCol == -1 {
		info.CompExprCol = len(exprs)
		exprs = append(exprs, aeWrap(compExpr))
	}
	if info.UpdateExprCol == -1 {
		info.UpdateExprCol = len(exprs)
		exprs = append(exprs, aeWrap(castedExpr))
	}
	return info, exprs
}

func getCastedUpdateExpression(updatedTable *vindexes.Table, updExpr *sqlparser.UpdateExpr) sqlparser.Expr {
	castTypeStr := getCastTypeForColumn(updatedTable, updExpr)
	if castTypeStr == "" {
		return updExpr.Expr
	}
	return &sqlparser.CastExpr{
		Expr: updExpr.Expr,
		Type: &sqlparser.ConvertType{
			Type: castTypeStr,
		},
	}
}

func getCastTypeForColumn(updatedTable *vindexes.Table, updExpr *sqlparser.UpdateExpr) string {
	var ty querypb.Type
	for _, column := range updatedTable.Columns {
		if updExpr.Name.Name.Equal(column.Name) {
			ty = column.Type
			break
		}
	}
	switch {
	case sqltypes.IsNull(ty):
		return ""
	case sqltypes.IsSigned(ty):
		return "SIGNED"
	case sqltypes.IsUnsigned(ty):
		return "UNSIGNED"
	case sqltypes.IsFloat(ty):
		return "FLOAT"
	case sqltypes.IsDecimal(ty):
		return "DECIMAL"
	case sqltypes.IsDateOrTime(ty):
		return "DATETIME"
	case sqltypes.IsBinary(ty):
		return "BINARY"
	case sqltypes.IsText(ty):
		return "CHAR"
	default:
		return ""
	}
}

// createFkChildForUpdate creates the update query operator for the child table based on the foreign key constraints.
func createFkChildForUpdate(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, selectOffsets []int, nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo, updatedTable *vindexes.Table) (*FkChild, error) {
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

	// In the case of non-literal updates, we need to assign bindvariables for storing the updated value of the columns
	// coming from the SELECT query.
	if len(nonLiteralUpdateInfo) > 0 {
		for idx, info := range nonLiteralUpdateInfo {
			info.UpdateExprBvName = ctx.ReservedVars.ReserveVariable(foreignKeyUpdateExpr)
			nonLiteralUpdateInfo[idx] = info
		}
	}

	var childOp ops.Operator
	var err error
	switch fk.OnUpdate {
	case sqlparser.Cascade:
		childOp, err = buildChildUpdOpForCascade(ctx, fk, childWhereExpr, nonLiteralUpdateInfo, updatedTable)
	case sqlparser.SetNull:
		childOp, err = buildChildUpdOpForSetNull(ctx, fk, childWhereExpr, nonLiteralUpdateInfo, updatedTable)
	case sqlparser.SetDefault:
		return nil, vterrors.VT09016()
	}
	if err != nil {
		return nil, err
	}

	return &FkChild{
		BVName:         bvName,
		Cols:           selectOffsets,
		Op:             childOp,
		NonLiteralInfo: nonLiteralUpdateInfo,
	}, nil
}

// buildChildUpdOpForCascade builds the child update statement operator for the CASCADE type foreign key constraint.
// The query looks like this -
//
//	`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query> WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`
func buildChildUpdOpForCascade(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, childWhereExpr sqlparser.Expr, nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo, updatedTable *vindexes.Table) (ops.Operator, error) {
	// The update expressions are the same as the update expressions in the parent update query
	// with the column names replaced with the child column names.
	var childUpdateExprs sqlparser.UpdateExprs
	for idx, updateExpr := range ctx.SemTable.GetUpdateExpressionsForFk(fk.String(updatedTable)) {
		colIdx := fk.ParentColumns.FindColumn(updateExpr.Name.Name)
		if colIdx == -1 {
			continue
		}

		// The where condition is the same as the comparison expression above
		// with the column names replaced with the child column names.
		childUpdateExpr := updateExpr.Expr
		if len(nonLiteralUpdateInfo) > 0 && nonLiteralUpdateInfo[idx].UpdateExprBvName != "" {
			childUpdateExpr = sqlparser.NewArgument(nonLiteralUpdateInfo[idx].UpdateExprBvName)
		}
		childUpdateExprs = append(childUpdateExprs, &sqlparser.UpdateExpr{
			Name: sqlparser.NewColName(fk.ChildColumns[colIdx].String()),
			Expr: childUpdateExpr,
		})
	}
	// Because we could be updating the child to a non-null value,
	// We have to run with foreign key checks OFF because the parent isn't guaranteed to have
	// the data being updated to.
	parsedComments := (&sqlparser.ParsedComments{}).SetMySQLSetVarValue(sysvars.ForeignKeyChecks.Name, "OFF").Parsed()
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
func buildChildUpdOpForSetNull(
	ctx *plancontext.PlanningContext,
	fk vindexes.ChildFKInfo,
	childWhereExpr sqlparser.Expr,
	nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo,
	updatedTable *vindexes.Table,
) (ops.Operator, error) {
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
	updateExprs := ctx.SemTable.GetUpdateExpressionsForFk(fk.String(updatedTable))
	compExpr := nullSafeNotInComparison(ctx,
		updatedTable,
		updateExprs, fk, updatedTable.GetTableName(), nonLiteralUpdateInfo, false /* appendQualifier */)
	if compExpr != nil {
		childWhereExpr = &sqlparser.AndExpr{
			Left:  childWhereExpr,
			Right: compExpr,
		}
	}
	parsedComments := getParsedCommentsForFkChecks(ctx)
	childUpdStmt := &sqlparser.Update{
		Exprs:      childUpdateExprs,
		Comments:   parsedComments,
		TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
		Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: childWhereExpr},
	}
	return createOpFromStmt(ctx, childUpdStmt, false, "")
}

// getParsedCommentsForFkChecks gets the parsed comments to be set on a child query related to foreign_key_checks session variable.
// We only use this function if foreign key checks are either unspecified or on.
// If foreign key checks are explicity turned on, then we should add the set_var parsed comment too
// since underlying MySQL might have foreign_key_checks as off.
// We run with foreign key checks on because the DML might still fail on MySQL due to a child table
// with RESTRICT constraints.
func getParsedCommentsForFkChecks(ctx *plancontext.PlanningContext) (parsedComments *sqlparser.ParsedComments) {
	fkState := ctx.VSchema.GetForeignKeyChecksState()
	if fkState != nil && *fkState {
		parsedComments = parsedComments.SetMySQLSetVarValue(sysvars.ForeignKeyChecks.Name, "ON").Parsed()
	}
	return parsedComments
}

// createFKVerifyOp creates the verify operator for the parent foreign key constraints.
func createFKVerifyOp(
	ctx *plancontext.PlanningContext,
	childOp ops.Operator,
	updStmt *sqlparser.Update,
	parentFks []vindexes.ParentFKInfo,
	restrictChildFks []vindexes.ChildFKInfo,
	updatedTable *vindexes.Table,
) (ops.Operator, error) {
	if len(parentFks) == 0 && len(restrictChildFks) == 0 {
		return childOp, nil
	}

	var Verify []*VerifyOp
	// This validates that new values exists on the parent table.
	for _, fk := range parentFks {
		op, err := createFkVerifyOpForParentFKForUpdate(ctx, updatedTable, updStmt, fk)
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
		op, err := createFkVerifyOpForChildFKForUpdate(ctx, updatedTable, updStmt, fk)
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
// where <parent columns are null> and <unchanged child columns not null> and <updated expressions not null> and <clause same as original update> and <comparison of updated expression with original values> limit 1
// E.g:
// Child (c1, c2) references Parent (p1, p2)
// update Child set c1 = c2 + 1 where id = 1
// verify query:
// select 1 from Child left join Parent on Parent.p1 = Child.c2 + 1 and Parent.p2 = Child.c2
// where Parent.p1 is null and Parent.p2 is null and Child.id = 1 and Child.c2 + 1 is not null
// and Child.c2 is not null and not ((Child.c1) <=> (Child.c2 + 1))
// limit 1
func createFkVerifyOpForParentFKForUpdate(ctx *plancontext.PlanningContext, updatedTable *vindexes.Table, updStmt *sqlparser.Update, pFK vindexes.ParentFKInfo) (ops.Operator, error) {
	childTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	childTbl, err := childTblExpr.TableName()
	if err != nil {
		return nil, err
	}
	parentTbl := pFK.Table.GetTableName()
	var whereCond sqlparser.Expr
	var joinCond sqlparser.Expr
	var notEqualColNames sqlparser.ValTuple
	var notEqualExprs sqlparser.ValTuple
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
				Left: predicate,
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
			notEqualColNames = append(notEqualColNames, prefixColNames(ctx, childTbl, matchedExpr.Name))
			prefixedMatchExpr := prefixColNames(ctx, childTbl, getCastedUpdateExpression(updatedTable, matchedExpr))
			notEqualExprs = append(notEqualExprs, prefixedMatchExpr)
			joinExpr = &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
				Right:    prefixedMatchExpr,
			}
			predicate = &sqlparser.AndExpr{
				Left: predicate,
				Right: &sqlparser.IsExpr{
					Left:  prefixedMatchExpr,
					Right: sqlparser.IsNotNullOp,
				},
			}
		}

		if idx == 0 {
			joinCond, whereCond = joinExpr, predicate
			continue
		}
		joinCond = &sqlparser.AndExpr{Left: joinCond, Right: joinExpr}
		whereCond = &sqlparser.AndExpr{Left: whereCond, Right: predicate}
	}
	whereCond = &sqlparser.AndExpr{
		Left: whereCond,
		Right: &sqlparser.NotExpr{
			Expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NullSafeEqualOp,
				Left:     notEqualColNames,
				Right:    notEqualExprs,
			},
		},
	}
	// add existing where condition on the update statement
	if updStmt.Where != nil {
		whereCond = &sqlparser.AndExpr{Left: whereCond, Right: prefixColNames(ctx, childTbl, updStmt.Where.Expr)}
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
		nil,
		sqlparser.NewLimitWithoutOffset(1),
		sqlparser.ForShareLockNoWait)
}

// Each child foreign key constraint is verified by a join query of the form:
// select 1 from child_tbl join parent_tbl on <columns in fk> where <clause same as original update> [AND ({<bind variables in the SET clause of the original update> IS NULL OR}... <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>))] limit 1
// E.g:
// Child (c1, c2) references Parent (p1, p2)
// update Parent set p1 = col + 1 where id = 1
// verify query:
// select 1 from Child join Parent on Parent.p1 = Child.c1 and Parent.p2 = Child.c2
// where Parent.id = 1 and ((Parent.col + 1) IS NULL OR (child.c1) NOT IN ((Parent.col + 1))) limit 1
func createFkVerifyOpForChildFKForUpdate(ctx *plancontext.PlanningContext, updatedTable *vindexes.Table, updStmt *sqlparser.Update, cFk vindexes.ChildFKInfo) (ops.Operator, error) {
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
		whereCond = prefixColNames(ctx, parentTbl, updStmt.Where.Expr)
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
	compExpr := nullSafeNotInComparison(ctx, updatedTable, updStmt.Exprs, cFk, parentTbl, nil /* nonLiteralUpdateInfo */, true /* appendQualifier */)
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
		nil,
		sqlparser.NewLimitWithoutOffset(1),
		sqlparser.ForShareLockNoWait)
}

// nullSafeNotInComparison is used to compare the child columns in the foreign key constraint aren't the same as the updateExpressions exactly.
// This comparison has to be null safe, so we create an expression which looks like the following for a query like `update child cola = :v1 and colb = :v2` -
// `:v1 IS NULL OR :v2 IS NULL OR (cola, colb) NOT IN ((:v1,:v2))`
// So, if either of :v1 or :v2 is NULL, then the entire condition is true (which is the same as not having the condition when :v1 or :v2 is NULL)
// This expression is used in cascading SET NULLs and in verifying whether an update should be restricted.
func nullSafeNotInComparison(ctx *plancontext.PlanningContext, updatedTable *vindexes.Table, updateExprs sqlparser.UpdateExprs, cFk vindexes.ChildFKInfo, parentTbl sqlparser.TableName, nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo, appendQualifier bool) sqlparser.Expr {
	var valTuple sqlparser.ValTuple
	var updateValues sqlparser.ValTuple
	for idx, updateExpr := range updateExprs {
		colIdx := cFk.ParentColumns.FindColumn(updateExpr.Name.Name)
		if colIdx >= 0 {
			if sqlparser.IsNull(updateExpr.Expr) {
				return nil
			}
			childUpdateExpr := prefixColNames(ctx, parentTbl, getCastedUpdateExpression(updatedTable, updateExpr))
			if len(nonLiteralUpdateInfo) > 0 && nonLiteralUpdateInfo[idx].UpdateExprBvName != "" {
				childUpdateExpr = sqlparser.NewArgument(nonLiteralUpdateInfo[idx].UpdateExprBvName)
			}
			updateValues = append(updateValues, childUpdateExpr)
			if appendQualifier {
				valTuple = append(valTuple, sqlparser.NewColNameWithQualifier(cFk.ChildColumns[colIdx].String(), cFk.Table.GetTableName()))
			} else {
				valTuple = append(valTuple, sqlparser.NewColName(cFk.ChildColumns[colIdx].String()))
			}
		}
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
