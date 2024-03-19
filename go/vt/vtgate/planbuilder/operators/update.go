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

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Update struct {
		*DMLCommon

		Assignments         []SetExpr
		ChangedVindexValues map[string]*engine.VindexValues

		// these subqueries cannot be merged as they are part of the changed vindex values
		// these values are needed to be sent over to lookup vindex for update.
		// On merging this information will be lost, so subquery merge is blocked.
		SubQueriesArgOnChangedVindex []string

		VerifyAll bool

		noColumns
		noPredicates
	}

	SetExpr struct {
		Name *sqlparser.ColName
		Expr *ProjExpr
	}
)

func (u *Update) Inputs() []Operator {
	if u.Source == nil {
		return nil
	}
	return []Operator{u.Source}
}

func (u *Update) SetInputs(inputs []Operator) {
	if len(inputs) != 1 {
		panic(vterrors.VT13001("unexpected number of inputs for Update operator"))
	}
	u.Source = inputs[0]
}

func (se SetExpr) String() string {
	return fmt.Sprintf("%s = %s", sqlparser.String(se.Name), sqlparser.String(se.Expr.EvalExpr))
}

// Clone implements the Operator interface
func (u *Update) Clone(inputs []Operator) Operator {
	upd := *u
	upd.Assignments = slices.Clone(u.Assignments)
	upd.ChangedVindexValues = maps.Clone(u.ChangedVindexValues)
	upd.SetInputs(inputs)
	return &upd
}

func (u *Update) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

func (u *Update) TablesUsed() []string {
	return SingleQualifiedIdentifier(u.Target.VTable.Keyspace, u.Target.VTable.Name)
}

func (u *Update) ShortDescription() string {
	return shortDesc(u.Target, u.OwnedVindexQuery)
}

func createOperatorFromUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update) (op Operator) {
	errIfUpdateNotSupported(ctx, updStmt)
	parentFks := ctx.SemTable.GetParentForeignKeysForTargets()
	childFks := ctx.SemTable.GetChildForeignKeysForTargets()

	// We check if dml with input plan is required. DML with input planning is generally
	// slower, because it does a selection and then creates an update statement wherein we have to
	// list all the primary key values.
	if updateWithInputPlanningRequired(ctx, childFks, parentFks, updStmt) {
		return createUpdateWithInputOp(ctx, updStmt)
	}

	var updClone *sqlparser.Update
	var targetTbl TargetTable
	op, targetTbl, updClone = createUpdateOperator(ctx, updStmt)

	op = &LockAndComment{
		Source:   op,
		Comments: updStmt.Comments,
		Lock:     sqlparser.ShareModeLock,
	}

	parentFks = ctx.SemTable.GetParentForeignKeysForTableSet(targetTbl.ID)
	childFks = ctx.SemTable.GetChildForeignKeysForTableSet(targetTbl.ID)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return op
	}
	return buildFkOperator(ctx, op, updClone, parentFks, childFks, targetTbl)
}

func updateWithInputPlanningRequired(
	ctx *plancontext.PlanningContext,
	childFks []vindexes.ChildFKInfo,
	parentFks []vindexes.ParentFKInfo,
	updateStmt *sqlparser.Update,
) bool {
	if isMultiTargetUpdate(ctx, updateStmt) {
		return true
	}
	// If there are no foreign keys, we don't need to use delete with input.
	if len(childFks) == 0 && len(parentFks) == 0 {
		return false
	}
	// Limit requires dml with input.
	if updateStmt.Limit != nil {
		return true
	}
	return false
}

func isMultiTargetUpdate(ctx *plancontext.PlanningContext, updateStmt *sqlparser.Update) bool {
	var targetTS semantics.TableSet
	for _, ue := range updateStmt.Exprs {
		targetTS = targetTS.Merge(ctx.SemTable.DirectDeps(ue.Name))
	}
	return targetTS.NumberOfTables() > 1
}

func createUpdateWithInputOp(ctx *plancontext.PlanningContext, upd *sqlparser.Update) (op Operator) {
	updClone := ctx.SemTable.Clone(upd).(*sqlparser.Update)
	upd.Limit = nil

	var updOps []dmlOp
	for _, target := range ctx.SemTable.Targets.Constituents() {
		op := createUpdateOpWithTarget(ctx, target, upd)
		updOps = append(updOps, op)
	}

	updOps = sortDmlOps(updOps)

	selectStmt := &sqlparser.Select{
		From:    updClone.TableExprs,
		Where:   updClone.Where,
		OrderBy: updClone.OrderBy,
		Limit:   updClone.Limit,
		Lock:    sqlparser.ForUpdateLock,
	}

	// now map the operator and column list.
	var colsList [][]*sqlparser.ColName
	dmls := slice.Map(updOps, func(from dmlOp) Operator {
		colsList = append(colsList, from.cols)
		for _, col := range from.cols {
			selectStmt.SelectExprs = append(selectStmt.SelectExprs, aeWrap(col))
		}
		return from.op
	})

	op = &DMLWithInput{
		DML:    dmls,
		Source: createOperatorFromSelect(ctx, selectStmt),
		cols:   colsList,
	}

	if upd.Comments != nil {
		op = &LockAndComment{
			Source:   op,
			Comments: upd.Comments,
		}
	}
	return op
}

func createUpdateOpWithTarget(ctx *plancontext.PlanningContext, target semantics.TableSet, updStmt *sqlparser.Update) dmlOp {
	var updExprs sqlparser.UpdateExprs
	for _, ue := range updStmt.Exprs {
		if ctx.SemTable.DirectDeps(ue.Name) == target {
			updExprs = append(updExprs, ue)
		}
	}

	if len(updExprs) == 0 {
		panic(vterrors.VT13001("no update expression for the target"))
	}

	ti, err := ctx.SemTable.TableInfoFor(target)
	if err != nil {
		panic(vterrors.VT13001(err.Error()))
	}
	vTbl := ti.GetVindexTable()
	tblName, err := ti.Name()
	if err != nil {
		panic(err)
	}

	var leftComp sqlparser.ValTuple
	cols := make([]*sqlparser.ColName, 0, len(vTbl.PrimaryKey))
	for _, col := range vTbl.PrimaryKey {
		colName := sqlparser.NewColNameWithQualifier(col.String(), tblName)
		cols = append(cols, colName)
		leftComp = append(leftComp, colName)
		ctx.SemTable.Recursive[colName] = target
	}
	// optimize for case when there is only single column on left hand side.
	var lhs sqlparser.Expr = leftComp
	if len(leftComp) == 1 {
		lhs = leftComp[0]
	}
	compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, lhs, sqlparser.ListArg(engine.DmlVals), nil)

	upd := &sqlparser.Update{
		Ignore:     updStmt.Ignore,
		TableExprs: sqlparser.TableExprs{ti.GetAliasedTableExpr()},
		Exprs:      updExprs,
		Where:      sqlparser.NewWhere(sqlparser.WhereClause, compExpr),
		OrderBy:    updStmt.OrderBy,
	}
	return dmlOp{
		createOperatorFromUpdate(ctx, upd),
		vTbl,
		cols,
	}
}

func errIfUpdateNotSupported(ctx *plancontext.PlanningContext, stmt *sqlparser.Update) {
	for _, ue := range stmt.Exprs {
		tblInfo, err := ctx.SemTable.TableInfoForExpr(ue.Name)
		if err != nil {
			panic(err)
		}
		if _, isATable := tblInfo.(*semantics.RealTable); !isATable {
			var tblName string
			ate := tblInfo.GetAliasedTableExpr()
			if ate != nil {
				tblName = sqlparser.String(ate)
			}
			panic(vterrors.VT03032(tblName))
		}
	}

	// Now we check if any of the foreign key columns that are being udpated have dependencies on other updated columns.
	// This is unsafe, and we currently don't support this in Vitess.
	if err := ctx.SemTable.ErrIfFkDependentColumnUpdated(stmt.Exprs); err != nil {
		panic(err)
	}
}

func createUpdateOperator(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update) (Operator, TargetTable, *sqlparser.Update) {
	op := crossJoin(ctx, updStmt.TableExprs)

	sqc := &SubQueryBuilder{}
	if updStmt.Where != nil {
		op = addWherePredsToSubQueryBuilder(ctx, updStmt.Where.Expr, op, sqc)
	}

	outerID := TableID(op)
	assignments := make([]SetExpr, len(updStmt.Exprs))
	// updClone is used in foreign key planning to create the selection statements to be used for verification and selection.
	// If we encounter subqueries, we want to fix the updClone to use the replaced expression, so that the pulled out subquery's
	// result is used everywhere instead of running the subquery multiple times, which is wasteful.
	updClone := sqlparser.CloneRefOfUpdate(updStmt)
	var tblInfo semantics.TableInfo
	var err error
	for idx, updExpr := range updStmt.Exprs {
		expr, subqs := sqc.pullOutValueSubqueries(ctx, updExpr.Expr, outerID, true)
		if len(subqs) == 0 {
			expr = updExpr.Expr
		} else {
			updClone.Exprs[idx].Expr = sqlparser.CloneExpr(expr)
			ctx.SemTable.UpdateChildFKExpr(updExpr, expr)
		}
		proj := newProjExpr(aeWrap(expr))
		if len(subqs) != 0 {
			proj.Info = SubQueryExpression(subqs)
		}
		assignments[idx] = SetExpr{
			Name: updExpr.Name,
			Expr: proj,
		}
		tblInfo, err = ctx.SemTable.TableInfoForExpr(updExpr.Name)
		if err != nil {
			panic(err)
		}
	}

	tblID := ctx.SemTable.TableSetFor(tblInfo.GetAliasedTableExpr())
	vTbl := tblInfo.GetVindexTable()
	// Reference table should update the source table.
	if vTbl.Type == vindexes.TypeReference && vTbl.Source != nil {
		vTbl = updateQueryGraphWithSource(ctx, op, tblID, vTbl)
	}

	name, err := tblInfo.Name()
	if err != nil {
		panic(err)
	}

	targetTbl := TargetTable{
		ID:     tblID,
		VTable: vTbl,
		Name:   name,
	}

	cvv, ovq, subQueriesArgOnChangedVindex := getUpdateVindexInformation(ctx, updStmt, targetTbl, assignments)

	updOp := &Update{
		DMLCommon: &DMLCommon{
			Ignore:           updStmt.Ignore,
			Target:           targetTbl,
			OwnedVindexQuery: ovq,
			Source:           op,
		},
		Assignments:                  assignments,
		ChangedVindexValues:          cvv,
		SubQueriesArgOnChangedVindex: subQueriesArgOnChangedVindex,
		VerifyAll:                    ctx.VerifyAllFKs,
	}

	if len(updStmt.OrderBy) > 0 {
		addOrdering(ctx, updStmt.OrderBy, updOp)
	}

	if updStmt.Limit != nil {
		updOp.Source = &Limit{
			Source: updOp.Source,
			AST:    updStmt.Limit,
		}
	}

	return sqc.getRootOperator(updOp, nil), targetTbl, updClone
}

func getUpdateVindexInformation(
	ctx *plancontext.PlanningContext,
	updStmt *sqlparser.Update,
	table TargetTable,
	assignments []SetExpr,
) (map[string]*engine.VindexValues, *sqlparser.Select, []string) {
	if !table.VTable.Keyspace.Sharded {
		return nil, nil, nil
	}

	primaryVindex := getVindexInformation(table.ID, table.VTable)
	changedVindexValues, ownedVindexQuery, subQueriesArgOnChangedVindex := buildChangedVindexesValues(ctx, updStmt, table.VTable, primaryVindex.Columns, assignments)
	return changedVindexValues, ownedVindexQuery, subQueriesArgOnChangedVindex
}

func buildFkOperator(ctx *plancontext.PlanningContext, updOp Operator, updClone *sqlparser.Update, parentFks []vindexes.ParentFKInfo, childFks []vindexes.ChildFKInfo, targetTbl TargetTable) Operator {
	// If there is a subquery container above update operator, we want to do the foreign key planning inside it,
	// because we want the Inner of the subquery to execute first and its result be used for the entire foreign key update planning.
	foundSubqc := false
	TopDown(updOp, TableID, func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		if op, isSubqc := in.(*SubQueryContainer); isSubqc {
			foundSubqc = true
			op.Outer = buildFkOperator(ctx, op.Outer, updClone, parentFks, childFks, targetTbl)
		}
		return in, NoRewrite
	}, stopAtUpdateOp)
	if foundSubqc {
		return updOp
	}

	restrictChildFks, cascadeChildFks := splitChildFks(childFks)

	op := createFKCascadeOp(ctx, updOp, updClone, cascadeChildFks, targetTbl)

	return createFKVerifyOp(ctx, op, updClone, parentFks, restrictChildFks, targetTbl.VTable)
}

func stopAtUpdateOp(operator Operator) VisitRule {
	_, isUpdate := operator.(*Update)
	return VisitRule(!isUpdate)
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

func createFKCascadeOp(ctx *plancontext.PlanningContext, parentOp Operator, updStmt *sqlparser.Update, childFks []vindexes.ChildFKInfo, targetTbl TargetTable) Operator {
	if len(childFks) == 0 {
		return parentOp
	}

	var fkChildren []*FkChild
	var selectExprs []sqlparser.SelectExpr

	for _, fk := range childFks {
		// We should have already filtered out update restrict foreign keys.
		if fk.OnUpdate.IsRestrict() {
			panic(vterrors.VT13001("ON UPDATE RESTRICT foreign keys should already be filtered"))
		}

		// We need to select all the parent columns for the foreign key constraint, to use in the update of the child table.
		var selectOffsets []int
		selectOffsets, selectExprs = addColumns(ctx, fk.ParentColumns, selectExprs, targetTbl.Name)

		// If we are updating a foreign key column to a non-literal value then, need information about
		// 1. whether the new value is different from the old value
		// 2. the new value itself.
		// 3. the bind variable to assign to this value.
		var nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo
		ue := ctx.SemTable.GetUpdateExpressionsForFk(fk.String(targetTbl.VTable))
		// We only need to store these offsets and add these expressions to SELECT when there are non-literal updates present.
		if hasNonLiteralUpdate(ue) {
			for _, updExpr := range ue {
				// We add the expression and a comparison expression to the SELECT exprssion while storing their offsets.
				var info engine.NonLiteralUpdateInfo
				info, selectExprs = addNonLiteralUpdExprToSelect(ctx, targetTbl.VTable, updExpr, selectExprs)
				nonLiteralUpdateInfo = append(nonLiteralUpdateInfo, info)
			}
		}

		fkChild := createFkChildForUpdate(ctx, fk, selectOffsets, nonLiteralUpdateInfo, targetTbl.VTable)
		fkChildren = append(fkChildren, fkChild)
	}

	selectionOp := createSelectionOp(ctx, selectExprs, updStmt.TableExprs, updStmt.Where, updStmt.OrderBy, nil, getUpdateLock(targetTbl.VTable))

	return &FkCascade{
		Selection: selectionOp,
		Children:  fkChildren,
		Parent:    parentOp,
	}
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
func addColumns(ctx *plancontext.PlanningContext, columns sqlparser.Columns, exprs []sqlparser.SelectExpr, tableName sqlparser.TableName) ([]int, []sqlparser.SelectExpr) {
	var offsets []int
	selectExprs := exprs
	for _, column := range columns {
		ae := aeWrap(sqlparser.NewColNameWithQualifier(column.String(), tableName))
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
func createFkChildForUpdate(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, selectOffsets []int, nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo, updatedTable *vindexes.Table) *FkChild {
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

	var childOp Operator
	switch fk.OnUpdate {
	case sqlparser.Cascade:
		childOp = buildChildUpdOpForCascade(ctx, fk, childWhereExpr, nonLiteralUpdateInfo, updatedTable)
	case sqlparser.SetNull:
		childOp = buildChildUpdOpForSetNull(ctx, fk, childWhereExpr, nonLiteralUpdateInfo, updatedTable)
	case sqlparser.SetDefault:
		panic(vterrors.VT09016())
	}

	return &FkChild{
		BVName:         bvName,
		Cols:           selectOffsets,
		Op:             childOp,
		NonLiteralInfo: nonLiteralUpdateInfo,
	}
}

// buildChildUpdOpForCascade builds the child update statement operator for the CASCADE type foreign key constraint.
// The query looks like this -
//
//	`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query> WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`
func buildChildUpdOpForCascade(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, childWhereExpr sqlparser.Expr, nonLiteralUpdateInfo []engine.NonLiteralUpdateInfo, updatedTable *vindexes.Table) Operator {
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
	parsedComments := (&sqlparser.ParsedComments{}).SetMySQLSetVarValue(sysvars.ForeignKeyChecks, "OFF").Parsed()
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
) Operator {
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
		parsedComments = parsedComments.SetMySQLSetVarValue(sysvars.ForeignKeyChecks, "ON").Parsed()
	}
	return parsedComments
}

// createFKVerifyOp creates the verify operator for the parent foreign key constraints.
func createFKVerifyOp(
	ctx *plancontext.PlanningContext,
	childOp Operator,
	updStmt *sqlparser.Update,
	parentFks []vindexes.ParentFKInfo,
	restrictChildFks []vindexes.ChildFKInfo,
	updatedTable *vindexes.Table,
) Operator {
	if len(parentFks) == 0 && len(restrictChildFks) == 0 {
		return childOp
	}

	var Verify []*VerifyOp
	// This validates that new values exists on the parent table.
	for _, fk := range parentFks {
		op := createFkVerifyOpForParentFKForUpdate(ctx, updatedTable, updStmt, fk)
		Verify = append(Verify, &VerifyOp{
			Op:  op,
			Typ: engine.ParentVerify,
		})
	}
	// This validates that the old values don't exist on the child table.
	for _, fk := range restrictChildFks {
		op := createFkVerifyOpForChildFKForUpdate(ctx, updatedTable, updStmt, fk)

		Verify = append(Verify, &VerifyOp{
			Op:  op,
			Typ: engine.ChildVerify,
		})
	}

	return &FkVerify{
		Verify: Verify,
		Input:  childOp,
	}
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
func createFkVerifyOpForParentFKForUpdate(ctx *plancontext.PlanningContext, updatedTable *vindexes.Table, updStmt *sqlparser.Update, pFK vindexes.ParentFKInfo) Operator {
	childTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	childTbl, err := childTblExpr.TableName()
	if err != nil {
		panic(err)
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
		getVerifyLock(updatedTable))
}

func getVerifyLock(vTbl *vindexes.Table) sqlparser.Lock {
	if len(vTbl.UniqueKeys) > 0 {
		return sqlparser.ForShareLockNoWait
	}
	return sqlparser.ForShareLock
}

func getUpdateLock(vTbl *vindexes.Table) sqlparser.Lock {
	if len(vTbl.UniqueKeys) > 0 {
		return sqlparser.ForUpdateLockNoWait
	}
	return sqlparser.ForUpdateLock
}

// Each child foreign key constraint is verified by a join query of the form:
// select 1 from child_tbl join parent_tbl on <columns in fk> where <clause same as original update> [AND ({<bind variables in the SET clause of the original update> IS NULL OR}... <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>))] limit 1
// E.g:
// Child (c1, c2) references Parent (p1, p2)
// update Parent set p1 = col + 1 where id = 1
// verify query:
// select 1 from Child join Parent on Parent.p1 = Child.c1 and Parent.p2 = Child.c2
// where Parent.id = 1 and ((Parent.col + 1) IS NULL OR (child.c1) NOT IN ((Parent.col + 1))) limit 1
func createFkVerifyOpForChildFKForUpdate(ctx *plancontext.PlanningContext, updatedTable *vindexes.Table, updStmt *sqlparser.Update, cFk vindexes.ChildFKInfo) Operator {
	// ON UPDATE RESTRICT foreign keys that require validation, should only be allowed in the case where we
	// are verifying all the FKs on vtgate level.
	if !ctx.VerifyAllFKs {
		panic(vterrors.VT12002())
	}
	parentTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	parentTbl, err := parentTblExpr.TableName()
	if err != nil {
		panic(err)
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
		getVerifyLock(updatedTable))
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

func buildChangedVindexesValues(
	ctx *plancontext.PlanningContext,
	update *sqlparser.Update,
	table *vindexes.Table,
	ksidCols []sqlparser.IdentifierCI,
	assignments []SetExpr,
) (changedVindexes map[string]*engine.VindexValues, ovq *sqlparser.Select, subQueriesArgOnChangedVindex []string) {
	changedVindexes = make(map[string]*engine.VindexValues)
	selExprs, offset := initialQuery(ksidCols, table)
	for i, vindex := range table.ColumnVindexes {
		vindexValueMap := make(map[string]evalengine.Expr)
		var compExprs []sqlparser.Expr
		for _, vcol := range vindex.Columns {
			subQueriesArgOnChangedVindex, compExprs =
				createAssignmentExpressions(ctx, assignments, vcol, subQueriesArgOnChangedVindex, vindexValueMap, compExprs)
		}
		if len(vindexValueMap) == 0 {
			// Vindex not changing, continue
			continue
		}
		if i == 0 {
			panic(vterrors.VT12001(fmt.Sprintf("you cannot UPDATE primary vindex columns; invalid update on vindex: %v", vindex.Name)))
		}
		if _, ok := vindex.Vindex.(vindexes.Lookup); !ok {
			panic(vterrors.VT12001(fmt.Sprintf("you can only UPDATE lookup vindexes; invalid update on vindex: %v", vindex.Name)))
		}

		// Checks done, let's actually add the expressions and the vindex map
		selExprs = append(selExprs, aeWrap(sqlparser.AndExpressions(compExprs...)))
		changedVindexes[vindex.Name] = &engine.VindexValues{
			EvalExprMap: vindexValueMap,
			Offset:      offset,
		}
		offset++
	}
	if len(changedVindexes) == 0 {
		return nil, nil, nil
	}
	// generate rest of the owned vindex query.
	ovq = &sqlparser.Select{
		SelectExprs: selExprs,
		OrderBy:     update.OrderBy,
		Limit:       update.Limit,
		Lock:        sqlparser.ForUpdateLock,
	}
	return changedVindexes, ovq, subQueriesArgOnChangedVindex
}

func initialQuery(ksidCols []sqlparser.IdentifierCI, table *vindexes.Table) (sqlparser.SelectExprs, int) {
	var selExprs sqlparser.SelectExprs
	offset := 0
	for _, col := range ksidCols {
		selExprs = append(selExprs, aeWrap(sqlparser.NewColName(col.String())))
		offset++
	}
	for _, cv := range table.Owned {
		for _, column := range cv.Columns {
			selExprs = append(selExprs, aeWrap(sqlparser.NewColName(column.String())))
			offset++
		}
	}
	return selExprs, offset
}

func createAssignmentExpressions(
	ctx *plancontext.PlanningContext,
	assignments []SetExpr,
	vcol sqlparser.IdentifierCI,
	subQueriesArgOnChangedVindex []string,
	vindexValueMap map[string]evalengine.Expr,
	compExprs []sqlparser.Expr,
) ([]string, []sqlparser.Expr) {
	// Searching in order of columns in colvindex.
	found := false
	for _, assignment := range assignments {
		if !vcol.Equal(assignment.Name.Name) {
			continue
		}
		if found {
			panic(vterrors.VT03015(assignment.Name.Name))
		}
		found = true
		pv, err := evalengine.Translate(assignment.Expr.EvalExpr, &evalengine.Config{
			ResolveType: ctx.SemTable.TypeForExpr,
			Collation:   ctx.SemTable.Collation,
			Environment: ctx.VSchema.Environment(),
		})
		if err != nil {
			panic(invalidUpdateExpr(assignment.Name.Name.String(), assignment.Expr.EvalExpr))
		}

		if assignment.Expr.Info != nil {
			sqe, ok := assignment.Expr.Info.(SubQueryExpression)
			if ok {
				for _, sq := range sqe {
					subQueriesArgOnChangedVindex = append(subQueriesArgOnChangedVindex, sq.ArgName)
				}
			}
		}

		vindexValueMap[vcol.String()] = pv
		compExprs = append(compExprs, sqlparser.NewComparisonExpr(sqlparser.EqualOp, assignment.Name, assignment.Expr.EvalExpr, nil))
	}
	return subQueriesArgOnChangedVindex, compExprs
}

func invalidUpdateExpr(upd string, expr sqlparser.Expr) error {
	return vterrors.VT12001(fmt.Sprintf("only values are supported; invalid update on column: `%s` with expr: [%s]", upd, sqlparser.String(expr)))
}
