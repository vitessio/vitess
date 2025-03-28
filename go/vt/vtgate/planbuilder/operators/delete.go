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
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Delete struct {
	*DMLCommon

	noColumns
	noPredicates
}

// Clone implements the Operator interface
func (d *Delete) Clone(inputs []Operator) Operator {
	newD := *d
	newD.SetInputs(inputs)
	return &newD
}

func (d *Delete) Inputs() []Operator {
	return []Operator{d.Source}
}

func (d *Delete) SetInputs(inputs []Operator) {
	d.Source = inputs[0]
}

func (d *Delete) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

func (d *Delete) TablesUsed(in []string) []string {
	return append(in, QualifiedString(d.Target.VTable.Keyspace, d.Target.VTable.Name.String()))
}

func (d *Delete) ShortDescription() string {
	return shortDesc(d.Target, d.OwnedVindexQuery)
}

func createOperatorFromDelete(ctx *plancontext.PlanningContext, deleteStmt *sqlparser.Delete) (op Operator) {
	childFks := ctx.SemTable.GetChildForeignKeysForTargets()

	// We check if delete with input plan is required. DML with input planning is generally
	// slower, because it does a selection and then creates a delete statement wherein we have to
	// list all the primary key values.
	if deleteWithInputPlanningRequired(childFks, deleteStmt) {
		return createDeleteWithInputOp(ctx, deleteStmt)
	}

	delClone := sqlparser.Clone(deleteStmt)
	var vTbl *vindexes.BaseTable
	op, vTbl = createDeleteOperator(ctx, deleteStmt)

	if deleteStmt.Comments != nil {
		op = newLockAndComment(op, deleteStmt.Comments, sqlparser.NoLock)
	}

	var err error
	childFks, err = ctx.SemTable.GetChildForeignKeysForTable(deleteStmt.Targets[0])
	if err != nil {
		panic(err)
	}
	// If there are no foreign key constraints, then we don't need to do anything special.
	if len(childFks) == 0 {
		return op
	}

	return createFkCascadeOpForDelete(ctx, op, delClone, childFks, vTbl)
}

func deleteWithInputPlanningRequired(childFks []vindexes.ChildFKInfo, deleteStmt *sqlparser.Delete) bool {
	if len(deleteStmt.Targets) > 1 {
		return true
	}
	// If there are no foreign keys, we don't need to use delete with input.
	if len(childFks) == 0 {
		return false
	}
	// Limit requires delete with input.
	if deleteStmt.Limit != nil {
		return true
	}
	// If there are no limit clauses, and it is not a multi-delete, we don't need delete with input.
	// TODO: In the future, we can check if the tables involved in the multi-table delete are related by foreign keys or not.
	// If they aren't then we don't need the multi-table delete. But this check isn't so straight-forward. We need to check if the two
	// tables are connected in the undirected graph built from the tables related by foreign keys.
	return !deleteStmt.IsSingleAliasExpr()
}

func createDeleteWithInputOp(ctx *plancontext.PlanningContext, del *sqlparser.Delete) (op Operator) {
	delClone := ctx.SemTable.Clone(del).(*sqlparser.Delete)
	del.Limit = nil
	del.OrderBy = nil

	selectStmt := &sqlparser.Select{
		From:    delClone.TableExprs,
		Where:   delClone.Where,
		OrderBy: delClone.OrderBy,
		Limit:   delClone.Limit,
		Lock:    sqlparser.ForUpdateLock,
	}

	var delOps []dmlOp
	for _, target := range ctx.SemTable.DMLTargets.Constituents() {
		op := createDeleteOpWithTarget(ctx, target, del.Ignore)
		delOps = append(delOps, op)
	}

	delOps = sortDmlOps(delOps)

	// now map the operator and column list.
	var colsList [][]*sqlparser.ColName
	dmls := slice.Map(delOps, func(from dmlOp) Operator {
		colsList = append(colsList, from.cols)
		for _, col := range from.cols {
			selectStmt.AddSelectExpr(aeWrap(col))
		}
		return from.op
	})

	op = &DMLWithInput{
		DML:    dmls,
		Source: createOperatorFromSelect(ctx, selectStmt),
		cols:   colsList,
	}

	if del.Comments != nil {
		op = newLockAndComment(op, del.Comments, sqlparser.NoLock)
	}
	return op
}

// getFirstVindex returns the first Vindex, if available
func getFirstVindex(vTbl *vindexes.BaseTable) vindexes.Vindex {
	if len(vTbl.ColumnVindexes) > 0 {
		return vTbl.ColumnVindexes[0].Vindex
	}
	return nil
}

func createDeleteOpWithTarget(ctx *plancontext.PlanningContext, target semantics.TableSet, ignore sqlparser.Ignore) dmlOp {
	ti, err := ctx.SemTable.TableInfoFor(target)
	if err != nil {
		panic(vterrors.VT13001(err.Error()))
	}

	vTbl := ti.GetVindexTable()
	if len(vTbl.PrimaryKey) == 0 {
		panic(vterrors.VT09015())
	}
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

	del := &sqlparser.Delete{
		Ignore:     ignore,
		TableExprs: sqlparser.TableExprs{ti.GetAliasedTableExpr()},
		Targets:    sqlparser.TableNames{tblName},
		Where:      sqlparser.NewWhere(sqlparser.WhereClause, compExpr),
	}
	return dmlOp{
		op:   createOperatorFromDelete(ctx, del),
		vTbl: vTbl,
		cols: cols,
	}
}

func createDeleteOperator(ctx *plancontext.PlanningContext, del *sqlparser.Delete) (Operator, *vindexes.BaseTable) {
	op := crossJoin(ctx, del.TableExprs)

	sqc := &SubQueryBuilder{}
	if del.Where != nil {
		op = addWherePredsToSubQueryBuilder(ctx, del.Where.Expr, op, sqc)
	}

	tblID, err := ctx.SemTable.GetTargetTableSetForTableName(del.Targets[0])
	if err != nil {
		panic(err)
	}
	tblInfo, err := ctx.SemTable.TableInfoFor(tblID)
	if err != nil {
		panic(err)
	}

	vTbl := tblInfo.GetVindexTable()
	// Reference table should delete from the source table.
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

	var ovq *sqlparser.Select
	if vTbl.Keyspace.Sharded && vTbl.Type == vindexes.TypeTable {
		primaryVindex := getVindexInformation(tblID, vTbl)
		if len(vTbl.Owned) > 0 {
			ovq = generateOwnedVindexQuery(del, targetTbl, primaryVindex.Columns)
		}
	}

	delOp := &Delete{
		DMLCommon: &DMLCommon{
			Ignore:           del.Ignore,
			Target:           targetTbl,
			OwnedVindexQuery: ovq,
		},
	}

	if del.Limit != nil {
		delOp.Source = newLimit(addOrdering(ctx, op, del.OrderBy), del.Limit, false)
	} else {
		delOp.Source = op
	}

	return sqc.getRootOperator(delOp, nil), vTbl
}

func generateOwnedVindexQuery(del *sqlparser.Delete, table TargetTable, ksidCols []sqlparser.IdentifierCI) *sqlparser.Select {
	var selExprs []sqlparser.SelectExpr
	for _, col := range ksidCols {
		colName := makeColName(col, table, sqlparser.MultiTable(del.TableExprs))
		selExprs = append(selExprs, aeWrap(colName))
	}
	for _, cv := range table.VTable.Owned {
		for _, col := range cv.Columns {
			colName := makeColName(col, table, sqlparser.MultiTable(del.TableExprs))
			selExprs = append(selExprs, aeWrap(colName))
		}
	}
	sel := &sqlparser.Select{
		OrderBy: del.OrderBy,
		Limit:   del.Limit,
		Lock:    sqlparser.ForUpdateLock,
	}
	sel.SetSelectExprs(selExprs...)
	return sel
}

func makeColName(col sqlparser.IdentifierCI, table TargetTable, isMultiTbl bool) *sqlparser.ColName {
	if isMultiTbl {
		return sqlparser.NewColNameWithQualifier(col.String(), table.Name)
	}
	return sqlparser.NewColName(col.String())
}

func addOrdering(ctx *plancontext.PlanningContext, op Operator, orderBy sqlparser.OrderBy) Operator {
	es := &expressionSet{}
	var order []OrderBy
	for _, ord := range orderBy {
		if sqlparser.IsNull(ord.Expr) || !es.add(ctx, ord.Expr) {
			// ORDER BY null, or expression repeated can safely be ignored
			continue
		}

		order = append(order, OrderBy{
			Inner:          sqlparser.Clone(ord),
			SimplifiedExpr: ord.Expr,
		})
	}
	if len(order) == 0 {
		return op
	}
	return newOrdering(op, order)
}

func updateQueryGraphWithSource(ctx *plancontext.PlanningContext, input Operator, tblID semantics.TableSet, vTbl *vindexes.BaseTable) *vindexes.BaseTable {
	sourceTable, _, _, _, _, err := ctx.VSchema.FindTableOrVindex(vTbl.Source.TableName)
	if err != nil {
		panic(err)
	}
	vTbl = sourceTable
	TopDown(input, TableID, func(op Operator, lhsTables semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		qg, ok := op.(*QueryGraph)
		if !ok {
			return op, NoRewrite
		}
		if len(qg.Tables) > 1 {
			panic(vterrors.VT12001("DML on reference table with join"))
		}
		for _, tbl := range qg.Tables {
			if tbl.ID != tblID {
				continue
			}
			tbl.Alias = sqlparser.NewAliasedTableExpr(sqlparser.NewTableNameWithQualifier(vTbl.Name.String(), vTbl.Keyspace.Name), tbl.Alias.As.String())
			tbl.Table, _ = tbl.Alias.TableName()
		}
		return op, Rewrote("change query table point to source table")
	}, func(operator Operator) VisitRule {
		_, ok := operator.(*QueryGraph)
		return VisitRule(ok)
	})
	return vTbl
}

func createFkCascadeOpForDelete(ctx *plancontext.PlanningContext, parentOp Operator, delStmt *sqlparser.Delete, childFks []vindexes.ChildFKInfo, deletedTbl *vindexes.BaseTable) Operator {
	var fkChildren []*FkChild
	var selectExprs []sqlparser.SelectExpr
	tblName := delStmt.Targets[0]
	for _, fk := range childFks {
		// Any RESTRICT type foreign keys that arrive here,
		// are cross-shard/cross-keyspace RESTRICT cases, which we don't currently support.
		if fk.OnDelete.IsRestrict() {
			panic(vterrors.VT12002())
		}

		// We need to select all the parent columns for the foreign key constraint, to use in the update of the child table.
		var offsets []int
		offsets, selectExprs = addColumns(ctx, fk.ParentColumns, selectExprs, tblName)

		fkChildren = append(fkChildren,
			createFkChildForDelete(ctx, fk, offsets))
	}
	selectionOp := createSelectionOp(ctx, selectExprs, delStmt.TableExprs, delStmt.Where, nil, nil, getUpdateLock(deletedTbl))

	return &FkCascade{
		Selection: selectionOp,
		Children:  fkChildren,
		Parent:    parentOp,
	}
}

func createFkChildForDelete(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, cols []int) *FkChild {
	bvName := ctx.ReservedVars.ReserveVariable(foreignKeyConstraintValues)
	parsedComments := getParsedCommentsForFkChecks(ctx)
	var childStmt sqlparser.Statement
	switch fk.OnDelete {
	case sqlparser.Cascade:
		// We now construct the delete query for the child table.
		// The query looks something like this - `DELETE FROM <child_table> WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`
		var valTuple sqlparser.ValTuple
		for _, column := range fk.ChildColumns {
			valTuple = append(valTuple, sqlparser.NewColName(column.String()))
		}
		compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, valTuple, sqlparser.NewListArg(bvName), nil)
		childStmt = &sqlparser.Delete{
			Comments:   parsedComments,
			TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
			Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: compExpr},
		}
	case sqlparser.SetNull:
		// We now construct the update query for the child table.
		// The query looks something like this - `UPDATE <child_table> SET <child_column_in_fk> = NULL [AND <another_child_column_in_fk> = NULL]... WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`
		var valTuple sqlparser.ValTuple
		var updExprs sqlparser.UpdateExprs
		for _, column := range fk.ChildColumns {
			valTuple = append(valTuple, sqlparser.NewColName(column.String()))
			updExprs = append(updExprs, &sqlparser.UpdateExpr{
				Name: sqlparser.NewColName(column.String()),
				Expr: &sqlparser.NullVal{},
			})
		}
		compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, valTuple, sqlparser.NewListArg(bvName), nil)
		childStmt = &sqlparser.Update{
			Exprs:      updExprs,
			Comments:   parsedComments,
			TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
			Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: compExpr},
		}
	case sqlparser.SetDefault:
		panic(vterrors.VT09016())
	}

	// For the child statement of a DELETE query, we don't need to verify all the FKs on VTgate or ignore any foreign key explicitly.
	childOp := createOpFromStmt(ctx, childStmt, false /* verifyAllFKs */, "" /* fkToIgnore */)

	return &FkChild{
		BVName: bvName,
		Cols:   cols,
		Op:     childOp,
	}
}
