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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Delete struct {
	Target           TargetTable
	OwnedVindexQuery string
	OrderBy          sqlparser.OrderBy
	Limit            *sqlparser.Limit
	Ignore           bool
	Source           Operator

	noColumns
	noPredicates
}

type TargetTable struct {
	ID     semantics.TableSet
	VTable *vindexes.Table
	Name   sqlparser.TableName
}

// Introduces implements the PhysicalOperator interface
func (d *Delete) introducesTableID() semantics.TableSet {
	return d.Target.ID
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
	if len(inputs) != 1 {
		panic(vterrors.VT13001("unexpected number of inputs to Delete operator"))
	}
	d.Source = inputs[0]
}

func (d *Delete) TablesUsed() []string {
	return SingleQualifiedIdentifier(d.Target.VTable.Keyspace, d.Target.VTable.Name)
}

func (d *Delete) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

func (d *Delete) ShortDescription() string {
	limit := ""
	orderBy := ""
	if d.Limit != nil {
		limit = " " + sqlparser.String(d.Limit)
	}
	if len(d.OrderBy) > 0 {
		orderBy = " " + sqlparser.String(d.OrderBy)
	}

	return fmt.Sprintf("%s.%s%s%s", d.Target.VTable.Keyspace.Name, d.Target.VTable.Name.String(), orderBy, limit)
}

func createOperatorFromDelete(ctx *plancontext.PlanningContext, deleteStmt *sqlparser.Delete) Operator {
	delClone := sqlparser.CloneRefOfDelete(deleteStmt)

	delOp := createDeleteOperator(ctx, deleteStmt)

	if deleteStmt.Comments != nil {
		delOp = &LockAndComment{
			Source:   delOp,
			Comments: deleteStmt.Comments,
		}
	}

	childFks := ctx.SemTable.GetChildForeignKeysList()
	// If there are no foreign key constraints, then we don't need to do anything.
	if len(childFks) == 0 {
		return delOp
	}
	// If the delete statement has a limit, we don't support it yet.
	if delClone.Limit != nil {
		panic(vterrors.VT12001("foreign keys management at vitess with limit"))
	}

	return createFkCascadeOpForDelete(ctx, delOp, delClone, childFks)
}

func createDeleteOperator(ctx *plancontext.PlanningContext, del *sqlparser.Delete) Operator {
	op := crossJoin(ctx, del.TableExprs)

	if del.Where != nil {
		op = addWherePredicates(ctx, del.Where.Expr, op)
	}

	target := del.Targets[0]
	tblID, exists := ctx.SemTable.Targets[target.Name]
	if !exists {
		panic(vterrors.VT13001("delete target table should be part of semantic analyzer"))
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

	var ovq string
	if vTbl.Keyspace.Sharded && vTbl.Type == vindexes.TypeTable {
		primaryVindex, _ := getVindexInformation(tblID, vTbl)
		ate := tblInfo.GetAliasedTableExpr()
		if len(vTbl.Owned) > 0 {
			ovq = generateOwnedVindexQuery(ate, del, vTbl, primaryVindex.Columns)
		}
	}

	name, err := tblInfo.Name()
	if err != nil {
		panic(err)
	}

	return &Delete{
		Target: TargetTable{
			ID:     tblID,
			VTable: vTbl,
			Name:   name,
		},
		Source:           op,
		Ignore:           bool(del.Ignore),
		Limit:            del.Limit,
		OrderBy:          del.OrderBy,
		OwnedVindexQuery: ovq,
	}
}

func updateQueryGraphWithSource(ctx *plancontext.PlanningContext, input Operator, tblID semantics.TableSet, vTbl *vindexes.Table) *vindexes.Table {
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
			panic(vterrors.VT12001("DELETE on reference table with join"))
		}
		for _, tbl := range qg.Tables {
			if tbl.ID != tblID {
				continue
			}
			tbl.Alias = sqlparser.NewAliasedTableExpr(sqlparser.NewTableName(vTbl.Name.String()), tbl.Alias.As.String())
			tbl.Table, _ = tbl.Alias.TableName()
		}
		return op, Rewrote("change query table point to source table")
	}, func(operator Operator) VisitRule {
		_, ok := operator.(*QueryGraph)
		return VisitRule(ok)
	})
	return vTbl
}

func createFkCascadeOpForDelete(ctx *plancontext.PlanningContext, parentOp Operator, delStmt *sqlparser.Delete, childFks []vindexes.ChildFKInfo) Operator {
	var fkChildren []*FkChild
	var selectExprs []sqlparser.SelectExpr
	for _, fk := range childFks {
		// Any RESTRICT type foreign keys that arrive here,
		// are cross-shard/cross-keyspace RESTRICT cases, which we don't currently support.
		if fk.OnDelete.IsRestrict() {
			panic(vterrors.VT12002())
		}

		// We need to select all the parent columns for the foreign key constraint, to use in the update of the child table.
		var offsets []int
		offsets, selectExprs = addColumns(ctx, fk.ParentColumns, selectExprs)

		fkChildren = append(fkChildren,
			createFkChildForDelete(ctx, fk, offsets))
	}
	selectionOp := createSelectionOp(ctx, selectExprs, delStmt.TableExprs, delStmt.Where, nil, nil, sqlparser.ForUpdateLockNoWait)

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
