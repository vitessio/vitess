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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Delete struct {
	QTable           *QueryTable
	VTable           *vindexes.Table
	OwnedVindexQuery string
	AST              *sqlparser.Delete

	noInputs
	noColumns
	noPredicates
}

// Introduces implements the PhysicalOperator interface
func (d *Delete) introducesTableID() semantics.TableSet {
	return d.QTable.ID
}

// Clone implements the Operator interface
func (d *Delete) Clone([]Operator) Operator {
	return &Delete{
		QTable:           d.QTable,
		VTable:           d.VTable,
		OwnedVindexQuery: d.OwnedVindexQuery,
		AST:              d.AST,
	}
}

func (d *Delete) TablesUsed() []string {
	if d.VTable != nil {
		return SingleQualifiedIdentifier(d.VTable.Keyspace, d.VTable.Name)
	}
	return nil
}

func (d *Delete) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

func (d *Delete) ShortDescription() string {
	return fmt.Sprintf("%s.%s %s", d.VTable.Keyspace.Name, d.VTable.Name.String(), sqlparser.String(d.AST.Where))
}

func (d *Delete) Statement() sqlparser.Statement {
	return d.AST
}

func createOperatorFromDelete(ctx *plancontext.PlanningContext, deleteStmt *sqlparser.Delete) Operator {
	tableInfo, qt := createQueryTableForDML(ctx, deleteStmt.TableExprs[0], deleteStmt.Where)
	vindexTable, routing := buildVindexTableForDML(ctx, tableInfo, qt, "delete")

	delClone := sqlparser.CloneRefOfDelete(deleteStmt)
	// Create the delete operator first.
	delOp := createDeleteOperator(ctx, deleteStmt, qt, vindexTable, routing)
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
	if deleteStmt.Limit != nil {
		panic(vterrors.VT12001("foreign keys management at vitess with limit"))
	}

	return createFkCascadeOpForDelete(ctx, delOp, delClone, childFks)
}

func createDeleteOperator(
	ctx *plancontext.PlanningContext,
	deleteStmt *sqlparser.Delete,
	qt *QueryTable,
	vindexTable *vindexes.Table,
	routing Routing) Operator {
	del := &Delete{
		QTable: qt,
		VTable: vindexTable,
		AST:    deleteStmt,
	}
	route := &Route{
		Source:  del,
		Routing: routing,
	}

	if !vindexTable.Keyspace.Sharded {
		return route
	}

	primaryVindex, vindexAndPredicates := getVindexInformation(qt.ID, vindexTable)

	tr, ok := routing.(*ShardedRouting)
	if ok {
		tr.VindexPreds = vindexAndPredicates
	}

	var ovq string
	if len(vindexTable.Owned) > 0 {
		tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: vindexTable.Name}, As: qt.Alias.As}
		ovq = generateOwnedVindexQuery(tblExpr, deleteStmt, vindexTable, primaryVindex.Columns)
	}

	del.OwnedVindexQuery = ovq

	sqc := &SubQueryBuilder{}
	for _, predicate := range qt.Predicates {
		subq := sqc.handleSubquery(ctx, predicate, qt.ID)
		if subq != nil {
			continue
		}

		routing = UpdateRoutingLogic(ctx, predicate, routing)
	}

	if routing.OpCode() == engine.Scatter && deleteStmt.Limit != nil {
		// TODO systay: we should probably check for other op code types - IN could also hit multiple shards (2022-04-07)
		panic(vterrors.VT12001("multi shard DELETE with LIMIT"))
	}

	return sqc.getRootOperator(route, nil)
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
