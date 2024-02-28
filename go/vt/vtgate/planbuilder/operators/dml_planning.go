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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type DMLCommon struct {
	Ignore           sqlparser.Ignore
	Target           TargetTable
	OwnedVindexQuery *sqlparser.Select
	Source           Operator
}

type TargetTable struct {
	ID     semantics.TableSet
	VTable *vindexes.Table
	Name   sqlparser.TableName
}

func shortDesc(target TargetTable, ovq *sqlparser.Select) string {
	ovqString := ""
	if ovq != nil {
		var cols, orderby, limit string
		cols = fmt.Sprintf("COLUMNS: [%s]", sqlparser.String(ovq.SelectExprs))
		if len(ovq.OrderBy) > 0 {
			orderby = fmt.Sprintf(" ORDERBY: [%s]", sqlparser.String(ovq.OrderBy))
		}
		if ovq.Limit != nil {
			limit = fmt.Sprintf(" LIMIT: [%s]", sqlparser.String(ovq.Limit))
		}
		ovqString = fmt.Sprintf(" vindexQuery(%s%s%s)", cols, orderby, limit)
	}
	return fmt.Sprintf("%s.%s%s", target.VTable.Keyspace.Name, target.VTable.Name.String(), ovqString)
}

// getVindexInformation returns the vindex and VindexPlusPredicates for the DML,
// If it cannot find a unique vindex match, it returns an error.
func getVindexInformation(id semantics.TableSet, table *vindexes.Table) (
	*vindexes.ColumnVindex,
	[]*VindexPlusPredicates) {

	// Check that we have a primary vindex which is valid
	if len(table.ColumnVindexes) == 0 || !table.ColumnVindexes[0].IsUnique() {
		panic(vterrors.VT09001(table.Name))
	}
	primaryVindex := table.ColumnVindexes[0]

	var vindexesAndPredicates []*VindexPlusPredicates
	for _, colVindex := range table.Ordered {
		if lu, isLu := colVindex.Vindex.(vindexes.LookupBackfill); isLu && lu.IsBackfilling() {
			// Checking if the Vindex is currently backfilling or not, if it isn't we can read from the vindex table,
			// and we will be able to do a delete equal. Otherwise, we continue to look for next best vindex.
			continue
		}

		vindexesAndPredicates = append(vindexesAndPredicates, &VindexPlusPredicates{
			ColVindex: colVindex,
			TableID:   id,
		})
	}
	return primaryVindex, vindexesAndPredicates
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

func buildChangedVindexesValues(
	ctx *plancontext.PlanningContext,
	update *sqlparser.Update,
	table *vindexes.Table,
	ksidCols []sqlparser.IdentifierCI,
	assignments []SetExpr,
) (vv map[string]*engine.VindexValues, ownedVindexQuery *sqlparser.Select, subQueriesArgOnChangedVindex []string) {
	changedVindexes := make(map[string]*engine.VindexValues)
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
	ovq := &sqlparser.Select{
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

func invalidUpdateExpr(upd string, expr sqlparser.Expr) error {
	return vterrors.VT12001(fmt.Sprintf("only values are supported; invalid update on column: `%s` with expr: [%s]", upd, sqlparser.String(expr)))
}
