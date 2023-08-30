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
	"strconv"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

const foriegnKeyContraintValues = "fkc_vals"

// translateQueryToOp creates an operator tree that represents the input SELECT or UNION query
func translateQueryToOp(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (op ops.Operator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelect(ctx, node)
	case *sqlparser.Union:
		op, err = createOperatorFromUnion(ctx, node)
	case *sqlparser.Update:
		op, err = createOperatorFromUpdate(ctx, node)
	case *sqlparser.Delete:
		op, err = createOperatorFromDelete(ctx, node)
	case *sqlparser.Insert:
		op, err = createOperatorFromInsert(ctx, node)
	default:
		err = vterrors.VT12001(fmt.Sprintf("operator: %T", selStmt))
	}
	if err != nil {
		return nil, err
	}

	return op, nil
}

func createOperatorFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) (ops.Operator, error) {
	op, err := crossJoin(ctx, sel.From)
	if err != nil {
		return nil, err
	}

	if sel.Where == nil {
		return newHorizon(op, sel), nil
	}

	src, err := addWherePredicates(ctx, sel.Where.Expr, op)
	if err != nil {
		return nil, err
	}

	return newHorizon(src, sel), nil
}

func addWherePredicates(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op ops.Operator) (ops.Operator, error) {
	sqc := &SubQueryContainer{}
	outerID := TableID(op)
	exprs := sqlparser.SplitAndExpression(nil, expr)
	for _, expr := range exprs {
		sqlparser.RemoveKeyspaceFromColName(expr)
		isSubq, err := sqc.handleSubquery(ctx, expr, outerID)
		if err != nil {
			return nil, err
		}
		if isSubq {
			continue
		}
		op, err = op.AddPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		addColumnEquality(ctx, expr)
	}
	return sqc.getRootOperator(op), nil
}

func (sq *SubQueryContainer) handleSubquery(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	outerID semantics.TableSet,
) (bool, error) {
	subq := getSubQuery(expr)
	if subq == nil {
		return false, nil
	}

	sqInner, err := createSubquery(ctx, expr, subq, outerID)
	if err != nil {
		return false, err
	}
	sq.Inner = append(sq.Inner, sqInner)

	return true, nil
}

func (sq *SubQueryContainer) getRootOperator(op ops.Operator) ops.Operator {
	if len(sq.Inner) == 0 {
		return op
	}

	sq.Outer = op
	return sq
}

func getSubQuery(expr sqlparser.Expr) *sqlparser.Subquery {
	var subqueryExprExists *sqlparser.Subquery
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if subq, ok := node.(*sqlparser.Subquery); ok {
			subqueryExprExists = subq
			return false, nil
		}
		return true, nil
	}, expr)
	return subqueryExprExists
}

func createSubquery(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	subq *sqlparser.Subquery,
	outerID semantics.TableSet,
) (SubQuery, error) {
	switch expr := expr.(type) {
	case *sqlparser.NotExpr:
		switch inner := expr.Expr.(type) {
		case *sqlparser.ExistsExpr:
			return createExistsSubquery(ctx, expr, subq, outerID, opcode.PulloutNotExists)
		case *sqlparser.ComparisonExpr:
			cmp := *inner
			cmp.Operator = sqlparser.Inverse(cmp.Operator)
			return createComparisonSubQuery(ctx, &cmp, subq, outerID)
		default:
			return createValueSubquery(ctx, expr, subq, outerID)
		}
	case *sqlparser.ExistsExpr:
		return createExistsSubquery(ctx, expr, subq, outerID, opcode.PulloutExists)
	case *sqlparser.ComparisonExpr:
		return createComparisonSubQuery(ctx, expr, subq, outerID)
	case *sqlparser.Subquery:
		return createValueSubquery(ctx, expr, subq, outerID)
	default:
		return nil, vterrors.VT12001("subquery: " + sqlparser.String(expr))
	}
}

// cloneASTAndSemState clones the AST and the semantic state of the input node.
func cloneASTAndSemState[T sqlparser.SQLNode](ctx *plancontext.PlanningContext, original T) T {
	return sqlparser.CopyOnRewrite(original, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		sqlNode, ok := cursor.Node().(sqlparser.Expr)
		if !ok {
			return
		}
		node := sqlparser.CloneExpr(sqlNode)
		cursor.Replace(node)
	}, ctx.SemTable.CopyDependenciesOnSQLNodes).(T)
}

func createSubqueryFilter(
	ctx *plancontext.PlanningContext,
	original sqlparser.Expr,
	subq *sqlparser.Subquery,
	outerID semantics.TableSet,
	predicate sqlparser.Expr,
	filterType opcode.PulloutOpcode,
) (*SubQueryFilter, error) {
	innerSel, ok := subq.Select.(*sqlparser.Select)
	if !ok {
		return nil, vterrors.VT13001("yucki unions")
	}

	subqID := ctx.SemTable.StatementIDs[innerSel]
	totalID := subqID.Merge(outerID)
	jpc := &joinPredicateCollector{
		totalID: totalID,
		subqID:  subqID,
		outerID: outerID,
	}

	sqL := &SubQueryContainer{}

	// we can have connecting predicates both on the inside of the subquery, and in the comparison to the outer query
	if innerSel.Where != nil {
		for _, predicate := range sqlparser.SplitAndExpression(nil, innerSel.Where.Expr) {
			sqlparser.RemoveKeyspaceFromColName(predicate)
			isSubq, err := sqL.handleSubquery(ctx, predicate, totalID)
			if err != nil {
				return nil, err
			}
			if isSubq {
				continue
			}
			jpc.inspectPredicate(ctx, predicate)
		}
	}

	if len(jpc.remainingPredicates) == 0 {
		innerSel.Where = nil
	} else {
		innerSel.Where.Expr = sqlparser.AndExpressions(jpc.remainingPredicates...)
	}

	opInner, err := translateQueryToOp(ctx, innerSel)
	if err != nil {
		return nil, err
	}

	opInner = sqL.getRootOperator(opInner)

	return &SubQueryFilter{
		FilterType:     filterType,
		Subquery:       opInner,
		Predicates:     jpc.predicates,
		OuterPredicate: predicate,
		Original:       original,
	}, nil

}

func createComparisonSubQuery(
	ctx *plancontext.PlanningContext,
	original *sqlparser.ComparisonExpr,
	subFromOutside *sqlparser.Subquery,
	outerID semantics.TableSet,
) (SubQuery, error) {
	subq, outside := semantics.GetSubqueryAndOtherSide(original)
	if outside == nil || subq != subFromOutside {
		panic("uh oh")
	}
	original = cloneASTAndSemState(ctx, original)

	ae, ok := subq.Select.GetColumns()[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, vterrors.VT13001("can't use unexpanded projections here")
	}

	// this is a predicate that will only be used to check if we can merge the subquery with the outer query
	predicate := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     outside,
		Right:    ae.Expr,
	}

	filterType := opcode.PulloutValue
	switch original.Operator {
	case sqlparser.InOp:
		filterType = opcode.PulloutIn
	case sqlparser.NotInOp:
		filterType = opcode.PulloutNotIn
	}

	return createSubqueryFilter(ctx, original, subq, outerID, predicate, filterType)
}

func createExistsSubquery(
	ctx *plancontext.PlanningContext,
	org sqlparser.Expr,
	sq *sqlparser.Subquery,
	outerID semantics.TableSet,
	filterType opcode.PulloutOpcode,
) (*SubQueryFilter, error) {
	org = cloneASTAndSemState(ctx, org)
	return createSubqueryFilter(ctx, org, sq, outerID, nil, filterType)
}

func createValueSubquery(
	ctx *plancontext.PlanningContext,
	org sqlparser.Expr,
	sq *sqlparser.Subquery,
	outerID semantics.TableSet,
) (SubQuery, error) {
	org = cloneASTAndSemState(ctx, org)

	return createSubqueryFilter(ctx, org, sq, outerID, nil, opcode.PulloutValue)
}

type joinPredicateCollector struct {
	predicates          sqlparser.Exprs
	remainingPredicates sqlparser.Exprs

	totalID,
	subqID,
	outerID semantics.TableSet
}

func (jpc *joinPredicateCollector) inspectPredicate(
	ctx *plancontext.PlanningContext,
	predicate sqlparser.Expr,
) {
	deps := ctx.SemTable.RecursiveDeps(predicate)
	// if neither of the two sides of the predicate is enough, but together we have all we need,
	// then we can use this predicate to connect the subquery to the outer query
	if !deps.IsSolvedBy(jpc.subqID) && !deps.IsSolvedBy(jpc.outerID) && deps.IsSolvedBy(jpc.totalID) {
		jpc.addPredicate(predicate)
	} else {
		jpc.remainingPredicates = append(jpc.remainingPredicates, predicate)
	}
}

func (jpc *joinPredicateCollector) addPredicate(predicate sqlparser.Expr) {
	jpc.predicates = append(jpc.predicates, predicate)
}

func createOperatorFromUnion(ctx *plancontext.PlanningContext, node *sqlparser.Union) (ops.Operator, error) {
	opLHS, err := translateQueryToOp(ctx, node.Left)
	if err != nil {
		return nil, err
	}

	_, isRHSUnion := node.Right.(*sqlparser.Union)
	if isRHSUnion {
		return nil, vterrors.VT12001("nesting of UNIONs on the right-hand side")
	}
	opRHS, err := translateQueryToOp(ctx, node.Right)
	if err != nil {
		return nil, err
	}

	lexprs := ctx.SemTable.SelectExprs(node.Left)
	rexprs := ctx.SemTable.SelectExprs(node.Right)

	unionCols := ctx.SemTable.SelectExprs(node)
	union := newUnion([]ops.Operator{opLHS, opRHS}, []sqlparser.SelectExprs{lexprs, rexprs}, unionCols, node.Distinct)
	return newHorizon(union, node), nil
}

func createOpFromStmt(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (ops.Operator, error) {
	newCtx, err := plancontext.CreatePlanningContext(stmt, ctx.ReservedVars, ctx.VSchema, ctx.PlannerVersion)
	if err != nil {
		return nil, err
	}
	ctx = newCtx

	return PlanQuery(ctx, stmt)
}

func createOperatorFromInsert(ctx *plancontext.PlanningContext, ins *sqlparser.Insert) (ops.Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, ins.Table, nil)
	if err != nil {
		return nil, err
	}

	vindexTable, routing, err := buildVindexTableForDML(ctx, tableInfo, qt, "insert")
	if err != nil {
		return nil, err
	}

	if _, target := routing.(*TargetedRouting); target {
		return nil, vterrors.VT12001("INSERT with a target destination")
	}

	insOp := &Insert{
		VTable: vindexTable,
		AST:    ins,
	}
	route := &Route{
		Source:  insOp,
		Routing: routing,
	}

	// Find the foreign key mode and store the ParentFKs that we need to verify.
	ksMode, err := ctx.VSchema.ForeignKeyMode(vindexTable.Keyspace.Name)
	if err != nil {
		return nil, err
	}
	if ksMode == vschemapb.Keyspace_FK_MANAGED {
		parentFKs := vindexTable.ParentFKsNeedsHandling()
		if len(parentFKs) > 0 {
			return nil, vterrors.VT12002()
		}
	}

	// Table column list is nil then add all the columns
	// If the column list is empty then add only the auto-inc column and
	// this happens on calling modifyForAutoinc
	if ins.Columns == nil && valuesProvided(ins.Rows) {
		if vindexTable.ColumnListAuthoritative {
			ins = populateInsertColumnlist(ins, vindexTable)
		} else {
			return nil, vterrors.VT09004()
		}
	}

	// modify column list or values for autoincrement column.
	autoIncGen, err := modifyForAutoinc(ins, vindexTable)
	if err != nil {
		return nil, err
	}
	insOp.AutoIncrement = autoIncGen

	// set insert ignore.
	insOp.Ignore = bool(ins.Ignore) || ins.OnDup != nil

	insOp.ColVindexes = getColVindexes(insOp)
	switch rows := ins.Rows.(type) {
	case sqlparser.Values:
		route.Source, err = insertRowsPlan(insOp, ins, rows)
		if err != nil {
			return nil, err
		}
	case sqlparser.SelectStatement:
		route.Source, err = insertSelectPlan(ctx, insOp, ins, rows)
		if err != nil {
			return nil, err
		}
	}
	return route, nil
}

func insertSelectPlan(ctx *plancontext.PlanningContext, insOp *Insert, ins *sqlparser.Insert, sel sqlparser.SelectStatement) (*Insert, error) {
	if columnMismatch(insOp.AutoIncrement, ins, sel) {
		return nil, vterrors.VT03006()
	}

	selOp, err := PlanQuery(ctx, sel)
	if err != nil {
		return nil, err
	}

	// select plan will be taken as input to insert rows into the table.
	insOp.Input = selOp

	// When the table you are steaming data from and table you are inserting from are same.
	// Then due to locking of the index range on the table we might not be able to insert into the table.
	// Therefore, instead of streaming, this flag will ensure the records are first read and then inserted.
	insertTbl := insOp.TablesUsed()[0]
	selTables := TablesUsed(selOp)
	for _, tbl := range selTables {
		if insertTbl == tbl {
			insOp.ForceNonStreaming = true
			break
		}
	}

	if len(insOp.ColVindexes) == 0 {
		return insOp, nil
	}

	colVindexes := insOp.ColVindexes
	vv := make([][]int, len(colVindexes))
	for idx, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			err := checkAndErrIfVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col)
			if err != nil {
				return nil, err
			}

			colNum := findColumn(ins, col)
			// sharding column values should be provided in the insert.
			if colNum == -1 && idx == 0 {
				return nil, vterrors.VT09003(col)
			}
			vv[idx] = append(vv[idx], colNum)
		}
	}
	insOp.VindexValueOffset = vv
	return insOp, nil
}

func columnMismatch(gen *Generate, ins *sqlparser.Insert, sel sqlparser.SelectStatement) bool {
	origColCount := len(ins.Columns)
	if gen != nil && gen.added {
		// One column got added to the insert query ast for auto increment column.
		// adjusting it here for comparison.
		origColCount--
	}
	if origColCount < sel.GetColumnCount() {
		return true
	}
	if origColCount > sel.GetColumnCount() {
		sel := sqlparser.GetFirstSelect(sel)
		var hasStarExpr bool
		for _, sExpr := range sel.SelectExprs {
			if _, hasStarExpr = sExpr.(*sqlparser.StarExpr); hasStarExpr {
				break
			}
		}
		if !hasStarExpr {
			return true
		}
	}
	return false
}

func insertRowsPlan(insOp *Insert, ins *sqlparser.Insert, rows sqlparser.Values) (*Insert, error) {
	for _, row := range rows {
		if len(ins.Columns) != len(row) {
			return nil, vterrors.VT03006()
		}
	}

	if len(insOp.ColVindexes) == 0 {
		return insOp, nil
	}

	colVindexes := insOp.ColVindexes
	routeValues := make([][][]evalengine.Expr, len(colVindexes))
	for vIdx, colVindex := range colVindexes {
		routeValues[vIdx] = make([][]evalengine.Expr, len(colVindex.Columns))
		for colIdx, col := range colVindex.Columns {
			err := checkAndErrIfVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col)
			if err != nil {
				return nil, err
			}
			routeValues[vIdx][colIdx] = make([]evalengine.Expr, len(rows))
			colNum, _ := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				innerpv, err := evalengine.Translate(row[colNum], nil)
				if err != nil {
					return nil, err
				}
				routeValues[vIdx][colIdx][rowNum] = innerpv
			}
		}
	}
	// here we are replacing the row value with the argument.
	for _, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			colNum, _ := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				name := engine.InsertVarName(col, rowNum)
				row[colNum] = sqlparser.NewArgument(name)
			}
		}
	}
	insOp.VindexValues = routeValues
	return insOp, nil
}

func valuesProvided(rows sqlparser.InsertRows) bool {
	switch values := rows.(type) {
	case sqlparser.Values:
		return len(values) >= 0 && len(values[0]) > 0
	case sqlparser.SelectStatement:
		return true
	}
	return false
}

func getColVindexes(insOp *Insert) (colVindexes []*vindexes.ColumnVindex) {
	// For unsharded table the Column Vindex does not mean anything.
	// And therefore should be ignored.
	if !insOp.VTable.Keyspace.Sharded {
		return
	}
	for _, colVindex := range insOp.VTable.ColumnVindexes {
		if colVindex.IsPartialVindex() {
			continue
		}
		colVindexes = append(colVindexes, colVindex)
	}
	return
}

func checkAndErrIfVindexChanging(setClauses sqlparser.UpdateExprs, col sqlparser.IdentifierCI) error {
	for _, assignment := range setClauses {
		if col.Equal(assignment.Name.Name) {
			valueExpr, isValuesFuncExpr := assignment.Expr.(*sqlparser.ValuesFuncExpr)
			// update on duplicate key is changing the vindex column, not supported.
			if !isValuesFuncExpr || !valueExpr.Name.Name.Equal(assignment.Name.Name) {
				return vterrors.VT12001("DML cannot update vindex column")
			}
			return nil
		}
	}
	return nil
}

// findOrAddColumn finds the position of a column in the insert. If it's
// absent it appends it to the with NULL values.
// It returns the position of the column and also boolean representing whether it was added or already present.
func findOrAddColumn(ins *sqlparser.Insert, col sqlparser.IdentifierCI) (int, bool) {
	colNum := findColumn(ins, col)
	if colNum >= 0 {
		return colNum, false
	}
	colOffset := len(ins.Columns)
	ins.Columns = append(ins.Columns, col)
	if rows, ok := ins.Rows.(sqlparser.Values); ok {
		for i := range rows {
			rows[i] = append(rows[i], &sqlparser.NullVal{})
		}
	}
	return colOffset, true
}

// findColumn returns the column index where it is placed on the insert column list.
// Otherwise, return -1 when not found.
func findColumn(ins *sqlparser.Insert, col sqlparser.IdentifierCI) int {
	for i, column := range ins.Columns {
		if col.Equal(column) {
			return i
		}
	}
	return -1
}

func populateInsertColumnlist(ins *sqlparser.Insert, table *vindexes.Table) *sqlparser.Insert {
	cols := make(sqlparser.Columns, 0, len(table.Columns))
	for _, c := range table.Columns {
		cols = append(cols, c.Name)
	}
	ins.Columns = cols
	return ins
}

// modifyForAutoinc modifies the AST and the plan to generate necessary autoinc values.
// For row values cases, bind variable names are generated using baseName.
func modifyForAutoinc(ins *sqlparser.Insert, vTable *vindexes.Table) (*Generate, error) {
	if vTable.AutoIncrement == nil {
		return nil, nil
	}
	gen := &Generate{
		Keyspace:  vTable.AutoIncrement.Sequence.Keyspace,
		TableName: sqlparser.TableName{Name: vTable.AutoIncrement.Sequence.Name},
	}
	colNum, newColAdded := findOrAddColumn(ins, vTable.AutoIncrement.Column)
	switch rows := ins.Rows.(type) {
	case sqlparser.SelectStatement:
		gen.Offset = colNum
		gen.added = newColAdded
	case sqlparser.Values:
		autoIncValues := make([]evalengine.Expr, 0, len(rows))
		for rowNum, row := range rows {
			// Support the DEFAULT keyword by treating it as null
			if _, ok := row[colNum].(*sqlparser.Default); ok {
				row[colNum] = &sqlparser.NullVal{}
			}
			expr, err := evalengine.Translate(row[colNum], nil)
			if err != nil {
				return nil, err
			}
			autoIncValues = append(autoIncValues, expr)
			row[colNum] = sqlparser.NewArgument(engine.SeqVarName + strconv.Itoa(rowNum))
		}
		gen.Values = evalengine.NewTupleExpr(autoIncValues...)
	}
	return gen, nil
}

func getOperatorFromTableExpr(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, onlyTable bool) (ops.Operator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return getOperatorFromAliasedTableExpr(ctx, tableExpr, onlyTable)
	case *sqlparser.JoinTableExpr:
		return getOperatorFromJoinTableExpr(ctx, tableExpr)
	case *sqlparser.ParenTableExpr:
		return crossJoin(ctx, tableExpr.Exprs)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T table type", tableExpr))
	}
}

func getOperatorFromJoinTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr) (ops.Operator, error) {
	lhs, err := getOperatorFromTableExpr(ctx, tableExpr.LeftExpr, false)
	if err != nil {
		return nil, err
	}
	rhs, err := getOperatorFromTableExpr(ctx, tableExpr.RightExpr, false)
	if err != nil {
		return nil, err
	}

	switch tableExpr.Join {
	case sqlparser.NormalJoinType:
		return createInnerJoin(ctx, tableExpr, lhs, rhs)
	case sqlparser.LeftJoinType, sqlparser.RightJoinType:
		return createOuterJoin(tableExpr, lhs, rhs)
	default:
		return nil, vterrors.VT13001("unsupported: %s", tableExpr.Join.ToString())
	}
}

func getOperatorFromAliasedTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.AliasedTableExpr, onlyTable bool) (ops.Operator, error) {
	tableID := ctx.SemTable.TableSetFor(tableExpr)
	switch tbl := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			return nil, err
		}

		if vt, isVindex := tableInfo.(*semantics.VindexTable); isVindex {
			solves := tableID
			return &Vindex{
				Table: VindexTable{
					TableID: tableID,
					Alias:   tableExpr,
					Table:   tbl,
					VTable:  vt.Table.GetVindexTable(),
				},
				Vindex: vt.Vindex,
				Solved: solves,
			}, nil
		}
		qg := newQueryGraph()
		isInfSchema := tableInfo.IsInfSchema()
		qt := &QueryTable{Alias: tableExpr, Table: tbl, ID: tableID, IsInfSchema: isInfSchema}
		qg.Tables = append(qg.Tables, qt)
		return qg, nil
	case *sqlparser.DerivedTable:
		if onlyTable && tbl.Select.GetLimit() == nil {
			tbl.Select.SetOrderBy(nil)
		}

		inner, err := translateQueryToOp(ctx, tbl.Select)
		if err != nil {
			return nil, err
		}
		if horizon, ok := inner.(*Horizon); ok {
			horizon.TableId = &tableID
			horizon.Alias = tableExpr.As.String()
			horizon.ColumnAliases = tableExpr.Columns
			qp, err := CreateQPFromSelectStatement(ctx, tbl.Select)
			if err != nil {
				return nil, err
			}
			horizon.QP = qp
		}

		return inner, nil
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T", tbl))
	}
}

func crossJoin(ctx *plancontext.PlanningContext, exprs sqlparser.TableExprs) (ops.Operator, error) {
	var output ops.Operator
	for _, tableExpr := range exprs {
		op, err := getOperatorFromTableExpr(ctx, tableExpr, len(exprs) == 1)
		if err != nil {
			return nil, err
		}
		if output == nil {
			output = op
		} else {
			output = createJoin(ctx, output, op)
		}
	}
	return output, nil
}

func createQueryTableForDML(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, whereClause *sqlparser.Where) (semantics.TableInfo, *QueryTable, error) {
	alTbl, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, vterrors.VT13001("expected AliasedTableExpr")
	}
	tblName, ok := alTbl.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, vterrors.VT13001("expected TableName")
	}

	tableID := ctx.SemTable.TableSetFor(alTbl)
	tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
	if err != nil {
		return nil, nil, err
	}

	if tableInfo.IsInfSchema() {
		return nil, nil, vterrors.VT12001("update information schema tables")
	}

	var predicates []sqlparser.Expr
	if whereClause != nil {
		predicates = sqlparser.SplitAndExpression(nil, whereClause.Expr)
	}
	qt := &QueryTable{
		ID:         tableID,
		Alias:      alTbl,
		Table:      tblName,
		Predicates: predicates,
	}
	return tableInfo, qt, nil
}

func addColumnEquality(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator != sqlparser.EqualOp {
			return
		}

		if left, isCol := expr.Left.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(left, expr.Right)
		}
		if right, isCol := expr.Right.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(right, expr.Left)
		}
	}
}

func isRestrict(onDelete sqlparser.ReferenceAction) bool {
	switch onDelete {
	case sqlparser.Restrict, sqlparser.NoAction, sqlparser.DefaultAction:
		return true
	default:
		return false
	}
}

// createSelectionOp creates the selection operator to select the parent columns for the foreign key constraints.
// The Select statement looks something like this - `SELECT <parent_columns_in_fk for all the foreign key constraints> FROM <parent_table> WHERE <where_clause_of_update>`
// TODO (@Harshit, @GuptaManan100): Compress the columns in the SELECT statement, if there are multiple foreign key constraints using the same columns.
func createSelectionOp(ctx *plancontext.PlanningContext, selectExprs []sqlparser.SelectExpr, tableExprs sqlparser.TableExprs, where *sqlparser.Where) (ops.Operator, error) {
	selectionStmt := &sqlparser.Select{
		SelectExprs: selectExprs,
		From:        tableExprs,
		Where:       where,
	}
	return createOpFromStmt(ctx, selectionStmt)
}

func selectParentColumns(fk vindexes.ChildFKInfo, lastOffset int) ([]int, []sqlparser.SelectExpr) {
	var cols []int
	var exprs []sqlparser.SelectExpr
	for _, column := range fk.ParentColumns {
		cols = append(cols, lastOffset)
		exprs = append(exprs, aeWrap(sqlparser.NewColName(column.String())))
		lastOffset++
	}
	return cols, exprs
}
