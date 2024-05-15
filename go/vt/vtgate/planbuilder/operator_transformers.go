/*
Copyright 2021 The Vitess Authors.

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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func transformToLogicalPlan(ctx *plancontext.PlanningContext, op ops.Operator) (logicalPlan, error) {
	switch op := op.(type) {
	case *operators.Route:
		return transformRoutePlan(ctx, op)
	case *operators.ApplyJoin:
		return transformApplyJoinPlan(ctx, op)
	case *operators.Union:
		return transformUnionPlan(ctx, op)
	case *operators.Vindex:
		return transformVindexPlan(ctx, op)
	case *operators.SubQuery:
		return transformSubQuery(ctx, op)
	case *operators.Filter:
		return transformFilter(ctx, op)
	case *operators.Horizon:
		panic("should have been solved in the operator")
	case *operators.Projection:
		return transformProjection(ctx, op)
	case *operators.Limit:
		return transformLimit(ctx, op)
	case *operators.Ordering:
		return transformOrdering(ctx, op)
	case *operators.Aggregator:
		return transformAggregator(ctx, op)
	case *operators.Distinct:
		return transformDistinct(ctx, op)
	case *operators.FkCascade:
		return transformFkCascade(ctx, op)
	case *operators.FkVerify:
		return transformFkVerify(ctx, op)
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToLogicalPlan)", op))
}

// transformFkCascade transforms a FkCascade operator into a logical plan.
func transformFkCascade(ctx *plancontext.PlanningContext, fkc *operators.FkCascade) (logicalPlan, error) {
	// We convert the parent operator to a logical plan.
	parentLP, err := transformToLogicalPlan(ctx, fkc.Parent)
	if err != nil {
		return nil, nil
	}

	// Once we have the parent logical plan, we can create the selection logical plan and the primitives for the children operators.
	// For all of these, we don't need the semTable anymore. We set it to nil, to avoid using an incorrect one.
	ctx.SemTable = nil
	selLP, err := transformToLogicalPlan(ctx, fkc.Selection)
	if err != nil {
		return nil, err
	}

	// Go over the children and convert them to Primitives too.
	var children []*engine.FkChild
	for _, child := range fkc.Children {
		childLP, err := transformToLogicalPlan(ctx, child.Op)
		if err != nil {
			return nil, err
		}
		err = childLP.Wireup(ctx)
		if err != nil {
			return nil, err
		}
		childEngine := childLP.Primitive()
		children = append(children, &engine.FkChild{
			BVName: child.BVName,
			Cols:   child.Cols,
			Exec:   childEngine,
		})
	}

	return newFkCascade(parentLP, selLP, children), nil
}

func transformSubQuery(ctx *plancontext.PlanningContext, op *operators.SubQuery) (logicalPlan, error) {
	outer, err := transformToLogicalPlan(ctx, op.Outer)
	if err != nil {
		return nil, err
	}

	inner, err := transformToLogicalPlan(ctx, op.Subquery)
	if err != nil {
		return nil, err
	}

	cols, err := op.GetJoinColumns(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		// no correlation, so uncorrelated it is
		return newUncorrelatedSubquery(op.FilterType, op.SubqueryValueName, op.HasValuesName, inner, outer), nil
	}

	lhsCols, err := op.OuterExpressionsNeeded(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	return newSemiJoin(outer, inner, op.Vars, lhsCols), nil
}

// transformFkVerify transforms a FkVerify operator into a logical plan.
func transformFkVerify(ctx *plancontext.PlanningContext, fkv *operators.FkVerify) (logicalPlan, error) {
	inputLP, err := transformToLogicalPlan(ctx, fkv.Input)
	if err != nil {
		return nil, err
	}

	// Once we have the input logical plan, we can create the primitives for the verification operators.
	// For all of these, we don't need the semTable anymore. We set it to nil, to avoid using an incorrect one.
	ctx.SemTable = nil

	// Go over the children and convert them to Primitives too.
	var verify []*verifyLP
	for _, v := range fkv.Verify {
		lp, err := transformToLogicalPlan(ctx, v.Op)
		if err != nil {
			return nil, err
		}
		verify = append(verify, &verifyLP{
			verify: lp,
			typ:    v.Typ,
		})
	}

	return newFkVerify(inputLP, verify), nil
}

func transformAggregator(ctx *plancontext.PlanningContext, op *operators.Aggregator) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	oa := &orderedAggregate{
		resultsBuilder: newResultsBuilder(plan, nil),
	}

	for _, aggr := range op.Aggregations {
		if aggr.OpCode == opcode.AggregateUnassigned {
			return nil, vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original)))
		}
		aggrParam := engine.NewAggregateParam(aggr.OpCode, aggr.ColOffset, aggr.Alias)
		aggrParam.Expr = aggr.Func
		aggrParam.Original = aggr.Original
		aggrParam.OrigOpcode = aggr.OriginalOpCode
		aggrParam.WCol = aggr.WSOffset
		aggrParam.Type, aggrParam.CollationID = aggr.GetTypeCollation(ctx)
		oa.aggregates = append(oa.aggregates, aggrParam)
	}
	for _, groupBy := range op.Grouping {
		typ, col, _ := ctx.SemTable.TypeForExpr(groupBy.SimplifiedExpr)
		oa.groupByKeys = append(oa.groupByKeys, &engine.GroupByParams{
			KeyCol:          groupBy.ColOffset,
			WeightStringCol: groupBy.WSOffset,
			Expr:            groupBy.AsAliasedExpr().Expr,
			Type:            typ,
			CollationID:     col,
		})
	}

	if err != nil {
		return nil, err
	}
	oa.truncateColumnCount = op.ResultColumns
	return oa, nil
}

func transformDistinct(ctx *plancontext.PlanningContext, op *operators.Distinct) (logicalPlan, error) {
	src, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}
	return newDistinct(src, op.Columns, op.Truncate), nil
}

func transformOrdering(ctx *plancontext.PlanningContext, op *operators.Ordering) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	return createMemorySort(ctx, plan, op)
}

func createMemorySort(ctx *plancontext.PlanningContext, src logicalPlan, ordering *operators.Ordering) (logicalPlan, error) {
	primitive := &engine.MemorySort{
		TruncateColumnCount: ordering.ResultColumns,
	}
	ms := &memorySort{
		resultsBuilder: newResultsBuilder(src, primitive),
		eMemorySort:    primitive,
	}

	for idx, order := range ordering.Order {
		typ, collationID, _ := ctx.SemTable.TypeForExpr(order.SimplifiedExpr)
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderByParams{
			Col:               ordering.Offset[idx],
			WeightStringCol:   ordering.WOffset[idx],
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: ordering.Offset[idx],
			Type:              typ,
			CollationID:       collationID,
		})
	}

	return ms, nil
}

func transformProjection(ctx *plancontext.PlanningContext, op *operators.Projection) (logicalPlan, error) {
	src, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	if cols := op.AllOffsets(); cols != nil {
		// if all this op is doing is passing through columns from the input, we
		// can use the faster SimpleProjection
<<<<<<< HEAD
		return useSimpleProjection(ctx, op, cols, src)
=======
		return useSimpleProjection(cols, colNames, src)
>>>>>>> 951f2732f3 (Fix aliasing in queries by keeping required projections (#15943))
	}

	ap, err := op.GetAliasedProjections()
	if err != nil {
		return nil, err
	}

	var exprs []sqlparser.Expr
	var evalengineExprs []evalengine.Expr
	var columnNames []string
	for _, pe := range ap {
		ee, err := getEvalEngingeExpr(ctx, pe)
		if err != nil {
			return nil, err
		}
		evalengineExprs = append(evalengineExprs, ee)
		exprs = append(exprs, pe.EvalExpr)
		columnNames = append(columnNames, pe.Original.ColumnName())
	}

	primitive := &engine.Projection{
		Cols:  columnNames,
		Exprs: evalengineExprs,
	}

	return &projection{
		source:      src,
		columnNames: columnNames,
		columns:     exprs,
		primitive:   primitive,
	}, nil
}

func getEvalEngingeExpr(ctx *plancontext.PlanningContext, pe *operators.ProjExpr) (evalengine.Expr, error) {
	switch e := pe.Info.(type) {
	case *operators.EvalEngine:
		return e.EExpr, nil
	case operators.Offset:
		typ, col, _ := ctx.SemTable.TypeForExpr(pe.EvalExpr)
		return evalengine.NewColumn(int(e), typ, col), nil
	default:
		return nil, vterrors.VT13001("project not planned for: %s", pe.String())
	}

}

// useSimpleProjection uses nothing at all if the output is already correct,
// or SimpleProjection when we have to reorder or truncate the columns
<<<<<<< HEAD
func useSimpleProjection(ctx *plancontext.PlanningContext, op *operators.Projection, cols []int, src logicalPlan) (logicalPlan, error) {
	columns, err := op.Source.GetColumns(ctx)
	if err != nil {
		return nil, err
	}
	if len(columns) == len(cols) && elementsMatchIndices(cols) {
		// the columns are already in the right order. we don't need anything at all here
		return src, nil
	}
=======
func useSimpleProjection(cols []int, colNames []string, src logicalPlan) (logicalPlan, error) {
>>>>>>> 951f2732f3 (Fix aliasing in queries by keeping required projections (#15943))
	return &simpleProjection{
		logicalPlanCommon: newBuilderCommon(src),
		eSimpleProj: &engine.SimpleProjection{
			Cols: cols,
		},
	}, nil
}

// elementsMatchIndices checks if the elements of the input slice match
// their corresponding index values. It returns true if all elements match
// their indices, and false otherwise.
func elementsMatchIndices(in []int) bool {
	for idx, val := range in {
		if val != idx {
			return false
		}
	}
	return true
}

func transformFilter(ctx *plancontext.PlanningContext, op *operators.Filter) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	predicate := op.PredicateWithOffsets
	ast := ctx.SemTable.AndExpressions(op.Predicates...)

	if predicate == nil {
		panic("this should have already been done")
	}

	return &filter{
		logicalPlanCommon: newBuilderCommon(plan),
		efilter: &engine.Filter{
			Predicate:    predicate,
			ASTPredicate: ast,
			Truncate:     op.Truncate,
		},
	}, nil
}

func transformApplyJoinPlan(ctx *plancontext.PlanningContext, n *operators.ApplyJoin) (logicalPlan, error) {
	lhs, err := transformToLogicalPlan(ctx, n.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToLogicalPlan(ctx, n.RHS)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if n.LeftJoin {
		opCode = engine.LeftJoin
	}

	return &join{
		Left:   lhs,
		Right:  rhs,
		Cols:   n.Columns,
		Vars:   n.Vars,
		Opcode: opCode,
	}, nil
}

func routeToEngineRoute(ctx *plancontext.PlanningContext, op *operators.Route) (*engine.Route, error) {
	tableNames, err := getAllTableNames(op)
	if err != nil {
		return nil, err
	}

	rp := newRoutingParams(ctx, op.Routing.OpCode())
	err = op.Routing.UpdateRoutingParams(ctx, rp)
	if err != nil {
		return nil, err
	}

	return &engine.Route{
		TableName:           strings.Join(tableNames, ", "),
		RoutingParameters:   rp,
		TruncateColumnCount: op.ResultColumns,
	}, nil
}

func newRoutingParams(ctx *plancontext.PlanningContext, opCode engine.Opcode) *engine.RoutingParameters {
	ks, _ := ctx.VSchema.DefaultKeyspace()
	if ks == nil {
		// if we don't have a selected keyspace, any keyspace will do
		// this is used by operators that do not set the keyspace
		ks, _ = ctx.VSchema.AnyKeyspace()
	}
	return &engine.RoutingParameters{
		Opcode:   opCode,
		Keyspace: ks,
	}
}

func transformRoutePlan(ctx *plancontext.PlanningContext, op *operators.Route) (logicalPlan, error) {
	stmt, dmlOp, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	if stmtWithComments, ok := stmt.(sqlparser.Commented); ok && op.Comments != nil {
		stmtWithComments.SetComments(op.Comments.GetComments())
	}

	switch stmt := stmt.(type) {
	case sqlparser.SelectStatement:
		if op.Lock != sqlparser.NoLock {
			stmt.SetLock(op.Lock)
		}
		return buildRouteLogicalPlan(ctx, op, stmt)
	case *sqlparser.Update:
		return buildUpdateLogicalPlan(ctx, op, dmlOp, stmt)
	case *sqlparser.Delete:
		return buildDeleteLogicalPlan(ctx, op, dmlOp)
	case *sqlparser.Insert:
		return buildInsertLogicalPlan(ctx, op, dmlOp, stmt)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("dont know how to %T", stmt))
	}
}

func buildRouteLogicalPlan(ctx *plancontext.PlanningContext, op *operators.Route, stmt sqlparser.SelectStatement) (logicalPlan, error) {
	condition := getVindexPredicate(op)
	eroute, err := routeToEngineRoute(ctx, op)
	for _, order := range op.Ordering {
		typ, collation, _ := ctx.SemTable.TypeForExpr(order.AST)
		eroute.OrderBy = append(eroute.OrderBy, engine.OrderByParams{
			Col:             order.Offset,
			WeightStringCol: order.WOffset,
			Desc:            order.Direction == sqlparser.DescOrder,
			Type:            typ,
			CollationID:     collation,
		})
	}
	if err != nil {
		return nil, err
	}
	return &route{
		eroute:    eroute,
		Select:    stmt,
		tables:    operators.TableID(op),
		condition: condition,
	}, nil
}

func buildInsertLogicalPlan(ctx *plancontext.PlanningContext, rb *operators.Route, op ops.Operator, stmt *sqlparser.Insert) (logicalPlan, error) {
	ins := op.(*operators.Insert)
	eins := &engine.Insert{
		Opcode:            mapToInsertOpCode(rb.Routing.OpCode(), ins.Input != nil),
		Keyspace:          rb.Routing.Keyspace(),
		TableName:         ins.VTable.Name.String(),
		Ignore:            ins.Ignore,
		ForceNonStreaming: ins.ForceNonStreaming,
		Generate:          autoIncGenerate(ins.AutoIncrement),
		ColVindexes:       ins.ColVindexes,
		VindexValues:      ins.VindexValues,
		VindexValueOffset: ins.VindexValueOffset,
	}
	lp := &insert{eInsert: eins}

	// we would need to generate the query on the fly. The only exception here is
	// when unsharded query with autoincrement for that there is no input operator.
	if eins.Opcode != engine.InsertUnsharded || ins.Input != nil {
		eins.Prefix, eins.Mid, eins.Suffix = generateInsertShardedQuery(ins.AST)
	}

	if ins.Input == nil {
		eins.Query = generateQuery(stmt)
	} else {
		newSrc, err := transformToLogicalPlan(ctx, ins.Input)
		if err != nil {
			return nil, err
		}
		lp.source = newSrc
	}

	return lp, nil
}

func mapToInsertOpCode(code engine.Opcode, insertSelect bool) engine.InsertOpcode {
	if code == engine.Unsharded {
		return engine.InsertUnsharded
	}
	if insertSelect {
		return engine.InsertSelect
	}
	return engine.InsertSharded
}

func autoIncGenerate(gen *operators.Generate) *engine.Generate {
	if gen == nil {
		return nil
	}
	selNext := &sqlparser.Select{
		From:        []sqlparser.TableExpr{&sqlparser.AliasedTableExpr{Expr: gen.TableName}},
		SelectExprs: sqlparser.SelectExprs{&sqlparser.Nextval{Expr: &sqlparser.Argument{Name: "n", Type: sqltypes.Int64}}},
	}
	return &engine.Generate{
		Keyspace: gen.Keyspace,
		Query:    sqlparser.String(selNext),
		Values:   gen.Values,
		Offset:   gen.Offset,
	}
}

func generateInsertShardedQuery(ins *sqlparser.Insert) (prefix string, mids sqlparser.Values, suffix sqlparser.OnDup) {
	mids, isValues := ins.Rows.(sqlparser.Values)
	prefixFormat := "insert %v%sinto %v%v "
	if isValues {
		// the mid values are filled differently
		// with select uses sqlparser.String for sqlparser.Values
		// with rows uses string.
		prefixFormat += "values "
	}
	prefixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	prefixBuf.Myprintf(prefixFormat,
		ins.Comments, ins.Ignore.ToString(),
		ins.Table, ins.Columns)
	prefix = prefixBuf.String()

	suffix = sqlparser.CopyOnRewrite(ins.OnDup, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		if tblName, ok := cursor.Node().(sqlparser.TableName); ok {
			if tblName.Qualifier != sqlparser.NewIdentifierCS("") {
				cursor.Replace(sqlparser.NewTableName(tblName.Name.String()))
			}
		}
	}, nil).(sqlparser.OnDup)
	return
}

// dmlFormatter strips out keyspace name from dmls.
func dmlFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case sqlparser.TableName:
		node.Name.Format(buf)
		return
	}
	node.Format(buf)
}

func buildUpdateLogicalPlan(
	ctx *plancontext.PlanningContext,
	rb *operators.Route,
	dmlOp ops.Operator,
	stmt *sqlparser.Update,
) (logicalPlan, error) {
	upd := dmlOp.(*operators.Update)
	rp := newRoutingParams(ctx, rb.Routing.OpCode())
	err := rb.Routing.UpdateRoutingParams(ctx, rp)
	if err != nil {
		return nil, err
	}
	edml := &engine.DML{
		Query:             generateQuery(stmt),
		TableNames:        []string{upd.VTable.Name.String()},
		Vindexes:          upd.VTable.ColumnVindexes,
		OwnedVindexQuery:  upd.OwnedVindexQuery,
		RoutingParameters: rp,
	}

	transformDMLPlan(upd.VTable, edml, rb.Routing, len(upd.ChangedVindexValues) > 0)

	e := &engine.Update{
		ChangedVindexValues: upd.ChangedVindexValues,
		DML:                 edml,
	}

	return &primitiveWrapper{prim: e}, nil
}

func buildDeleteLogicalPlan(
	ctx *plancontext.PlanningContext,
	rb *operators.Route,
	dmlOp ops.Operator,
) (logicalPlan, error) {
	del := dmlOp.(*operators.Delete)
	rp := newRoutingParams(ctx, rb.Routing.OpCode())
	err := rb.Routing.UpdateRoutingParams(ctx, rp)
	if err != nil {
		return nil, err
	}
	edml := &engine.DML{
		Query:             generateQuery(del.AST),
		TableNames:        []string{del.VTable.Name.String()},
		Vindexes:          del.VTable.Owned,
		OwnedVindexQuery:  del.OwnedVindexQuery,
		RoutingParameters: rp,
	}

	transformDMLPlan(del.VTable, edml, rb.Routing, del.OwnedVindexQuery != "")

	e := &engine.Delete{
		DML: edml,
	}

	return &primitiveWrapper{prim: e}, nil
}

func transformDMLPlan(vtable *vindexes.Table, edml *engine.DML, routing operators.Routing, setVindex bool) {
	if routing.OpCode() != engine.Unsharded && setVindex {
		primary := vtable.ColumnVindexes[0]
		edml.KsidVindex = primary.Vindex
		edml.KsidLength = len(primary.Columns)
	}
}

func getVindexPredicate(op *operators.Route) sqlparser.Expr {
	tr, ok := op.Routing.(*operators.ShardedRouting)
	if !ok || tr.Selected == nil {
		return nil
	}
	var condition sqlparser.Expr
	if len(tr.Selected.ValueExprs) > 0 {
		condition = tr.Selected.ValueExprs[0]
	}
	_, isMultiColumn := tr.Selected.FoundVindex.(vindexes.MultiColumn)
	for idx, expr := range tr.Selected.Predicates {
		cmp, ok := expr.(*sqlparser.ComparisonExpr)
		if !ok || cmp.Operator != sqlparser.InOp {
			continue
		}
		_, ok = cmp.Left.(*sqlparser.ColName)
		if !ok {
			continue
		}

		var argName string
		if isMultiColumn {
			argName = engine.ListVarName + strconv.Itoa(idx)
		} else {
			argName = engine.ListVarName
		}

		cmp.Right = sqlparser.ListArg(argName)
	}
	return condition
}

func getAllTableNames(op *operators.Route) ([]string, error) {
	tableNameMap := map[string]any{}
	err := rewrite.Visit(op, func(op ops.Operator) error {
		tbl, isTbl := op.(*operators.Table)
		var name string
		if isTbl {
			if tbl.QTable.IsInfSchema {
				name = sqlparser.String(tbl.QTable.Table)
			} else {
				name = sqlparser.String(tbl.QTable.Table.Name)
			}
			tableNameMap[name] = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	return tableNames, nil
}

func transformUnionPlan(ctx *plancontext.PlanningContext, op *operators.Union) (logicalPlan, error) {
	sources, err := slice.MapWithError(op.Sources, func(src ops.Operator) (logicalPlan, error) {
		plan, err := transformToLogicalPlan(ctx, src)
		if err != nil {
			return nil, err
		}
		return plan, nil
	})
	if err != nil {
		return nil, err
	}

	if len(sources) == 1 {
		return sources[0], nil
	}
	return &concatenate{
		sources:           sources,
		noNeedToTypeCheck: nil,
	}, nil

}

func transformLimit(ctx *plancontext.PlanningContext, op *operators.Limit) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	return createLimit(plan, op.AST)
}
