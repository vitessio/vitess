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
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slices2"
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

func transformToLogicalPlan(ctx *plancontext.PlanningContext, op ops.Operator, isRoot bool) (logicalPlan, error) {
	switch op := op.(type) {
	case *operators.Route:
		return transformRoutePlan(ctx, op)
	case *operators.ApplyJoin:
		return transformApplyJoinPlan(ctx, op)
	case *operators.Union:
		return transformUnionPlan(ctx, op, isRoot)
	case *operators.Vindex:
		return transformVindexPlan(ctx, op)
	case *operators.SubQueryOp:
		return transformSubQueryPlan(ctx, op)
	case *operators.CorrelatedSubQueryOp:
		return transformCorrelatedSubQueryPlan(ctx, op)
	case *operators.Filter:
		return transformFilter(ctx, op)
	case *operators.Horizon:
		return transformHorizon(ctx, op, isRoot)
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
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToLogicalPlan)", op))
}

func transformAggregator(ctx *plancontext.PlanningContext, op *operators.Aggregator) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source, false)
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
	src, err := transformToLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}
	return newDistinct(src, op.Columns, op.Truncate), nil
}

func transformOrdering(ctx *plancontext.PlanningContext, op *operators.Ordering) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source, false)
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
	src, err := transformToLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	if cols := op.AllOffsets(); cols != nil {
		// if all this op is doing is passing through columns from the input, we
		// can use the faster SimpleProjection
		return useSimpleProjection(op, cols, src)
	}

	expressions := slices2.Map(op.Projections, func(from operators.ProjExpr) sqlparser.Expr {
		return from.GetExpr()
	})

	failed := false
	evalengineExprs := slices2.Map(op.Projections, func(from operators.ProjExpr) evalengine.Expr {
		switch e := from.(type) {
		case operators.Eval:
			return e.EExpr
		case operators.Offset:
			typ, col, _ := ctx.SemTable.TypeForExpr(e.Expr)
			return evalengine.NewColumn(e.Offset, typ, col)
		default:
			failed = true
			return nil
		}
	})
	var primitive *engine.Projection
	columnNames := slices2.Map(op.Columns, func(from *sqlparser.AliasedExpr) string {
		return from.ColumnName()
	})

	if !failed {
		primitive = &engine.Projection{
			Cols:  columnNames,
			Exprs: evalengineExprs,
		}
	}

	return &projection{
		source:      src,
		columnNames: columnNames,
		columns:     expressions,
		primitive:   primitive,
	}, nil
}

// useSimpleProjection uses nothing at all if the output is already correct,
// or SimpleProjection when we have to reorder or truncate the columns
func useSimpleProjection(op *operators.Projection, cols []int, src logicalPlan) (logicalPlan, error) {
	columns, err := op.Source.GetColumns()
	if err != nil {
		return nil, err
	}
	if len(columns) == len(cols) && elementsMatchIndices(cols) {
		// the columns are already in the right order. we don't need anything at all here
		return src, nil
	}
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
	plan, err := transformToLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	predicate := op.FinalPredicate
	ast := ctx.SemTable.AndExpressions(op.Predicates...)

	// this might already have been done on the operators
	if predicate == nil {
		predicate, err = evalengine.Translate(ast, &evalengine.Config{
			ResolveType:   ctx.SemTable.TypeForExpr,
			ResolveColumn: resolveFromPlan(ctx, plan, true),
			Collation:     ctx.SemTable.Collation,
		})
		if err != nil {
			return nil, err
		}
	}

	return &filter{
		logicalPlanCommon: newBuilderCommon(plan),
		efilter: &engine.Filter{
			Predicate:    predicate,
			ASTPredicate: ast,
		},
	}, nil
}

func transformHorizon(ctx *plancontext.PlanningContext, op *operators.Horizon, isRoot bool) (logicalPlan, error) {
	if op.IsDerived() {
		return transformDerivedPlan(ctx, op)
	}
	source, err := transformToLogicalPlan(ctx, op.Source, isRoot)
	if err != nil {
		return nil, err
	}
	switch node := op.Query.(type) {
	case *sqlparser.Select:
		hp := horizonPlanning{
			sel: node,
		}

		replaceSubQuery(ctx, node)
		plan, err := hp.planHorizon(ctx, source, true)
		if err != nil {
			return nil, err
		}
		return planLimit(node.Limit, plan)
	case *sqlparser.Union:
		var err error
		rb, isRoute := source.(*route)
		if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
			return nil, ctx.SemTable.NotSingleRouteErr
		}
		var plan logicalPlan
		if isRoute && rb.isSingleShard() {
			err = planSingleRoutePlan(node, rb)
			plan = rb
		} else {
			plan, err = planOrderByOnUnion(ctx, source, node)
		}
		if err != nil {
			return nil, err
		}

		return planLimit(node.Limit, plan)
	}
	return nil, vterrors.VT13001("only SELECT and UNION implement the SelectStatement interface")
}

func transformApplyJoinPlan(ctx *plancontext.PlanningContext, n *operators.ApplyJoin) (logicalPlan, error) {
	lhs, err := transformToLogicalPlan(ctx, n.LHS, false)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToLogicalPlan(ctx, n.RHS, false)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if n.LeftJoin {
		opCode = engine.LeftJoin
	}

	return &join{
		Left:       lhs,
		Right:      rhs,
		Cols:       n.Columns,
		Vars:       n.Vars,
		LHSColumns: n.LHSColumns,
		Opcode:     opCode,
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
	switch src := op.Source.(type) {
	case *operators.Insert:
		return transformInsertPlan(ctx, op, src)
	case *operators.Update:
		return transformUpdatePlan(ctx, op, src)
	case *operators.Delete:
		return transformDeletePlan(ctx, op, src)
	}
	condition := getVindexPredicate(ctx, op)
	sel, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}
	replaceSubQuery(ctx, sel)
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
		Select:    sel,
		tables:    operators.TableID(op),
		condition: condition,
	}, nil

}

func transformInsertPlan(ctx *plancontext.PlanningContext, op *operators.Route, ins *operators.Insert) (i *insert, err error) {
	eins := &engine.Insert{
		Opcode:            mapToInsertOpCode(op.Routing.OpCode(), ins.Input != nil),
		Keyspace:          op.Routing.Keyspace(),
		TableName:         ins.VTable.Name.String(),
		Ignore:            ins.Ignore,
		ForceNonStreaming: ins.ForceNonStreaming,
		Generate:          autoIncGenerate(ins.AutoIncrement),
		ColVindexes:       ins.ColVindexes,
		VindexValues:      ins.VindexValues,
		VindexValueOffset: ins.VindexValueOffset,
	}
	i = &insert{eInsert: eins}

	// we would need to generate the query on the fly. The only exception here is
	// when unsharded query with autoincrement for that there is no input operator.
	if eins.Opcode != engine.InsertUnsharded || ins.Input != nil {
		eins.Prefix, eins.Mid, eins.Suffix = generateInsertShardedQuery(ins.AST)
	}

	if ins.Input == nil {
		eins.Query = generateQuery(ins.AST)
	} else {
		i.source, err = transformToLogicalPlan(ctx, ins.Input, true)
		if err != nil {
			return
		}
	}

	return
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

func generateInsertShardedQuery(ins *sqlparser.Insert) (prefix string, mid []string, suffix string) {
	valueTuples, isValues := ins.Rows.(sqlparser.Values)
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

	suffixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	suffixBuf.Myprintf("%v", ins.OnDup)
	suffix = suffixBuf.String()

	if !isValues {
		// this is a insert query using select to insert the rows.
		return
	}

	midBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	mid = make([]string, len(valueTuples))
	for rowNum, val := range valueTuples {
		midBuf.Myprintf("%v", val)
		mid[rowNum] = midBuf.String()
		midBuf.Reset()
	}
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

func transformUpdatePlan(ctx *plancontext.PlanningContext, op *operators.Route, upd *operators.Update) (logicalPlan, error) {
	ast := upd.AST
	replaceSubQuery(ctx, ast)
	rp := newRoutingParams(ctx, op.Routing.OpCode())
	err := op.Routing.UpdateRoutingParams(ctx, rp)
	if err != nil {
		return nil, err
	}
	edml := &engine.DML{
		Query:             generateQuery(ast),
		TableNames:        []string{upd.VTable.Name.String()},
		Vindexes:          upd.VTable.ColumnVindexes,
		OwnedVindexQuery:  upd.OwnedVindexQuery,
		RoutingParameters: rp,
	}

	transformDMLPlan(upd.VTable, edml, op.Routing, len(upd.ChangedVindexValues) > 0)

	e := &engine.Update{
		ChangedVindexValues: upd.ChangedVindexValues,
		DML:                 edml,
	}

	return &primitiveWrapper{prim: e}, nil
}

func transformDeletePlan(ctx *plancontext.PlanningContext, op *operators.Route, del *operators.Delete) (logicalPlan, error) {
	ast := del.AST
	replaceSubQuery(ctx, ast)
	rp := newRoutingParams(ctx, op.Routing.OpCode())
	err := op.Routing.UpdateRoutingParams(ctx, rp)
	if err != nil {
		return nil, err
	}
	edml := &engine.DML{
		Query:             generateQuery(ast),
		TableNames:        []string{del.VTable.Name.String()},
		Vindexes:          del.VTable.Owned,
		OwnedVindexQuery:  del.OwnedVindexQuery,
		RoutingParameters: rp,
	}

	transformDMLPlan(del.VTable, edml, op.Routing, del.OwnedVindexQuery != "")

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

func replaceSubQuery(ctx *plancontext.PlanningContext, sel sqlparser.Statement) {
	extractedSubqueries := ctx.SemTable.GetSubqueryNeedingRewrite()
	if len(extractedSubqueries) == 0 {
		return
	}
	sqr := &subQReplacer{subqueryToReplace: extractedSubqueries}
	sqlparser.SafeRewrite(sel, nil, sqr.replacer)
	for sqr.replaced {
		// to handle subqueries inside subqueries, we need to do this again and again until no replacements are left
		sqr.replaced = false
		sqlparser.SafeRewrite(sel, nil, sqr.replacer)
	}
}

func getVindexPredicate(ctx *plancontext.PlanningContext, op *operators.Route) sqlparser.Expr {
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

		if subq, isSubq := cmp.Right.(*sqlparser.Subquery); isSubq {
			extractedSubquery := ctx.SemTable.FindSubqueryReference(subq)
			if extractedSubquery != nil {
				extractedSubquery.SetArgName(argName)
			}
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

func transformUnionPlan(ctx *plancontext.PlanningContext, op *operators.Union, isRoot bool) (logicalPlan, error) {
	var sources []logicalPlan
	var err error
	if op.Distinct {
		sources, err = transformAndMerge(ctx, op)
		if err != nil {
			return nil, err
		}
		for _, source := range sources {
			pushDistinct(source)
		}
	} else {
		sources, err = transformAndMergeInOrder(ctx, op)
		if err != nil {
			return nil, err
		}
	}
	var result logicalPlan
	if len(sources) == 1 {
		src := sources[0]
		if rb, isRoute := src.(*route); isRoute && rb.isSingleShard() {
			// if we have a single shard route, we don't need to do anything to make it distinct
			// TODO
			// rb.Select.SetLimit(op.limit)
			// rb.Select.SetOrderBy(op.ordering)
			return src, nil
		}
		result = src
	} else {
		if len(op.Ordering) > 0 {
			return nil, vterrors.VT12001("ORDER BY on top of UNION")
		}
		result = &concatenate{sources: sources}
	}
	if op.Distinct {
		colls := getCollationsFor(ctx, op)
		checkCols, err := getCheckColsForUnion(ctx, result, colls)
		if err != nil {
			return nil, err
		}
		return newDistinctGen4Legacy(result, checkCols, isRoot), nil
	}
	return result, nil

}

func getWeightStringForSelectExpr(selectExpr sqlparser.SelectExpr) (*sqlparser.AliasedExpr, error) {
	expr, isAliased := selectExpr.(*sqlparser.AliasedExpr)
	if !isAliased {
		return nil, vterrors.VT12001("get weight string expression for non-aliased expression")
	}
	return &sqlparser.AliasedExpr{Expr: weightStringFor(expr.Expr)}, nil
}

func getCheckColsForUnion(ctx *plancontext.PlanningContext, result logicalPlan, colls []collationInfo) ([]engine.CheckCol, error) {
	checkCols := make([]engine.CheckCol, 0, len(colls))
	for i, coll := range colls {
		checkCol := engine.CheckCol{Col: i, Type: coll.typ, Collation: coll.col}
		if coll.typ >= 0 {
			checkCols = append(checkCols, checkCol)
			continue
		}
		// We might need a weight string - let's push one
		// `might` because we just don't know what type we are dealing with.
		// If we encounter a numerical value, we don't need any weight_string values
		newOffset, err := pushWeightStringForDistinct(ctx, result, i)
		if err != nil {
			return nil, err
		}
		checkCol.WsCol = &newOffset
		checkCols = append(checkCols, checkCol)
	}
	return checkCols, nil
}

// pushWeightStringForDistinct adds a weight_string projection
func pushWeightStringForDistinct(ctx *plancontext.PlanningContext, plan logicalPlan, offset int) (newOffset int, err error) {
	switch node := plan.(type) {
	case *route:
		allSelects := sqlparser.GetAllSelects(node.Select)
		for _, sel := range allSelects {
			expr, err := getWeightStringForSelectExpr(sel.SelectExprs[offset])
			if err != nil {
				return 0, err
			}
			if i := checkIfAlreadyExists(expr, sel, ctx.SemTable); i != -1 {
				return i, nil
			}
			sel.SelectExprs = append(sel.SelectExprs, expr)
			newOffset = len(sel.SelectExprs) - 1
		}
		// we leave the responsibility of truncating to distinct
		node.eroute.TruncateColumnCount = 0
	case *concatenate:
		for _, source := range node.sources {
			newOffset, err = pushWeightStringForDistinct(ctx, source, offset)
			if err != nil {
				return 0, err
			}
		}
		node.noNeedToTypeCheck = append(node.noNeedToTypeCheck, newOffset)
	case *join:
		joinOffset := node.Cols[offset]
		switch {
		case joinOffset < 0:
			offset, err = pushWeightStringForDistinct(ctx, node.Left, -(joinOffset + 1))
			offset = -(offset + 1)
		case joinOffset > 0:
			offset, err = pushWeightStringForDistinct(ctx, node.Right, joinOffset-1)
			offset = offset + 1
		default:
			return 0, vterrors.VT13001("wrong column offset in join plan to push DISTINCT WEIGHT_STRING")
		}
		if err != nil {
			return 0, err
		}
		newOffset = len(node.Cols)
		node.Cols = append(node.Cols, offset)
	default:
		return 0, vterrors.VT13001(fmt.Sprintf("pushWeightStringForDistinct on %T", plan))
	}
	return
}

func transformAndMerge(ctx *plancontext.PlanningContext, op *operators.Union) (sources []logicalPlan, err error) {
	for _, source := range op.Sources {
		// first we go over all the operator inputs and turn them into logical plans,
		// including horizon planning
		plan, err := transformToLogicalPlan(ctx, source, false)
		if err != nil {
			return nil, err
		}
		sources = append(sources, plan)
	}

	// next we'll go over all the plans from and check if any two can be merged. if they can, they are merged,
	// and we continue checking for pairs of plans that can be merged into a single route
	idx := 0
	for idx < len(sources) {
		keep := make([]bool, len(sources))
		srcA := sources[idx]
		merged := false
		for j, srcB := range sources {
			if j <= idx {
				continue
			}
			newPlan := mergeUnionLogicalPlans(ctx, srcA, srcB)
			if newPlan != nil {
				sources[idx] = newPlan
				srcA = newPlan
				merged = true
			} else {
				keep[j] = true
			}
		}
		if !merged {
			return sources, nil
		}
		var phase []logicalPlan
		for i, source := range sources {
			if keep[i] || i <= idx {
				phase = append(phase, source)
			}
		}
		idx++
		sources = phase
	}
	return sources, nil
}

func transformAndMergeInOrder(ctx *plancontext.PlanningContext, op *operators.Union) (sources []logicalPlan, err error) {
	// We go over all the input operators and turn them into logical plans
	for i, source := range op.Sources {
		plan, err := transformToLogicalPlan(ctx, source, false)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			sources = append(sources, plan)
			continue
		}

		// next we check if the last plan we produced can be merged with this new plan
		last := sources[len(sources)-1]
		newPlan := mergeUnionLogicalPlans(ctx, last, plan)
		if newPlan != nil {
			// if we could merge them, let's replace the last plan with this new merged one
			sources[len(sources)-1] = newPlan
			continue
		}
		// else we just add the new plan to the end of list
		sources = append(sources, plan)
	}
	return sources, nil
}

type collationInfo struct {
	typ sqltypes.Type
	col collations.ID
}

func getCollationsFor(ctx *plancontext.PlanningContext, n *operators.Union) []collationInfo {
	// TODO: coerce selects' select expressions' collations
	var colls []collationInfo

	sel, err := n.GetSelectFor(0)
	if err != nil {
		return nil
	}
	for _, expr := range sel.SelectExprs {
		aliasedE, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil
		}
		typ, col, _ := ctx.SemTable.TypeForExpr(aliasedE.Expr)
		colls = append(colls, collationInfo{typ: typ, col: col})
	}
	return colls
}

func transformDerivedPlan(ctx *plancontext.PlanningContext, op *operators.Horizon) (logicalPlan, error) {
	// transforming the inner part of the derived table into a logical plan
	// so that we can do horizon planning on the inner. If the logical plan
	// we've produced is a Route, we set its Select.From field to be an aliased
	// expression containing our derived table's inner select and the derived
	// table's alias.

	plan, err := transformToLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	plan, err = planHorizon(ctx, plan, op.Query, false)
	if err != nil {
		return nil, err
	}

	rb, isRoute := plan.(*route)
	if !isRoute {
		return &simpleProjection{
			logicalPlanCommon: newBuilderCommon(plan),
			eSimpleProj: &engine.SimpleProjection{
				Cols: op.ColumnsOffset,
			},
		}, nil
	}
	innerSelect := rb.Select
	derivedTable := &sqlparser.DerivedTable{Select: innerSelect}
	tblExpr := &sqlparser.AliasedTableExpr{
		Expr:    derivedTable,
		As:      sqlparser.NewIdentifierCS(op.Alias),
		Columns: op.ColumnAliases,
	}
	selectExprs := sqlparser.SelectExprs{}
	for _, colName := range op.Columns {
		selectExprs = append(selectExprs, &sqlparser.AliasedExpr{
			Expr: colName,
		})
	}
	rb.Select = &sqlparser.Select{
		From:        []sqlparser.TableExpr{tblExpr},
		SelectExprs: selectExprs,
	}
	return plan, nil
}

func transformLimit(ctx *plancontext.PlanningContext, op *operators.Limit) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	return createLimit(plan, op.AST)
}

type subQReplacer struct {
	subqueryToReplace []*sqlparser.ExtractedSubquery
	replaced          bool
}

func (sqr *subQReplacer) replacer(cursor *sqlparser.Cursor) bool {
	ext, ok := cursor.Node().(*sqlparser.ExtractedSubquery)
	if !ok {
		return true
	}
	for _, replaceByExpr := range sqr.subqueryToReplace {
		// we are comparing the ArgNames in case the expressions have been cloned
		if ext.GetArgName() == replaceByExpr.GetArgName() {
			cursor.Replace(ext.Original)
			sqr.replaced = true
			return true
		}
	}
	return true
}

func pushDistinct(plan logicalPlan) {
	switch n := plan.(type) {
	case *route:
		n.Select.MakeDistinct()
	case *concatenate:
		for _, source := range n.sources {
			pushDistinct(source)
		}
	}
}

func mergeUnionLogicalPlans(ctx *plancontext.PlanningContext, left logicalPlan, right logicalPlan) logicalPlan {
	lroute, ok := left.(*route)
	if !ok {
		return nil
	}
	rroute, ok := right.(*route)
	if !ok {
		return nil
	}

	if canMergeUnionPlans(ctx, lroute, rroute) {
		lroute.Select = &sqlparser.Union{Left: lroute.Select, Distinct: false, Right: rroute.Select}
		return mergeSystemTableInformation(lroute, rroute)
	}
	return nil
}

func canMergeUnionPlans(ctx *plancontext.PlanningContext, a, b *route) bool {
	// this method should be close to tryMerge below. it does the same thing, but on logicalPlans instead of queryTrees
	if a.eroute.Keyspace.Name != b.eroute.Keyspace.Name {
		return false
	}
	switch a.eroute.Opcode {
	case engine.Unsharded, engine.Reference:
		return a.eroute.Opcode == b.eroute.Opcode
	case engine.DBA:
		return canSelectDBAMerge(a, b)
	case engine.EqualUnique:
		// Check if they target the same shard.
		if b.eroute.Opcode == engine.EqualUnique &&
			a.eroute.Vindex == b.eroute.Vindex &&
			a.condition != nil &&
			b.condition != nil &&
			gen4ValuesEqual(ctx, []sqlparser.Expr{a.condition}, []sqlparser.Expr{b.condition}) {
			return true
		}
	case engine.Scatter:
		return b.eroute.Opcode == engine.Scatter
	case engine.Next:
		return false
	}
	return false
}

func canSelectDBAMerge(a, b *route) bool {
	if a.eroute.Opcode != engine.DBA {
		return false
	}
	if b.eroute.Opcode != engine.DBA {
		return false
	}

	// safe to merge when any 1 table name or schema matches, since either the routing will match or either side would be throwing an error
	// during run-time which we want to preserve. For example outer side has User in sys table schema and inner side has User and Main in sys table schema
	// Inner might end up throwing an error at runtime, but if it doesn't then it is safe to merge.
	for _, aExpr := range a.eroute.SysTableTableSchema {
		for _, bExpr := range b.eroute.SysTableTableSchema {
			if evalengine.FormatExpr(aExpr) == evalengine.FormatExpr(bExpr) {
				return true
			}
		}
	}
	for _, aExpr := range a.eroute.SysTableTableName {
		for _, bExpr := range b.eroute.SysTableTableName {
			if evalengine.FormatExpr(aExpr) == evalengine.FormatExpr(bExpr) {
				return true
			}
		}
	}

	// if either/both of the side does not have any routing information, then they can be merged.
	return (len(a.eroute.SysTableTableSchema) == 0 && len(a.eroute.SysTableTableName) == 0) ||
		(len(b.eroute.SysTableTableSchema) == 0 && len(b.eroute.SysTableTableName) == 0)
}

func gen4ValuesEqual(ctx *plancontext.PlanningContext, a, b []sqlparser.Expr) bool {
	if len(a) != len(b) {
		return false
	}

	// TODO: check SemTable's columnEqualities for better plan

	for i, aExpr := range a {
		bExpr := b[i]
		if !gen4ValEqual(ctx, aExpr, bExpr) {
			return false
		}
	}
	return true
}

func gen4ValEqual(ctx *plancontext.PlanningContext, a, b sqlparser.Expr) bool {
	switch a := a.(type) {
	case *sqlparser.ColName:
		if b, ok := b.(*sqlparser.ColName); ok {
			if !a.Name.Equal(b.Name) {
				return false
			}

			return ctx.SemTable.DirectDeps(a) == ctx.SemTable.DirectDeps(b)
		}
	case *sqlparser.Argument:
		b, ok := b.(*sqlparser.Argument)
		if !ok {
			return false
		}
		return a.Name == b.Name
	case *sqlparser.Literal:
		b, ok := b.(*sqlparser.Literal)
		if !ok {
			return false
		}
		switch a.Type {
		case sqlparser.StrVal:
			switch b.Type {
			case sqlparser.StrVal:
				return a.Val == b.Val
			case sqlparser.HexVal:
				return hexEqual(b, a)
			}
		case sqlparser.HexVal:
			return hexEqual(a, b)
		case sqlparser.IntVal:
			if b.Type == (sqlparser.IntVal) {
				return a.Val == b.Val
			}
		}
	}
	return false
}

func hexEqual(a, b *sqlparser.Literal) bool {
	v, err := a.HexDecode()
	if err != nil {
		return false
	}
	switch b.Type {
	case sqlparser.StrVal:
		return bytes.Equal(v, b.Bytes())
	case sqlparser.HexVal:
		v2, err := b.HexDecode()
		if err != nil {
			return false
		}
		return bytes.Equal(v, v2)
	}
	return false
}
