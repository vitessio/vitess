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
	case *operators.SubQueryOp:
		return transformSubQueryPlan(ctx, op)
	case *operators.CorrelatedSubQueryOp:
		return transformCorrelatedSubQueryPlan(ctx, op)
	case *operators.Filter:
		return transformFilter(ctx, op)
	case *operators.Horizon:
		return transformHorizon(ctx, op)
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
		return useSimpleProjection(ctx, op, cols, src)
	}

	expressions := slice.Map(op.Projections, func(from operators.ProjExpr) sqlparser.Expr {
		return from.GetExpr()
	})

	failed := false
	evalengineExprs := slice.Map(op.Projections, func(from operators.ProjExpr) evalengine.Expr {
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
	columnNames := slice.Map(op.Columns, func(from *sqlparser.AliasedExpr) string {
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
func useSimpleProjection(ctx *plancontext.PlanningContext, op *operators.Projection, cols []int, src logicalPlan) (logicalPlan, error) {
	columns, err := op.Source.GetColumns(ctx)
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
	plan, err := transformToLogicalPlan(ctx, op.Source)
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
			Truncate:     op.Truncate,
		},
	}, nil
}

func transformHorizon(ctx *plancontext.PlanningContext, op *operators.Horizon) (logicalPlan, error) {
	if op.IsDerived() {
		return transformDerivedPlan(ctx, op)
	}
	source, err := transformToLogicalPlan(ctx, op.Source)
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
	stmt, dmlOp, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	replaceSubQuery(ctx, stmt)

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
		return buildDeleteLogicalPlan(ctx, op, dmlOp, stmt)
	case *sqlparser.Insert:
		return buildInsertLogicalPlan(ctx, op, dmlOp, stmt)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("dont know how to %T", stmt))
	}
}

func buildRouteLogicalPlan(ctx *plancontext.PlanningContext, op *operators.Route, stmt sqlparser.SelectStatement) (logicalPlan, error) {
	condition := getVindexPredicate(ctx, op)
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

func buildUpdateLogicalPlan(
	ctx *plancontext.PlanningContext,
	op *operators.Route,
	dmlOp ops.Operator,
	stmt *sqlparser.Update,
) (logicalPlan, error) {
	upd := dmlOp.(*operators.Update)
	rp := newRoutingParams(ctx, op.Routing.OpCode())
	err := op.Routing.UpdateRoutingParams(ctx, rp)
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

	transformDMLPlan(upd.VTable, edml, op.Routing, len(upd.ChangedVindexValues) > 0)

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
	ast *sqlparser.Delete,
) (logicalPlan, error) {
	del := dmlOp.(*operators.Delete)
	rp := newRoutingParams(ctx, rb.Routing.OpCode())
	err := rb.Routing.UpdateRoutingParams(ctx, rp)
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

func transformDerivedPlan(ctx *plancontext.PlanningContext, op *operators.Horizon) (logicalPlan, error) {
	// transforming the inner part of the derived table into a logical plan
	// so that we can do horizon planning on the inner. If the logical plan
	// we've produced is a Route, we set its Select.From field to be an aliased
	// expression containing our derived table's inner select and the derived
	// table's alias.

	plan, err := transformToLogicalPlan(ctx, op.Source)
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
	plan, err := transformToLogicalPlan(ctx, op.Source)
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
