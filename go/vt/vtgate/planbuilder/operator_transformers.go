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

	"vitess.io/vitess/go/slices2"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"
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
	case *operators.Derived:
		return transformDerivedPlan(ctx, op)
	case *operators.Filter:
		return transformFilter(ctx, op)
	case *operators.Horizon:
		return transformHorizon(ctx, op, isRoot)
	case *operators.Projection:
		return transformProjection(ctx, op)
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToLogicalPlan)", op))
}

func transformProjection(ctx *plancontext.PlanningContext, op *operators.Projection) (logicalPlan, error) {
	src, err := transformToLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	if cols := op.AllOffsets(); cols != nil {
		// if all this op is doing is passing through columns from the input, we
		// can use the faster SimpleProjection
		return simplifyPlan(op, cols, src)
	}

	expressions := slices2.Map(op.Columns, func(from operators.ProjExpr) sqlparser.Expr {
		return from.GetExpr()
	})

	return &projection{
		source:      src,
		columnNames: op.ColumnNames,
		columns:     expressions,
	}, nil
}

func simplifyPlan(op *operators.Projection, cols []int, src logicalPlan) (logicalPlan, error) {
	// all columns are just passing through something from below
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
	source, err := transformToLogicalPlan(ctx, op.Source, isRoot)
	if err != nil {
		return nil, err
	}
	switch node := op.Select.(type) {
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
		rb, isRoute := source.(*routeGen4)
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

	return &joinGen4{
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
		TableName:         strings.Join(tableNames, ", "),
		RoutingParameters: rp,
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
	if err != nil {
		return nil, err
	}
	return &routeGen4{
		eroute:    eroute,
		Select:    sel,
		tables:    operators.TableID(op),
		condition: condition,
	}, nil

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
		Query: generateQuery(ast),
		Table: []*vindexes.Table{
			upd.VTable,
		},
		OwnedVindexQuery:  upd.OwnedVindexQuery,
		RoutingParameters: rp,
	}

	transformDMLPlan(upd.AST, upd.VTable, edml, op.Routing, len(upd.ChangedVindexValues) > 0)

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
		Query: generateQuery(ast),
		Table: []*vindexes.Table{
			del.VTable,
		},
		OwnedVindexQuery:  del.OwnedVindexQuery,
		RoutingParameters: rp,
	}

	transformDMLPlan(del.AST, del.VTable, edml, op.Routing, del.OwnedVindexQuery != "")

	e := &engine.Delete{
		DML: edml,
	}

	return &primitiveWrapper{prim: e}, nil
}

func transformDMLPlan(stmt sqlparser.Commented, vtable *vindexes.Table, edml *engine.DML, routing operators.Routing, setVindex bool) {
	directives := stmt.GetParsedComments().Directives()
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		edml.MultiShardAutocommit = true
	}
	edml.QueryTimeout = queryTimeout(directives)

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
		if rb, isRoute := src.(*routeGen4); isRoute && rb.isSingleShard() {
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
		result = &concatenateGen4{sources: sources}
	}
	if op.Distinct {
		colls := getCollationsFor(ctx, op)
		checkCols, err := getCheckColsForUnion(ctx, result, colls)
		if err != nil {
			return nil, err
		}
		return newDistinct(result, checkCols, isRoot), nil
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

func getCheckColsForUnion(ctx *plancontext.PlanningContext, result logicalPlan, colls []collations.ID) ([]engine.CheckCol, error) {
	checkCols := make([]engine.CheckCol, 0, len(colls))
	for i, coll := range colls {
		checkCol := engine.CheckCol{Col: i, Collation: coll}
		if coll != collations.Unknown {
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
	case *routeGen4:
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
	case *concatenateGen4:
		for _, source := range node.sources {
			newOffset, err = pushWeightStringForDistinct(ctx, source, offset)
			if err != nil {
				return 0, err
			}
		}
		node.noNeedToTypeCheck = append(node.noNeedToTypeCheck, newOffset)
	case *joinGen4:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		expr := node.OutputColumns()[offset]
		aliasedExpr, isAliased := expr.(*sqlparser.AliasedExpr)
		if !isAliased {
			return 0, vterrors.VT13001("cannot convert JOIN output columns to an aliased-expression")
		}
		deps := ctx.SemTable.RecursiveDeps(aliasedExpr.Expr)
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, err = pushWeightStringForDistinct(ctx, node.Left, offset)
			node.Cols = append(node.Cols, -(offset + 1))
		case deps.IsSolvedBy(rhsSolves):
			offset, err = pushWeightStringForDistinct(ctx, node.Right, offset)
			node.Cols = append(node.Cols, offset+1)
		default:
			return 0, vterrors.VT12001("push DISTINCT WEIGHT_STRING to both sides of the join")
		}
		newOffset = len(node.Cols) - 1
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

func getCollationsFor(ctx *plancontext.PlanningContext, n *operators.Union) []collations.ID {
	// TODO: coerce selects' select expressions' collations
	var colls []collations.ID

	sel, err := n.GetSelectFor(0)
	if err != nil {
		return nil
	}
	for _, expr := range sel.SelectExprs {
		aliasedE, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil
		}
		typ := ctx.SemTable.CollationForExpr(aliasedE.Expr)
		if typ == collations.Unknown {
			if t, hasT := ctx.SemTable.ExprTypes[aliasedE.Expr]; hasT && sqltypes.IsNumber(t.Type) {
				typ = collations.CollationBinaryID
			}
		}
		colls = append(colls, typ)
	}
	return colls
}

func transformDerivedPlan(ctx *plancontext.PlanningContext, op *operators.Derived) (logicalPlan, error) {
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

	rb, isRoute := plan.(*routeGen4)
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
	case *routeGen4:
		n.Select.MakeDistinct()
	case *concatenateGen4:
		for _, source := range n.sources {
			pushDistinct(source)
		}
	}
}

func mergeUnionLogicalPlans(ctx *plancontext.PlanningContext, left logicalPlan, right logicalPlan) logicalPlan {
	lroute, ok := left.(*routeGen4)
	if !ok {
		return nil
	}
	rroute, ok := right.(*routeGen4)
	if !ok {
		return nil
	}

	if canMergeUnionPlans(ctx, lroute, rroute) {
		lroute.Select = &sqlparser.Union{Left: lroute.Select, Distinct: false, Right: rroute.Select}
		return mergeSystemTableInformation(lroute, rroute)
	}
	return nil
}

func canMergeUnionPlans(ctx *plancontext.PlanningContext, a, b *routeGen4) bool {
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

func canSelectDBAMerge(a, b *routeGen4) bool {
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
