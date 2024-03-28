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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func transformToLogicalPlan(ctx *plancontext.PlanningContext, op operators.Operator) (logicalPlan, error) {
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
	case *operators.InsertSelection:
		return transformInsertionSelection(ctx, op)
	case *operators.Upsert:
		return transformUpsert(ctx, op)
	case *operators.HashJoin:
		return transformHashJoin(ctx, op)
	case *operators.Sequential:
		return transformSequential(ctx, op)
	case *operators.DMLWithInput:
		return transformDMLWithInput(ctx, op)
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToLogicalPlan)", op))
}

func transformDMLWithInput(ctx *plancontext.PlanningContext, op *operators.DMLWithInput) (logicalPlan, error) {
	input, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	var dmls []logicalPlan
	for _, dml := range op.DML {
		del, err := transformToLogicalPlan(ctx, dml)
		if err != nil {
			return nil, err
		}
		dmls = append(dmls, del)
	}
	return &dmlWithInput{
		input:      input,
		dmls:       dmls,
		outputCols: op.Offsets,
	}, nil
}

func transformUpsert(ctx *plancontext.PlanningContext, op *operators.Upsert) (logicalPlan, error) {
	u := &upsert{}
	for _, source := range op.Sources {
		iLp, uLp, err := transformOneUpsert(ctx, source)
		if err != nil {
			return nil, err
		}
		u.insert = append(u.insert, iLp)
		u.update = append(u.update, uLp)
	}
	return u, nil
}

func transformOneUpsert(ctx *plancontext.PlanningContext, source operators.UpsertSource) (iLp, uLp logicalPlan, err error) {
	iLp, err = transformToLogicalPlan(ctx, source.Insert)
	if err != nil {
		return
	}
	if ins, ok := iLp.(*insert); ok {
		ins.eInsert.PreventAutoCommit = true
	}
	uLp, err = transformToLogicalPlan(ctx, source.Update)
	return
}

func transformSequential(ctx *plancontext.PlanningContext, op *operators.Sequential) (logicalPlan, error) {
	var lps []logicalPlan
	for _, source := range op.Sources {
		lp, err := transformToLogicalPlan(ctx, source)
		if err != nil {
			return nil, err
		}
		if ins, ok := lp.(*insert); ok {
			ins.eInsert.PreventAutoCommit = true
		}
		lps = append(lps, lp)
	}
	return &sequential{
		sources: lps,
	}, nil
}

func transformInsertionSelection(ctx *plancontext.PlanningContext, op *operators.InsertSelection) (logicalPlan, error) {
	rb, isRoute := op.Insert.(*operators.Route)
	if !isRoute {
		return nil, vterrors.VT13001(fmt.Sprintf("Incorrect type encountered: %T (transformInsertionSelection)", op.Insert))
	}

	stmt, dmlOp, err := operators.ToSQL(ctx, rb.Source)
	if err != nil {
		return nil, err
	}

	if stmtWithComments, ok := stmt.(sqlparser.Commented); ok && rb.Comments != nil {
		stmtWithComments.SetComments(rb.Comments.GetComments())
	}

	ins := dmlOp.(*operators.Insert)
	eins := &engine.InsertSelect{
		InsertCommon: engine.InsertCommon{
			Keyspace:          rb.Routing.Keyspace(),
			TableName:         ins.VTable.Name.String(),
			Ignore:            ins.Ignore,
			ForceNonStreaming: op.ForceNonStreaming,
			Generate:          autoIncGenerate(ins.AutoIncrement),
			ColVindexes:       ins.ColVindexes,
		},
		VindexValueOffset: ins.VindexValueOffset,
	}
	lp := &insert{eInsertSelect: eins}

	eins.Prefix, _, eins.Suffix = generateInsertShardedQuery(ins.AST)

	selectionPlan, err := transformToLogicalPlan(ctx, op.Select)
	if err != nil {
		return nil, err
	}
	lp.source = selectionPlan

	return lp, nil
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

		childEngine := childLP.Primitive()
		children = append(children, &engine.FkChild{
			BVName:         child.BVName,
			Cols:           child.Cols,
			NonLiteralInfo: child.NonLiteralInfo,
			Exec:           childEngine,
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

	lhsCols := op.OuterExpressionsNeeded(ctx, op.Outer)
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
		collationEnv:   ctx.VSchema.Environment().CollationEnv(),
	}

	for _, aggr := range op.Aggregations {
		if aggr.OpCode == opcode.AggregateUnassigned {
			return nil, vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original)))
		}
		aggrParam := engine.NewAggregateParam(aggr.OpCode, aggr.ColOffset, aggr.Alias, ctx.VSchema.Environment().CollationEnv())
		aggrParam.Expr = aggr.Func
		aggrParam.Original = aggr.Original
		aggrParam.OrigOpcode = aggr.OriginalOpCode
		aggrParam.WCol = aggr.WSOffset
		aggrParam.Type = aggr.GetTypeCollation(ctx)
		oa.aggregates = append(oa.aggregates, aggrParam)
	}
	for _, groupBy := range op.Grouping {
		typ, _ := ctx.SemTable.TypeForExpr(groupBy.Inner)
		oa.groupByKeys = append(oa.groupByKeys, &engine.GroupByParams{
			KeyCol:          groupBy.ColOffset,
			WeightStringCol: groupBy.WSOffset,
			Expr:            groupBy.Inner,
			Type:            typ,
			CollationEnv:    ctx.VSchema.Environment().CollationEnv(),
		})
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
		typ, _ := ctx.SemTable.TypeForExpr(order.SimplifiedExpr)
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, evalengine.OrderByParams{
			Col:             ordering.Offset[idx],
			WeightStringCol: ordering.WOffset[idx],
			Desc:            order.Inner.Direction == sqlparser.DescOrder,
			Type:            typ,
			CollationEnv:    ctx.VSchema.Environment().CollationEnv(),
		})
	}

	return ms, nil
}

func transformProjection(ctx *plancontext.PlanningContext, op *operators.Projection) (logicalPlan, error) {
	src, err := transformToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	if cols, colNames := op.AllOffsets(); cols != nil {
		// if all this op is doing is passing through columns from the input, we
		// can use the faster SimpleProjection
		return useSimpleProjection(ctx, op, cols, colNames, src)
	}

	ap, err := op.GetAliasedProjections()
	if err != nil {
		return nil, err
	}

	var evalengineExprs []evalengine.Expr
	var columnNames []string
	for _, pe := range ap {
		ee, err := getEvalEngingeExpr(ctx, pe)
		if err != nil {
			return nil, err
		}
		evalengineExprs = append(evalengineExprs, ee)
		columnNames = append(columnNames, pe.Original.ColumnName())
	}

	primitive := &engine.Projection{
		Cols:  columnNames,
		Exprs: evalengineExprs,
	}

	return &projection{
		source:    src,
		primitive: primitive,
	}, nil
}

func getEvalEngingeExpr(ctx *plancontext.PlanningContext, pe *operators.ProjExpr) (evalengine.Expr, error) {
	switch e := pe.Info.(type) {
	case *operators.EvalEngine:
		return e.EExpr, nil
	case operators.Offset:
		typ, _ := ctx.SemTable.TypeForExpr(pe.EvalExpr)
		return evalengine.NewColumn(int(e), typ, pe.EvalExpr), nil
	default:
		return nil, vterrors.VT13001("project not planned for: %s", pe.String())
	}

}

// useSimpleProjection uses nothing at all if the output is already correct,
// or SimpleProjection when we have to reorder or truncate the columns
func useSimpleProjection(ctx *plancontext.PlanningContext, op *operators.Projection, cols []int, colNames []string, src logicalPlan) (logicalPlan, error) {
	columns := op.Source.GetColumns(ctx)
	if len(columns) == len(cols) && elementsMatchIndices(cols) {
		// the columns are already in the right order. we don't need anything at all here
		return src, nil
	}
	return &simpleProjection{
		logicalPlanCommon: newBuilderCommon(src),
		eSimpleProj: &engine.SimpleProjection{
			Cols:     cols,
			ColNames: colNames,
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
	if !n.JoinType.IsInner() {
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

func routeToEngineRoute(ctx *plancontext.PlanningContext, op *operators.Route, hints *queryHints) (*engine.Route, error) {
	tableNames, err := getAllTableNames(op)
	if err != nil {
		return nil, err
	}

	rp := newRoutingParams(ctx, op.Routing.OpCode())
	op.Routing.UpdateRoutingParams(ctx, rp)

	e := &engine.Route{
		TableName:           strings.Join(tableNames, ", "),
		RoutingParameters:   rp,
		TruncateColumnCount: op.ResultColumns,
	}
	if hints != nil {
		e.ScatterErrorsAsWarnings = hints.scatterErrorsAsWarnings
		e.QueryTimeout = hints.queryTimeout
	}
	return e, nil
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

type queryHints struct {
	scatterErrorsAsWarnings,
	multiShardAutocommit bool
	queryTimeout int
}

func getHints(cmt *sqlparser.ParsedComments) *queryHints {
	if cmt == nil {
		return nil
	}
	directives := cmt.Directives()
	scatterAsWarns := directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings)
	timeout := queryTimeout(directives)
	multiShardAutoCommit := directives.IsSet(sqlparser.DirectiveMultiShardAutocommit)
	return &queryHints{
		scatterErrorsAsWarnings: scatterAsWarns,
		multiShardAutocommit:    multiShardAutoCommit,
		queryTimeout:            timeout,
	}
}

func transformRoutePlan(ctx *plancontext.PlanningContext, op *operators.Route) (logicalPlan, error) {
	stmt, dmlOp, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	if stmtWithComments, ok := stmt.(sqlparser.Commented); ok && op.Comments != nil {
		comments := op.Comments.GetComments()
		stmtWithComments.SetComments(comments)
	}

	hints := getHints(op.Comments)
	switch stmt := stmt.(type) {
	case sqlparser.SelectStatement:
		if op.Lock != sqlparser.NoLock {
			stmt.SetLock(op.Lock)
		}
		return buildRouteLogicalPlan(ctx, op, stmt, hints)
	case *sqlparser.Update:
		return buildUpdateLogicalPlan(ctx, op, dmlOp, stmt, hints)
	case *sqlparser.Delete:
		return buildDeleteLogicalPlan(ctx, op, dmlOp, stmt, hints)
	case *sqlparser.Insert:
		return buildInsertLogicalPlan(op, dmlOp, stmt, hints)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("dont know how to %T", stmt))
	}
}

func buildRouteLogicalPlan(ctx *plancontext.PlanningContext, op *operators.Route, stmt sqlparser.SelectStatement, hints *queryHints) (logicalPlan, error) {
	_ = updateSelectedVindexPredicate(op)

	eroute, err := routeToEngineRoute(ctx, op, hints)
	for _, order := range op.Ordering {
		typ, _ := ctx.SemTable.TypeForExpr(order.AST)
		eroute.OrderBy = append(eroute.OrderBy, evalengine.OrderByParams{
			Col:             order.Offset,
			WeightStringCol: order.WOffset,
			Desc:            order.Direction == sqlparser.DescOrder,
			Type:            typ,
			CollationEnv:    ctx.VSchema.Environment().CollationEnv(),
		})
	}
	if err != nil {
		return nil, err
	}
	r := &route{
		eroute: eroute,
		Select: stmt,
		tables: operators.TableID(op),
	}

	if err = r.Wireup(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func buildInsertLogicalPlan(
	rb *operators.Route, op operators.Operator, stmt *sqlparser.Insert,
	hints *queryHints,
) (logicalPlan, error) {
	ins := op.(*operators.Insert)

	ic := engine.InsertCommon{
		Opcode:      mapToInsertOpCode(rb.Routing.OpCode()),
		Keyspace:    rb.Routing.Keyspace(),
		TableName:   ins.VTable.Name.String(),
		Ignore:      ins.Ignore,
		Generate:    autoIncGenerate(ins.AutoIncrement),
		ColVindexes: ins.ColVindexes,
	}
	if hints != nil {
		ic.MultiShardAutocommit = hints.multiShardAutocommit
		ic.QueryTimeout = hints.queryTimeout
	}

	eins := &engine.Insert{
		InsertCommon: ic,
		VindexValues: ins.VindexValues,
	}
	lp := &insert{eInsert: eins}

	// we would need to generate the query on the fly. The only exception here is
	// when unsharded query with autoincrement for that there is no input operator.
	if eins.Opcode != engine.InsertUnsharded {
		eins.Prefix, eins.Mid, eins.Suffix = generateInsertShardedQuery(ins.AST)
	}

	eins.Query = generateQuery(stmt)
	return lp, nil
}

func mapToInsertOpCode(code engine.Opcode) engine.InsertOpcode {
	if code == engine.Unsharded {
		return engine.InsertUnsharded
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
	dmlOp operators.Operator,
	stmt *sqlparser.Update,
	hints *queryHints,
) (logicalPlan, error) {
	upd := dmlOp.(*operators.Update)
	var vindexes []*vindexes.ColumnVindex
	vQuery := ""
	if len(upd.ChangedVindexValues) > 0 {
		upd.OwnedVindexQuery.From = stmt.GetFrom()
		upd.OwnedVindexQuery.Where = stmt.Where
		vQuery = sqlparser.String(upd.OwnedVindexQuery)
		vindexes = upd.Target.VTable.ColumnVindexes
		if upd.OwnedVindexQuery.Limit != nil && len(upd.OwnedVindexQuery.OrderBy) == 0 {
			return nil, vterrors.VT12001("Vindex update should have ORDER BY clause when using LIMIT")
		}
	}
	if upd.VerifyAll {
		stmt.SetComments(stmt.GetParsedComments().SetMySQLSetVarValue(sysvars.ForeignKeyChecks, "OFF"))
	}

	edml := createDMLPrimitive(ctx, rb, hints, upd.Target.VTable, generateQuery(stmt), vindexes, vQuery)

	return &primitiveWrapper{prim: &engine.Update{
		DML:                 edml,
		ChangedVindexValues: upd.ChangedVindexValues,
	}}, nil
}

func buildDeleteLogicalPlan(ctx *plancontext.PlanningContext, rb *operators.Route, dmlOp operators.Operator, stmt *sqlparser.Delete, hints *queryHints) (logicalPlan, error) {
	del := dmlOp.(*operators.Delete)

	var vindexes []*vindexes.ColumnVindex
	vQuery := ""
	if del.OwnedVindexQuery != nil {
		del.OwnedVindexQuery.From = stmt.GetFrom()
		del.OwnedVindexQuery.Where = stmt.Where
		vQuery = sqlparser.String(del.OwnedVindexQuery)
		vindexes = del.Target.VTable.Owned
	}

	edml := createDMLPrimitive(ctx, rb, hints, del.Target.VTable, generateQuery(stmt), vindexes, vQuery)

	return &primitiveWrapper{prim: &engine.Delete{DML: edml}}, nil
}

func createDMLPrimitive(ctx *plancontext.PlanningContext, rb *operators.Route, hints *queryHints, vTbl *vindexes.Table, query string, colVindexes []*vindexes.ColumnVindex, vindexQuery string) *engine.DML {
	rp := newRoutingParams(ctx, rb.Routing.OpCode())
	rb.Routing.UpdateRoutingParams(ctx, rp)
	edml := &engine.DML{
		Query:             query,
		TableNames:        []string{vTbl.Name.String()},
		Vindexes:          colVindexes,
		OwnedVindexQuery:  vindexQuery,
		RoutingParameters: rp,
	}

	if rb.Routing.OpCode() != engine.Unsharded && vindexQuery != "" {
		primary := vTbl.ColumnVindexes[0]
		edml.KsidVindex = primary.Vindex
		edml.KsidLength = len(primary.Columns)
	}

	if hints != nil {
		edml.MultiShardAutocommit = hints.multiShardAutocommit
		edml.QueryTimeout = hints.queryTimeout
	}
	return edml
}

func updateSelectedVindexPredicate(op *operators.Route) sqlparser.Expr {
	tr, ok := op.Routing.(*operators.ShardedRouting)
	if !ok || tr.Selected == nil {
		return nil
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
	return nil
}

func getAllTableNames(op *operators.Route) ([]string, error) {
	tableNameMap := map[string]any{}
	err := operators.Visit(op, func(op operators.Operator) error {
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
	sources, err := slice.MapWithError(op.Sources, func(src operators.Operator) (logicalPlan, error) {
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

	return createLimit(plan, op.AST, ctx.VSchema.Environment(), ctx.VSchema.ConnCollation())
}

func createLimit(input logicalPlan, limit *sqlparser.Limit, env *vtenv.Environment, coll collations.ID) (logicalPlan, error) {
	plan := newLimit(input)
	cfg := &evalengine.Config{
		Collation:   coll,
		Environment: env,
	}
	pv, err := evalengine.Translate(limit.Rowcount, cfg)
	if err != nil {
		return nil, vterrors.Wrap(err, "unexpected expression in LIMIT")
	}
	plan.elimit.Count = pv

	if limit.Offset != nil {
		pv, err = evalengine.Translate(limit.Offset, cfg)
		if err != nil {
			return nil, vterrors.Wrap(err, "unexpected expression in OFFSET")
		}
		plan.elimit.Offset = pv
	}

	return plan, nil
}

func transformHashJoin(ctx *plancontext.PlanningContext, op *operators.HashJoin) (logicalPlan, error) {
	lhs, err := transformToLogicalPlan(ctx, op.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToLogicalPlan(ctx, op.RHS)
	if err != nil {
		return nil, err
	}

	if len(op.LHSKeys) != 1 {
		return nil, vterrors.VT12001("hash joins must have exactly one join predicate")
	}

	joinOp := engine.InnerJoin
	if op.LeftJoin {
		joinOp = engine.LeftJoin
	}

	var missingTypes []string

	ltyp, found := ctx.SemTable.TypeForExpr(op.JoinComparisons[0].LHS)
	if !found {
		missingTypes = append(missingTypes, sqlparser.String(op.JoinComparisons[0].LHS))
	}
	rtyp, found := ctx.SemTable.TypeForExpr(op.JoinComparisons[0].RHS)
	if !found {
		missingTypes = append(missingTypes, sqlparser.String(op.JoinComparisons[0].RHS))
	}

	if len(missingTypes) > 0 {
		return nil, vterrors.VT12001(
			fmt.Sprintf("missing type information for [%s]", strings.Join(missingTypes, ", ")))
	}

	comparisonType, err := evalengine.CoerceTypes(ltyp, rtyp, ctx.VSchema.Environment().CollationEnv())
	if err != nil {
		return nil, err
	}

	return &hashJoin{
		lhs: lhs,
		rhs: rhs,
		inner: &engine.HashJoin{
			Opcode:         joinOp,
			Cols:           op.ColumnOffsets,
			LHSKey:         op.LHSKeys[0],
			RHSKey:         op.RHSKeys[0],
			ASTPred:        op.JoinPredicate(),
			Collation:      comparisonType.Collation(),
			ComparisonType: comparisonType.Type(),
			CollationEnv:   ctx.VSchema.Environment().CollationEnv(),
		},
	}, nil
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}
