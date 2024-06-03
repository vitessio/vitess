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

func transformToPrimitive(ctx *plancontext.PlanningContext, op operators.Operator) (engine.Primitive, error) {
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

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToPrimitive)", op))
}

func transformDMLWithInput(ctx *plancontext.PlanningContext, op *operators.DMLWithInput) (engine.Primitive, error) {
	input, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	var dmls []engine.Primitive
	for _, dml := range op.DML {
		del, err := transformToPrimitive(ctx, dml)
		if err != nil {
			return nil, err
		}
		dmls = append(dmls, del)
	}

	return &engine.DMLWithInput{
		DMLs:       dmls,
		Input:      input,
		OutputCols: op.Offsets,
		BVList:     op.BvList,
	}, nil
}

func transformUpsert(ctx *plancontext.PlanningContext, op *operators.Upsert) (engine.Primitive, error) {
	upsert := &engine.Upsert{}
	for _, source := range op.Sources {
		iLp, uLp, err := transformOneUpsert(ctx, source)
		if err != nil {
			return nil, err
		}
		upsert.AddUpsert(iLp, uLp)
	}
	return upsert, nil
}

func transformOneUpsert(ctx *plancontext.PlanningContext, source operators.UpsertSource) (iLp, uLp engine.Primitive, err error) {
	iLp, err = transformToPrimitive(ctx, source.Insert)
	if err != nil {
		return
	}
	ins, ok := iLp.(*engine.Insert)
	if ok {
		ins.PreventAutoCommit = true
	}
	uLp, err = transformToPrimitive(ctx, source.Update)
	return
}

func transformSequential(ctx *plancontext.PlanningContext, op *operators.Sequential) (engine.Primitive, error) {
	var prims []engine.Primitive
	for _, source := range op.Sources {
		prim, err := transformToPrimitive(ctx, source)
		if err != nil {
			return nil, err
		}
		ins, ok := prim.(*engine.Insert)
		if ok {
			ins.PreventAutoCommit = true
		}

		prims = append(prims, prim)
	}

	return engine.NewSequential(prims), nil
}

func transformInsertionSelection(ctx *plancontext.PlanningContext, op *operators.InsertSelection) (engine.Primitive, error) {
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

	eins.Prefix, _, eins.Suffix = generateInsertShardedQuery(ins.AST)

	selectionPlan, err := transformToPrimitive(ctx, op.Select)
	if err != nil {
		return nil, err
	}

	eins.Input = selectionPlan
	return eins, nil
}

// transformFkCascade transforms a FkCascade operator into an engine primitive
func transformFkCascade(ctx *plancontext.PlanningContext, fkc *operators.FkCascade) (engine.Primitive, error) {
	// We convert the parent operator to a primitive
	parentLP, err := transformToPrimitive(ctx, fkc.Parent)
	if err != nil {
		return nil, nil
	}

	// Once we have the parent primitive, we can create the selection primitive and the primitives for the children operators.
	// For all of these, we don't need the semTable anymore. We set it to nil, to avoid using an incorrect one.
	ctx.SemTable = nil
	selLP, err := transformToPrimitive(ctx, fkc.Selection)
	if err != nil {
		return nil, err
	}

	// Go over the children and convert them to Primitives too.
	var children []*engine.FkChild
	for _, child := range fkc.Children {
		childLP, err := transformToPrimitive(ctx, child.Op)
		if err != nil {
			return nil, err
		}

		childEngine := childLP
		children = append(children, &engine.FkChild{
			BVName:         child.BVName,
			Cols:           child.Cols,
			NonLiteralInfo: child.NonLiteralInfo,
			Exec:           childEngine,
		})
	}

	return &engine.FkCascade{
		Selection: selLP,
		Children:  children,
		Parent:    parentLP,
	}, nil
}

func transformSubQuery(ctx *plancontext.PlanningContext, op *operators.SubQuery) (engine.Primitive, error) {
	outer, err := transformToPrimitive(ctx, op.Outer)
	if err != nil {
		return nil, err
	}

	inner, err := transformToPrimitive(ctx, op.Subquery)
	if err != nil {
		return nil, err
	}

	cols, err := op.GetJoinColumns(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		// no correlation, so uncorrelated it is
		return &engine.UncorrelatedSubquery{
			Opcode:         op.FilterType,
			SubqueryResult: op.SubqueryValueName,
			HasValues:      op.HasValuesName,
			Subquery:       inner,
			Outer:          outer,
		}, nil
	}

	return &engine.SemiJoin{
		Left:  outer,
		Right: inner,
		Vars:  op.Vars,
	}, nil
}

// transformFkVerify transforms a FkVerify operator into a engine primitive
func transformFkVerify(ctx *plancontext.PlanningContext, fkv *operators.FkVerify) (engine.Primitive, error) {
	inputLP, err := transformToPrimitive(ctx, fkv.Input)
	if err != nil {
		return nil, err
	}

	// Once we have the input primitive, we can create the primitives for the verification operators.
	// For all of these, we don't need the semTable anymore. We set it to nil, to avoid using an incorrect one.
	ctx.SemTable = nil

	// Go over the children and convert them to Primitives too.
	var verify []*engine.Verify
	for _, v := range fkv.Verify {
		lp, err := transformToPrimitive(ctx, v.Op)
		if err != nil {
			return nil, err
		}
		verify = append(verify, &engine.Verify{
			Exec: lp,
			Typ:  v.Typ,
		})
	}

	return &engine.FkVerify{
		Verify: verify,
		Exec:   inputLP,
	}, nil

}

func transformAggregator(ctx *plancontext.PlanningContext, op *operators.Aggregator) (engine.Primitive, error) {
	if op.WithRollup {
		return nil, vterrors.VT12001("GROUP BY WITH ROLLUP not supported for sharded queries")
	}
	src, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	var aggregates []*engine.AggregateParams
	var groupByKeys []*engine.GroupByParams

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
		aggregates = append(aggregates, aggrParam)
	}

	for _, groupBy := range op.Grouping {
		typ, _ := ctx.SemTable.TypeForExpr(groupBy.Inner)
		groupByKeys = append(groupByKeys, &engine.GroupByParams{
			KeyCol:          groupBy.ColOffset,
			WeightStringCol: groupBy.WSOffset,
			Expr:            groupBy.Inner,
			Type:            typ,
			CollationEnv:    ctx.VSchema.Environment().CollationEnv(),
		})
	}

	if len(groupByKeys) == 0 {
		return &engine.ScalarAggregate{
			Aggregates:          aggregates,
			TruncateColumnCount: op.ResultColumns,
			Input:               src,
		}, nil
	}

	return &engine.OrderedAggregate{
		Aggregates:          aggregates,
		GroupByKeys:         groupByKeys,
		TruncateColumnCount: op.ResultColumns,
		Input:               src,
	}, nil
}

func transformDistinct(ctx *plancontext.PlanningContext, op *operators.Distinct) (engine.Primitive, error) {
	src, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	return &engine.Distinct{
		Source:    src,
		CheckCols: op.Columns,
		Truncate:  op.Truncate,
	}, nil
}

func transformOrdering(ctx *plancontext.PlanningContext, op *operators.Ordering) (engine.Primitive, error) {
	plan, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	return createMemorySort(ctx, plan, op)
}

func createMemorySort(ctx *plancontext.PlanningContext, src engine.Primitive, ordering *operators.Ordering) (engine.Primitive, error) {
	prim := &engine.MemorySort{
		Input:               src,
		TruncateColumnCount: ordering.ResultColumns,
	}

	for idx, order := range ordering.Order {
		typ, _ := ctx.SemTable.TypeForExpr(order.SimplifiedExpr)
		prim.OrderBy = append(prim.OrderBy, evalengine.OrderByParams{
			Col:             ordering.Offset[idx],
			WeightStringCol: ordering.WOffset[idx],
			Desc:            order.Inner.Direction == sqlparser.DescOrder,
			Type:            typ,
			CollationEnv:    ctx.VSchema.Environment().CollationEnv(),
		})
	}

	return prim, nil
}

func transformProjection(ctx *plancontext.PlanningContext, op *operators.Projection) (engine.Primitive, error) {
	src, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	if cols, colNames := op.AllOffsets(); cols != nil {
		// if all this op is doing is passing through columns from the input, we
		// can use the faster SimpleProjection
		if len(op.Source.GetColumns(ctx)) == len(cols) && offsetInInputOrder(cols) {
			cols = nil
		}
		return newSimpleProjection(cols, colNames, src), nil
	}

	ap, err := op.GetAliasedProjections()
	if err != nil {
		return nil, err
	}

	var evalengineExprs []evalengine.Expr
	var columnNames []string
	for _, pe := range ap {
		ee, err := getEvalEngineExpr(ctx, pe)
		if err != nil {
			return nil, err
		}
		evalengineExprs = append(evalengineExprs, ee)
		columnNames = append(columnNames, pe.Original.ColumnName())
	}

	return &engine.Projection{
		Input: src,
		Cols:  columnNames,
		Exprs: evalengineExprs,
	}, nil
}

// offsetInInputOrder returns true if the columns are in the same order as the input
func offsetInInputOrder(cols []int) bool {
	for i, c := range cols {
		if c != i {
			return false
		}
	}
	return true
}

func getEvalEngineExpr(ctx *plancontext.PlanningContext, pe *operators.ProjExpr) (evalengine.Expr, error) {
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

// newSimpleProjection creates a simple projections
func newSimpleProjection(cols []int, colNames []string, src engine.Primitive) engine.Primitive {
	return &engine.SimpleProjection{
		Input:    src,
		Cols:     cols,
		ColNames: colNames,
	}
}

func transformFilter(ctx *plancontext.PlanningContext, op *operators.Filter) (engine.Primitive, error) {
	src, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	predicate := op.PredicateWithOffsets
	if predicate == nil {
		panic("this should have already been done")
	}

	return &engine.Filter{
		Input:        src,
		Predicate:    predicate,
		ASTPredicate: ctx.SemTable.AndExpressions(op.Predicates...),
		Truncate:     op.Truncate,
	}, nil
}

func transformApplyJoinPlan(ctx *plancontext.PlanningContext, n *operators.ApplyJoin) (engine.Primitive, error) {
	lhs, err := transformToPrimitive(ctx, n.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToPrimitive(ctx, n.RHS)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if !n.JoinType.IsInner() {
		opCode = engine.LeftJoin
	}

	return &engine.Join{
		Opcode: opCode,
		Left:   lhs,
		Right:  rhs,
		Cols:   n.Columns,
		Vars:   n.Vars,
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

func transformRoutePlan(ctx *plancontext.PlanningContext, op *operators.Route) (engine.Primitive, error) {
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
		return buildRoutePrimitive(ctx, op, stmt, hints)
	case *sqlparser.Update:
		return buildUpdatePrimitive(ctx, op, dmlOp, stmt, hints)
	case *sqlparser.Delete:
		return buildDeletePrimitive(ctx, op, dmlOp, stmt, hints)
	case *sqlparser.Insert:
		return buildInsertPrimitive(op, dmlOp, stmt, hints)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("dont know how to %T", stmt))
	}
}

func buildRoutePrimitive(ctx *plancontext.PlanningContext, op *operators.Route, stmt sqlparser.SelectStatement, hints *queryHints) (engine.Primitive, error) {
	_ = updateSelectedVindexPredicate(op.Routing)

	eroute, err := routeToEngineRoute(ctx, op, hints)
	if err != nil {
		return nil, err
	}

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

	prepareTheAST(stmt)

	res, err := WireupRoute(ctx, eroute, stmt)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func buildInsertPrimitive(
	rb *operators.Route, op operators.Operator, stmt *sqlparser.Insert,
	hints *queryHints,
) (engine.Primitive, error) {
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

	// we would need to generate the query on the fly. The only exception here is
	// when unsharded query with autoincrement for that there is no input operator.
	if eins.Opcode != engine.InsertUnsharded {
		eins.Prefix, eins.Mid, eins.Suffix = generateInsertShardedQuery(ins.AST)
		if ins.AST.RowAlias != nil {
			eins.Alias = sqlparser.String(ins.AST.RowAlias)
		}
	}

	eins.Query = generateQuery(stmt)
	return eins, nil
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
		ins.Table, ins.Columns, ins.RowAlias)
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

func buildUpdatePrimitive(
	ctx *plancontext.PlanningContext,
	rb *operators.Route,
	dmlOp operators.Operator,
	stmt *sqlparser.Update,
	hints *queryHints,
) (engine.Primitive, error) {
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
	_ = updateSelectedVindexPredicate(rb.Routing)
	edml := createDMLPrimitive(ctx, rb, hints, upd.Target.VTable, generateQuery(stmt), vindexes, vQuery)

	return &engine.Update{
		DML:                 edml,
		ChangedVindexValues: upd.ChangedVindexValues,
	}, nil
}

func buildDeletePrimitive(ctx *plancontext.PlanningContext, rb *operators.Route, dmlOp operators.Operator, stmt *sqlparser.Delete, hints *queryHints) (engine.Primitive, error) {
	del := dmlOp.(*operators.Delete)

	var vindexes []*vindexes.ColumnVindex
	vQuery := ""
	if del.OwnedVindexQuery != nil {
		del.OwnedVindexQuery.From = stmt.GetFrom()
		del.OwnedVindexQuery.Where = stmt.Where
		vQuery = sqlparser.String(del.OwnedVindexQuery)
		vindexes = del.Target.VTable.Owned
	}
	_ = updateSelectedVindexPredicate(rb.Routing)
	edml := createDMLPrimitive(ctx, rb, hints, del.Target.VTable, generateQuery(stmt), vindexes, vQuery)

	return &engine.Delete{DML: edml}, nil
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

func updateSelectedVindexPredicate(routing operators.Routing) sqlparser.Expr {
	tr, ok := routing.(*operators.ShardedRouting)
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
		if sqlparser.Equals.Expr(cmp.Right, sqlparser.ListArg(engine.DmlVals)) {
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

func transformUnionPlan(ctx *plancontext.PlanningContext, op *operators.Union) (engine.Primitive, error) {
	sources, err := slice.MapWithError(op.Sources, func(src operators.Operator) (engine.Primitive, error) {
		primitive, err := transformToPrimitive(ctx, src)
		if err != nil {
			return nil, err
		}
		return primitive, nil
	})
	if err != nil {
		return nil, err
	}

	if len(sources) == 1 {
		return sources[0], nil
	}

	return engine.NewConcatenate(sources, nil), nil
}

func transformLimit(ctx *plancontext.PlanningContext, op *operators.Limit) (engine.Primitive, error) {
	plan, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	return createLimit(plan, op.AST, ctx.VSchema.Environment(), ctx.VSchema.ConnCollation())
}

func createLimit(input engine.Primitive, limit *sqlparser.Limit, env *vtenv.Environment, coll collations.ID) (engine.Primitive, error) {
	cfg := &evalengine.Config{
		Collation:   coll,
		Environment: env,
	}
	count, err := evalengine.Translate(limit.Rowcount, cfg)
	if err != nil {
		return nil, vterrors.Wrap(err, "unexpected expression in LIMIT")
	}
	var offset evalengine.Expr
	if limit.Offset != nil {
		offset, err = evalengine.Translate(limit.Offset, cfg)
		if err != nil {
			return nil, vterrors.Wrap(err, "unexpected expression in OFFSET")
		}
	}

	return &engine.Limit{
		Input:  input,
		Count:  count,
		Offset: offset,
	}, nil
}

func transformHashJoin(ctx *plancontext.PlanningContext, op *operators.HashJoin) (engine.Primitive, error) {
	lhs, err := transformToPrimitive(ctx, op.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToPrimitive(ctx, op.RHS)
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

	return &engine.HashJoin{
		Left:           lhs,
		Right:          rhs,
		Opcode:         joinOp,
		Cols:           op.ColumnOffsets,
		LHSKey:         op.LHSKeys[0],
		RHSKey:         op.RHSKeys[0],
		ASTPred:        op.JoinPredicate(),
		Collation:      comparisonType.Collation(),
		ComparisonType: comparisonType.Type(),
		CollationEnv:   ctx.VSchema.Environment().CollationEnv(),
		Values:         comparisonType.Values(),
	}, nil
}

func transformVindexPlan(ctx *plancontext.PlanningContext, op *operators.Vindex) (engine.Primitive, error) {
	single, ok := op.Vindex.(vindexes.SingleColumn)
	if !ok {
		return nil, vterrors.VT12001("multi-column vindexes not supported")
	}

	expr, err := evalengine.Translate(op.Value, &evalengine.Config{
		Collation:   ctx.SemTable.Collation,
		ResolveType: ctx.SemTable.TypeForExpr,
		Environment: ctx.VSchema.Environment(),
	})
	if err != nil {
		return nil, err
	}
	prim := &engine.VindexFunc{
		Opcode: op.OpCode,
		Vindex: single,
		Value:  expr,
	}

	for _, col := range op.Columns {
		err := SupplyProjection(prim, &sqlparser.AliasedExpr{
			Expr: col,
			As:   sqlparser.IdentifierCI{},
		}, false)
		if err != nil {
			return nil, err
		}
	}
	return prim, nil
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}
