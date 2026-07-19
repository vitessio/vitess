/*
Copyright 2026 The Vitess Authors.

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
	"maps"
	"slices"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// This file guards vtgate-evaluated consumers of a cross-shard JSON
// aggregation merge (json_arrayagg_merge, json_objectagg_merge). Comparisons
// over merged JSON agree with MySQL and stay supported. GREATEST/LEAST are
// rejected as a conservative policy (MySQL evaluates them with warning 1235
// and different result metadata), and any vtgate operation that compares a
// merged document able to carry MySQL scalar subtypes (binary, bit, decimal,
// temporal) is rejected: the shard protocol's JSON text serialization erases
// those subtypes. Provenance is resolved through output offsets, and unknown
// provenance never silently degrades to independent.

type outputRisk uint8

const (
	// riskIndependent: the value does not depend on a vtgate JSON merge.
	riskIndependent outputRisk = iota
	// riskMergedJSON: the value is (or contains) a vtgate JSON merge result.
	riskMergedJSON
	// riskSubtypeLossyMergedJSON: as riskMergedJSON, and the document can
	// carry values whose MySQL JSON scalar subtype was erased in transit.
	riskSubtypeLossyMergedJSON
	// riskUnknown: the mapping could not be proved; never treated as independent.
	riskUnknown
)

func combineRisk(a, b outputRisk) outputRisk {
	switch {
	case a == riskUnknown || b == riskUnknown:
		return riskUnknown
	case a == riskSubtypeLossyMergedJSON || b == riskSubtypeLossyMergedJSON:
		return riskSubtypeLossyMergedJSON
	case a == riskMergedJSON || b == riskMergedJSON:
		return riskMergedJSON
	default:
		return riskIndependent
	}
}

type (
	outputRiskKey struct {
		op     operators.Operator
		offset int
	}

	outputRiskResolver struct {
		ctx      *plancontext.PlanningContext
		memo     map[outputRiskKey]outputRisk
		visiting map[outputRiskKey]bool
	}
)

func newOutputRiskResolver(ctx *plancontext.PlanningContext) *outputRiskResolver {
	return &outputRiskResolver{
		ctx:      ctx,
		memo:     map[outputRiskKey]outputRisk{},
		visiting: map[outputRiskKey]bool{},
	}
}

func (r *outputRiskResolver) riskForOutput(op operators.Operator, offset int) outputRisk {
	key := outputRiskKey{op: op, offset: offset}
	if risk, ok := r.memo[key]; ok {
		return risk
	}
	if r.visiting[key] {
		return riskUnknown
	}
	r.visiting[key] = true
	risk := r.computeRiskForOutput(op, offset)
	delete(r.visiting, key)
	r.memo[key] = risk
	return risk
}

func (r *outputRiskResolver) computeRiskForOutput(op operators.Operator, offset int) outputRisk {
	switch node := op.(type) {
	case *operators.Route:
		// A Route boundary: the value was produced by MySQL, not by a
		// vtgate JSON merge.
		return riskIndependent
	case *operators.Aggregator:
		return r.aggregatorRisk(node, offset)
	case *operators.Projection:
		return r.projectionRisk(node, offset)
	case *operators.Filter, *operators.Limit, *operators.Distinct, *operators.Ordering:
		// Pass-through operators: column layouts mirror their input.
		return r.riskForOutput(op.Inputs()[0], offset)
	case *operators.Union:
		// Union output columns are positionally aligned across every source,
		// so an output offset combines the risk of that offset in each source.
		risk := riskIndependent
		for _, src := range node.Sources {
			risk = combineRisk(risk, r.riskForOutput(src, offset))
		}
		return risk
	default:
		// Unmapped operators fail closed: an unproved mapping must not be
		// treated as independent.
		return riskUnknown
	}
}

func (r *outputRiskResolver) aggregatorRisk(agg *operators.Aggregator, offset int) outputRisk {
	if offset < 0 || offset >= len(agg.Columns) {
		return riskUnknown
	}
	for _, a := range agg.Aggregations {
		if a.ColOffset != offset {
			continue
		}
		switch a.OpCode {
		case opcode.AggregateJSONArrayMerge, opcode.AggregateJSONObjectMerge:
			if aggregateValueMayLoseJSONSubtype(r.ctx, a) {
				return riskSubtypeLossyMergedJSON
			}
			return riskMergedJSON
		case opcode.AggregateMin, opcode.AggregateMax, opcode.AggregateAnyValue:
			// Value-preserving: the output is one of the input values.
			return r.riskForOutput(agg.Inputs()[0], offset)
		case opcode.AggregateConstant,
			opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct,
			opcode.AggregateSum, opcode.AggregateSumDistinct, opcode.AggregateAvg,
			opcode.AggregateGroupConcat, opcode.AggregateGtid:
			// Type-changing: the output is never a JSON document.
			return riskIndependent
		default:
			return riskUnknown
		}
	}
	// Grouping and passthrough columns mirror the input column layout.
	return r.riskForOutput(agg.Inputs()[0], offset)
}

func (r *outputRiskResolver) projectionRisk(p *operators.Projection, offset int) outputRisk {
	ap, err := p.GetAliasedProjections()
	if err != nil || offset < 0 || offset >= len(ap) {
		return riskUnknown
	}
	pe := ap[offset]
	if off, ok := pe.Info.(operators.Offset); ok {
		return r.riskForOutput(p.Inputs()[0], int(off))
	}
	return r.riskForExpr(p.Inputs()[0], pe.EvalExpr)
}

// riskForExpr combines the risk of every input offset the expression reads;
// Offset.Original is display-only and never used for provenance.
func (r *outputRiskResolver) riskForExpr(source operators.Operator, expr sqlparser.Expr) outputRisk {
	risk := riskIndependent
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if off, ok := node.(*sqlparser.Offset); ok {
			risk = combineRisk(risk, r.riskForOutput(source, off.V))
		}
		return true, nil
	}, expr)
	return risk
}

// aggregateValueMayLoseJSONSubtype reports whether the merged documents of a
// JSON aggregation can contain values whose MySQL JSON scalar subtype is
// erased when the shard partials cross the query protocol as JSON text.
func aggregateValueMayLoseJSONSubtype(ctx *plancontext.PlanningContext, aggr operators.Aggr) bool {
	valueExpr, ok := jsonAggregateValueArgument(aggr)
	if !ok {
		return true
	}
	typ, found := ctx.TypeForExpr(valueExpr)
	if !found || !typ.Valid() {
		return true
	}
	t := typ.Type()
	switch {
	case t == sqltypes.TypeJSON:
		// A JSON document can already contain opaque internal values.
		return true
	case sqltypes.IsBinary(t), t == sqltypes.Bit, sqltypes.IsDateOrTime(t):
		return true
	case t == sqltypes.Decimal:
		// MySQL keeps DECIMAL values inside an aggregated JSON document as an
		// opaque decimal subtype, which the text round trip erases.
		return true
	case sqltypes.IsNumber(t), sqltypes.IsText(t), sqltypes.IsEnum(t), sqltypes.IsSet(t), t == sqltypes.Null:
		return false
	default:
		return true
	}
}

// jsonAggregateValueArgument returns the expression whose values become the
// members of the merged JSON document. For JSON_OBJECTAGG the key argument is
// converted to a member name and cannot retain a JSON scalar subtype.
func jsonAggregateValueArgument(aggr operators.Aggr) (sqlparser.Expr, bool) {
	switch fn := aggr.Func.(type) {
	case *sqlparser.JSONArrayAgg:
		return fn.Expr, true
	case *sqlparser.JSONObjectAgg:
		return fn.Value, true
	default:
		return nil, false
	}
}

type candidateAnalysis struct {
	risk      outputRisk
	sqlType   querypb.Type
	typeKnown bool
}

func (c candidateAnalysis) dependsOnMerge() bool {
	return c.risk == riskMergedJSON || c.risk == riskSubtypeLossyMergedJSON
}

func (c candidateAnalysis) subtypeLossy() bool {
	return c.risk == riskSubtypeLossyMergedJSON
}

func (c candidateAnalysis) isJSON() bool {
	return c.typeKnown && c.sqlType == querypb.Type_JSON
}

// mergedJSONOperand: a proved merge dependency that is JSON or of
// unresolvable type (fail closed).
func (c candidateAnalysis) mergedJSONOperand() bool {
	return c.dependsOnMerge() && (!c.typeKnown || c.isJSON())
}

// unknownMergeProvenance: an unproved dependency mapping with a JSON or
// unresolvable type; must not silently pass the restricted operations.
func (c candidateAnalysis) unknownMergeProvenance() bool {
	return c.risk == riskUnknown && (!c.typeKnown || c.isJSON())
}

// lossyJSONOperand: a proved subtype-lossy merge dependency that is JSON or
// of unresolvable type.
func (c candidateAnalysis) lossyJSONOperand() bool {
	return c.subtypeLossy() && (!c.typeKnown || c.isJSON())
}

type jsonWrapperKind uint8

const (
	wrapperUnclassified jsonWrapperKind = iota
	wrapperReturnsJSON
	wrapperReturnsText
)

// classifyKnownJSONWrapper recognizes the small set of expression forms whose
// result type is known even when the generic type resolver cannot type them;
// anything unrecognized stays unclassified.
func classifyKnownJSONWrapper(expr sqlparser.Expr) jsonWrapperKind {
	switch e := expr.(type) {
	case *sqlparser.JSONExtractExpr, *sqlparser.JSONObjectExpr, *sqlparser.JSONArrayExpr:
		return wrapperReturnsJSON
	case *sqlparser.JSONUnquoteExpr:
		return wrapperReturnsText
	case *sqlparser.CastExpr:
		return classifyConvertType(e.Type)
	case *sqlparser.ConvertExpr:
		return classifyConvertType(e.Type)
	default:
		return wrapperUnclassified
	}
}

func classifyConvertType(typ *sqlparser.ConvertType) jsonWrapperKind {
	if typ == nil {
		return wrapperUnclassified
	}
	switch strings.ToLower(typ.Type) {
	case "json":
		return wrapperReturnsJSON
	case "char", "nchar":
		return wrapperReturnsText
	default:
		return wrapperUnclassified
	}
}

type mergedJSONValidator struct {
	ctx      *plancontext.PlanningContext
	source   operators.Operator
	resolver *outputRiskResolver
}

// validateMergedJSONComparisons fails planning when a vtgate-evaluated
// expression consumes a merged JSON aggregate through an operation where the
// result could differ from MySQL. Expressions must be in offset-rewritten
// form: provenance is resolved through the Offset nodes.
func validateMergedJSONComparisons(ctx *plancontext.PlanningContext, source operators.Operator, exprs ...sqlparser.Expr) error {
	if !sourceHasMergedJSONAgg(source) {
		return nil
	}
	v := &mergedJSONValidator{
		ctx:      ctx,
		source:   source,
		resolver: newOutputRiskResolver(ctx),
	}
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		if err := v.validateExpr(expr); err != nil {
			return err
		}
	}
	return nil
}

// sourceHasMergedJSONAgg is a cheap pre-gate: queries without a vtgate JSON
// merge skip validation entirely. The walk stops at Route boundaries, where
// a JSON aggregation is executed by MySQL.
func sourceHasMergedJSONAgg(op operators.Operator) bool {
	if _, isRoute := op.(*operators.Route); isRoute {
		return false
	}
	if agg, ok := op.(*operators.Aggregator); ok {
		if slices.ContainsFunc(agg.Aggregations, func(a operators.Aggr) bool {
			return a.OpCode == opcode.AggregateJSONArrayMerge || a.OpCode == opcode.AggregateJSONObjectMerge
		}) {
			return true
		}
	}
	return slices.ContainsFunc(op.Inputs(), sourceHasMergedJSONAgg)
}

func (v *mergedJSONValidator) candidate(expr sqlparser.Expr) candidateAnalysis {
	risk := v.resolver.riskForExpr(v.source, expr)

	if off, ok := expr.(*sqlparser.Offset); ok {
		if risk == riskMergedJSON || risk == riskSubtypeLossyMergedJSON {
			// A merge-derived offset carries a JSON value: merge outputs are
			// JSON, and the risk-propagating aggregates preserve their inputs.
			return candidateAnalysis{risk: risk, sqlType: querypb.Type_JSON, typeKnown: true}
		}
		if typ, found := v.ctx.TypeForExpr(off.Original); found && typ.Valid() {
			return candidateAnalysis{risk: risk, sqlType: typ.Type(), typeKnown: true}
		}
		return candidateAnalysis{risk: risk}
	}

	if semantics.ValidAsMapKey(expr) {
		if typ, found := v.ctx.TypeForExpr(expr); found && typ.Valid() && typ.Type() != sqltypes.Unknown {
			return candidateAnalysis{risk: risk, sqlType: typ.Type(), typeKnown: true}
		}
	}

	switch classifyKnownJSONWrapper(expr) {
	case wrapperReturnsJSON:
		return candidateAnalysis{risk: risk, sqlType: querypb.Type_JSON, typeKnown: true}
	case wrapperReturnsText:
		return candidateAnalysis{risk: risk, sqlType: querypb.Type_VARCHAR, typeKnown: true}
	default:
		return candidateAnalysis{risk: risk}
	}
}

func (v *mergedJSONValidator) validateExpr(expr sqlparser.Expr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		var err error
		switch node := node.(type) {
		case *sqlparser.ComparisonExpr:
			switch node.Operator {
			case sqlparser.LikeOp, sqlparser.NotLikeOp, sqlparser.RegexpOp, sqlparser.NotRegexpOp:
				// String matching serializes a JSON operand to its text form
				// in both MySQL and the evalengine: no divergence to guard.
			case sqlparser.InOp, sqlparser.NotInOp:
				err = v.validateIn(node)
			default:
				err = v.validateOrdinaryComparison(node.Operator.ToString(), node.Left, node.Right, node)
			}
		case *sqlparser.BetweenExpr:
			err = v.validateBetween(node)
		case *sqlparser.CaseExpr:
			err = v.validateSimpleCase(node)
		case *sqlparser.FuncExpr:
			switch node.Name.Lowered() {
			case "nullif":
				if len(node.Exprs) == 2 {
					err = v.validateOrdinaryComparison("NULLIF", node.Exprs[0], node.Exprs[1], node)
				}
			case "greatest", "least":
				err = v.validateGreatestLeast(node)
			}
		}
		// Always keep descending: nested restricted operations inside the
		// operands of an already-validated node must still be discovered.
		return true, err
	}, expr)
}

// validateIn checks IN and NOT IN: the evalengine reproduces MySQL's
// comparison-domain aggregation, so only subtype-lossy merged operands and
// unproved provenance reject.
func (v *mergedJSONValidator) validateIn(node *sqlparser.ComparisonExpr) error {
	op := "IN"
	if node.Operator == sqlparser.NotInOp {
		op = "NOT IN"
	}
	candidates := []sqlparser.Expr{node.Left}
	if tuple, ok := node.Right.(sqlparser.ValTuple); ok {
		candidates = append(candidates, tuple...)
	} else {
		// A list bind variable has no merge provenance of its own; the left
		// operand decides.
		candidates = append(candidates, node.Right)
	}
	return v.rejectRestricted(op, candidates, node)
}

func (v *mergedJSONValidator) validateBetween(node *sqlparser.BetweenExpr) error {
	op := "BETWEEN"
	if !node.IsBetween {
		op = "NOT BETWEEN"
	}
	return v.rejectRestricted(op, []sqlparser.Expr{node.Left, node.From, node.To}, node)
}

// validateSimpleCase checks the comparison domain of a simple CASE (the base
// operand and every WHEN value). Searched CASE has no comparison domain; its
// nested expressions are validated by the surrounding walk.
func (v *mergedJSONValidator) validateSimpleCase(node *sqlparser.CaseExpr) error {
	if node.Expr == nil {
		return nil
	}
	candidates := []sqlparser.Expr{node.Expr}
	for _, when := range node.Whens {
		candidates = append(candidates, when.Cond)
	}
	return v.rejectRestricted("simple CASE", candidates, node)
}

func (v *mergedJSONValidator) validateGreatestLeast(node *sqlparser.FuncExpr) error {
	op := "GREATEST"
	if node.Name.Lowered() == "least" {
		op = "LEAST"
	}
	for _, arg := range node.Exprs {
		c := v.candidate(arg)
		if c.mergedJSONOperand() {
			return vterrors.VT12001(fmt.Sprintf(
				"%s over a vtgate-merged JSON aggregate is not yet supported (MySQL evaluates it with warning 1235 and different result metadata): %s",
				op, sqlparser.String(printableExpr(node))))
		}
		if c.unknownMergeProvenance() {
			return v.unknownProvenanceError(op, node)
		}
	}
	return nil
}

// validateOrdinaryComparison allows ordinary comparisons and NULLIF, where
// the evalengine agrees with MySQL: only subtype-lossy merged operands and
// unproved provenance reject.
func (v *mergedJSONValidator) validateOrdinaryComparison(op string, left, right sqlparser.Expr, node sqlparser.Expr) error {
	for _, expr := range []sqlparser.Expr{left, right} {
		c := v.candidate(expr)
		if c.lossyJSONOperand() {
			return vterrors.VT12001(fmt.Sprintf(
				"comparison consumes a vtgate-merged JSON aggregate whose MySQL binary, bit, decimal, or temporal scalar subtype is not preserved across shard serialization: %s",
				sqlparser.String(printableExpr(node))))
		}
		if c.unknownMergeProvenance() {
			return v.unknownProvenanceError(op, node)
		}
	}
	return nil
}

// rejectRestricted applies the shared comparison policy to the comparison
// domain of IN/NOT IN, BETWEEN/NOT BETWEEN and simple CASE: subtype-lossy
// merged operands and unproved provenance reject.
func (v *mergedJSONValidator) rejectRestricted(op string, candidates []sqlparser.Expr, node sqlparser.Expr) error {
	for _, expr := range candidates {
		c := v.candidate(expr)
		if c.lossyJSONOperand() {
			return vterrors.VT12001(fmt.Sprintf(
				"%s consumes a vtgate-merged JSON aggregate whose MySQL binary, bit, decimal, or temporal scalar subtype is not preserved across shard serialization: %s",
				op, sqlparser.String(printableExpr(node))))
		}
		if c.unknownMergeProvenance() {
			return v.unknownProvenanceError(op, node)
		}
	}
	return nil
}

func (v *mergedJSONValidator) unknownProvenanceError(op string, node sqlparser.Expr) error {
	return vterrors.VT12001(fmt.Sprintf(
		"cannot establish whether the JSON operand of %s depends on a vtgate-merged JSON aggregate: %s",
		op, sqlparser.String(printableExpr(node))))
}

// validateAggregatorOverMergedJSON guards vtgate aggregations that compare
// their input values (MIN, MAX and DISTINCT-marked aggregations) and vtgate
// grouping: a subtype-lossy merged JSON column produces the wrong result
// inside the aggregation itself.
func validateAggregatorOverMergedJSON(ctx *plancontext.PlanningContext, agg *operators.Aggregator) error {
	input := agg.Inputs()[0]
	if !sourceHasMergedJSONAgg(input) {
		return nil
	}
	resolver := newOutputRiskResolver(ctx)
	for _, a := range agg.Aggregations {
		switch {
		case a.OpCode == opcode.AggregateMin, a.OpCode == opcode.AggregateMax,
			a.OpCode == opcode.AggregateCountDistinct, a.OpCode == opcode.AggregateSumDistinct,
			a.Distinct:
		default:
			continue
		}
		err := validateOperatorInputOverMergedJSON(resolver, input, a.ColOffset,
			strings.ToUpper(opcode.AggregateName[a.OpCode]), a.Original.Expr)
		if err != nil {
			return err
		}
	}
	for _, g := range agg.Grouping {
		if err := validateOperatorInputOverMergedJSON(resolver, input, g.ColOffset, "GROUP BY", g.Inner); err != nil {
			return err
		}
	}
	return nil
}

// validateDistinctOverMergedJSON guards the vtgate Distinct primitive (also
// reached by UNION DISTINCT): it compares its input rows column by column.
func validateDistinctOverMergedJSON(ctx *plancontext.PlanningContext, op *operators.Distinct) error {
	if !sourceHasMergedJSONAgg(op.Source) {
		return nil
	}
	resolver := newOutputRiskResolver(ctx)
	cols := op.Source.GetColumns(ctx)
	for _, cc := range op.Columns {
		if err := validateOperatorInputOverMergedJSON(resolver, op.Source, cc.Col, "DISTINCT", printableColumn(cols, cc.Col)); err != nil {
			return err
		}
	}
	return nil
}

// validateOrderingOverMergedJSON guards the vtgate MemorySort primitive: it
// compares its input rows on the ordering columns.
func validateOrderingOverMergedJSON(ctx *plancontext.PlanningContext, op *operators.Ordering) error {
	if !sourceHasMergedJSONAgg(op.Source) {
		return nil
	}
	resolver := newOutputRiskResolver(ctx)
	for idx, order := range op.Order {
		if err := validateOperatorInputOverMergedJSON(resolver, op.Source, op.Offset[idx], "ORDER BY", order.Inner.Expr); err != nil {
			return err
		}
	}
	return nil
}

// validateHashJoinOverMergedJSON guards the vtgate HashJoin primitive: it
// hashes and compares the join key of each side.
func validateHashJoinOverMergedJSON(ctx *plancontext.PlanningContext, op *operators.HashJoin) error {
	resolver := newOutputRiskResolver(ctx)
	if sourceHasMergedJSONAgg(op.LHS) {
		if err := validateOperatorInputOverMergedJSON(resolver, op.LHS, op.LHSKeys[0], "join comparison", op.JoinComparisons[0].LHS); err != nil {
			return err
		}
	}
	if sourceHasMergedJSONAgg(op.RHS) {
		if err := validateOperatorInputOverMergedJSON(resolver, op.RHS, op.RHSKeys[0], "join comparison", op.JoinComparisons[0].RHS); err != nil {
			return err
		}
	}
	return nil
}

// validateSubQueryPulloutOverMergedJSON guards subquery pullouts that consume
// the subquery's value in the outer query via a bind variable: the value is
// consumed by MySQL after subtype erasure. EXISTS variants do not consume the
// value and are exempt.
func validateSubQueryPulloutOverMergedJSON(ctx *plancontext.PlanningContext, op *operators.SubQuery) error {
	var opName string
	switch op.FilterType {
	case opcode.PulloutValue:
		opName = "scalar subquery"
	case opcode.PulloutIn:
		opName = "IN subquery"
	case opcode.PulloutNotIn:
		opName = "NOT IN subquery"
	default:
		return nil
	}
	if !sourceHasMergedJSONAgg(op.Subquery) {
		return nil
	}
	return validateOperatorInputOverMergedJSON(newOutputRiskResolver(ctx), op.Subquery, 0, opName, op.Original)
}

// validateJoinBindVarsOverMergedJSON guards the values an ApplyJoin or
// SemiJoin copies into the other side's shard queries as bind variables:
// a merged JSON document crosses back into MySQL after subtype erasure.
func validateJoinBindVarsOverMergedJSON(ctx *plancontext.PlanningContext, source operators.Operator, vars map[string]int) error {
	if len(vars) == 0 || !sourceHasMergedJSONAgg(source) {
		return nil
	}
	resolver := newOutputRiskResolver(ctx)
	cols := source.GetColumns(ctx)
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		if err := validateOperatorInputOverMergedJSON(resolver, source, vars[name], "join bind variable", printableColumn(cols, vars[name])); err != nil {
			return err
		}
	}
	return nil
}

// validateOperatorInputOverMergedJSON checks one input column a vtgate
// operator consumes by comparing values: a subtype-lossy merged JSON input is
// rejected, and unproved provenance fails closed.
func validateOperatorInputOverMergedJSON(resolver *outputRiskResolver, source operators.Operator, offset int, op string, printable sqlparser.Expr) error {
	switch resolver.riskForOutput(source, offset) {
	case riskSubtypeLossyMergedJSON:
		return vterrors.VT12001(fmt.Sprintf(
			"%s consumes a vtgate-merged JSON aggregate whose MySQL binary, bit, decimal, or temporal scalar subtype is not preserved across shard serialization: %s",
			op, sqlparser.String(printable)))
	case riskUnknown:
		return vterrors.VT12001(fmt.Sprintf(
			"cannot establish whether the input of %s depends on a vtgate-merged JSON aggregate: %s",
			op, sqlparser.String(printable)))
	}
	return nil
}

// printableColumn returns a display-only expression for an input column
// offset, falling back to the bare offset when the column list does not cover
// it.
func printableColumn(cols []*sqlparser.AliasedExpr, offset int) sqlparser.Expr {
	if offset >= 0 && offset < len(cols) {
		return cols[offset].Expr
	}
	return sqlparser.NewIntLiteral(strconv.Itoa(offset))
}

// printableExpr rewrites Offset nodes back to their original expressions for
// diagnostics. The result is display-only and must never be fed back into
// provenance or type analysis.
func printableExpr(expr sqlparser.Expr) sqlparser.Expr {
	rewritten := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		if off, ok := cursor.Node().(*sqlparser.Offset); ok && off.Original != nil {
			cursor.Replace(sqlparser.CloneExpr(off.Original))
		}
	}, nil)
	if e, ok := rewritten.(sqlparser.Expr); ok {
		return e
	}
	return expr
}
