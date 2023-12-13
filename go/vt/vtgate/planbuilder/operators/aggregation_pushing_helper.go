/*
Copyright 2023 The Vitess Authors.
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
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// aggBuilder is a helper struct that aids in pushing down an Aggregator through a join
	// it accumulates the projections (if any) that need to be evaluated on top of the join
	aggBuilder struct {
		lhs, rhs    *joinPusher
		joinColumns joinColumns
		proj        *Projection
		outerJoin   bool
	}

	// joinPusher is a helper struct that aids in pushing down an Aggregator into one side of a Join.
	// It creates a new Aggregator that is pushed down and keeps track of the column dependencies that the new Aggregator has.
	joinPusher struct {
		orig    *Aggregator        // The original Aggregator before pushing.
		pushed  *Aggregator        // The new Aggregator created for push-down.
		columns []int              // List of column offsets used in the new Aggregator.
		tableID semantics.TableSet // The TableSet denoting the side of the Join where the new Aggregator is pushed.

		// csAE keeps the copy of the countStar expression that has already been added to split an aggregation.
		// No need to have multiple countStars, so we cache it here
		csAE *sqlparser.AliasedExpr
	}

	joinColumns interface {
		addLeft(expr sqlparser.Expr)
		addRight(expr sqlparser.Expr)
	}

	applyJoinColumns struct {
		columns []applyJoinColumn
	}

	hashJoinColumns struct {
		columns []hashJoinColumn
	}
)

func (jc *applyJoinColumns) addLeft(expr sqlparser.Expr) {
	jc.columns = append(jc.columns, applyJoinColumn{
		Original: expr,
		LHSExprs: []BindVarExpr{{Expr: expr}},
	})
}

func (jc *applyJoinColumns) addRight(expr sqlparser.Expr) {
	jc.columns = append(jc.columns, applyJoinColumn{
		Original: expr,
		RHSExpr:  expr,
	})
}

func (jc *applyJoinColumns) clone() *applyJoinColumns {
	return &applyJoinColumns{columns: slices.Clone(jc.columns)}
}

func (jc *applyJoinColumns) add(col applyJoinColumn) {
	jc.columns = append(jc.columns, col)
}

func (hj *hashJoinColumns) addLeft(expr sqlparser.Expr) {
	hj.columns = append(hj.columns, hashJoinColumn{
		expr: expr,
		side: Left,
	})
}

func (hj *hashJoinColumns) add(expr sqlparser.Expr) {
	hj.columns = append(hj.columns, hashJoinColumn{
		expr: expr,
	})
}

func (hj *hashJoinColumns) addRight(expr sqlparser.Expr) {
	hj.columns = append(hj.columns, hashJoinColumn{
		expr: expr,
		side: Right,
	})
}

func (hj *hashJoinColumns) clone() *hashJoinColumns {
	return &hashJoinColumns{columns: slices.Clone(hj.columns)}
}

func (ab *aggBuilder) leftCountStar(ctx *plancontext.PlanningContext) *sqlparser.AliasedExpr {
	ae, created := ab.lhs.countStar(ctx)
	if created {
		ab.joinColumns.addLeft(ae.Expr)
	}
	return ae
}

func (ab *aggBuilder) rightCountStar(ctx *plancontext.PlanningContext) *sqlparser.AliasedExpr {
	ae, created := ab.rhs.countStar(ctx)
	if created {
		ab.joinColumns.addRight(ae.Expr)
	}
	return ae
}

func (ab *aggBuilder) handleAggr(ctx *plancontext.PlanningContext, aggr Aggr) error {
	switch aggr.OpCode {
	case opcode.AggregateCountStar:
		ab.handleCountStar(ctx, aggr)
		return nil
	case opcode.AggregateCount, opcode.AggregateSum:
		return ab.handleAggrWithCountStarMultiplier(ctx, aggr)
	case opcode.AggregateMax, opcode.AggregateMin, opcode.AggregateAnyValue:
		return ab.handlePushThroughAggregation(ctx, aggr)
	case opcode.AggregateGroupConcat:
		f := aggr.Func.(*sqlparser.GroupConcatExpr)
		if f.Distinct || len(f.OrderBy) > 0 || f.Separator != "" {
			panic("fail here")
		}
		// this needs special handling, currently aborting the push of function
		// and later will try pushing the column instead.
		// TODO: this should be handled better by pushing the function down.
		return errAbortAggrPushing
	case opcode.AggregateUnassigned:
		panic(vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original))))
	case opcode.AggregateGtid:
		// this is only used for SHOW GTID queries that will never contain joins
		panic(vterrors.VT13001("cannot do join with vgtid"))
	case opcode.AggregateSumDistinct, opcode.AggregateCountDistinct:
		// we are not going to see values multiple times, so we don't need to multiply with the count(*) from the other side
		return ab.handlePushThroughAggregation(ctx, aggr)
	default:
		panic(vterrors.VT12001(fmt.Sprintf("aggregation not planned: %s", aggr.OpCode.String())))
	}
}

// pushThroughLeft and Right are used for extremums and random,
// which are not split and then arithmetics is used to aggregate the per-shard aggregations.
// For these, we just copy the aggregation to one side of the join and then pick the max of the max:es returned
func (ab *aggBuilder) pushThroughLeft(aggr Aggr) {
	ab.lhs.pushThroughAggr(aggr)
	ab.joinColumns.addLeft(aggr.Original.Expr)
}

func (ab *aggBuilder) pushThroughRight(aggr Aggr) {
	ab.rhs.pushThroughAggr(aggr)
	ab.joinColumns.addRight(aggr.Original.Expr)
}

func (ab *aggBuilder) handlePushThroughAggregation(ctx *plancontext.PlanningContext, aggr Aggr) error {
	ab.proj.addUnexploredExpr(aggr.Original, aggr.Original.Expr)

	deps := ctx.SemTable.RecursiveDeps(aggr.Original.Expr)
	switch {
	case deps.IsSolvedBy(ab.lhs.tableID):
		ab.pushThroughLeft(aggr)
	case deps.IsSolvedBy(ab.rhs.tableID):
		ab.pushThroughRight(aggr)
	default:
		return errAbortAggrPushing
	}
	return nil
}

func (ab *aggBuilder) handleCountStar(ctx *plancontext.PlanningContext, aggr Aggr) {
	// Add the aggregate to both sides of the join.
	lhsAE := ab.leftCountStar(ctx)
	rhsAE := ab.rightCountStar(ctx)

	ab.buildProjectionForAggr(lhsAE, rhsAE, aggr, true)
}

func (ab *aggBuilder) handleAggrWithCountStarMultiplier(ctx *plancontext.PlanningContext, aggr Aggr) error {
	var lhsAE, rhsAE *sqlparser.AliasedExpr
	var addCoalesce bool

	deps := ctx.SemTable.RecursiveDeps(aggr.Original.Expr)
	switch {
	case deps.IsSolvedBy(ab.lhs.tableID):
		ab.pushThroughLeft(aggr)
		lhsAE = aggr.Original
		rhsAE = ab.rightCountStar(ctx)
		if ab.outerJoin {
			addCoalesce = true
		}

	case deps.IsSolvedBy(ab.rhs.tableID):
		ab.pushThroughRight(aggr)
		lhsAE = ab.leftCountStar(ctx)
		rhsAE = aggr.Original

	default:
		return errAbortAggrPushing
	}

	ab.buildProjectionForAggr(lhsAE, rhsAE, aggr, addCoalesce)
	return nil
}

func (ab *aggBuilder) buildProjectionForAggr(lhsAE *sqlparser.AliasedExpr, rhsAE *sqlparser.AliasedExpr, aggr Aggr, coalesce bool) {
	// We expect the expressions to be different on each side of the join, otherwise it's an error.
	if lhsAE.Expr == rhsAE.Expr {
		panic(fmt.Sprintf("Need the two produced expressions to be different. %T %T", lhsAE, rhsAE))
	}

	rhsExpr := rhsAE.Expr

	// When dealing with outer joins, we don't want null values from the RHS to ruin the calculations we are doing,
	// so we use the MySQL `coalesce` after the join is applied to multiply the count from LHS with 1.
	if ab.outerJoin && coalesce {
		rhsExpr = coalesceFunc(rhsExpr)
	}

	// The final COUNT is obtained by multiplying the counts from both sides.
	// This is equivalent to transforming a "select count(*) from t1 join t2" into
	// "select count_t1*count_t2 from
	//    (select count(*) as count_t1 from t1) as x,
	//    (select count(*) as count_t2 from t2) as y".
	projExpr := &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     lhsAE.Expr,
		Right:    rhsExpr,
	}
	projAE := &sqlparser.AliasedExpr{
		Expr: aggr.Original.Expr,
		As:   sqlparser.NewIdentifierCI(aggr.Original.ColumnName()),
	}

	ab.proj.addUnexploredExpr(projAE, projExpr)
}

func (p *joinPusher) countStar(ctx *plancontext.PlanningContext) (*sqlparser.AliasedExpr, bool) {
	if p.csAE != nil {
		return p.csAE, false
	}
	cs := &sqlparser.CountStar{}
	ae := aeWrap(cs)
	csAggr := NewAggr(opcode.AggregateCountStar, cs, ae, "")
	expr := p.addAggr(ctx, csAggr)
	p.csAE = aeWrap(expr)
	return p.csAE, true
}

// addAggr creates a copy of the given aggregation, updates its column offset to point to the correct location in the new Aggregator,
// and adds it to the list of Aggregations of the new Aggregator. It also updates the semantic analysis information to reflect the new structure.
// It returns the expression of the aggregation as it should be used in the parent Aggregator.
func (p *joinPusher) addAggr(ctx *plancontext.PlanningContext, aggr Aggr) sqlparser.Expr {
	copyAggr := aggr
	expr := sqlparser.CloneExpr(aggr.Original.Expr)
	copyAggr.Original = aeWrap(expr)
	// copy dependencies so we can keep track of which side expressions need to be pushed to
	ctx.SemTable.Direct[expr] = p.tableID
	ctx.SemTable.Recursive[expr] = p.tableID
	copyAggr.ColOffset = len(p.pushed.Columns)
	p.pushed.Columns = append(p.pushed.Columns, copyAggr.Original)
	p.pushed.Aggregations = append(p.pushed.Aggregations, copyAggr)
	return expr
}

// pushThroughAggr pushes through an aggregation without changing dependencies.
// Can be used for aggregations we can push in one piece
func (p *joinPusher) pushThroughAggr(aggr Aggr) {
	newAggr := NewAggr(aggr.OpCode, aggr.Func, aggr.Original, aggr.Alias)
	newAggr.ColOffset = len(p.pushed.Columns)
	p.pushed.Columns = append(p.pushed.Columns, newAggr.Original)
	p.pushed.Aggregations = append(p.pushed.Aggregations, newAggr)
}

// addGrouping creates a copy of the given GroupBy, updates its column offset to point to the correct location in the new Aggregator,
// and adds it to the list of GroupBy expressions of the new Aggregator. It also updates the semantic analysis information to reflect the new structure.
// It returns the expression of the GroupBy as it should be used in the parent Aggregator.
func (p *joinPusher) addGrouping(ctx *plancontext.PlanningContext, gb GroupBy) sqlparser.Expr {
	copyGB := gb
	expr := sqlparser.CloneExpr(gb.Inner)
	// copy dependencies so we can keep track of which side expressions need to be pushed to
	ctx.SemTable.CopyDependencies(gb.Inner, expr)
	// if the column exists in the selection then copy it down to the pushed aggregator operator.
	if copyGB.ColOffset != -1 {
		offset := p.useColumn(copyGB.ColOffset)
		copyGB.ColOffset = offset
	} else {
		copyGB.ColOffset = len(p.pushed.Columns)
		p.pushed.Columns = append(p.pushed.Columns, aeWrap(copyGB.Inner))
	}
	p.pushed.Grouping = append(p.pushed.Grouping, copyGB)
	return expr
}

// useColumn checks whether the column corresponding to the given offset has been used in the new Aggregator.
// If it has not been used before, it adds the column to the new Aggregator
// and updates the columns mapping to reflect the new location of the column.
// It returns the offset of the column in the new Aggregator.
func (p *joinPusher) useColumn(offset int) int {
	if p.columns[offset] == -1 {
		p.columns[offset] = len(p.pushed.Columns)
		// still haven't used this expression on this side
		p.pushed.Columns = append(p.pushed.Columns, p.orig.Columns[offset])
	}
	return p.columns[offset]
}
