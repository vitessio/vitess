/*
Copyright 2019 The Vitess Authors.

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
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	popcode "vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var _ logicalPlan = (*orderedAggregate)(nil)

// orderedAggregate is the logicalPlan for engine.OrderedAggregate.
// This gets built if there are aggregations on a SelectScatter
// route. The primitive requests the underlying route to order
// the results by the grouping columns. This will allow the
// engine code to aggregate the results as they come.
// For example: 'select col1, col2, count(*) from t group by col1, col2'
// will be sent to the scatter route as:
// 'select col1, col2, count(*) from t group by col1, col2 order by col1, col2`
// The orderAggregate primitive built for this will be:
//
//	&engine.OrderedAggregate {
//	  // Aggregates has one column. It computes the count
//	  // using column 2 of the underlying route.
//	  Aggregates: []AggregateParams{{
//	    Opcode: AggregateCount,
//	    Col: 2,
//	  }},
//
//	  // Keys has the two group by values for col1 and col2.
//	  // The column numbers are from the underlying route.
//	  // These values will be used to perform the grouping
//	  // of the ordered results as they come from the underlying
//	  // route.
//	  Keys: []int{0, 1},
//	  Input: (Scatter Route with the order by request),
//	}
type orderedAggregate struct {
	resultsBuilder
	extraDistinct *sqlparser.ColName

	// aggregates specifies the aggregation parameters for each
	// aggregation function: function opcode and input column number.
	aggregates []*engine.AggregateParams

	// groupByKeys specifies the input values that must be used for
	// the aggregation key.
	groupByKeys []*engine.GroupByParams

	truncateColumnCount int
}

// checkAggregates analyzes the select expression for aggregates. If it determines
// that a primitive is needed to handle the aggregation, it builds an orderedAggregate
// primitive and returns it. It returns a groupByHandler if there is aggregation it
// can handle.
func (pb *primitiveBuilder) checkAggregates(sel *sqlparser.Select) error {
	rb, isRoute := pb.plan.(*route)
	if isRoute && rb.isSingleShard() {
		// since we can push down all of the aggregation to the route,
		// we don't need to do anything else here
		return nil
	}

	// Check if we can allow aggregates.
	hasAggregates := sqlparser.ContainsAggregation(sel.SelectExprs) || len(sel.GroupBy) > 0
	if !hasAggregates && !sel.Distinct {
		return nil
	}

	// The query has aggregates. We can proceed only
	// if the underlying primitive is a route because
	// we need the ability to push down group by and
	// order by clauses.
	if !isRoute {
		if hasAggregates {
			return vterrors.VT12001("cross-shard query with aggregates")
		}
		pb.plan = newDistinctV3(pb.plan)
		return nil
	}

	// If there is a distinct clause, we can check the select list
	// to see if it has a unique vindex reference. For example,
	// if the query was 'select distinct id, col from t' (with id
	// as a unique vindex), then the distinct operation can be
	// safely pushed down because the unique vindex guarantees
	// that each id can only be in a single shard. Without the
	// unique vindex property, the id could come from multiple
	// shards, which will require us to perform the grouping
	// at the vtgate level.
	if sel.Distinct {
		for _, selectExpr := range sel.SelectExprs {
			switch selectExpr := selectExpr.(type) {
			case *sqlparser.AliasedExpr:
				vindex := pb.st.Vindex(selectExpr.Expr, rb)
				if vindex != nil && vindex.IsUnique() {
					return nil
				}
			}
		}
	}

	// The group by clause could also reference a unique vindex. The above
	// example could itself have been written as
	// 'select id, col from t group by id, col', or a query could be like
	// 'select id, count(*) from t group by id'. In the above cases,
	// the grouping can be done at the shard level, which allows the entire query
	// to be pushed down. In order to perform this analysis, we're going to look
	// ahead at the group by clause to see if it references a unique vindex.
	if pb.groupByHasUniqueVindex(sel, rb) {
		return nil
	}

	// We need an aggregator primitive.
	oa := &orderedAggregate{}
	oa.resultsBuilder = newResultsBuilder(rb, oa)
	pb.plan = oa
	pb.plan.Reorder(0)
	return nil
}

// groupbyHasUniqueVindex looks ahead at the group by expression to see if
// it references a unique vindex.
//
// The vitess group by rules are different from MySQL because it's not possible
// to match the MySQL behavior without knowing the schema. For example:
// 'select id as val from t group by val' will have different interpretations
// under MySQL depending on whether t has a val column or not.
// In vitess, we always assume that 'val' references 'id'. This is achieved
// by the symbol table resolving against the select list before searching
// the tables.
//
// In order to look ahead, we have to overcome the chicken-and-egg problem:
// group by needs the select aliases to be built. Select aliases are built
// on push-down. But push-down decision depends on whether group by expressions
// reference a vindex.
// To overcome this, the look-ahead has to perform a search that matches
// the group by analyzer. The flow is similar to oa.PushGroupBy, except that
// we don't search the ResultColumns because they're not created yet. Also,
// error conditions are treated as no match for simplicity; They will be
// subsequently caught downstream.
func (pb *primitiveBuilder) groupByHasUniqueVindex(sel *sqlparser.Select, rb *route) bool {
	for _, expr := range sel.GroupBy {
		var matchedExpr sqlparser.Expr
		switch node := expr.(type) {
		case *sqlparser.ColName:
			if expr := findAlias(node, sel.SelectExprs); expr != nil {
				matchedExpr = expr
			} else {
				matchedExpr = node
			}
		case *sqlparser.Literal:
			if node.Type != sqlparser.IntVal {
				continue
			}
			num, err := strconv.ParseInt(node.Val, 0, 64)
			if err != nil {
				continue
			}
			if num < 1 || num > int64(len(sel.SelectExprs)) {
				continue
			}
			expr, ok := sel.SelectExprs[num-1].(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			matchedExpr = expr.Expr
		default:
			continue
		}
		vindex := pb.st.Vindex(matchedExpr, rb)
		if vindex != nil && vindex.IsUnique() {
			return true
		}
	}
	return false
}

func findAlias(colname *sqlparser.ColName, selects sqlparser.SelectExprs) sqlparser.Expr {
	// Qualified column names cannot match an (unqualified) alias.
	if !colname.Qualifier.IsEmpty() {
		return nil
	}
	// See if this references an alias.
	for _, selectExpr := range selects {
		selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if colname.Name.Equal(selectExpr.As) {
			return selectExpr.Expr
		}
	}
	return nil
}

// Primitive implements the logicalPlan interface
func (oa *orderedAggregate) Primitive() engine.Primitive {
	input := oa.input.Primitive()
	if len(oa.groupByKeys) == 0 {
		return &engine.ScalarAggregate{
			Aggregates:          oa.aggregates,
			TruncateColumnCount: oa.truncateColumnCount,
			Input:               input,
		}
	}

	return &engine.OrderedAggregate{
		Aggregates:          oa.aggregates,
		GroupByKeys:         oa.groupByKeys,
		TruncateColumnCount: oa.truncateColumnCount,
		Input:               input,
	}
}

func (oa *orderedAggregate) pushAggr(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin logicalPlan) (rc *resultColumn, colNumber int, err error) {
	aggrFunc, _ := expr.Expr.(sqlparser.AggrFunc)
	origOpcode := popcode.SupportedAggregates[strings.ToLower(aggrFunc.AggrName())]
	opcode := origOpcode
	if aggrFunc.GetArgs() != nil &&
		len(aggrFunc.GetArgs()) != 1 {
		return nil, 0, vterrors.VT12001(fmt.Sprintf("only one expression is allowed inside aggregates: %s", sqlparser.String(expr)))
	}

	handleDistinct, innerAliased, err := oa.needDistinctHandling(pb, expr, opcode)
	if err != nil {
		return nil, 0, err
	}
	if handleDistinct {
		if oa.extraDistinct != nil {
			return nil, 0, vterrors.VT12001(fmt.Sprintf("only one DISTINCT aggregation allowed in a SELECT: %s", sqlparser.String(expr)))
		}
		// Push the expression that's inside the aggregate.
		// The column will eventually get added to the group by and order by clauses.
		newBuilder, _, innerCol, err := planProjection(pb, oa.input, innerAliased, origin)
		if err != nil {
			return nil, 0, err
		}
		pb.plan = newBuilder
		col, err := BuildColName(oa.input.ResultColumns(), innerCol)
		if err != nil {
			return nil, 0, err
		}
		oa.extraDistinct = col
		switch opcode {
		case popcode.AggregateCount:
			opcode = popcode.AggregateCountDistinct
		case popcode.AggregateSum:
			opcode = popcode.AggregateSumDistinct
		}
		oa.aggregates = append(oa.aggregates, &engine.AggregateParams{
			Opcode:     opcode,
			Col:        innerCol,
			Alias:      expr.ColumnName(),
			OrigOpcode: origOpcode,
		})
	} else {
		newBuilder, _, innerCol, err := planProjection(pb, oa.input, expr, origin)
		if err != nil {
			return nil, 0, err
		}
		pb.plan = newBuilder
		oa.aggregates = append(oa.aggregates, &engine.AggregateParams{
			Opcode:     opcode,
			Col:        innerCol,
			OrigOpcode: origOpcode,
		})
	}

	// Build a new rc with oa as origin because it's semantically different
	// from the expression we pushed down.
	rc = newResultColumn(expr, oa)
	oa.resultColumns = append(oa.resultColumns, rc)
	return rc, len(oa.resultColumns) - 1, nil
}

// needDistinctHandling returns true if oa needs to handle the distinct clause.
// If true, it will also return the aliased expression that needs to be pushed
// down into the underlying route.
func (oa *orderedAggregate) needDistinctHandling(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, opcode popcode.AggregateOpcode) (bool, *sqlparser.AliasedExpr, error) {
	var innerAliased *sqlparser.AliasedExpr
	aggr, ok := expr.Expr.(sqlparser.AggrFunc)

	if !ok {
		return false, nil, vterrors.VT03012(sqlparser.String(expr))
	}

	if !sqlparser.IsDistinct(aggr) {
		return false, nil, nil
	}
	if opcode != popcode.AggregateCount && opcode != popcode.AggregateSum && opcode != popcode.AggregateCountStar {
		return false, nil, nil
	}

	innerAliased = &sqlparser.AliasedExpr{Expr: aggr.GetArg()}

	rb, ok := oa.input.(*route)
	if !ok {
		// Unreachable
		return true, innerAliased, nil
	}
	vindex := pb.st.Vindex(innerAliased.Expr, rb)
	if vindex != nil && vindex.IsUnique() {
		return false, nil, nil
	}
	return true, innerAliased, nil
}

// Wireup implements the logicalPlan interface
// If text columns are detected in the keys, then the function modifies
// the primitive to pull a corresponding weight_string from mysql and
// compare those instead. This is because we currently don't have the
// ability to mimic mysql's collation behavior.
func (oa *orderedAggregate) Wireup(plan logicalPlan, jt *jointab) error {
	for i, gbk := range oa.groupByKeys {
		rc := oa.resultColumns[gbk.KeyCol]
		if sqltypes.IsText(rc.column.typ) {
			weightcolNumber, err := oa.input.SupplyWeightString(gbk.KeyCol, gbk.FromGroupBy)
			if err != nil {
				_, isUnsupportedErr := err.(UnsupportedSupplyWeightString)
				if isUnsupportedErr {
					continue
				}
				return err
			}
			oa.weightStrings[rc] = weightcolNumber
			oa.groupByKeys[i].WeightStringCol = weightcolNumber
			oa.groupByKeys[i].KeyCol = weightcolNumber
			oa.truncateColumnCount = len(oa.resultColumns)
		}
	}
	for _, key := range oa.aggregates {
		switch key.Opcode {
		case popcode.AggregateCount:
			if key.Alias == "" {
				key.Alias = key.Opcode.String()
			}
			key.Opcode = popcode.AggregateSum
		}
	}

	return oa.input.Wireup(plan, jt)
}

func (oa *orderedAggregate) WireupGen4(ctx *plancontext.PlanningContext) error {
	return oa.input.WireupGen4(ctx)
}

// OutputColumns implements the logicalPlan interface
func (oa *orderedAggregate) OutputColumns() []sqlparser.SelectExpr {
	outputCols := sqlparser.CloneSelectExprs(oa.input.OutputColumns())
	for _, aggr := range oa.aggregates {
		outputCols[aggr.Col] = &sqlparser.AliasedExpr{Expr: aggr.Expr, As: sqlparser.NewIdentifierCI(aggr.Alias)}
	}
	if oa.truncateColumnCount > 0 {
		return outputCols[:oa.truncateColumnCount]
	}
	return outputCols
}

// SetTruncateColumnCount sets the truncate column count.
func (oa *orderedAggregate) SetTruncateColumnCount(count int) {
	oa.truncateColumnCount = count
}
