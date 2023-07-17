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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
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

func (oa *orderedAggregate) Wireup(ctx *plancontext.PlanningContext) error {
	return oa.input.Wireup(ctx)
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
