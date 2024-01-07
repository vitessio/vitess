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

package engine

import (
	"context"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*OrderedAggregate)(nil)

// OrderedAggregate is a primitive that expects the underlying primitive
// to feed results in an order sorted by the Keys. Rows with duplicate
// keys are aggregated using the Aggregate functions. The assumption
// is that the underlying primitive is a scatter select with pre-sorted
// rows.
type OrderedAggregate struct {
	// Aggregates specifies the aggregation parameters for each
	// aggregation function: function opcode and input column number.
	Aggregates []*AggregateParams

	// GroupByKeys specifies the input values that must be used for
	// the aggregation key.
	GroupByKeys []*GroupByParams

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`

	// Input is the primitive that will feed into this Primitive.
	Input Primitive

	CollationEnv *collations.Environment
}

// GroupByParams specify the grouping key to be used.
type GroupByParams struct {
	KeyCol          int
	WeightStringCol int
	Expr            sqlparser.Expr
	FromGroupBy     bool
	Type            evalengine.Type
	CollationEnv    *collations.Environment
}

// String returns a string. Used for plan descriptions
func (gbp GroupByParams) String() string {
	var out string
	if gbp.WeightStringCol == -1 || gbp.KeyCol == gbp.WeightStringCol {
		out = strconv.Itoa(gbp.KeyCol)
	} else {
		out = fmt.Sprintf("(%d|%d)", gbp.KeyCol, gbp.WeightStringCol)
	}

	if sqltypes.IsText(gbp.Type.Type()) && gbp.Type.Collation() != collations.Unknown {
		out += " COLLATE " + gbp.CollationEnv.LookupName(gbp.Type.Collation())
	}

	return out
}

// RouteType returns a description of the query routing type used by the primitive
func (oa *OrderedAggregate) RouteType() string {
	return oa.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (oa *OrderedAggregate) GetKeyspaceName() string {
	return oa.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (oa *OrderedAggregate) GetTableName() string {
	return oa.Input.GetTableName()
}

// SetTruncateColumnCount sets the truncate column count.
func (oa *OrderedAggregate) SetTruncateColumnCount(count int) {
	oa.TruncateColumnCount = count
}

// TryExecute is a Primitive function.
func (oa *OrderedAggregate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	qr, err := oa.execute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

func (oa *OrderedAggregate) executeGroupBy(result *sqltypes.Result) (*sqltypes.Result, error) {
	if len(result.Rows) < 1 {
		return result, nil
	}

	out := &sqltypes.Result{
		Fields: result.Fields,
		Rows:   result.Rows[:0],
	}

	var currentKey []sqltypes.Value
	var lastRow sqltypes.Row
	var err error
	for _, row := range result.Rows {
		var nextGroup bool

		currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
		if err != nil {
			return nil, err
		}
		if nextGroup {
			out.Rows = append(out.Rows, lastRow)
		}
		lastRow = row
	}
	out.Rows = append(out.Rows, lastRow)
	return out, nil
}

func (oa *OrderedAggregate) execute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(
		ctx,
		oa.Input,
		bindVars,
		true, /*wantFields - we need the input fields types to correctly calculate the output types*/
	)
	if err != nil {
		return nil, err
	}
	if len(oa.Aggregates) == 0 {
		return oa.executeGroupBy(result)
	}

	agg, fields, err := newAggregation(result.Fields, oa.Aggregates)
	if err != nil {
		return nil, err
	}

	out := &sqltypes.Result{
		Fields: fields,
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
	}

	var currentKey []sqltypes.Value
	for _, row := range result.Rows {
		var nextGroup bool

		currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
		if err != nil {
			return nil, err
		}

		if nextGroup {
			out.Rows = append(out.Rows, agg.finish())
			agg.reset()
		}

		if err := agg.add(row); err != nil {
			return nil, err
		}
	}

	if currentKey != nil {
		out.Rows = append(out.Rows, agg.finish())
	}

	return out, nil
}

func (oa *OrderedAggregate) executeStreamGroupBy(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(oa.TruncateColumnCount))
	}

	var fields []*querypb.Field
	var currentKey []sqltypes.Value
	var lastRow sqltypes.Row

	visitor := func(qr *sqltypes.Result) error {
		var err error
		if fields == nil && len(qr.Fields) > 0 {
			fields = qr.Fields
			if err = cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}
		for _, row := range qr.Rows {
			var nextGroup bool

			currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
			if err != nil {
				return err
			}

			if nextGroup {
				// this is a new grouping. let's yield the old one, and start a new
				if err := cb(&sqltypes.Result{Rows: []sqltypes.Row{lastRow}}); err != nil {
					return err
				}
			}

			lastRow = row
		}
		return nil
	}

	/* we need the input fields types to correctly calculate the output types */
	err := vcursor.StreamExecutePrimitive(ctx, oa.Input, bindVars, true, visitor)
	if err != nil {
		return err
	}

	if lastRow != nil {
		if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{lastRow}}); err != nil {
			return err
		}
	}
	return nil
}

// TryStreamExecute is a Primitive function.
func (oa *OrderedAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	if len(oa.Aggregates) == 0 {
		return oa.executeStreamGroupBy(ctx, vcursor, bindVars, callback)
	}

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(oa.TruncateColumnCount))
	}

	var agg aggregationState
	var fields []*querypb.Field
	var currentKey []sqltypes.Value

	visitor := func(qr *sqltypes.Result) error {
		var err error

		if agg == nil && len(qr.Fields) != 0 {
			agg, fields, err = newAggregation(qr.Fields, oa.Aggregates)
			if err != nil {
				return err
			}
			if err = cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}

		// This code is similar to the one in Execute.
		for _, row := range qr.Rows {
			var nextGroup bool

			currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
			if err != nil {
				return err
			}

			if nextGroup {
				// this is a new grouping. let's yield the old one, and start a new
				if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{agg.finish()}}); err != nil {
					return err
				}

				agg.reset()
			}

			if err := agg.add(row); err != nil {
				return err
			}
		}
		return nil
	}

	/* we need the input fields types to correctly calculate the output types */
	err := vcursor.StreamExecutePrimitive(ctx, oa.Input, bindVars, true, visitor)
	if err != nil {
		return err
	}

	if currentKey != nil {
		if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{agg.finish()}}); err != nil {
			return err
		}
	}
	return nil
}

// GetFields is a Primitive function.
func (oa *OrderedAggregate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := oa.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	_, fields, err := newAggregation(qr.Fields, oa.Aggregates)
	if err != nil {
		return nil, err
	}

	qr = &sqltypes.Result{Fields: fields}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

// Inputs returns the Primitive input for this aggregation
func (oa *OrderedAggregate) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{oa.Input}, nil
}

// NeedsTransaction implements the Primitive interface
func (oa *OrderedAggregate) NeedsTransaction() bool {
	return oa.Input.NeedsTransaction()
}

func (oa *OrderedAggregate) nextGroupBy(currentKey, nextRow []sqltypes.Value) (nextKey []sqltypes.Value, nextGroup bool, err error) {
	if currentKey == nil {
		return nextRow, false, nil
	}

	for _, gb := range oa.GroupByKeys {
		v1 := currentKey[gb.KeyCol]
		v2 := nextRow[gb.KeyCol]
		if v1.TinyWeightCmp(v2) != 0 {
			return nextRow, true, nil
		}

		cmp, err := evalengine.NullsafeCompare(v1, v2, oa.CollationEnv, gb.Type.Collation())
		if err != nil {
			_, isCollationErr := err.(evalengine.UnsupportedCollationError)
			if !isCollationErr || gb.WeightStringCol == -1 {
				return nil, false, err
			}
			gb.KeyCol = gb.WeightStringCol
			cmp, err = evalengine.NullsafeCompare(currentKey[gb.WeightStringCol], nextRow[gb.WeightStringCol], oa.CollationEnv, gb.Type.Collation())
			if err != nil {
				return nil, false, err
			}
		}
		if cmp != 0 {
			return nextRow, true, nil
		}
	}
	return currentKey, false, nil
}
func aggregateParamsToString(in any) string {
	return in.(*AggregateParams).String()
}

func groupByParamsToString(i any) string {
	return i.(*GroupByParams).String()
}

func (oa *OrderedAggregate) description() PrimitiveDescription {
	aggregates := GenericJoin(oa.Aggregates, aggregateParamsToString)
	groupBy := GenericJoin(oa.GroupByKeys, groupByParamsToString)
	other := map[string]any{
		"Aggregates": aggregates,
		"GroupBy":    groupBy,
	}
	if oa.TruncateColumnCount > 0 {
		other["ResultColumns"] = oa.TruncateColumnCount
	}
	return PrimitiveDescription{
		OperatorType: "Aggregate",
		Variant:      "Ordered",
		Other:        other,
	}
}
