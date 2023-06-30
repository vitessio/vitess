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

var (
	// Some predefined values
	countZero = sqltypes.MakeTrusted(sqltypes.Int64, []byte("0"))
	countOne  = sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))
	sumZero   = sqltypes.MakeTrusted(sqltypes.Decimal, []byte("0"))
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
}

// GroupByParams specify the grouping key to be used.
type GroupByParams struct {
	KeyCol          int
	WeightStringCol int
	Expr            sqlparser.Expr
	FromGroupBy     bool
	CollationID     collations.ID
}

// String returns a string. Used for plan descriptions
func (gbp GroupByParams) String() string {
	var out string
	if gbp.WeightStringCol == -1 || gbp.KeyCol == gbp.WeightStringCol {
		out = strconv.Itoa(gbp.KeyCol)
	} else {
		out = fmt.Sprintf("(%d|%d)", gbp.KeyCol, gbp.WeightStringCol)
	}

	if gbp.CollationID != collations.Unknown {
		collation := gbp.CollationID.Get()
		out += " COLLATE " + collation.Name()
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
	fields := convertFields(result.Fields, oa.Aggregates)
	out := &sqltypes.Result{
		Fields: fields,
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
	}

	// This code is similar to the one in StreamExecute.
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	for _, row := range result.Rows {
		// this is the first row. set up everything
		if current == nil {
			current, curDistincts = convertRow(fields, row, oa.Aggregates)
			continue
		}

		// not the first row. are we still in the old group, or is this a new grouping?=
		equal, err := oa.keysEqual(current, row)
		if err != nil {
			return nil, err
		}

		if equal {
			// we are continuing to add values to the current grouping
			current, curDistincts, err = merge(fields, current, row, curDistincts, oa.Aggregates)
			if err != nil {
				return nil, err
			}
			continue
		}

		// this is a new grouping. let's yield the old one, and start a new
		out.Rows = append(out.Rows, current)
		current, curDistincts = convertRow(fields, row, oa.Aggregates)
		continue
	}

	if current != nil {
		final, err := convertFinal(current, oa.Aggregates)
		if err != nil {
			return nil, err
		}
		out.Rows = append(out.Rows, final)
	}
	return out, nil
}

// TryStreamExecute is a Primitive function.
func (oa *OrderedAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	var fields []*querypb.Field

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(oa.TruncateColumnCount))
	}

	visitor := func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			fields = convertFields(qr.Fields, oa.Aggregates)
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}
		// This code is similar to the one in Execute.
		for _, row := range qr.Rows {
			// this is the first row. set up everything
			if current == nil {
				current, curDistincts = convertRow(fields, row, oa.Aggregates)
				continue
			}

			// not the first row. are we still in the old group, or is this a new grouping?
			equal, err := oa.keysEqual(current, row)
			if err != nil {
				return err
			}

			if equal {
				// we are continuing to add values to the current grouping
				current, curDistincts, err = merge(fields, current, row, curDistincts, oa.Aggregates)
				if err != nil {
					return err
				}
				continue
			}

			// this is a new grouping. let's yield the old one, and start a new
			if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
				return err
			}
			current, curDistincts = convertRow(fields, row, oa.Aggregates)
			continue
		}
		return nil
	}

	err := vcursor.StreamExecutePrimitive(ctx,
		oa.Input,
		bindVars,
		true, /* we need the input fields types to correctly calculate the output types */
		visitor)
	if err != nil {
		return err
	}

	if current != nil {
		if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
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
	qr = &sqltypes.Result{Fields: convertFields(qr.Fields, oa.Aggregates)}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

// Inputs returns the Primitive input for this aggregation
func (oa *OrderedAggregate) Inputs() []Primitive {
	return []Primitive{oa.Input}
}

// NeedsTransaction implements the Primitive interface
func (oa *OrderedAggregate) NeedsTransaction() bool {
	return oa.Input.NeedsTransaction()
}

func (oa *OrderedAggregate) keysEqual(row1, row2 []sqltypes.Value) (bool, error) {
	for _, gb := range oa.GroupByKeys {
		cmp, err := evalengine.NullsafeCompare(row1[gb.KeyCol], row2[gb.KeyCol], gb.CollationID)
		if err != nil {
			_, isComparisonErr := err.(evalengine.UnsupportedComparisonError)
			_, isCollationErr := err.(evalengine.UnsupportedCollationError)
			if !isComparisonErr && !isCollationErr || gb.WeightStringCol == -1 {
				return false, err
			}
			gb.KeyCol = gb.WeightStringCol
			cmp, err = evalengine.NullsafeCompare(row1[gb.WeightStringCol], row2[gb.WeightStringCol], gb.CollationID)
			if err != nil {
				return false, err
			}
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
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
