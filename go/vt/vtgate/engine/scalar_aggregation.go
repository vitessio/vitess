/*
Copyright 2022 The Vitess Authors.

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
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*ScalarAggregate)(nil)

// ScalarAggregate is a primitive used to do aggregations without grouping keys
type ScalarAggregate struct {
	// Aggregates specifies the aggregation parameters for each
	// aggregation function: function opcode and input column number.
	Aggregates []*AggregateParams

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`

	// Input is the primitive that will feed into this Primitive.
	Input Primitive
}

// RouteType implements the Primitive interface
func (sa *ScalarAggregate) RouteType() string {
	return sa.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (sa *ScalarAggregate) GetKeyspaceName() string {
	return sa.Input.GetKeyspaceName()

}

// GetTableName implements the Primitive interface
func (sa *ScalarAggregate) GetTableName() string {
	return sa.Input.GetTableName()
}

// GetFields implements the Primitive interface
func (sa *ScalarAggregate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := sa.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	_, fields, err := newAggregation(qr.Fields, sa.Aggregates)
	if err != nil {
		return nil, err
	}

	qr = &sqltypes.Result{Fields: fields}
	return qr.Truncate(sa.TruncateColumnCount), nil
}

// NeedsTransaction implements the Primitive interface
func (sa *ScalarAggregate) NeedsTransaction() bool {
	return sa.Input.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (sa *ScalarAggregate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(ctx, sa.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	agg, fields, err := newAggregation(result.Fields, sa.Aggregates)
	if err != nil {
		return nil, err
	}

	for _, row := range result.Rows {
		if err := agg.add(row); err != nil {
			return nil, err
		}
	}

	out := &sqltypes.Result{
		Fields: fields,
		Rows:   [][]sqltypes.Value{agg.finish()},
	}
	return out.Truncate(sa.TruncateColumnCount), nil
}

// TryStreamExecute implements the Primitive interface
func (sa *ScalarAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(sa.TruncateColumnCount))
	}

	var mu sync.Mutex
	var agg aggregationState
	var fields []*querypb.Field
	fieldsSent := !wantfields

	err := vcursor.StreamExecutePrimitive(ctx, sa.Input, bindVars, wantfields, func(result *sqltypes.Result) error {
		// as the underlying primitive call is not sync
		// and here scalar aggregate is using shared variables we have to sync the callback
		// for correct aggregation.
		mu.Lock()
		defer mu.Unlock()

		if agg == nil && len(result.Fields) != 0 {
			var err error
			agg, fields, err = newAggregation(result.Fields, sa.Aggregates)
			if err != nil {
				return err
			}
		}
		if !fieldsSent {
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
			fieldsSent = true
		}

		for _, row := range result.Rows {
			if err := agg.add(row); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return cb(&sqltypes.Result{Rows: [][]sqltypes.Value{agg.finish()}})
}

// Inputs implements the Primitive interface
func (sa *ScalarAggregate) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{sa.Input}, nil
}

// description implements the Primitive interface
func (sa *ScalarAggregate) description() PrimitiveDescription {
	aggregates := GenericJoin(sa.Aggregates, aggregateParamsToString)
	other := map[string]any{
		"Aggregates": aggregates,
	}
	if sa.TruncateColumnCount > 0 {
		other["ResultColumns"] = sa.TruncateColumnCount
	}
	return PrimitiveDescription{
		OperatorType: "Aggregate",
		Variant:      "Scalar",
		Other:        other,
	}

}
