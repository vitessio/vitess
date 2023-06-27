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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

var _ Primitive = (*ScalarAggregate)(nil)

// ScalarAggregate is a primitive used to do aggregations without grouping keys
type ScalarAggregate struct {
	// PreProcess is true if one of the aggregates needs preprocessing.
	PreProcess bool `json:",omitempty"`

	AggrOnEngine bool

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
	qr = &sqltypes.Result{Fields: convertFields(qr.Fields, sa.PreProcess, sa.Aggregates, sa.AggrOnEngine)}
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
	fields := convertFields(result.Fields, sa.PreProcess, sa.Aggregates, sa.AggrOnEngine)
	out := &sqltypes.Result{
		Fields: fields,
	}

	var resultRow []sqltypes.Value
	var curDistincts []sqltypes.Value
	for _, row := range result.Rows {
		if resultRow == nil {
			resultRow, curDistincts = convertRow(fields, row, sa.PreProcess, sa.Aggregates, sa.AggrOnEngine)
			continue
		}
		resultRow, curDistincts, err = merge(result.Fields, resultRow, row, curDistincts, sa.Aggregates)
		if err != nil {
			return nil, err
		}
	}

	if resultRow == nil {
		// When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
		// different aggregation functions
		resultRow, err = sa.createEmptyRow()
		if err != nil {
			return nil, err
		}
	} else {
		resultRow, err = convertFinal(resultRow, sa.Aggregates)
		if err != nil {
			return nil, err
		}
	}

	out.Rows = [][]sqltypes.Value{resultRow}
	return out.Truncate(sa.TruncateColumnCount), nil
}

// TryStreamExecute implements the Primitive interface
func (sa *ScalarAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(sa.TruncateColumnCount))
	}
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	var fields []*querypb.Field
	fieldsSent := false
	var mu sync.Mutex

	err := vcursor.StreamExecutePrimitive(ctx, sa.Input, bindVars, wantfields, func(result *sqltypes.Result) error {
		// as the underlying primitive call is not sync
		// and here scalar aggregate is using shared variables we have to sync the callback
		// for correct aggregation.
		mu.Lock()
		defer mu.Unlock()
		if len(result.Fields) != 0 && !fieldsSent {
			fields = convertFields(result.Fields, sa.PreProcess, sa.Aggregates, sa.AggrOnEngine)
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
			fieldsSent = true
		}

		// this code is very similar to the TryExecute method
		for _, row := range result.Rows {
			if current == nil {
				current, curDistincts = convertRow(fields, row, sa.PreProcess, sa.Aggregates, sa.AggrOnEngine)
				continue
			}
			var err error
			current, curDistincts, err = merge(fields, current, row, curDistincts, sa.Aggregates)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if current == nil {
		// When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
		// different aggregation functions
		current, err = sa.createEmptyRow()
		if err != nil {
			return err
		}
	} else {
		current, err = convertFinal(current, sa.Aggregates)
		if err != nil {
			return err
		}
	}

	return cb(&sqltypes.Result{Rows: [][]sqltypes.Value{current}})
}

// creates the empty row for the case when we are missing grouping keys and have empty input table
func (sa *ScalarAggregate) createEmptyRow() ([]sqltypes.Value, error) {
	out := make([]sqltypes.Value, len(sa.Aggregates))
	for i, aggr := range sa.Aggregates {
		op := aggr.Opcode
		if aggr.OrigOpcode != AggregateUnassigned {
			op = aggr.OrigOpcode
		}
		value, err := createEmptyValueFor(op)
		if err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}

func createEmptyValueFor(opcode AggregateOpcode) (sqltypes.Value, error) {
	switch opcode {
	case
		AggregateCountDistinct,
		AggregateCount,
		AggregateCountStar:
		return countZero, nil
	case
		AggregateSumDistinct,
		AggregateSum,
		AggregateMin,
		AggregateMax,
		AggregateRandom,
		AggregateGroupConcat:
		return sqltypes.NULL, nil

	}
	return sqltypes.NULL, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unknown aggregation %v", opcode)
}

// Inputs implements the Primitive interface
func (sa *ScalarAggregate) Inputs() []Primitive {
	return []Primitive{sa.Input}
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
