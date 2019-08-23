/*
Copyright 2017 Google Inc.

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
	"fmt"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*OrderedAggregate)(nil)

// OrderedAggregate is a primitive that expects the underlying primitive
// to feed results in an order sorted by the Keys. Rows with duplicate
// keys are aggregated using the Aggregate functions. The assumption
// is that the underlying primitive is a scatter select with pre-sorted
// rows.
type OrderedAggregate struct {
	// HasDistinct is true if one of the aggregates is distinct.
	HasDistinct bool `json:",omitempty"`
	// Aggregates specifies the aggregation parameters for each
	// aggregation function: function opcode and input column number.
	Aggregates []AggregateParams

	// Keys specifies the input values that must be used for
	// the aggregation key.
	Keys []int

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`

	// Input is the primitive that will feed into this Primitive.
	Input Primitive
}

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode AggregateOpcode
	Col    int
	// Alias is set only for distinct opcodes.
	Alias string `json:",omitempty"`
}

func (ap AggregateParams) isDistinct() bool {
	return ap.Opcode == AggregateCountDistinct || ap.Opcode == AggregateSumDistinct
}

// AggregateOpcode is the aggregation Opcode.
type AggregateOpcode int

// These constants list the possible aggregate opcodes.
const (
	AggregateCount = AggregateOpcode(iota)
	AggregateSum
	AggregateMin
	AggregateMax
	AggregateCountDistinct
	AggregateSumDistinct
)

var (
	opcodeType = map[AggregateOpcode]querypb.Type{
		AggregateCountDistinct: sqltypes.Int64,
		AggregateSumDistinct:   sqltypes.Decimal,
	}
	// Some predefined values
	countZero = sqltypes.MakeTrusted(sqltypes.Int64, []byte("0"))
	countOne  = sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))
	sumZero   = sqltypes.MakeTrusted(sqltypes.Decimal, []byte("0"))
)

// SupportedAggregates maps the list of supported aggregate
// functions to their opcodes.
var SupportedAggregates = map[string]AggregateOpcode{
	"count": AggregateCount,
	"sum":   AggregateSum,
	"min":   AggregateMin,
	"max":   AggregateMax,
	// These functions don't exist in mysql, but are used
	// to display the plan.
	"count_distinct": AggregateCountDistinct,
	"sum_distinct":   AggregateSumDistinct,
}

func (code AggregateOpcode) String() string {
	for k, v := range SupportedAggregates {
		if v == code {
			return k
		}
	}
	panic("unreachable")
}

// MarshalJSON serializes the AggregateOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code AggregateOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
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

// Execute is a Primitive function.
func (oa *OrderedAggregate) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := oa.execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

func (oa *OrderedAggregate) execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := oa.Input.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	out := &sqltypes.Result{
		Fields: oa.convertFields(result.Fields),
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
		Extras: result.Extras,
	}
	// This code is similar to the one in StreamExecute.
	var current []sqltypes.Value
	var curDistinct sqltypes.Value
	for _, row := range result.Rows {
		if current == nil {
			current, curDistinct = oa.convertRow(row)
			continue
		}

		equal, err := oa.keysEqual(current, row)
		if err != nil {
			return nil, err
		}

		if equal {
			current, curDistinct, err = oa.merge(result.Fields, current, row, curDistinct)
			if err != nil {
				return nil, err
			}
			continue
		}
		out.Rows = append(out.Rows, current)
		current, curDistinct = oa.convertRow(row)
	}

	if len(result.Rows) == 0 && len(oa.Keys) == 0 {
		// When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
		// different aggregation functions
		row, err := oa.createEmptyRow()
		if err != nil {
			return nil, err
		}
		out.Rows = append(out.Rows, row)
	}

	if current != nil {
		out.Rows = append(out.Rows, current)
	}
	out.RowsAffected = uint64(len(out.Rows))
	return out, nil
}

// StreamExecute is a Primitive function.
func (oa *OrderedAggregate) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var current []sqltypes.Value
	var curDistinct sqltypes.Value
	var fields []*querypb.Field

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(oa.TruncateColumnCount))
	}

	err := oa.Input.StreamExecute(vcursor, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			fields = oa.convertFields(qr.Fields)
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}
		// This code is similar to the one in Execute.
		for _, row := range qr.Rows {
			if current == nil {
				current, curDistinct = oa.convertRow(row)
				continue
			}

			equal, err := oa.keysEqual(current, row)
			if err != nil {
				return err
			}

			if equal {
				current, curDistinct, err = oa.merge(fields, current, row, curDistinct)
				if err != nil {
					return err
				}
				continue
			}
			if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
				return err
			}
			current, curDistinct = oa.convertRow(row)
		}
		return nil
	})
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

func (oa *OrderedAggregate) convertFields(fields []*querypb.Field) (newFields []*querypb.Field) {
	if !oa.HasDistinct {
		return fields
	}
	newFields = append(newFields, fields...)
	for _, aggr := range oa.Aggregates {
		if !aggr.isDistinct() {
			continue
		}
		newFields[aggr.Col] = &querypb.Field{
			Name: aggr.Alias,
			Type: opcodeType[aggr.Opcode],
		}
	}
	return newFields
}

func (oa *OrderedAggregate) convertRow(row []sqltypes.Value) (newRow []sqltypes.Value, curDistinct sqltypes.Value) {
	if !oa.HasDistinct {
		return row, sqltypes.NULL
	}
	newRow = append(newRow, row...)
	for _, aggr := range oa.Aggregates {
		switch aggr.Opcode {
		case AggregateCountDistinct:
			curDistinct = row[aggr.Col]
			// Type is int64. Ok to call MakeTrusted.
			if row[aggr.Col].IsNull() {
				newRow[aggr.Col] = countZero
			} else {
				newRow[aggr.Col] = countOne
			}
		case AggregateSumDistinct:
			curDistinct = row[aggr.Col]
			var err error
			newRow[aggr.Col], err = sqltypes.Cast(row[aggr.Col], opcodeType[aggr.Opcode])
			if err != nil {
				newRow[aggr.Col] = sumZero
			}
		}
	}
	return newRow, curDistinct
}

// GetFields is a Primitive function.
func (oa *OrderedAggregate) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := oa.Input.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	qr = &sqltypes.Result{Fields: oa.convertFields(qr.Fields)}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

func (oa *OrderedAggregate) keysEqual(row1, row2 []sqltypes.Value) (bool, error) {
	for _, key := range oa.Keys {
		cmp, err := sqltypes.NullsafeCompare(row1[key], row2[key])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (oa *OrderedAggregate) merge(fields []*querypb.Field, row1, row2 []sqltypes.Value, curDistinct sqltypes.Value) ([]sqltypes.Value, sqltypes.Value, error) {
	result := sqltypes.CopyRow(row1)
	for _, aggr := range oa.Aggregates {
		if aggr.isDistinct() {
			if row2[aggr.Col].IsNull() {
				continue
			}
			cmp, err := sqltypes.NullsafeCompare(curDistinct, row2[aggr.Col])
			if err != nil {
				return nil, sqltypes.NULL, err
			}
			if cmp == 0 {
				continue
			}
			curDistinct = row2[aggr.Col]
		}
		var err error
		switch aggr.Opcode {
		case AggregateCount, AggregateSum:
			result[aggr.Col], err = sqltypes.NullsafeAdd(row1[aggr.Col], row2[aggr.Col], fields[aggr.Col].Type)
		case AggregateMin:
			result[aggr.Col], err = sqltypes.Min(row1[aggr.Col], row2[aggr.Col])
		case AggregateMax:
			result[aggr.Col], err = sqltypes.Max(row1[aggr.Col], row2[aggr.Col])
		case AggregateCountDistinct:
			result[aggr.Col], err = sqltypes.NullsafeAdd(row1[aggr.Col], countOne, opcodeType[aggr.Opcode])
		case AggregateSumDistinct:
			result[aggr.Col], err = sqltypes.NullsafeAdd(row1[aggr.Col], row2[aggr.Col], opcodeType[aggr.Opcode])
		default:
			return nil, sqltypes.NULL, fmt.Errorf("BUG: Unexpected opcode: %v", aggr.Opcode)
		}
		if err != nil {
			return nil, sqltypes.NULL, err
		}
	}
	return result, curDistinct, nil
}

// creates the empty row for the case when we are missing grouping keys and have empty input table
func (oa *OrderedAggregate) createEmptyRow() ([]sqltypes.Value, error) {
	out := make([]sqltypes.Value, len(oa.Aggregates))
	for i, aggr := range oa.Aggregates {
		value, err := createEmptyValueFor(aggr.Opcode)
		if err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}

func createEmptyValueFor(opcode AggregateOpcode) (sqltypes.Value, error) {
	switch opcode {
	case AggregateCountDistinct:
		return countZero, nil
	case AggregateSumDistinct:
		return sqltypes.NULL, nil
	}
	return sqltypes.NULL, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unknown aggregation %v", opcode)
}
