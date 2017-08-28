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

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
	Aggregates []AggregateParams

	// Keys specifies the input values that must be used for
	// the aggregation key.
	Keys []int

	// Input is the primitive that will feed into this Primitive.
	Input Primitive
}

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode AggregateOpcode
	Col    int
}

// AggregateOpcode is the aggregation Opcode.
type AggregateOpcode int

// These constants list the possible aggregate opcodes.
const (
	AggregateCount = AggregateOpcode(iota)
	AggregateSum
	AggregateMin
	AggregateMax
)

// SupportedAggregates maps the list of supported aggregate
// functions to their opcodes.
var SupportedAggregates = map[string]AggregateOpcode{
	"count": AggregateCount,
	"sum":   AggregateSum,
	"min":   AggregateMin,
	"max":   AggregateMax,
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

// Execute is a Primitive function.
func (oa *OrderedAggregate) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := oa.Input.Execute(vcursor, bindVars, joinVars, wantfields)
	if err != nil {
		return nil, err
	}
	out := &sqltypes.Result{
		Fields: result.Fields,
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
		Extras: result.Extras,
	}
	// This code is similar to the one in StreamExecute.
	var current []sqltypes.Value
	for _, row := range result.Rows {
		if current == nil {
			current = row
			continue
		}

		equal, err := oa.keysEqual(current, row)
		if err != nil {
			return nil, err
		}

		if equal {
			current, err = oa.merge(result.Fields, current, row)
			if err != nil {
				return nil, err
			}
			continue
		}
		out.Rows = append(out.Rows, current)
		current = row
	}
	if current != nil {
		out.Rows = append(out.Rows, current)
	}
	out.RowsAffected = uint64(len(out.Rows))
	return out, nil
}

// StreamExecute is a Primitive function.
func (oa *OrderedAggregate) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var current []sqltypes.Value
	var fields []*querypb.Field
	err := oa.Input.StreamExecute(vcursor, bindVars, joinVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			fields = qr.Fields
			if err := callback(&sqltypes.Result{Fields: qr.Fields}); err != nil {
				return err
			}
		}
		// This code is similar to the one in Execute.
		for _, row := range qr.Rows {
			if current == nil {
				current = row
				continue
			}

			equal, err := oa.keysEqual(current, row)
			if err != nil {
				return err
			}

			if equal {
				current, err = oa.merge(fields, current, row)
				if err != nil {
					return err
				}
				continue
			}
			if err := callback(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
				return err
			}
			current = row
		}
		return nil
	})
	if err != nil {
		return err
	}

	if current != nil {
		if err := callback(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
			return err
		}
	}
	return nil
}

// GetFields is a Primitive function.
func (oa *OrderedAggregate) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return oa.Input.GetFields(vcursor, bindVars, joinVars)
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

func (oa *OrderedAggregate) merge(fields []*querypb.Field, row1, row2 []sqltypes.Value) ([]sqltypes.Value, error) {
	result := sqltypes.CopyRow(row1)
	for _, aggr := range oa.Aggregates {
		var err error
		switch aggr.Opcode {
		case AggregateCount, AggregateSum:
			result[aggr.Col], err = sqltypes.NullsafeAdd(row1[aggr.Col], row2[aggr.Col], fields[aggr.Col].Type)
		case AggregateMin:
			result[aggr.Col], err = sqltypes.Min(row1[aggr.Col], row2[aggr.Col])
		case AggregateMax:
			result[aggr.Col], err = sqltypes.Max(row1[aggr.Col], row2[aggr.Col])
		default:
			return nil, fmt.Errorf("BUG: Unexpected opcode: %v", aggr.Opcode)
		}
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
