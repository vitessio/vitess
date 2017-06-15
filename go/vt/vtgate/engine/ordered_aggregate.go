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
	"errors"
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
func (oa *OrderedAggregate) Execute(vcursor VCursor, bindVars, joinVars map[string]interface{}, wantfields bool) (*sqltypes.Result, error) {
	result, err := oa.Input.Execute(vcursor, bindVars, joinVars, wantfields)
	if err != nil {
		return nil, err
	}
	out := &sqltypes.Result{
		Fields: result.Fields,
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
		Extras: result.Extras,
	}
	curRow := 0
	if err := oa.aggregate(
		result.Fields,
		func() []sqltypes.Value {
			if curRow >= len(result.Rows) {
				return nil
			}
			curRow++
			return result.Rows[curRow-1]
		},
		func(row []sqltypes.Value) {
			out.Rows = append(out.Rows, row)
		},
	); err != nil {
		return nil, err
	}
	out.RowsAffected = uint64(len(out.Rows))
	return out, nil
}

// StreamExecute is a Primitive function.
func (oa *OrderedAggregate) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]interface{}, wantfields bool, callback func(*sqltypes.Result) error) error {
	return errors.New("unimplemented")
}

// GetFields is a Primitive function.
func (oa *OrderedAggregate) GetFields(vcursor VCursor, bindVars, joinVars map[string]interface{}) (*sqltypes.Result, error) {
	return oa.Input.GetFields(vcursor, bindVars, joinVars)
}

func (oa *OrderedAggregate) aggregate(fields []*querypb.Field, nextRow func() []sqltypes.Value, emit func([]sqltypes.Value)) error {
	current := nextRow()
	if current == nil {
		return nil
	}
	for {
		next := nextRow()
		if next == nil {
			emit(current)
			return nil
		}

		equal, err := oa.keysEqual(current, next)
		if err != nil {
			return err
		}

		if equal {
			if err := oa.merge(fields, current, next); err != nil {
				return err
			}
			next = nil
		} else {
			emit(current)
			current = next
		}
	}
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

func (oa *OrderedAggregate) merge(fields []*querypb.Field, row1, row2 []sqltypes.Value) error {
	for _, aggr := range oa.Aggregates {
		switch aggr.Opcode {
		case AggregateCount, AggregateSum:
			var err error
			row1[aggr.Col], err = sqltypes.Add(row1[aggr.Col], row2[aggr.Col], fields[aggr.Col].Type)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
