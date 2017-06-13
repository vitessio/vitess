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
)

var _ Primitive = (*OrderedAggregate)(nil)

// OrderedAggregate is a primitive that expects the underlying primitive
// to feed results in an order sorted by the Keys. Rows with duplicate
// keys are aggregated using the Aggregate functions. The assumption
// is that the underlying primitive is a scatter select with pre-sorted
// rows.
type OrderedAggregate struct {
	// Results specifies where to get the value from. If the number is
	// >=0, then the result is taken from the input primitive. If it's
	// negative then the result is pulled from the aggregate function
	// at -index-1.
	Results []int

	// Aggregates specifies the aggregation parameters for each
	// aggregation function: function opcode and input column number.
	// The negative indexes of Results point to this slice.
	Aggregates []AggregateParams

	// Keys specifies the input values that must be used for
	// the aggregation key.
	Keys []AggregateKey

	// Input is the primitive that will feed into this Primitive.
	// TODO(sougou): still need to work out the sorting mechanism.
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

// AggregateKey specifies the input value to build the
// key and if the sorting on it is ascending or descending.
type AggregateKey struct {
	Col  int
	Desc bool
}

// Execute is a Primitive function.
func (oa *OrderedAggregate) Execute(vcursor VCursor, bindVars, joinVars map[string]interface{}, wantfields bool) (*sqltypes.Result, error) {
	return nil, errors.New("unimplemented")
}

// StreamExecute is a Primitive function.
func (oa *OrderedAggregate) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]interface{}, wantfields bool, callback func(*sqltypes.Result) error) error {
	return errors.New("unimplemented")
}

// GetFields is a Primitive function.
func (oa *OrderedAggregate) GetFields(vcursor VCursor, bindVars, joinVars map[string]interface{}) (*sqltypes.Result, error) {
	return nil, errors.New("unimplemented")
}
