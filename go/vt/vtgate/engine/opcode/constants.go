/*
Copyright 2023 The Vitess Authors.

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

package opcode

import (
	"fmt"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// PulloutOpcode is a number representing the opcode
// for the PulloutSubquery primitive.
type PulloutOpcode int

// This is the list of PulloutOpcode values.
const (
	PulloutValue = PulloutOpcode(iota)
	PulloutIn
	PulloutNotIn
	PulloutExists
)

var pulloutName = map[PulloutOpcode]string{
	PulloutValue:  "PulloutValue",
	PulloutIn:     "PulloutIn",
	PulloutNotIn:  "PulloutNotIn",
	PulloutExists: "PulloutExists",
}

func (code PulloutOpcode) String() string {
	return pulloutName[code]
}

// MarshalJSON serializes the PulloutOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code PulloutOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}

// AggregateOpcode is the aggregation Opcode.
type AggregateOpcode int

// These constants list the possible aggregate opcodes.
const (
	AggregateUnassigned = AggregateOpcode(iota)
	AggregateCount
	AggregateSum
	AggregateMin
	AggregateMax
	AggregateCountDistinct
	AggregateSumDistinct
	AggregateGtid
	AggregateRandom
	AggregateCountStar
	AggregateGroupConcat
	_NumOfOpCodes // This line must be last of the opcodes!
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
	"vgtid":          AggregateGtid,
	"count_star":     AggregateCountStar,
	"random":         AggregateRandom,
	"group_concat":   AggregateGroupConcat,
}

func (code AggregateOpcode) String() string {
	for k, v := range SupportedAggregates {
		if v == code {
			return k
		}
	}
	return "ERROR"
}

// MarshalJSON serializes the AggregateOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code AggregateOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}

// Type returns the opcode return sql type, and a bool telling is we are sure about this type or not
func (code AggregateOpcode) Type(typ *querypb.Type) (querypb.Type, bool) {
	switch code {
	case AggregateUnassigned:
		return sqltypes.Null, false
	case AggregateGroupConcat:
		if typ == nil {
			return sqltypes.Text, false
		}
		if sqltypes.IsBinary(*typ) {
			return sqltypes.Blob, true
		}
		return sqltypes.Text, true
	case AggregateMax, AggregateMin, AggregateRandom:
		if typ == nil {
			return sqltypes.Null, false
		}
		return *typ, true
	case AggregateSumDistinct, AggregateSum:
		if typ == nil {
			return sqltypes.Float64, false
		}
		if sqltypes.IsIntegral(*typ) || sqltypes.IsDecimal(*typ) {
			return sqltypes.Decimal, true
		}
		return sqltypes.Float64, true
	case AggregateCount, AggregateCountStar, AggregateCountDistinct:
		return sqltypes.Int64, true
	case AggregateGtid:
		return sqltypes.VarChar, true
	default:
		panic(code.String()) // we have a unit test checking we never reach here
	}
}
