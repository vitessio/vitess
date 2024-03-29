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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
	PulloutNotExists
)

var pulloutName = map[PulloutOpcode]string{
	PulloutValue:     "PulloutValue",
	PulloutIn:        "PulloutIn",
	PulloutNotIn:     "PulloutNotIn",
	PulloutExists:    "PulloutExists",
	PulloutNotExists: "PulloutNotExists",
}

func (code PulloutOpcode) String() string {
	return pulloutName[code]
}

func (code PulloutOpcode) NeedsListArg() bool {
	return code == PulloutIn || code == PulloutNotIn
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
	AggregateAnyValue
	AggregateCountStar
	AggregateGroupConcat
	AggregateAvg
	_NumOfOpCodes // This line must be last of the opcodes!
)

var (
	// OpcodeType keeps track of the known output types for different aggregate functions
	OpcodeType = map[AggregateOpcode]querypb.Type{
		AggregateCountDistinct: sqltypes.Int64,
		AggregateCount:         sqltypes.Int64,
		AggregateCountStar:     sqltypes.Int64,
		AggregateSumDistinct:   sqltypes.Decimal,
		AggregateSum:           sqltypes.Decimal,
		AggregateAvg:           sqltypes.Decimal,
		AggregateGtid:          sqltypes.VarChar,
	}
)

// SupportedAggregates maps the list of supported aggregate
// functions to their opcodes.
var SupportedAggregates = map[string]AggregateOpcode{
	"count": AggregateCount,
	"sum":   AggregateSum,
	"min":   AggregateMin,
	"max":   AggregateMax,
	"avg":   AggregateAvg,
	// These functions don't exist in mysql, but are used
	// to display the plan.
	"count_distinct": AggregateCountDistinct,
	"sum_distinct":   AggregateSumDistinct,
	"vgtid":          AggregateGtid,
	"count_star":     AggregateCountStar,
	"any_value":      AggregateAnyValue,
	"group_concat":   AggregateGroupConcat,
}

var AggregateName = map[AggregateOpcode]string{
	AggregateCount:         "count",
	AggregateSum:           "sum",
	AggregateMin:           "min",
	AggregateMax:           "max",
	AggregateCountDistinct: "count_distinct",
	AggregateSumDistinct:   "sum_distinct",
	AggregateGtid:          "vgtid",
	AggregateCountStar:     "count_star",
	AggregateGroupConcat:   "group_concat",
	AggregateAnyValue:      "any_value",
	AggregateAvg:           "avg",
}

func (code AggregateOpcode) String() string {
	name := AggregateName[code]
	if name == "" {
		name = "ERROR"
	}
	return name
}

// MarshalJSON serializes the AggregateOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code AggregateOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}

// Type returns the opcode return sql type, and a bool telling is we are sure about this type or not
func (code AggregateOpcode) SQLType(typ querypb.Type) querypb.Type {
	switch code {
	case AggregateUnassigned:
		return sqltypes.Null
	case AggregateGroupConcat:
		if typ == sqltypes.Unknown {
			return sqltypes.Unknown
		}
		if sqltypes.IsBinary(typ) {
			return sqltypes.Blob
		}
		return sqltypes.Text
	case AggregateMax, AggregateMin, AggregateAnyValue:
		return typ
	case AggregateSumDistinct, AggregateSum, AggregateAvg:
		if typ == sqltypes.Unknown {
			return sqltypes.Unknown
		}
		if sqltypes.IsIntegral(typ) || sqltypes.IsDecimal(typ) {
			return sqltypes.Decimal
		}
		return sqltypes.Float64
	case AggregateCount, AggregateCountStar, AggregateCountDistinct:
		return sqltypes.Int64
	case AggregateGtid:
		return sqltypes.VarChar
	default:
		panic(code.String()) // we have a unit test checking we never reach here
	}
}

func (code AggregateOpcode) Nullable() bool {
	switch code {
	case AggregateCount, AggregateCountStar:
		return false
	default:
		return true
	}
}

func (code AggregateOpcode) ResolveType(t evalengine.Type, env *collations.Environment) evalengine.Type {
	sqltype := code.SQLType(t.Type())
	collation := collations.CollationForType(sqltype, env.DefaultConnectionCharset())
	nullable := code.Nullable()
	size := t.Size()

	scale := t.Scale()
	if code == AggregateAvg {
		scale += 4
	}
	return evalengine.NewTypeEx(sqltype, collation, nullable, size, scale)
}

func (code AggregateOpcode) NeedsComparableValues() bool {
	switch code {
	case AggregateCountDistinct, AggregateSumDistinct, AggregateMin, AggregateMax:
		return true
	default:
		return false
	}
}

func (code AggregateOpcode) IsDistinct() bool {
	switch code {
	case AggregateCountDistinct, AggregateSumDistinct:
		return true
	default:
		return false
	}
}
