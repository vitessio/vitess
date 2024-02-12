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

package engine

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode AggregateOpcode
	Col    int

	// These are used only for distinct opcodes.
	KeyCol int
	WCol   int
	Type   evalengine.Type

	Alias    string `json:",omitempty"`
	Expr     sqlparser.Expr
	Original *sqlparser.AliasedExpr

	// This is based on the function passed in the select expression and
	// not what we use to aggregate at the engine primitive level.
	OrigOpcode AggregateOpcode

	CollationEnv *collations.Environment
}

func NewAggregateParam(opcode AggregateOpcode, col int, alias string, collationEnv *collations.Environment) *AggregateParams {
	out := &AggregateParams{
		Opcode:       opcode,
		Col:          col,
		Alias:        alias,
		WCol:         -1,
		CollationEnv: collationEnv,
	}
	if opcode.NeedsComparableValues() {
		out.KeyCol = col
	}
	return out
}

func (ap *AggregateParams) WAssigned() bool {
	return ap.WCol >= 0
}

func (ap *AggregateParams) String() string {
	keyCol := strconv.Itoa(ap.Col)
	if ap.WAssigned() {
		keyCol = fmt.Sprintf("%s|%d", keyCol, ap.WCol)
	}
	if sqltypes.IsText(ap.Type.Type()) && ap.CollationEnv.IsSupported(ap.Type.Collation()) {
		keyCol += " COLLATE " + ap.CollationEnv.LookupName(ap.Type.Collation())
	}
	dispOrigOp := ""
	if ap.OrigOpcode != AggregateUnassigned && ap.OrigOpcode != ap.Opcode {
		dispOrigOp = "_" + ap.OrigOpcode.String()
	}
	if ap.Alias != "" {
		return fmt.Sprintf("%s%s(%s) AS %s", ap.Opcode.String(), dispOrigOp, keyCol, ap.Alias)
	}
	return fmt.Sprintf("%s%s(%s)", ap.Opcode.String(), dispOrigOp, keyCol)
}

func (ap *AggregateParams) typ(inputType querypb.Type) querypb.Type {
	if ap.OrigOpcode != AggregateUnassigned {
		return ap.OrigOpcode.SQLType(inputType)
	}
	return ap.Opcode.SQLType(inputType)
}

type aggregator interface {
	add(row []sqltypes.Value) error
	finish() sqltypes.Value
	reset()
}

type aggregatorDistinct struct {
	column       int
	last         sqltypes.Value
	coll         collations.ID
	collationEnv *collations.Environment
}

func (a *aggregatorDistinct) shouldReturn(row []sqltypes.Value) (bool, error) {
	if a.column >= 0 {
		last := a.last
		next := row[a.column]
		if !last.IsNull() {
			if last.TinyWeightCmp(next) == 0 {
				cmp, err := evalengine.NullsafeCompare(last, next, a.collationEnv, a.coll)
				if err != nil {
					return true, err
				}
				if cmp == 0 {
					return true, nil
				}
			}
		}
		a.last = next
	}
	return false, nil
}

func (a *aggregatorDistinct) reset() {
	a.last = sqltypes.NULL
}

type aggregatorCount struct {
	from     int
	n        int64
	distinct aggregatorDistinct
}

func (a *aggregatorCount) add(row []sqltypes.Value) error {
	if row[a.from].IsNull() {
		return nil
	}
	if ret, err := a.distinct.shouldReturn(row); ret {
		return err
	}
	a.n++
	return nil
}

func (a *aggregatorCount) finish() sqltypes.Value {
	return sqltypes.NewInt64(a.n)
}

func (a *aggregatorCount) reset() {
	a.n = 0
	a.distinct.reset()
}

type aggregatorCountStar struct {
	n int64
}

func (a *aggregatorCountStar) add(_ []sqltypes.Value) error {
	a.n++
	return nil
}

func (a *aggregatorCountStar) finish() sqltypes.Value {
	return sqltypes.NewInt64(a.n)
}

func (a *aggregatorCountStar) reset() {
	a.n = 0
}

type aggregatorMinMax struct {
	from   int
	minmax evalengine.MinMax
}

type aggregatorMin struct {
	aggregatorMinMax
}

func (a *aggregatorMin) add(row []sqltypes.Value) (err error) {
	return a.minmax.Min(row[a.from])
}

type aggregatorMax struct {
	aggregatorMinMax
}

func (a *aggregatorMax) add(row []sqltypes.Value) (err error) {
	return a.minmax.Max(row[a.from])
}

func (a *aggregatorMinMax) finish() sqltypes.Value {
	return a.minmax.Result()
}

func (a *aggregatorMinMax) reset() {
	a.minmax.Reset()
}

type aggregatorSum struct {
	from     int
	sum      evalengine.Sum
	distinct aggregatorDistinct
}

func (a *aggregatorSum) add(row []sqltypes.Value) error {
	if row[a.from].IsNull() {
		return nil
	}
	if ret, err := a.distinct.shouldReturn(row); ret {
		return err
	}
	return a.sum.Add(row[a.from])
}

func (a *aggregatorSum) finish() sqltypes.Value {
	return a.sum.Result()
}

func (a *aggregatorSum) reset() {
	a.sum.Reset()
	a.distinct.reset()
}

type aggregatorScalar struct {
	from    int
	current sqltypes.Value
	init    bool
}

func (a *aggregatorScalar) add(row []sqltypes.Value) error {
	if !a.init {
		a.current = row[a.from]
		a.init = true
	}
	return nil
}

func (a *aggregatorScalar) finish() sqltypes.Value {
	return a.current
}

func (a *aggregatorScalar) reset() {
	a.current = sqltypes.NULL
	a.init = false
}

type aggregatorGroupConcat struct {
	from  int
	type_ sqltypes.Type

	concat []byte
	n      int
}

func (a *aggregatorGroupConcat) add(row []sqltypes.Value) error {
	if row[a.from].IsNull() {
		return nil
	}
	if a.n > 0 {
		a.concat = append(a.concat, ',')
	}
	a.concat = append(a.concat, row[a.from].Raw()...)
	a.n++
	return nil
}

func (a *aggregatorGroupConcat) finish() sqltypes.Value {
	if a.n == 0 {
		return sqltypes.NULL
	}
	return sqltypes.MakeTrusted(a.type_, a.concat)
}

func (a *aggregatorGroupConcat) reset() {
	a.n = 0
	a.concat = nil // not safe to reuse this byte slice as it's returned as MakeTrusted
}

type aggregatorGtid struct {
	from   int
	shards []*binlogdatapb.ShardGtid
}

func (a *aggregatorGtid) add(row []sqltypes.Value) error {
	a.shards = append(a.shards, &binlogdatapb.ShardGtid{
		Keyspace: row[a.from-1].ToString(),
		Shard:    row[a.from+1].ToString(),
		Gtid:     row[a.from].ToString(),
	})
	return nil
}

func (a *aggregatorGtid) finish() sqltypes.Value {
	gtid := binlogdatapb.VGtid{ShardGtids: a.shards}
	return sqltypes.NewVarChar(gtid.String())
}

func (a *aggregatorGtid) reset() {
	a.shards = a.shards[:0] // safe to reuse because only the serialized form of a.shards is returned
}

type aggregationState []aggregator

func (a aggregationState) add(row []sqltypes.Value) error {
	for _, st := range a {
		if err := st.add(row); err != nil {
			return err
		}
	}
	return nil
}

func (a aggregationState) finish() (row []sqltypes.Value) {
	row = make([]sqltypes.Value, 0, len(a))
	for _, st := range a {
		row = append(row, st.finish())
	}
	return
}

func (a aggregationState) reset() {
	for _, st := range a {
		st.reset()
	}
}

func isComparable(typ sqltypes.Type) bool {
	if typ == sqltypes.Null || sqltypes.IsNumber(typ) || sqltypes.IsBinary(typ) {
		return true
	}
	switch typ {
	case sqltypes.Timestamp,
		sqltypes.Date,
		sqltypes.Time,
		sqltypes.Datetime,
		sqltypes.Enum,
		sqltypes.Set,
		sqltypes.TypeJSON,
		sqltypes.Bit:
		return true
	}
	return false
}

func newAggregation(fields []*querypb.Field, aggregates []*AggregateParams) (aggregationState, []*querypb.Field, error) {
	fields = slice.Map(fields, func(from *querypb.Field) *querypb.Field { return from.CloneVT() })

	agstate := make([]aggregator, len(fields))
	for _, aggr := range aggregates {
		sourceType := fields[aggr.Col].Type
		targetType := aggr.typ(sourceType)

		var ag aggregator
		var distinct = -1

		if aggr.Opcode.IsDistinct() {
			distinct = aggr.KeyCol
			if aggr.WAssigned() && !isComparable(sourceType) {
				distinct = aggr.WCol
			}
		}

		if aggr.Opcode == AggregateMin || aggr.Opcode == AggregateMax {
			if aggr.WAssigned() && !isComparable(sourceType) {
				return nil, nil, vterrors.VT12001("min/max on types that are not comparable is not supported")
			}
		}

		switch aggr.Opcode {
		case AggregateCountStar:
			ag = &aggregatorCountStar{}

		case AggregateCount, AggregateCountDistinct:
			ag = &aggregatorCount{
				from: aggr.Col,
				distinct: aggregatorDistinct{
					column:       distinct,
					coll:         aggr.Type.Collation(),
					collationEnv: aggr.CollationEnv,
				},
			}

		case AggregateSum, AggregateSumDistinct:
			var sum evalengine.Sum
			switch aggr.OrigOpcode {
			case AggregateCount, AggregateCountStar, AggregateCountDistinct:
				sum = evalengine.NewSumOfCounts()
			default:
				sum = evalengine.NewAggregationSum(sourceType)
			}

			ag = &aggregatorSum{
				from: aggr.Col,
				sum:  sum,
				distinct: aggregatorDistinct{
					column:       distinct,
					coll:         aggr.Type.Collation(),
					collationEnv: aggr.CollationEnv,
				},
			}

		case AggregateMin:
			ag = &aggregatorMin{
				aggregatorMinMax{
					from:   aggr.Col,
					minmax: evalengine.NewAggregationMinMax(sourceType, aggr.CollationEnv, aggr.Type.Collation()),
				},
			}

		case AggregateMax:
			ag = &aggregatorMax{
				aggregatorMinMax{
					from:   aggr.Col,
					minmax: evalengine.NewAggregationMinMax(sourceType, aggr.CollationEnv, aggr.Type.Collation()),
				},
			}

		case AggregateGtid:
			ag = &aggregatorGtid{from: aggr.Col}

		case AggregateAnyValue:
			ag = &aggregatorScalar{from: aggr.Col}

		case AggregateGroupConcat:
			ag = &aggregatorGroupConcat{from: aggr.Col, type_: targetType}

		default:
			panic("BUG: unexpected Aggregation opcode")
		}

		agstate[aggr.Col] = ag
		fields[aggr.Col].Type = targetType
		if aggr.Alias != "" {
			fields[aggr.Col].Name = aggr.Alias
		}
	}

	for i, a := range agstate {
		if a == nil {
			agstate[i] = &aggregatorScalar{from: i}
		}
	}

	return agstate, fields, nil
}
