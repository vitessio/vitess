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
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vthash"
)

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode opcode.AggregateOpcode

	// Input source specification - exactly one of these should be set:
	// Col: Column index for simple column references (e.g., SUM(column_name))
	// EExpr: Evaluated expression for literals, parameters
	Col   int
	EExpr evalengine.Expr

	// These are used only for distinct opcodes.
	KeyCol int
	WCol   int
	Type   evalengine.Type

	Alias    string
	Func     sqlparser.AggrFunc
	Original *sqlparser.AliasedExpr

	// This is based on the function passed in the select expression and
	// not what we use to aggregate at the engine primitive level.
	OrigOpcode opcode.AggregateOpcode

	CollationEnv *collations.Environment

	// UseHashDistinct indicates this aggregation should use hash-based
	// distinct tracking instead of sort-based. This is used when multiple
	// distinct aggregations with different expressions exist in a query.
	UseHashDistinct bool
}

// NewAggregateParam creates a new aggregate param
func NewAggregateParam(
	oc opcode.AggregateOpcode,
	col int,
	expr evalengine.Expr,
	alias string,
	collationEnv *collations.Environment,
) *AggregateParams {
	if expr != nil && oc != opcode.AggregateConstant {
		panic(vterrors.VT13001("expr should be nil"))
	}
	out := &AggregateParams{
		Opcode:       oc,
		Col:          col,
		EExpr:        expr,
		Alias:        alias,
		WCol:         -1,
		CollationEnv: collationEnv,
	}
	if oc.NeedsComparableValues() {
		out.KeyCol = col
	}
	return out
}

func (ap *AggregateParams) WAssigned() bool {
	return ap.WCol >= 0
}

func (ap *AggregateParams) String() string {
	keyCol := strconv.Itoa(ap.Col)
	if ap.EExpr != nil {
		keyCol = sqlparser.String(ap.EExpr)
	}
	if ap.WAssigned() {
		keyCol = fmt.Sprintf("%s|%d", keyCol, ap.WCol)
	}
	if sqltypes.IsText(ap.Type.Type()) && ap.CollationEnv.IsSupported(ap.Type.Collation()) {
		keyCol += " COLLATE " + ap.CollationEnv.LookupName(ap.Type.Collation())
	}
	dispOrigOp := ""
	if ap.OrigOpcode != opcode.AggregateUnassigned && ap.OrigOpcode != ap.Opcode {
		dispOrigOp = "_" + ap.OrigOpcode.String()
	}
	if ap.Alias != "" {
		return fmt.Sprintf("%s%s(%s) AS %s", ap.Opcode.String(), dispOrigOp, keyCol, ap.Alias)
	}
	return fmt.Sprintf("%s%s(%s)", ap.Opcode.String(), dispOrigOp, keyCol)
}

func (ap *AggregateParams) typ(inputType querypb.Type, env *evalengine.ExpressionEnv, collID collations.ID) querypb.Type {
	if ap.EExpr != nil {
		value, err := eval(env, ap.EExpr, collID)
		if err != nil {
			return sqltypes.Unknown
		}
		return value.Type()
	}
	if ap.OrigOpcode != opcode.AggregateUnassigned {
		return ap.OrigOpcode.SQLType(inputType)
	}
	return ap.Opcode.SQLType(inputType)
}

type aggregator interface {
	add(row []sqltypes.Value) error
	finish(env *evalengine.ExpressionEnv, coll collations.ID) (sqltypes.Value, error)
	reset()
}

// distinctTracker is an interface for tracking distinct values.
// It can be implemented by sort-based (aggregatorDistinct) or
// hash-based (aggregatorDistinctHash) trackers.
type distinctTracker interface {
	shouldReturn(row []sqltypes.Value) (bool, error)
	reset()
}

type aggregatorDistinct struct {
	column       int
	last         sqltypes.Value
	coll         collations.ID
	collationEnv *collations.Environment
	values       *evalengine.EnumSetValues
}

func (a *aggregatorDistinct) shouldReturn(row []sqltypes.Value) (bool, error) {
	if a.column >= 0 {
		last := a.last
		next := row[a.column]
		if !last.IsNull() {
			if last.TinyWeightCmp(next) == 0 {
				cmp, err := evalengine.NullsafeCompare(last, next, a.collationEnv, a.coll, a.values)
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

// aggregatorDistinctHash is a hash-based distinct tracker that uses hash sets
// to track seen values. Unlike aggregatorDistinct which requires sorted data,
// this can handle arbitrary input order. It's used when multiple distinct
// aggregations with different expressions exist in a single query.
type aggregatorDistinctHash struct {
	column       int
	wsColumn     int // weight string column for collation support
	seen         map[vthash.Hash]struct{}
	hasher       vthash.Hasher
	coll         collations.ID
	collationEnv *collations.Environment
	typ          querypb.Type
	sqlmode      evalengine.SQLMode
	values       *evalengine.EnumSetValues
}

func (a *aggregatorDistinctHash) shouldReturn(row []sqltypes.Value) (bool, error) {
	val := row[a.column]
	if val.IsNull() {
		// NULL values are never counted as distinct duplicates
		return false, nil
	}

	a.hasher.Reset()
	err := evalengine.NullsafeHashcode128(&a.hasher, val, a.coll, a.typ, a.sqlmode, a.values)
	if err != nil {
		// Fallback to weight string if available
		if a.wsColumn >= 0 {
			val = row[a.wsColumn]
			a.hasher.Reset()
			err = evalengine.NullsafeHashcode128(&a.hasher, val, collations.Unknown, sqltypes.VarBinary, a.sqlmode, nil)
		}
		if err != nil {
			return false, err
		}
	}

	hash := a.hasher.Sum128()
	if _, found := a.seen[hash]; found {
		return true, nil // Already seen, skip
	}
	a.seen[hash] = struct{}{}
	return false, nil
}

func (a *aggregatorDistinctHash) reset() {
	a.seen = make(map[vthash.Hash]struct{})
}

type aggregatorCount struct {
	from     int
	n        int64
	distinct distinctTracker
}

func (a *aggregatorCount) add(row []sqltypes.Value) error {
	if row[a.from].IsNull() {
		return nil
	}
	if a.distinct != nil {
		if ret, err := a.distinct.shouldReturn(row); ret {
			return err
		}
	}
	a.n++
	return nil
}

func (a *aggregatorCount) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	return sqltypes.NewInt64(a.n), nil
}

func (a *aggregatorCount) reset() {
	a.n = 0
	if a.distinct != nil {
		a.distinct.reset()
	}
}

type aggregatorCountStar struct {
	n int64
}

func (a *aggregatorCountStar) add([]sqltypes.Value) error {
	a.n++
	return nil
}

func (a *aggregatorCountStar) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	return sqltypes.NewInt64(a.n), nil
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

func (a *aggregatorMinMax) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	return a.minmax.Result(), nil
}

func (a *aggregatorMinMax) reset() {
	a.minmax.Reset()
}

type aggregatorSum struct {
	from     int
	sum      evalengine.Sum
	distinct distinctTracker
}

func (a *aggregatorSum) add(row []sqltypes.Value) error {
	if row[a.from].IsNull() {
		return nil
	}
	if a.distinct != nil {
		if ret, err := a.distinct.shouldReturn(row); ret {
			return err
		}
	}
	return a.sum.Add(row[a.from])
}

func (a *aggregatorSum) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	return a.sum.Result(), nil
}

func (a *aggregatorSum) reset() {
	a.sum.Reset()
	if a.distinct != nil {
		a.distinct.reset()
	}
}

type aggregatorScalar struct {
	from     int
	current  sqltypes.Value
	hasValue bool
}

func (a *aggregatorScalar) add(row []sqltypes.Value) error {
	if !a.hasValue {
		a.current = row[a.from]
		a.hasValue = true
	}
	return nil
}

func (a *aggregatorScalar) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	return a.current, nil
}

func (a *aggregatorScalar) reset() {
	a.current = sqltypes.NULL
	a.hasValue = false
}

type aggregatorConstant struct {
	expr evalengine.Expr
}

func (*aggregatorConstant) add([]sqltypes.Value) error {
	return nil
}

func (a *aggregatorConstant) finish(env *evalengine.ExpressionEnv, coll collations.ID) (sqltypes.Value, error) {
	return eval(env, a.expr, coll)
}

func eval(env *evalengine.ExpressionEnv, eexpr evalengine.Expr, coll collations.ID) (sqltypes.Value, error) {
	v, err := env.Evaluate(eexpr)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return v.Value(coll), nil
}

func (*aggregatorConstant) reset() {}

type aggregatorGroupConcat struct {
	from      int
	type_     sqltypes.Type
	separator []byte

	concat []byte
	n      int
}

func (a *aggregatorGroupConcat) add(row []sqltypes.Value) error {
	if row[a.from].IsNull() {
		return nil
	}
	if a.n > 0 {
		a.concat = append(a.concat, a.separator...)
	}
	a.concat = append(a.concat, row[a.from].Raw()...)
	a.n++
	return nil
}

func (a *aggregatorGroupConcat) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	if a.n == 0 {
		return sqltypes.NULL, nil
	}
	return sqltypes.MakeTrusted(a.type_, a.concat), nil
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

func (a *aggregatorGtid) finish(*evalengine.ExpressionEnv, collations.ID) (sqltypes.Value, error) {
	gtid := binlogdatapb.VGtid{ShardGtids: a.shards}
	return sqltypes.NewVarChar(gtid.String()), nil
}

func (a *aggregatorGtid) reset() {
	a.shards = a.shards[:0] // safe to reuse because only the serialized form of a.shards is returned
}

type aggregationState struct {
	env         *evalengine.ExpressionEnv
	aggregators []aggregator
	coll        collations.ID
}

func (a *aggregationState) add(row []sqltypes.Value) error {
	for _, st := range a.aggregators {
		if err := st.add(row); err != nil {
			return err
		}
	}
	return nil
}

func (a *aggregationState) finish() ([]sqltypes.Value, error) {
	row := make([]sqltypes.Value, 0, len(a.aggregators))
	for _, st := range a.aggregators {
		v, err := st.finish(a.env, a.coll)
		if err != nil {
			return nil, err
		}
		row = append(row, v)
	}
	return row, nil
}

func (a *aggregationState) reset() {
	for _, st := range a.aggregators {
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
		sqltypes.Bit,
		sqltypes.Vector:
		return true
	}
	return false
}

// createDistinctTracker creates the appropriate distinct tracker based on the aggregation parameters.
// If UseHashDistinct is true, it creates a hash-based tracker that can handle arbitrary input order.
// Otherwise, it creates a sort-based tracker that requires sorted input.
func createDistinctTracker(aggr *AggregateParams, distinctCol int, wsCol int, sourceType querypb.Type) distinctTracker {
	if distinctCol < 0 {
		return nil // No distinct tracking needed
	}

	if aggr.UseHashDistinct {
		return &aggregatorDistinctHash{
			column:       distinctCol,
			wsColumn:     wsCol,
			seen:         make(map[vthash.Hash]struct{}),
			hasher:       vthash.New(),
			coll:         aggr.Type.Collation(),
			collationEnv: aggr.CollationEnv,
			typ:          sourceType,
			values:       aggr.Type.Values(),
		}
	}

	return &aggregatorDistinct{
		column:       distinctCol,
		coll:         aggr.Type.Collation(),
		collationEnv: aggr.CollationEnv,
		values:       aggr.Type.Values(),
	}
}

func newAggregation(fields []*querypb.Field, aggregates []*AggregateParams, env *evalengine.ExpressionEnv, collation collations.ID) (*aggregationState, []*querypb.Field, error) {
	fields = slice.Map(fields, func(from *querypb.Field) *querypb.Field { return from.CloneVT() })

	aggregators := make([]aggregator, len(fields))
	for _, aggr := range aggregates {
		var sourceType querypb.Type
		if aggr.Col < len(fields) {
			sourceType = fields[aggr.Col].Type
		}
		targetType := aggr.typ(sourceType, env, collation)

		var ag aggregator
		var distinctCol = -1
		var wsCol = -1

		if aggr.Opcode.IsDistinct() {
			distinctCol = aggr.KeyCol
			wsCol = aggr.WCol
			if aggr.WAssigned() && !isComparable(sourceType) {
				distinctCol = aggr.WCol
			}
		}

		if aggr.Opcode == opcode.AggregateMin || aggr.Opcode == opcode.AggregateMax {
			if aggr.WAssigned() && !isComparable(sourceType) {
				return nil, nil, vterrors.VT12001("min/max on types that are not comparable is not supported")
			}
		}

		switch aggr.Opcode {
		case opcode.AggregateCountStar:
			ag = &aggregatorCountStar{}

		case opcode.AggregateCount, opcode.AggregateCountDistinct:
			ag = &aggregatorCount{
				from:     aggr.Col,
				distinct: createDistinctTracker(aggr, distinctCol, wsCol, sourceType),
			}

		case opcode.AggregateSum, opcode.AggregateSumDistinct:
			var sum evalengine.Sum
			switch aggr.OrigOpcode {
			case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct:
				sum = evalengine.NewSumOfCounts()
			default:
				sum = evalengine.NewAggregationSum(sourceType)
			}

			ag = &aggregatorSum{
				from:     aggr.Col,
				sum:      sum,
				distinct: createDistinctTracker(aggr, distinctCol, wsCol, sourceType),
			}

		case opcode.AggregateMin:
			ag = &aggregatorMin{
				aggregatorMinMax{
					from:   aggr.Col,
					minmax: evalengine.NewAggregationMinMax(sourceType, aggr.CollationEnv, aggr.Type.Collation(), aggr.Type.Values()),
				},
			}

		case opcode.AggregateMax:
			ag = &aggregatorMax{
				aggregatorMinMax{
					from:   aggr.Col,
					minmax: evalengine.NewAggregationMinMax(sourceType, aggr.CollationEnv, aggr.Type.Collation(), aggr.Type.Values()),
				},
			}

		case opcode.AggregateGtid:
			ag = &aggregatorGtid{from: aggr.Col}

		case opcode.AggregateAnyValue:
			ag = &aggregatorScalar{from: aggr.Col}

		case opcode.AggregateGroupConcat:
			gcFunc := aggr.Func.(*sqlparser.GroupConcatExpr)
			separator := []byte(gcFunc.Separator)
			ag = &aggregatorGroupConcat{
				from:      aggr.Col,
				type_:     targetType,
				separator: separator,
			}

		case opcode.AggregateConstant:
			ag = &aggregatorConstant{expr: aggr.EExpr}

		default:
			panic("BUG: unexpected Aggregation opcode")
		}

		aggregators[aggr.Col] = ag
		fields[aggr.Col].Type = targetType
		if aggr.Alias != "" {
			fields[aggr.Col].Name = aggr.Alias
		}
	}

	for i, a := range aggregators {
		if a == nil {
			aggregators[i] = &aggregatorScalar{from: i}
		}
	}

	return &aggregationState{aggregators: aggregators, env: env, coll: collation}, fields, nil
}
