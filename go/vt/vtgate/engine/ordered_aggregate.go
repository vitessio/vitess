/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/sqlparser"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*OrderedAggregate)(nil)

// OrderedAggregate is a primitive that expects the underlying primitive
// to feed results in an order sorted by the Keys. Rows with duplicate
// keys are aggregated using the Aggregate functions. The assumption
// is that the underlying primitive is a scatter select with pre-sorted
// rows.
type OrderedAggregate struct {
	// PreProcess is true if one of the aggregates needs preprocessing.
	PreProcess bool `json:",omitempty"`

	// Aggregates specifies the aggregation parameters for each
	// aggregation function: function opcode and input column number.
	Aggregates []*AggregateParams

	AggrOnEngine bool

	// GroupByKeys specifies the input values that must be used for
	// the aggregation key.
	GroupByKeys []*GroupByParams

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`

	// Collations stores the collation ID per column offset.
	// It is used for grouping keys and distinct aggregate functions
	Collations map[int]collations.ID

	// Input is the primitive that will feed into this Primitive.
	Input Primitive
}

// GroupByParams specify the grouping key to be used.
type GroupByParams struct {
	KeyCol          int
	WeightStringCol int
	Expr            sqlparser.Expr
	FromGroupBy     bool
	CollationID     collations.ID
}

// String returns a string. Used for plan descriptions
func (gbp GroupByParams) String() string {
	var out string
	if gbp.WeightStringCol == -1 || gbp.KeyCol == gbp.WeightStringCol {
		out = strconv.Itoa(gbp.KeyCol)
	} else {
		out = fmt.Sprintf("(%d|%d)", gbp.KeyCol, gbp.WeightStringCol)
	}

	if gbp.CollationID != collations.Unknown {
		collation := gbp.CollationID.Get()
		out += " COLLATE " + collation.Name()
	}

	return out
}

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode AggregateOpcode
	Col    int

	// These are used only for distinct opcodes.
	KeyCol      int
	WCol        int
	WAssigned   bool
	CollationID collations.ID

	Alias    string `json:",omitempty"`
	Expr     sqlparser.Expr
	Original *sqlparser.AliasedExpr

	// This is based on the function passed in the select expression and
	// not what we use to aggregate at the engine primitive level.
	OrigOpcode AggregateOpcode
}

func (ap *AggregateParams) isDistinct() bool {
	return ap.Opcode == AggregateCountDistinct || ap.Opcode == AggregateSumDistinct
}

func (ap *AggregateParams) preProcess() bool {
	return ap.Opcode == AggregateCountDistinct || ap.Opcode == AggregateSumDistinct || ap.Opcode == AggregateGtid || ap.Opcode == AggregateCount
}

func (ap *AggregateParams) String() string {
	keyCol := strconv.Itoa(ap.Col)
	if ap.WAssigned {
		keyCol = fmt.Sprintf("%s|%d", keyCol, ap.WCol)
	}
	if ap.CollationID != collations.Unknown {
		keyCol += " COLLATE " + ap.CollationID.Get().Name()
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
)

var (
	// OpcodeType keeps track of the known output types for different aggregate functions
	OpcodeType = map[AggregateOpcode]querypb.Type{
		AggregateCountDistinct: sqltypes.Int64,
		AggregateCount:         sqltypes.Int64,
		AggregateCountStar:     sqltypes.Int64,
		AggregateSumDistinct:   sqltypes.Decimal,
		AggregateSum:           sqltypes.Decimal,
		AggregateGtid:          sqltypes.VarChar,
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
	"vgtid":          AggregateGtid,
	"count_star":     AggregateCountStar,
	"random":         AggregateRandom,
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

// TryExecute is a Primitive function.
func (oa *OrderedAggregate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := oa.execute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

func (oa *OrderedAggregate) execute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(ctx, oa.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	out := &sqltypes.Result{
		Fields: convertFields(result.Fields, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine),
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
	}
	// This code is similar to the one in StreamExecute.
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	for _, row := range result.Rows {
		if current == nil {
			current, curDistincts = convertRow(row, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine)
			continue
		}
		equal, err := oa.keysEqual(current, row, oa.Collations)
		if err != nil {
			return nil, err
		}

		if equal {
			current, curDistincts, err = merge(result.Fields, current, row, curDistincts, oa.Collations, oa.Aggregates)
			if err != nil {
				return nil, err
			}
			continue
		}
		out.Rows = append(out.Rows, current)
		current, curDistincts = convertRow(row, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine)
	}

	if current != nil {
		final, err := convertFinal(current, oa.Aggregates)
		if err != nil {
			return nil, err
		}
		out.Rows = append(out.Rows, final)
	}
	return out, nil
}

// TryStreamExecute is a Primitive function.
func (oa *OrderedAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	var fields []*querypb.Field

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(oa.TruncateColumnCount))
	}

	err := vcursor.StreamExecutePrimitive(ctx, oa.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			fields = convertFields(qr.Fields, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine)
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}
		// This code is similar to the one in Execute.
		for _, row := range qr.Rows {
			if current == nil {
				current, curDistincts = convertRow(row, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine)
				continue
			}

			equal, err := oa.keysEqual(current, row, oa.Collations)
			if err != nil {
				return err
			}

			if equal {
				current, curDistincts, err = merge(fields, current, row, curDistincts, oa.Collations, oa.Aggregates)
				if err != nil {
					return err
				}
				continue
			}
			if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
				return err
			}
			current, curDistincts = convertRow(row, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine)
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

func convertFields(fields []*querypb.Field, preProcess bool, aggrs []*AggregateParams, aggrOnEngine bool) []*querypb.Field {
	if !preProcess {
		return fields
	}
	for _, aggr := range aggrs {
		if !aggr.preProcess() && !aggrOnEngine {
			continue
		}
		fields[aggr.Col] = &querypb.Field{
			Name: aggr.Alias,
			Type: OpcodeType[aggr.Opcode],
		}
		if aggr.isDistinct() {
			aggr.KeyCol = aggr.Col
		}
	}
	return fields
}

func convertRow(row []sqltypes.Value, preProcess bool, aggregates []*AggregateParams, aggrOnEngine bool) (newRow []sqltypes.Value, curDistincts []sqltypes.Value) {
	if !preProcess {
		return row, nil
	}
	newRow = append(newRow, row...)
	curDistincts = make([]sqltypes.Value, len(aggregates))
	for index, aggr := range aggregates {
		switch aggr.Opcode {
		case AggregateCountStar:
			newRow[aggr.Col] = countOne
		case AggregateCount:
			val := countOne
			if row[aggr.Col].IsNull() {
				val = countZero
			}
			newRow[aggr.Col] = val
		case AggregateCountDistinct:
			curDistincts[index] = findComparableCurrentDistinct(row, aggr)
			// Type is int64. Ok to call MakeTrusted.
			if row[aggr.KeyCol].IsNull() {
				newRow[aggr.Col] = countZero
			} else {
				newRow[aggr.Col] = countOne
			}
		case AggregateSum:
			if !aggrOnEngine {
				break
			}
			if row[aggr.Col].IsNull() {
				break
			}
			var err error
			newRow[aggr.Col], err = evalengine.Cast(row[aggr.Col], OpcodeType[aggr.Opcode])
			if err != nil {
				newRow[aggr.Col] = sumZero
			}
		case AggregateSumDistinct:
			curDistincts[index] = findComparableCurrentDistinct(row, aggr)
			var err error
			newRow[aggr.Col], err = evalengine.Cast(row[aggr.Col], OpcodeType[aggr.Opcode])
			if err != nil {
				newRow[aggr.Col] = sumZero
			}
		case AggregateGtid:
			vgtid := &binlogdatapb.VGtid{}
			vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: row[aggr.Col-1].ToString(),
				Shard:    row[aggr.Col+1].ToString(),
				Gtid:     row[aggr.Col].ToString(),
			})
			data, _ := proto.Marshal(vgtid)
			val, _ := sqltypes.NewValue(sqltypes.VarBinary, data)
			newRow[aggr.Col] = val
		}
	}
	return newRow, curDistincts
}

func findComparableCurrentDistinct(row []sqltypes.Value, aggr *AggregateParams) sqltypes.Value {
	curDistinct := row[aggr.KeyCol]
	if aggr.WAssigned && !curDistinct.IsComparable() {
		aggr.KeyCol = aggr.WCol
		curDistinct = row[aggr.KeyCol]
	}
	return curDistinct
}

// GetFields is a Primitive function.
func (oa *OrderedAggregate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := oa.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	qr = &sqltypes.Result{Fields: convertFields(qr.Fields, oa.PreProcess, oa.Aggregates, oa.AggrOnEngine)}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

// Inputs returns the Primitive input for this aggregation
func (oa *OrderedAggregate) Inputs() []Primitive {
	return []Primitive{oa.Input}
}

// NeedsTransaction implements the Primitive interface
func (oa *OrderedAggregate) NeedsTransaction() bool {
	return oa.Input.NeedsTransaction()
}

func (oa *OrderedAggregate) keysEqual(row1, row2 []sqltypes.Value, colls map[int]collations.ID) (bool, error) {
	for _, key := range oa.GroupByKeys {
		cmp, err := evalengine.NullsafeCompare(row1[key.KeyCol], row2[key.KeyCol], colls[key.KeyCol])
		if err != nil {
			_, isComparisonErr := err.(evalengine.UnsupportedComparisonError)
			_, isCollationErr := err.(evalengine.UnsupportedCollationError)
			if !isComparisonErr && !isCollationErr || key.WeightStringCol == -1 {
				return false, err
			}
			key.KeyCol = key.WeightStringCol
			cmp, err = evalengine.NullsafeCompare(row1[key.WeightStringCol], row2[key.WeightStringCol], colls[key.KeyCol])
			if err != nil {
				return false, err
			}
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func merge(
	fields []*querypb.Field,
	row1, row2 []sqltypes.Value,
	curDistincts []sqltypes.Value,
	colls map[int]collations.ID,
	aggregates []*AggregateParams,
) ([]sqltypes.Value, []sqltypes.Value, error) {
	result := sqltypes.CopyRow(row1)
	for index, aggr := range aggregates {
		if aggr.isDistinct() {
			if row2[aggr.KeyCol].IsNull() {
				continue
			}
			cmp, err := evalengine.NullsafeCompare(curDistincts[index], row2[aggr.KeyCol], colls[aggr.KeyCol])
			if err != nil {
				return nil, nil, err
			}
			if cmp == 0 {
				continue
			}
			curDistincts[index] = findComparableCurrentDistinct(row2, aggr)
		}
		var err error
		switch aggr.Opcode {
		case AggregateCountStar:
			value := row1[aggr.Col]
			result[aggr.Col], err = evalengine.NullSafeAdd(value, countOne, fields[aggr.Col].Type)
		case AggregateCount:
			val := countOne
			if row2[aggr.Col].IsNull() {
				val = countZero
			}
			result[aggr.Col], err = evalengine.NullSafeAdd(row1[aggr.Col], val, fields[aggr.Col].Type)
		case AggregateSum:
			value := row1[aggr.Col]
			v2 := row2[aggr.Col]
			if value.IsNull() && v2.IsNull() {
				result[aggr.Col] = sqltypes.NULL
				break
			}
			result[aggr.Col], err = evalengine.NullSafeAdd(value, v2, fields[aggr.Col].Type)
		case AggregateMin:
			result[aggr.Col], err = evalengine.Min(row1[aggr.Col], row2[aggr.Col], colls[aggr.Col])
		case AggregateMax:
			result[aggr.Col], err = evalengine.Max(row1[aggr.Col], row2[aggr.Col], colls[aggr.Col])
		case AggregateCountDistinct:
			result[aggr.Col], err = evalengine.NullSafeAdd(row1[aggr.Col], countOne, OpcodeType[aggr.Opcode])
		case AggregateSumDistinct:
			result[aggr.Col], err = evalengine.NullSafeAdd(row1[aggr.Col], row2[aggr.Col], OpcodeType[aggr.Opcode])
		case AggregateGtid:
			vgtid := &binlogdatapb.VGtid{}
			rowBytes, err := row1[aggr.Col].ToBytes()
			if err != nil {
				return nil, nil, err
			}
			err = proto.Unmarshal(rowBytes, vgtid)
			if err != nil {
				return nil, nil, err
			}
			vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: row2[aggr.Col-1].ToString(),
				Shard:    row2[aggr.Col+1].ToString(),
				Gtid:     row2[aggr.Col].ToString(),
			})
			data, _ := proto.Marshal(vgtid)
			val, _ := sqltypes.NewValue(sqltypes.VarBinary, data)
			result[aggr.Col] = val
		case AggregateRandom:
			// we just grab the first value per grouping. no need to do anything more complicated here
		default:
			return nil, nil, fmt.Errorf("BUG: Unexpected opcode: %v", aggr.Opcode)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return result, curDistincts, nil
}

func aggregateParamsToString(in any) string {
	return in.(*AggregateParams).String()
}

func groupByParamsToString(i any) string {
	return i.(*GroupByParams).String()
}

func (oa *OrderedAggregate) description() PrimitiveDescription {
	aggregates := GenericJoin(oa.Aggregates, aggregateParamsToString)
	groupBy := GenericJoin(oa.GroupByKeys, groupByParamsToString)
	other := map[string]any{
		"Aggregates": aggregates,
		"GroupBy":    groupBy,
	}
	if oa.TruncateColumnCount > 0 {
		other["ResultColumns"] = oa.TruncateColumnCount
	}
	return PrimitiveDescription{
		OperatorType: "Aggregate",
		Variant:      "Ordered",
		Other:        other,
	}
}

func convertFinal(current []sqltypes.Value, aggregates []*AggregateParams) ([]sqltypes.Value, error) {
	result := sqltypes.CopyRow(current)
	for _, aggr := range aggregates {
		switch aggr.Opcode {
		case AggregateGtid:
			vgtid := &binlogdatapb.VGtid{}
			currentBytes, err := current[aggr.Col].ToBytes()
			if err != nil {
				return nil, err
			}
			err = proto.Unmarshal(currentBytes, vgtid)
			if err != nil {
				return nil, err
			}
			result[aggr.Col] = sqltypes.NewVarChar(vgtid.String())
		}
	}
	return result, nil
}
