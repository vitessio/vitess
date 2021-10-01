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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

	// GroupByKeys specifies the input values that must be used for
	// the aggregation key.
	GroupByKeys []*GroupByParams

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`

	// Input is the primitive that will feed into this Primitive.
	Input Primitive
}

// GroupByParams specify the grouping key to be used.
type GroupByParams struct {
	KeyCol          int
	WeightStringCol int
	Expr            sqlparser.Expr
	FromGroupBy     bool
}

// String returns a string. Used for plan descriptions
func (gbp GroupByParams) String() string {
	if gbp.WeightStringCol == -1 || gbp.KeyCol == gbp.WeightStringCol {
		return strconv.Itoa(gbp.KeyCol)
	}
	return fmt.Sprintf("(%d|%d)", gbp.KeyCol, gbp.WeightStringCol)
}

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode AggregateOpcode
	Col    int

	// These are used only for distinct opcodes.
	KeyCol    int
	WCol      int
	WAssigned bool

	Alias string `json:",omitempty"`
	Expr  sqlparser.Expr
}

func (ap *AggregateParams) isDistinct() bool {
	return ap.Opcode == AggregateCountDistinct || ap.Opcode == AggregateSumDistinct
}

func (ap *AggregateParams) preProcess() bool {
	return ap.Opcode == AggregateCountDistinct || ap.Opcode == AggregateSumDistinct || ap.Opcode == AggregateGtid
}

func (ap *AggregateParams) String() string {
	keyCol := strconv.Itoa(ap.Col)
	if ap.WAssigned {
		keyCol = fmt.Sprintf("%s|%d", keyCol, ap.WCol)
	}
	if ap.Alias != "" {
		return fmt.Sprintf("%s(%s) AS %s", ap.Opcode.String(), keyCol, ap.Alias)
	}
	return fmt.Sprintf("%s(%s)", ap.Opcode.String(), keyCol)
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
	AggregateGtid
)

var (
	// OpcodeType keeps track of the known output types for different aggregate functions
	OpcodeType = map[AggregateOpcode]querypb.Type{
		AggregateCountDistinct: sqltypes.Int64,
		AggregateCount:         sqltypes.Int64,
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

// TryExecute is a Primitive function.
func (oa *OrderedAggregate) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := oa.execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

func (oa *OrderedAggregate) execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(oa.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	out := &sqltypes.Result{
		Fields: oa.convertFields(result.Fields),
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
	}
	// This code is similar to the one in StreamExecute.
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	for _, row := range result.Rows {
		if current == nil {
			current, curDistincts = oa.convertRow(row)
			continue
		}

		equal, err := oa.keysEqual(current, row)
		if err != nil {
			return nil, err
		}

		if equal {
			current, curDistincts, err = oa.merge(result.Fields, current, row, curDistincts)
			if err != nil {
				return nil, err
			}
			continue
		}
		out.Rows = append(out.Rows, current)
		current, curDistincts = oa.convertRow(row)
	}

	if len(result.Rows) == 0 && len(oa.GroupByKeys) == 0 {
		// When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
		// different aggregation functions
		row, err := oa.createEmptyRow()
		if err != nil {
			return nil, err
		}
		out.Rows = append(out.Rows, row)
	}

	if current != nil {
		final, err := oa.convertFinal(current)
		if err != nil {
			return nil, err
		}
		out.Rows = append(out.Rows, final)
	}
	return out, nil
}

// TryStreamExecute is a Primitive function.
func (oa *OrderedAggregate) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var current []sqltypes.Value
	var curDistincts []sqltypes.Value
	var fields []*querypb.Field

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(oa.TruncateColumnCount))
	}

	err := vcursor.StreamExecutePrimitive(oa.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			fields = oa.convertFields(qr.Fields)
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}
		// This code is similar to the one in Execute.
		for _, row := range qr.Rows {
			if current == nil {
				current, curDistincts = oa.convertRow(row)
				continue
			}

			equal, err := oa.keysEqual(current, row)
			if err != nil {
				return err
			}

			if equal {
				current, curDistincts, err = oa.merge(fields, current, row, curDistincts)
				if err != nil {
					return err
				}
				continue
			}
			if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{current}}); err != nil {
				return err
			}
			current, curDistincts = oa.convertRow(row)
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

func (oa *OrderedAggregate) convertFields(fields []*querypb.Field) []*querypb.Field {
	if !oa.PreProcess {
		return fields
	}
	for _, aggr := range oa.Aggregates {
		if !aggr.preProcess() {
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

func (oa *OrderedAggregate) convertRow(row []sqltypes.Value) (newRow []sqltypes.Value, curDistincts []sqltypes.Value) {
	if !oa.PreProcess {
		return row, nil
	}
	newRow = append(newRow, row...)
	curDistincts = make([]sqltypes.Value, len(oa.Aggregates))
	for index, aggr := range oa.Aggregates {
		switch aggr.Opcode {
		case AggregateCountDistinct:
			curDistincts[index] = findComparableCurrentDistinct(row, aggr)
			// Type is int64. Ok to call MakeTrusted.
			if row[aggr.KeyCol].IsNull() {
				newRow[aggr.Col] = countZero
			} else {
				newRow[aggr.Col] = countOne
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
func (oa *OrderedAggregate) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := oa.Input.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	qr = &sqltypes.Result{Fields: oa.convertFields(qr.Fields)}
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

func (oa *OrderedAggregate) keysEqual(row1, row2 []sqltypes.Value) (bool, error) {
	for _, key := range oa.GroupByKeys {
		cmp, err := evalengine.NullsafeCompare(row1[key.KeyCol], row2[key.KeyCol])
		if err != nil {
			_, isComparisonErr := err.(evalengine.UnsupportedComparisonError)
			if !(isComparisonErr && key.WeightStringCol != -1) {
				return false, err
			}
			key.KeyCol = key.WeightStringCol
			cmp, err = evalengine.NullsafeCompare(row1[key.WeightStringCol], row2[key.WeightStringCol])
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

func (oa *OrderedAggregate) merge(fields []*querypb.Field, row1, row2 []sqltypes.Value, curDistincts []sqltypes.Value) ([]sqltypes.Value, []sqltypes.Value, error) {
	result := sqltypes.CopyRow(row1)
	for index, aggr := range oa.Aggregates {
		if aggr.isDistinct() {
			if row2[aggr.KeyCol].IsNull() {
				continue
			}
			cmp, err := evalengine.NullsafeCompare(curDistincts[index], row2[aggr.KeyCol])
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
		case AggregateCount, AggregateSum:
			value := row1[aggr.Col]
			v2 := row2[aggr.Col]
			result[aggr.Col] = evalengine.NullsafeAdd(value, v2, fields[aggr.Col].Type)
		case AggregateMin:
			result[aggr.Col], err = evalengine.Min(row1[aggr.Col], row2[aggr.Col])
		case AggregateMax:
			result[aggr.Col], err = evalengine.Max(row1[aggr.Col], row2[aggr.Col])
		case AggregateCountDistinct:
			result[aggr.Col] = evalengine.NullsafeAdd(row1[aggr.Col], countOne, OpcodeType[aggr.Opcode])
		case AggregateSumDistinct:
			result[aggr.Col] = evalengine.NullsafeAdd(row1[aggr.Col], row2[aggr.Col], OpcodeType[aggr.Opcode])
		case AggregateGtid:
			vgtid := &binlogdatapb.VGtid{}
			err = proto.Unmarshal(row1[aggr.Col].ToBytes(), vgtid)
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
		default:
			return nil, nil, fmt.Errorf("BUG: Unexpected opcode: %v", aggr.Opcode)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return result, curDistincts, nil
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
	case
		AggregateCountDistinct,
		AggregateCount:
		return countZero, nil
	case
		AggregateSumDistinct,
		AggregateSum,
		AggregateMin,
		AggregateMax:
		return sqltypes.NULL, nil

	}
	return sqltypes.NULL, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unknown aggregation %v", opcode)
}

func aggregateParamsToString(in interface{}) string {
	return in.(*AggregateParams).String()
}

func groupByParamsToString(i interface{}) string {
	return i.(*GroupByParams).String()
}

func (oa *OrderedAggregate) description() PrimitiveDescription {
	aggregates := GenericJoin(oa.Aggregates, aggregateParamsToString)
	groupBy := GenericJoin(oa.GroupByKeys, groupByParamsToString)
	other := map[string]interface{}{
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

func (oa *OrderedAggregate) convertFinal(current []sqltypes.Value) ([]sqltypes.Value, error) {
	result := sqltypes.CopyRow(current)
	for _, aggr := range oa.Aggregates {
		switch aggr.Opcode {
		case AggregateGtid:
			vgtid := &binlogdatapb.VGtid{}
			err := proto.Unmarshal(current[aggr.Col].ToBytes(), vgtid)
			if err != nil {
				return nil, err
			}
			result[aggr.Col] = sqltypes.NewVarChar(vgtid.String())
		}
	}
	return result, nil
}
