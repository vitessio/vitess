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

	"vitess.io/vitess/go/vt/vterrors"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// AggregateParams specify the parameters for each aggregation.
// It contains the opcode and input column number.
type AggregateParams struct {
	Opcode AggregateOpcode
	Col    int

	// These are used only for distinct opcodes.
	KeyCol      int
	WCol        int
	Type        sqltypes.Type
	CollationID collations.ID

	Alias    string `json:",omitempty"`
	Expr     sqlparser.Expr
	Original *sqlparser.AliasedExpr

	// This is based on the function passed in the select expression and
	// not what we use to aggregate at the engine primitive level.
	OrigOpcode AggregateOpcode
}

func NewAggregateParam(opcode AggregateOpcode, col int, alias string) *AggregateParams {
	out := &AggregateParams{
		Opcode: opcode,
		Col:    col,
		Alias:  alias,
		WCol:   -1,
		Type:   sqltypes.Unknown,
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
	if sqltypes.IsText(ap.Type) && ap.CollationID != collations.Unknown {
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

func (ap *AggregateParams) typ(inputType querypb.Type) querypb.Type {
	opCode := ap.Opcode
	if ap.OrigOpcode != AggregateUnassigned {
		opCode = ap.OrigOpcode
	}
	typ, _ := opCode.Type(&inputType)
	return typ
}

func convertRow(
	fields []*querypb.Field,
	row []sqltypes.Value,
	aggregates []*AggregateParams,
) (newRow []sqltypes.Value, curDistincts []sqltypes.Value) {
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
			if row[aggr.Col].IsNull() {
				break
			}
			var err error
			newRow[aggr.Col], err = evalengine.Cast(row[aggr.Col], fields[aggr.Col].Type)
			if err != nil {
				newRow[aggr.Col] = sumZero
			}
		case AggregateSumDistinct:
			curDistincts[index] = findComparableCurrentDistinct(row, aggr)
			var err error
			newRow[aggr.Col], err = evalengine.Cast(row[aggr.Col], fields[aggr.Col].Type)
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
			data, _ := vgtid.MarshalVT()
			val, _ := sqltypes.NewValue(sqltypes.VarBinary, data)
			newRow[aggr.Col] = val
		case AggregateGroupConcat:
			if !row[aggr.Col].IsNull() {
				newRow[aggr.Col] = sqltypes.MakeTrusted(fields[aggr.Col].Type, []byte(row[aggr.Col].ToString()))
			}
		}
	}
	return newRow, curDistincts
}

func merge(
	fields []*querypb.Field,
	row1, row2 []sqltypes.Value,
	curDistincts []sqltypes.Value,
	aggregates []*AggregateParams,
) ([]sqltypes.Value, []sqltypes.Value, error) {
	result := sqltypes.CopyRow(row1)
	for index, aggr := range aggregates {
		if aggr.Opcode.IsDistinct() {
			if row2[aggr.KeyCol].IsNull() {
				continue
			}
			cmp, err := evalengine.NullsafeCompare(curDistincts[index], row2[aggr.KeyCol], aggr.CollationID)
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
			result[aggr.Col], err = evalengine.NullSafeAdd(row1[aggr.Col], countOne, fields[aggr.Col].Type)
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
			if aggr.WAssigned() && !row2[aggr.Col].IsComparable() {
				return minMaxWeightStringError()
			}
			result[aggr.Col], err = evalengine.Min(row1[aggr.Col], row2[aggr.Col], aggr.CollationID)
		case AggregateMax:
			if aggr.WAssigned() && !row2[aggr.Col].IsComparable() {
				return minMaxWeightStringError()
			}
			result[aggr.Col], err = evalengine.Max(row1[aggr.Col], row2[aggr.Col], aggr.CollationID)
		case AggregateCountDistinct:
			result[aggr.Col], err = evalengine.NullSafeAdd(row1[aggr.Col], countOne, fields[aggr.Col].Type)
		case AggregateSumDistinct:
			result[aggr.Col], err = evalengine.NullSafeAdd(row1[aggr.Col], row2[aggr.Col], fields[aggr.Col].Type)
		case AggregateGtid:
			vgtid := &binlogdatapb.VGtid{}
			rowBytes, err := row1[aggr.Col].ToBytes()
			if err != nil {
				return nil, nil, err
			}
			err = vgtid.UnmarshalVT(rowBytes)
			if err != nil {
				return nil, nil, err
			}
			vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: row2[aggr.Col-1].ToString(),
				Shard:    row2[aggr.Col+1].ToString(),
				Gtid:     row2[aggr.Col].ToString(),
			})
			data, _ := vgtid.MarshalVT()
			val, _ := sqltypes.NewValue(sqltypes.VarBinary, data)
			result[aggr.Col] = val
		case AggregateAnyValue:
			// we just grab the first value per grouping. no need to do anything more complicated here
		case AggregateGroupConcat:
			if row2[aggr.Col].IsNull() {
				break
			}
			if result[aggr.Col].IsNull() {
				result[aggr.Col] = sqltypes.MakeTrusted(fields[aggr.Col].Type, []byte(row2[aggr.Col].ToString()))
				break
			}
			concat := row1[aggr.Col].ToString() + "," + row2[aggr.Col].ToString()
			result[aggr.Col] = sqltypes.MakeTrusted(fields[aggr.Col].Type, []byte(concat))
		default:
			return nil, nil, fmt.Errorf("BUG: Unexpected opcode: %v", aggr.Opcode)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return result, curDistincts, nil
}

func minMaxWeightStringError() ([]sqltypes.Value, []sqltypes.Value, error) {
	return nil, nil, vterrors.VT12001("min/max on types that are not comparable is not supported")
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
			err = vgtid.UnmarshalVT(currentBytes)
			if err != nil {
				return nil, err
			}
			result[aggr.Col] = sqltypes.NewVarChar(vgtid.String())
		}
	}
	return result, nil
}

func convertFields(fields []*querypb.Field, aggrs []*AggregateParams) []*querypb.Field {
	fields = slices2.Map(fields, func(from *querypb.Field) *querypb.Field {
		return proto.Clone(from).(*querypb.Field)
	})
	for _, aggr := range aggrs {
		fields[aggr.Col].Type = aggr.typ(fields[aggr.Col].Type)
		if aggr.Alias != "" {
			fields[aggr.Col].Name = aggr.Alias
		}
	}
	return fields
}

func findComparableCurrentDistinct(row []sqltypes.Value, aggr *AggregateParams) sqltypes.Value {
	curDistinct := row[aggr.KeyCol]
	if aggr.WAssigned() && !curDistinct.IsComparable() {
		aggr.KeyCol = aggr.WCol
		curDistinct = row[aggr.KeyCol]
	}
	return curDistinct
}
