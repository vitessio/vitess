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
	"context"
	"strconv"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

const nextValBV = "n"

type InsertRows struct {
	// Generate is only set for inserts where a sequence must be generated.
	Generate *Generate

	RowsFromValues sqlparser.Values

	// Input is a select query plan to retrieve results for inserting data.
	RowsFromSelect Primitive
}

func NewInsertRowsFromSelect(generate *Generate, rowsFromSelect Primitive) *InsertRows {
	return &InsertRows{Generate: generate, RowsFromSelect: rowsFromSelect}
}

func NewInsertRows(generate *Generate) *InsertRows {
	return &InsertRows{Generate: generate}
}

type insertRowsResult struct {
	rows     []sqltypes.Row
	insertID int64
}

func (ir *InsertRows) Inputs() ([]Primitive, []map[string]any) {
	if ir == nil || ir.RowsFromSelect == nil {
		return nil, nil
	}
	return []Primitive{ir.RowsFromSelect}, nil
}

func (ir *InsertRows) hasSelectInput() bool {
	return ir != nil && ir.RowsFromSelect != nil
}

func (ir *InsertRows) execSelect(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (insertRowsResult, error) {
	// run the SELECT query
	if ir.RowsFromSelect == nil {
		return insertRowsResult{}, vterrors.VT13001("something went wrong planning INSERT SELECT")
	}

	res, err := vcursor.ExecutePrimitive(ctx, ir.RowsFromSelect, bindVars, false)
	if err != nil || len(res.Rows) == 0 {
		return insertRowsResult{}, err
	}

	insertID, err := ir.processGenerateFromSelect(ctx, vcursor, res.Rows)
	if err != nil {
		return insertRowsResult{}, err
	}

	return insertRowsResult{
		rows:     res.Rows,
		insertID: insertID,
	}, nil
}

func (ir *InsertRows) execSelectStreaming(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	callback func(irr insertRowsResult) error,
) error {
	return vcursor.StreamExecutePrimitiveStandalone(ctx, ir.RowsFromSelect, bindVars, false, func(result *sqltypes.Result) error {
		insertID, err := ir.processGenerateFromSelect(ctx, vcursor, result.Rows)
		if err != nil {
			return err
		}

		return callback(insertRowsResult{
			rows:     result.Rows,
			insertID: insertID,
		})
	})
}

// processGenerateFromSelect generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ir *InsertRows) processGenerateFromSelect(
	ctx context.Context,
	vcursor VCursor,
	rows []sqltypes.Row,
) (insertID int64, err error) {
	if ir.Generate == nil {
		return 0, nil
	}
	var count int64
	offset := ir.Generate.Offset
	genColPresent := offset < len(rows[0])
	if genColPresent {
		for _, row := range rows {
			if shouldGenerate(row[offset]) {
				count++
			}
		}
	} else {
		count = int64(len(rows))
	}

	if count == 0 {
		return 0, nil
	}

	insertID, err = ir.execGenerate(ctx, vcursor, count)
	if err != nil {
		return 0, err
	}

	used := insertID
	for idx, val := range rows {
		if genColPresent {
			if shouldGenerate(val[offset]) {
				val[offset] = sqltypes.NewInt64(used)
				used++
			}
		} else {
			rows[idx] = append(val, sqltypes.NewInt64(used))
			used++
		}
	}

	return insertID, nil
}

// processGenerateFromValues generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ir *InsertRows) processGenerateFromValues(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (insertID int64, err error) {
	if ir.Generate == nil {
		return 0, nil
	}

	// Scan input values to compute the number of values to generate, and
	// keep track of where they should be filled.
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	resolved, err := env.Evaluate(ir.Generate.Values)
	if err != nil {
		return 0, err
	}
	count := int64(0)
	values := resolved.TupleValues()
	for _, val := range values {
		if shouldGenerate(val) {
			count++
		}
	}

	// If generation is needed, generate the requested number of values (as one call).
	if count != 0 {
		insertID, err = ir.execGenerate(ctx, vcursor, count)
		if err != nil {
			return 0, err
		}
	}

	// Fill the holes where no value was supplied.
	cur := insertID
	for i, v := range values {
		if shouldGenerate(v) {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.Int64BindVariable(cur)
			cur++
		} else {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.ValueBindVariable(v)
		}
	}
	return insertID, nil
}

func (ir *InsertRows) execGenerate(ctx context.Context, vcursor VCursor, count int64) (int64, error) {
	// If generation is needed, generate the requested number of values (as one call).
	rss, _, err := vcursor.ResolveDestinations(ctx, ir.Generate.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return 0, err
	}
	if len(rss) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "auto sequence generation can happen through single shard only, it is getting routed to %d shards", len(rss))
	}
	bindVars := map[string]*querypb.BindVariable{nextValBV: sqltypes.Int64BindVariable(count)}
	qr, err := vcursor.ExecuteStandalone(ctx, nil, ir.Generate.Query, bindVars, rss[0])
	if err != nil {
		return 0, err
	}
	// If no rows are returned, it's an internal error, and the code
	// must panic, which will be caught and reported.
	return qr.Rows[0][0].ToCastInt64()
}

// shouldGenerate determines if a sequence value should be generated for a given value
func shouldGenerate(v sqltypes.Value) bool {
	if v.IsNull() {
		return true
	}

	// Unless the NO_AUTO_VALUE_ON_ZERO sql mode is active in mysql, it also
	// treats 0 as a value that should generate a new sequence.
	value, err := evalengine.CoerceTo(v, sqltypes.Uint64)
	if err != nil {
		return false
	}

	id, err := value.ToCastUint64()
	if err != nil {
		return false
	}

	return id == 0
}
