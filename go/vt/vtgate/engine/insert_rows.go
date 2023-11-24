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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

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

type insertRowsResult struct {
	rows     []sqltypes.Row
	insertID uint64
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
	if err != nil {
		return insertRowsResult{}, err
	}

	return insertRowsResult{
		rows:     res.Rows,
		insertID: 0, // TODO
	}, nil
}

func (ir *InsertRows) execSelectStreaming(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	callback func(result *sqltypes.Result) error,
) error {
	return vcursor.StreamExecutePrimitiveStandalone(ctx, ir.RowsFromSelect, bindVars, false, callback)
}
