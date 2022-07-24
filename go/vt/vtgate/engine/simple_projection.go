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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*SimpleProjection)(nil)

// SimpleProjection selects which columns to keep from the input
type SimpleProjection struct {
	// Cols defines the column numbers from the underlying primitive
	// to be returned.
	Cols  []int
	Input Primitive
}

// NeedsTransaction implements the Primitive interface
func (sc *SimpleProjection) NeedsTransaction() bool {
	return sc.Input.NeedsTransaction()
}

// RouteType returns a description of the query routing type used by the primitive
func (sc *SimpleProjection) RouteType() string {
	return sc.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (sc *SimpleProjection) GetKeyspaceName() string {
	return sc.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (sc *SimpleProjection) GetTableName() string {
	return sc.Input.GetTableName()
}

// TryExecute performs a non-streaming exec.
func (sc *SimpleProjection) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	inner, err := vcursor.ExecutePrimitive(ctx, sc.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return sc.buildResult(inner), nil
}

// TryStreamExecute performs a streaming exec.
func (sc *SimpleProjection) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return vcursor.StreamExecutePrimitive(ctx, sc.Input, bindVars, wantfields, func(inner *sqltypes.Result) error {
		return callback(sc.buildResult(inner))
	})
}

// GetFields fetches the field info.
func (sc *SimpleProjection) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	inner, err := sc.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{Fields: sc.buildFields(inner)}, nil
}

// Inputs returns the input to this primitive
func (sc *SimpleProjection) Inputs() []Primitive {
	return []Primitive{sc.Input}
}

// buildResult builds a new result by pulling the necessary columns from
// the input in the requested order.
func (sc *SimpleProjection) buildResult(inner *sqltypes.Result) *sqltypes.Result {
	result := &sqltypes.Result{Fields: sc.buildFields(inner)}
	result.Rows = make([][]sqltypes.Value, 0, len(inner.Rows))
	for _, innerRow := range inner.Rows {
		row := make([]sqltypes.Value, 0, len(sc.Cols))
		for _, col := range sc.Cols {
			row = append(row, innerRow[col])
		}
		result.Rows = append(result.Rows, row)
	}
	result.RowsAffected = inner.RowsAffected
	return result
}

func (sc *SimpleProjection) buildFields(inner *sqltypes.Result) []*querypb.Field {
	if len(inner.Fields) == 0 {
		return nil
	}
	fields := make([]*querypb.Field, 0, len(sc.Cols))
	for _, col := range sc.Cols {
		fields = append(fields, inner.Fields[col])
	}
	return fields
}

func (sc *SimpleProjection) description() PrimitiveDescription {
	other := map[string]any{
		"Columns": sc.Cols,
	}
	return PrimitiveDescription{
		OperatorType: "SimpleProjection",
		Other:        other,
	}
}
