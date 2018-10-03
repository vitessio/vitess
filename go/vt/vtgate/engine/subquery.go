/*
Copyright 2017 Google Inc.

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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Subquery)(nil)

// Subquery specifies the parameters for a subquery primitive.
type Subquery struct {
	// Cols defines the column numbers from the underlying primitive
	// to be returned.
	Cols     []int
	Subquery Primitive
}

// RouteType returns a description of the query routing type used by the primitive
func (sq *Subquery) RouteType() string {
	return sq.Subquery.RouteType()
}

// Execute performs a non-streaming exec.
func (sq *Subquery) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	inner, err := sq.Subquery.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return sq.buildResult(inner), nil
}

// StreamExecute performs a streaming exec.
func (sq *Subquery) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return sq.Subquery.StreamExecute(vcursor, bindVars, wantfields, func(inner *sqltypes.Result) error {
		return callback(sq.buildResult(inner))
	})
}

// GetFields fetches the field info.
func (sq *Subquery) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	inner, err := sq.Subquery.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{Fields: sq.buildFields(inner)}, nil
}

// buildResult builds a new result by pulling the necessary columns from
// the subquery in the requested order.
func (sq *Subquery) buildResult(inner *sqltypes.Result) *sqltypes.Result {
	result := &sqltypes.Result{Fields: sq.buildFields(inner)}
	result.Rows = make([][]sqltypes.Value, 0, len(inner.Rows))
	for _, innerRow := range inner.Rows {
		row := make([]sqltypes.Value, 0, len(sq.Cols))
		for _, col := range sq.Cols {
			row = append(row, innerRow[col])
		}
		result.Rows = append(result.Rows, row)
	}
	result.RowsAffected = inner.RowsAffected
	return result
}

func (sq *Subquery) buildFields(inner *sqltypes.Result) []*querypb.Field {
	if len(inner.Fields) == 0 {
		return nil
	}
	fields := make([]*querypb.Field, 0, len(sq.Cols))
	for _, col := range sq.Cols {
		fields = append(fields, inner.Fields[col])
	}
	return fields
}
