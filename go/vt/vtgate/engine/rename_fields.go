/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*RenameFields)(nil)

// RenameFields is a primitive that renames the fields
type RenameFields struct {
	Cols    []string
	Indices []int
	Input   Primitive
	noTxNeeded
}

// NewRenameField creates a new rename field
func NewRenameField(cols []string, indices []int, input Primitive) (*RenameFields, error) {
	if len(cols) != len(indices) {
		return nil, vterrors.New(vtrpc.Code_INTERNAL, "Unequal length of columns and indices in RenameField primitive")
	}
	return &RenameFields{
		Cols:    cols,
		Indices: indices,
		Input:   input,
	}, nil
}

// RouteType implements the primitive interface
func (r *RenameFields) RouteType() string {
	return r.Input.RouteType()
}

// GetKeyspaceName implements the primitive interface
func (r *RenameFields) GetKeyspaceName() string {
	return r.Input.GetKeyspaceName()
}

// GetTableName implements the primitive interface
func (r *RenameFields) GetTableName() string {
	return r.Input.GetTableName()
}

// Execute implements the primitive interface
func (r *RenameFields) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := r.Input.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	if wantfields {
		r.renameFields(qr)
	}
	return qr, nil
}

func (r *RenameFields) renameFields(qr *sqltypes.Result) {
	for ind, index := range r.Indices {
		colName := r.Cols[ind]
		qr.Fields[index].Name = colName
	}
}

// StreamExecute implements the primitive interface
func (r *RenameFields) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if wantfields {
		innerCallback := callback
		callback = func(result *sqltypes.Result) error {
			r.renameFields(result)
			return innerCallback(result)
		}
	}
	return r.Input.StreamExecute(vcursor, bindVars, wantfields, callback)
}

// GetFields implements the primitive interface
func (r *RenameFields) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := r.Input.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	r.renameFields(qr)
	return qr, nil
}

// Inputs implements the primitive interface
func (r *RenameFields) Inputs() []Primitive {
	return []Primitive{r.Input}
}

// description implements the primitive interface
func (r *RenameFields) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "RenameFields",
		Other: map[string]interface{}{
			"Indices": r.Indices,
			"Columns": r.Cols,
		},
	}
}
