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
)

var _ Primitive = (*ReplaceVariables)(nil)

// ReplaceVariables is used in SHOW VARIABLES statements so that it replaces the values for vitess-aware variables
type ReplaceVariables struct {
	Input Primitive
	noTxNeeded
}

// NewReplaceVariables is used to create a new ReplaceVariables primitive
func NewReplaceVariables(input Primitive) *ReplaceVariables {
	return &ReplaceVariables{Input: input}
}

// RouteType implements the Primitive interface
func (r *ReplaceVariables) RouteType() string {
	return r.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (r *ReplaceVariables) GetKeyspaceName() string {
	return r.Input.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (r *ReplaceVariables) GetTableName() string {
	return r.Input.GetTableName()
}

// Execute implements the Primitive interface
func (r *ReplaceVariables) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := r.Input.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	replaceVariables(qr, bindVars)
	return qr, nil
}

// StreamExecute implements the Primitive interface
func (r *ReplaceVariables) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	innerCallback := callback
	callback = func(result *sqltypes.Result) error {
		replaceVariables(result, bindVars)
		return innerCallback(result)
	}
	return r.Input.StreamExecute(vcursor, bindVars, wantfields, callback)
}

// GetFields implements the Primitive interface
func (r *ReplaceVariables) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Input.GetFields(vcursor, bindVars)
}

// Inputs implements the Primitive interface
func (r *ReplaceVariables) Inputs() []Primitive {
	return []Primitive{r.Input}
}

// description implements the Primitive interface
func (r *ReplaceVariables) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "ReplaceVariables",
	}
}

func replaceVariables(qr *sqltypes.Result, bindVars map[string]*querypb.BindVariable) {
	for i, row := range qr.Rows {
		variableName := row[0].ToString()
		res, found := bindVars["__vt"+variableName]
		if found {
			qr.Rows[i][1] = sqltypes.NewVarChar(string(res.GetValue()))
		}
	}
}
