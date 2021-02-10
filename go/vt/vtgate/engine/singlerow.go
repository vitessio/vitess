/*
Copyright 2020 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*SingleRow)(nil)

// SingleRow defines an empty result
type SingleRow struct {
	noInputs
	noTxNeeded
}

// RouteType returns a description of the query routing type used by the primitive
func (s *SingleRow) RouteType() string {
	return ""
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (s *SingleRow) GetKeyspaceName() string {
	return ""
}

// GetTableName specifies the table that this primitive routes to.
func (s *SingleRow) GetTableName() string {
	return ""
}

// Execute performs a non-streaming exec.
func (s *SingleRow) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result := sqltypes.Result{
		Rows: [][]sqltypes.Value{
			{},
		},
	}
	return &result, nil
}

// StreamExecute performs a streaming exec.
func (s *SingleRow) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	result := sqltypes.Result{
		Rows: [][]sqltypes.Value{
			{},
		},
	}
	return callback(&result)
}

// GetFields fetches the field info.
func (s *SingleRow) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (s *SingleRow) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SingleRow",
	}
}
