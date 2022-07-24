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
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

// TryExecute performs a non-streaming exec.
func (s *SingleRow) TryExecute(context.Context, VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	result := sqltypes.Result{
		Rows: [][]sqltypes.Value{
			{},
		},
	}
	return &result, nil
}

// TryStreamExecute performs a streaming exec.
func (s *SingleRow) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := s.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (s *SingleRow) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (s *SingleRow) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SingleRow",
	}
}
