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
	"vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*AlterVSchema)(nil)

// AlterVSchema operator applies changes to VSchema
type AlterVSchema struct {
	Keyspace *vindexes.Keyspace

	AlterVschemaDDL *sqlparser.AlterVschema

	noTxNeeded

	noInputs
}

func (v *AlterVSchema) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "AlterVSchema",
		Keyspace:     v.Keyspace,
		Other: map[string]any{
			"query": sqlparser.String(v.AlterVschemaDDL),
		},
	}
}

// RouteType implements the Primitive interface
func (v *AlterVSchema) RouteType() string {
	return "AlterVSchema"
}

// GetKeyspaceName implements the Primitive interface
func (v *AlterVSchema) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *AlterVSchema) GetTableName() string {
	return v.AlterVschemaDDL.Table.Name.String()
}

// TryExecute implements the Primitive interface
func (v *AlterVSchema) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	err := vcursor.ExecuteVSchema(ctx, v.Keyspace.Name, v.AlterVschemaDDL)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements the Primitive interface
func (v *AlterVSchema) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := v.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields implements the Primitive interface
func (v *AlterVSchema) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.NewErrorf(vtrpcpb.Code_UNIMPLEMENTED, vterrors.UnsupportedPS, "This command is not supported in the prepared statement protocol yet")
}
