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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*CreateKeyspace)(nil)

//CreateKeyspace represents the instructions to create a new keyspace via the user issuing a "CREATE DATABASE" type
//statement. As the actual creation logic is outside of the scope of vitess, the request is submitted to a service.
type CreateKeyspace struct {
	Keyspace    string
	IfNotExists bool
	noTxNeeded
	noInputs
}

func (v *CreateKeyspace) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "CreateKeyspace",
		Keyspace:     nil,
		Other: map[string]interface{}{
			"keyspace": v.Keyspace,
		},
	}
}

//RouteType implements the Primitive interface
func (v *CreateKeyspace) RouteType() string {
	return "CreateKeyspace"
}

//GetKeyspaceName implements the Primitive interface
func (v *CreateKeyspace) GetKeyspaceName() string {
	return "" // FIXME: david - don't think this is reachable in a way that makes sense
}

//GetTableName implements the Primitive interface
func (v *CreateKeyspace) GetTableName() string {
	return "" // FIXME: david - don't this is reachable in a way that makes sense
}

//Execute implements the Primitive interface
func (v *CreateKeyspace) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	err = vcursor.ExecuteCreateKeyspace(v.Keyspace, v.IfNotExists)
	if err != nil {
		return nil, err
	}

	result = &sqltypes.Result{
		Fields: []*querypb.Field{},
		Rows: [][]sqltypes.Value{},
		RowsAffected: 1,
	}
	return result, err
}

//StreamExecute implements the Primitive interface
func (v *CreateKeyspace) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // FIXME: david - copied from online_ddl.go, also have no idea if this should work
}

//GetFields implements the Primitive interface
func (v *CreateKeyspace) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // FIXME: david - copied from online_ddl.go, also have no idea if this should work
}
