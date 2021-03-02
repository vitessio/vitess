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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*AlterVSchema)(nil)

//AlterVSchema operator applies changes to VSchema
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
		Other: map[string]interface{}{
			"query": sqlparser.String(v.AlterVschemaDDL),
		},
	}
}

//RouteType implements the Primitive interface
func (v *AlterVSchema) RouteType() string {
	return "AlterVSchema"
}

//GetKeyspaceName implements the Primitive interface
func (v *AlterVSchema) GetKeyspaceName() string {
	return v.Keyspace.Name
}

//GetTableName implements the Primitive interface
func (v *AlterVSchema) GetTableName() string {
	return v.AlterVschemaDDL.Table.Name.String()
}

//Execute implements the Primitive interface
func (v *AlterVSchema) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	err := vcursor.ExecuteVSchema(v.Keyspace.Name, v.AlterVschemaDDL)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

//StreamExecute implements the Primitive interface
func (v *AlterVSchema) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Alter vschema not supported in streaming")
}

//GetFields implements the Primitive interface
func (v *AlterVSchema) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.NewErrorf(vtrpcpb.Code_UNIMPLEMENTED, vterrors.UnsupportedPS, "This command is not supported in the prepared statement protocol yet")
}
