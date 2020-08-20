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
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*OnlineDDL)(nil)

//OnlineDDL represents the instructions to perform an onlins chema change via vtctld
type OnlineDDL struct {
	Keyspace *vindexes.Keyspace
	DDL      *sqlparser.DDL
	SQL      string
	Strategy sqlparser.DDLStrategy
	Options  string

	noTxNeeded

	noInputs
}

func (v *OnlineDDL) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "OnlineDDL",
		Keyspace:     v.Keyspace,
		Other: map[string]interface{}{
			"query": sqlparser.String(v.DDL),
		},
	}
}

//RouteType implements the Primitive interface
func (v *OnlineDDL) RouteType() string {
	return "OnlineDDL"
}

//GetKeyspaceName implements the Primitive interface
func (v *OnlineDDL) GetKeyspaceName() string {
	return v.Keyspace.Name
}

//GetTableName implements the Primitive interface
func (v *OnlineDDL) GetTableName() string {
	return v.DDL.Table.Name.String()
}

//Execute implements the Primitive interface
func (v *OnlineDDL) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	onlineDDL, err := schema.NewOnlineDDL(v.GetKeyspaceName(), v.GetTableName(), v.SQL, v.Strategy, v.Options)
	if err != nil {
		return result, err
	}
	err = vcursor.SubmitOnlineDDL(onlineDDL)
	if err != nil {
		return result, err
	}

	result = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "uuid",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary(onlineDDL.UUID),
			},
		},
		RowsAffected: 1,
	}
	return result, err
}

//StreamExecute implements the Primitive interface
func (v *OnlineDDL) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // TODO: shlomi - have no idea if this should work
}

//GetFields implements the Primitive interface
func (v *OnlineDDL) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // TODO: shlomi - have no idea if this should work
}
