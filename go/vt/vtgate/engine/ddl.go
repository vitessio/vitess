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

var _ Primitive = (*DDL)(nil)

//DDL represents the a DDL statement, either normal or online DDL
type DDL struct {
	Keyspace *vindexes.Keyspace
	SQL      string
	DDL      *sqlparser.DDL

	NormalDDL *Send
	OnlineDDL *OnlineDDL

	noTxNeeded

	noInputs
}

func (v *DDL) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "DDL",
		Keyspace:     v.Keyspace,
		Other: map[string]interface{}{
			"query": sqlparser.String(v.DDL),
		},
	}
}

//RouteType implements the Primitive interface
func (v *DDL) RouteType() string {
	return "DDL"
}

//GetKeyspaceName implements the Primitive interface
func (v *DDL) GetKeyspaceName() string {
	return v.Keyspace.Name
}

//GetTableName implements the Primitive interface
func (v *DDL) GetTableName() string {
	return v.DDL.Table.Name.String()
}

//Execute implements the Primitive interface
func (v *DDL) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	if sqlparser.IsOnlineSchemaDDL(v.DDL, v.SQL) {
		return v.OnlineDDL.Execute(vcursor, bindVars, wantfields)
	}
	//TODO (shlomi): check session variable
	return v.NormalDDL.Execute(vcursor, bindVars, wantfields)
}

//StreamExecute implements the Primitive interface
func (v *DDL) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // TODO: shlomi - have no idea if this should work
}

//GetFields implements the Primitive interface
func (v *DDL) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // TODO: shlomi - have no idea if this should work
}
