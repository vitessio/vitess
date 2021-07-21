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
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*ShowMigrationLogs)(nil)

// ShowMigrationLogs represents the instructions to perform an online schema change via vtctld
type ShowMigrationLogs struct {
	Keyspace          *vindexes.Keyspace
	Stmt              *sqlparser.ShowMigrationLogs
	Query             string
	TargetDestination key.Destination

	noTxNeeded

	noInputs
}

func (v *ShowMigrationLogs) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "ShowMigrationLogs",
		Keyspace:     v.Keyspace,
		Other: map[string]interface{}{
			"query": v.Query,
		},
	}
}

// RouteType implements the Primitive interface
func (v *ShowMigrationLogs) RouteType() string {
	return "ShowMigrationLogs"
}

// GetKeyspaceName implements the Primitive interface
func (v *ShowMigrationLogs) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *ShowMigrationLogs) GetTableName() string {
	return ""
}

// Execute implements the Primitive interface
func (v *ShowMigrationLogs) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	s := Send{
		Keyspace:          v.Keyspace,
		TargetDestination: v.TargetDestination,
		Query:             v.Query,
		IsDML:             false,
		SingleShardOnly:   false,
	}
	return s.Execute(vcursor, bindVars, wantfields)
}

//StreamExecute implements the Primitive interface
func (v *ShowMigrationLogs) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := v.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
}

//GetFields implements the Primitive interface
func (v *ShowMigrationLogs) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields is not reachable")
}
