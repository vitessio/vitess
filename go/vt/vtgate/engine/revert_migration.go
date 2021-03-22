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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*RevertMigration)(nil)

//RevertMigration represents the instructions to perform an online schema change via vtctld
type RevertMigration struct {
	Keyspace *vindexes.Keyspace
	Stmt     *sqlparser.RevertMigration
	Query    string

	noTxNeeded

	noInputs
}

func (v *RevertMigration) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "RevertMigration",
		Keyspace:     v.Keyspace,
		Other: map[string]interface{}{
			"query": v.Query,
		},
	}
}

// RouteType implements the Primitive interface
func (v *RevertMigration) RouteType() string {
	return "RevertMigration"
}

// GetKeyspaceName implements the Primitive interface
func (v *RevertMigration) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *RevertMigration) GetTableName() string {
	return ""
}

// Execute implements the Primitive interface
func (v *RevertMigration) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	sql := fmt.Sprintf("revert %s", v.Stmt.UUID)
	onlineDDL, err := schema.NewOnlineDDL(v.GetKeyspaceName(), "", sql, schema.DDLStrategyOnline, "", fmt.Sprintf("vtgate:%s", vcursor.Session().GetSessionUUID()))
	if err != nil {
		return result, err
	}
	err = vcursor.SubmitOnlineDDL(onlineDDL)
	if err != nil {
		return result, err
	}
	rows := [][]sqltypes.Value{}
	rows = append(rows, []sqltypes.Value{
		sqltypes.NewVarChar(onlineDDL.UUID),
	})
	result = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "uuid",
				Type: sqltypes.VarChar,
			},
		},
		Rows: rows,
	}
	return result, err
}

//StreamExecute implements the Primitive interface
func (v *RevertMigration) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := v.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
}

//GetFields implements the Primitive interface
func (v *RevertMigration) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields is not reachable")
}
