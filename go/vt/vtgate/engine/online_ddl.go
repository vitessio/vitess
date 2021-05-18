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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*OnlineDDL)(nil)

// OnlineDDL represents the instructions to perform an online schema change via vtctld
type OnlineDDL struct {
	Keyspace           *vindexes.Keyspace
	DDL                sqlparser.DDLStatement
	SQL                string
	DDLStrategySetting *schema.DDLStrategySetting
	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	noTxNeeded

	noInputs
}

func (v *OnlineDDL) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "OnlineDDL",
		Keyspace:     v.Keyspace,
		Other: map[string]interface{}{
			"query": v.SQL,
		},
	}
}

// RouteType implements the Primitive interface
func (v *OnlineDDL) RouteType() string {
	return "OnlineDDL"
}

// GetKeyspaceName implements the Primitive interface
func (v *OnlineDDL) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *OnlineDDL) GetTableName() string {
	return v.DDL.GetTable().Name.String()
}

// Execute implements the Primitive interface
func (v *OnlineDDL) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	result = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "uuid",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	onlineDDLs, err := schema.NewOnlineDDLs(v.GetKeyspaceName(), v.SQL, v.DDL,
		v.DDLStrategySetting, fmt.Sprintf("vtgate:%s", vcursor.Session().GetSessionUUID()),
	)
	if err != nil {
		return result, err
	}
	for _, onlineDDL := range onlineDDLs {
		if onlineDDL.StrategySetting().IsSkipTopo() {
			// Go directly to tablets, much like Send primitive does
			s := Send{
				Keyspace:          v.Keyspace,
				TargetDestination: v.TargetDestination,
				Query:             onlineDDL.SQL,
				IsDML:             false,
				SingleShardOnly:   false,
			}
			if _, err := s.Execute(vcursor, bindVars, wantfields); err != nil {
				return result, err
			}
		} else {
			// Submit a request entry in topo. vtctld will take it from there
			if err := vcursor.SubmitOnlineDDL(onlineDDL); err != nil {
				return result, err
			}
		}
		result.Rows = append(result.Rows, []sqltypes.Value{
			sqltypes.NewVarChar(onlineDDL.UUID),
		})
	}
	return result, err
}

//StreamExecute implements the Primitive interface
func (v *OnlineDDL) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := v.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
}

//GetFields implements the Primitive interface
func (v *OnlineDDL) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields is not reachable")
}
