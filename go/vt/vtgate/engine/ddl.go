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
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*DDL)(nil)

// DDL represents a DDL statement, either normal or online DDL
type DDL struct {
	Keyspace *vindexes.Keyspace
	SQL      string
	DDL      sqlparser.DDLStatement

	NormalDDL *Send
	OnlineDDL *OnlineDDL

	DirectDDLEnabled bool
	OnlineDDLEnabled bool

	CreateTempTable bool

	noTxNeeded

	noInputs
}

func (ddl *DDL) description() PrimitiveDescription {
	other := map[string]any{
		"Query": ddl.SQL,
	}
	if ddl.CreateTempTable {
		other["TempTable"] = true
	}
	return PrimitiveDescription{
		OperatorType: "DDL",
		Keyspace:     ddl.Keyspace,
		Other:        other,
	}
}

// RouteType implements the Primitive interface
func (ddl *DDL) RouteType() string {
	return "DDL"
}

// GetKeyspaceName implements the Primitive interface
func (ddl *DDL) GetKeyspaceName() string {
	return ddl.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (ddl *DDL) GetTableName() string {
	return ddl.DDL.GetTable().Name.String()
}

// IsOnlineSchemaDDL returns true if the query is an online schema change DDL
func (ddl *DDL) isOnlineSchemaDDL() bool {
	switch ddl.DDL.GetAction() {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction, sqlparser.AlterDDLAction:
		return !ddl.OnlineDDL.DDLStrategySetting.Strategy.IsDirect()
	}
	return false
}

// TryExecute implements the Primitive interface
func (ddl *DDL) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	if ddl.CreateTempTable {
		vcursor.Session().HasCreatedTempTable()
		vcursor.Session().NeedsReservedConn()
		return vcursor.ExecutePrimitive(ctx, ddl.NormalDDL, bindVars, wantfields)
	}

	ddlStrategySetting, err := schema.ParseDDLStrategy(vcursor.Session().GetDDLStrategy())
	if err != nil {
		return nil, err
	}
	ddl.OnlineDDL.DDLStrategySetting = ddlStrategySetting

	switch {
	case ddl.isOnlineSchemaDDL():
		if !ddl.OnlineDDLEnabled {
			return nil, schema.ErrOnlineDDLDisabled
		}
		return vcursor.ExecutePrimitive(ctx, ddl.OnlineDDL, bindVars, wantfields)
	default: // non online-ddl
		if !ddl.DirectDDLEnabled {
			return nil, schema.ErrDirectDDLDisabled
		}
		return vcursor.ExecutePrimitive(ctx, ddl.NormalDDL, bindVars, wantfields)
	}
}

// TryStreamExecute implements the Primitive interface
func (ddl *DDL) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := ddl.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
}

// GetFields implements the Primitive interface
func (ddl *DDL) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields in not reachable")
}
