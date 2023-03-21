/*
Copyright 2023 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*ShowMigrations)(nil)

// TODO: (shlomi) remove in v18
// Backwards compatibility: this is introduced in v17, But vttablets may still be on v16 and
// are not aware of this new statement. The logic in this primitive supports both new and old options.
// In v18 this struct should be removed and replaced with a simple Send (see buildShowVMigrationsPlan())

// ShowMigrations represents the instructions to perform an online schema change via vtctld
type ShowMigrations struct {
	Keyspace          *vindexes.Keyspace
	Stmt              *sqlparser.ShowMigrations
	Query             string
	TargetDestination key.Destination

	noTxNeeded

	noInputs
}

func (v *ShowMigrations) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "ShowMigrations",
		Keyspace:     v.Keyspace,
		Other: map[string]any{
			"query": v.Query,
		},
	}
}

// RouteType implements the Primitive interface
func (v *ShowMigrations) RouteType() string {
	return "ShowMigrations"
}

// GetKeyspaceName implements the Primitive interface
func (v *ShowMigrations) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *ShowMigrations) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (v *ShowMigrations) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	s := Send{
		Keyspace:          v.Keyspace,
		TargetDestination: v.TargetDestination,
		Query:             v.Query,
		IsDML:             false,
		SingleShardOnly:   false,
	}
	result, err = vcursor.ExecutePrimitive(ctx, &s, bindVars, wantfields)
	if err == nil {
		return result, nil
	}
	// TODO: (shlomi) remove in v18
	// Backwards compatibility: in v17 we introduce ShowMigrations ast statement. But vttablets may still be on v16 and
	// are not aware of this new statement. In such case they return a vtrpcpb.Code_INVALID_ARGUMENT.
	// If we intercept this return code, we fall back to a simple Send() with a generated query.
	if vterrors.Code(err) != vtrpcpb.Code_INVALID_ARGUMENT {
		return result, err
	}
	// Old method: construct a query here. Remove in v18.
	sidecarDBID, err := sidecardb.GetIdentifierForKeyspace(v.GetKeyspaceName())
	if err != nil {
		log.Errorf("Failed to read sidecar database identifier for keyspace %q from the cache: %v", v.GetKeyspaceName(), err)
		return nil, vterrors.VT14005(v.GetKeyspaceName())
	}

	sql := sqlparser.BuildParsedQuery("SELECT * FROM %s.schema_migrations", sidecarDBID).Query

	if v.Stmt.Filter != nil {
		if v.Stmt.Filter.Filter != nil {
			sql += fmt.Sprintf(" where %s", sqlparser.String(v.Stmt.Filter.Filter))
		} else if v.Stmt.Filter.Like != "" {
			lit := sqlparser.String(sqlparser.NewStrLiteral(v.Stmt.Filter.Like))
			sql += fmt.Sprintf(" where migration_uuid LIKE %s OR migration_context LIKE %s OR migration_status LIKE %s", lit, lit, lit)
		}
	}
	s = Send{
		Keyspace:          v.Keyspace,
		TargetDestination: v.TargetDestination,
		Query:             sql,
	}
	result, err = vcursor.ExecutePrimitive(ctx, &s, bindVars, wantfields)
	return result, err
}

// TryStreamExecute implements the Primitive interface
func (v *ShowMigrations) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := v.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
}

// GetFields implements the Primitive interface
func (v *ShowMigrations) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields is not reachable")
}
