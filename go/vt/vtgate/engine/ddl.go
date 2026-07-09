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
	"vitess.io/vitess/go/vt/vtgate/dynamicconfig"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*DDL)(nil)

// DDL represents a DDL statement, either normal or online DDL
type DDL struct {
	noTxNeeded
	noInputs

	Keyspace *vindexes.Keyspace
	SQL      string
	DDL      sqlparser.DDLStatement

	NormalDDL *Send
	OnlineDDL *OnlineDDL

	Config dynamicconfig.DDL

	// TempTableDDL marks a temporary-table DDL (CREATE or DROP TEMPORARY).
	// Such statements must run on the session's reserved connection and must
	// not implicitly commit or take the online-DDL path.
	TempTableDDL bool
}

func (ddl *DDL) description() PrimitiveDescription {
	other := map[string]any{
		"Query": ddl.SQL,
	}
	if ddl.TempTableDDL {
		other["TempTable"] = true
	}
	return PrimitiveDescription{
		OperatorType: "DDL",
		Keyspace:     ddl.Keyspace,
		Other:        other,
	}
}

// IsOnlineSchemaDDL returns true if the query is an online schema change DDL
func (ddl *DDL) isOnlineSchemaDDL() bool {
	switch ddl.DDL.GetAction() {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction, sqlparser.AlterDDLAction:
		if ddl.OnlineDDL == nil || ddl.OnlineDDL.DDLStrategySetting == nil {
			return false
		}
		return !ddl.OnlineDDL.DDLStrategySetting.Strategy.IsDirect()
	}
	return false
}

// TryExecute implements the Primitive interface
func (ddl *DDL) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	if ddl.TempTableDDL {
		// A temporary-table DDL must not implicitly commit the open
		// transaction (MySQL does not) and must not take the online-DDL
		// path, so it runs here rather than falling through.
		_, isCreate := ddl.DDL.(*sqlparser.CreateTable)
		if isCreate {
			// Only a CREATE needs to reserve a connection; a DROP of an
			// existing temp table is already routed to the session's
			// reserved connection, and a drop-only session (e.g. DROP
			// TEMPORARY TABLE IF EXISTS) must not reserve one at all.
			vcursor.Session().NeedsReservedConn()
		}
		result, err = vcursor.ExecutePrimitive(ctx, ddl.NormalDDL, bindVars, wantfields)
		// Mark the session as holding temp tables for a CREATE even if it
		// returned an error: the tablet may have reserved a connection and
		// created the table before failing (the reserved id is recorded on
		// error), and losing that table to the idle timeout is worse than
		// the cost of marking. That cost is only that plan caching is
		// disabled for this connection — heartbeats are keyed on the reserved
		// connection actually existing (a create that reserved nothing is
		// never beaten), so a fully-failed create produces no keepalives. A
		// DROP never marks (a drop-only session created nothing).
		if isCreate {
			vcursor.Session().HasCreatedTempTable()
		}
		return result, err
	}

	// Commit any open transaction before executing the ddl query.
	if err = vcursor.Session().Commit(ctx); err != nil {
		return nil, err
	}

	ddlStrategySetting, err := schema.ParseDDLStrategy(vcursor.Session().GetDDLStrategy())
	if err != nil {
		return nil, err
	}
	ddl.OnlineDDL.DDLStrategySetting = ddlStrategySetting

	switch {
	case ddl.isOnlineSchemaDDL():
		if !ddl.Config.OnlineEnabled() {
			return nil, schema.ErrOnlineDDLDisabled
		}
		return vcursor.ExecutePrimitive(ctx, ddl.OnlineDDL, bindVars, wantfields)
	default: // non online-ddl
		if !ddl.Config.DirectEnabled() {
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
