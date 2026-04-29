/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// GetSchema returns the schema.
func (tm *TabletManager) GetSchema(ctx context.Context, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tm.MysqlDaemon.GetSchema(ctx, topoproto.TabletDbName(tm.Tablet()), request)
}

// ReloadSchema will reload the schema
// This doesn't need the action mutex because periodic schema reloads happen
// in the background anyway.
func (tm *TabletManager) ReloadSchema(ctx context.Context, waitPosition string) error {
	if tm.DBConfigs.IsZero() {
		// we skip this for test instances that can't connect to the DB anyway
		return nil
	}

	if waitPosition != "" {
		pos, err := replication.DecodePosition(waitPosition)
		if err != nil {
			return vterrors.Wrapf(err, "ReloadSchema: can't parse wait position (%q)", waitPosition)
		}
		log.Info(fmt.Sprintf("ReloadSchema: waiting for replication position: %v", waitPosition))
		if err := tm.MysqlDaemon.WaitSourcePos(ctx, pos); err != nil {
			return err
		}
	}

	log.Info("ReloadSchema requested via RPC")
	return tm.QueryServiceControl.ReloadSchema(ctx)
}

// ResetSequences will reset the auto-inc counters on the specified tables.
func (tm *TabletManager) ResetSequences(ctx context.Context, tables []string) error {
	return tm.QueryServiceControl.SchemaEngine().ResetSequences(tables)
}

// PreflightSchema will try out the schema changes in "changes".
func (tm *TabletManager) PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	// get the db name from the tablet
	dbName := topoproto.TabletDbName(tm.Tablet())

	// and preflight the change
	return tm.MysqlDaemon.PreflightSchemaChange(ctx, dbName, changes)
}

// ApplySchema will apply a schema change
func (tm *TabletManager) ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	// Reject any CREATE TABLE that would push the schema engine past its
	// configured table-count limit before we touch mysqld.
	if err := checkCreateTableLimitForSQL(tm.Env.Parser(), tm.QueryServiceControl.SchemaEngine(), change.SQL); err != nil {
		return nil, err
	}

	// get the db name from the tablet
	dbName := topoproto.TabletDbName(tm.Tablet())

	// apply the change
	scr, err := tm.MysqlDaemon.ApplySchemaChange(ctx, dbName, change)
	if err != nil {
		return nil, err
	}

	// and if it worked, reload the schema
	tm.ReloadSchema(ctx, "") //nolint:errcheck
	return scr, nil
}

// checkCreateTableLimitForSQL splits the given multi-statement SQL into
// individual statements and applies the schema engine's CREATE TABLE
// table-count gate to each one. Statements that fail to parse are skipped
// so we never reject otherwise-valid SQL on parser quirks.
func checkCreateTableLimitForSQL(parser *sqlparser.Parser, se *schema.Engine, sql string) error {
	queries, err := parser.SplitStatementToPieces(sql)
	if err != nil {
		return nil
	}
	for _, query := range queries {
		stmt, parseErr := parser.Parse(query)
		if parseErr != nil {
			continue
		}
		if err := schema.CheckCreateTableLimit(se, stmt); err != nil {
			return err
		}
	}
	return nil
}
