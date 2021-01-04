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
	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// GetSchema returns the schema.
func (tm *TabletManager) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tm.MysqlDaemon.GetSchema(ctx, topoproto.TabletDbName(tm.Tablet()), tables, excludeTables, includeViews)
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
		pos, err := mysql.DecodePosition(waitPosition)
		if err != nil {
			return vterrors.Wrapf(err, "ReloadSchema: can't parse wait position (%q)", waitPosition)
		}
		log.Infof("ReloadSchema: waiting for replication position: %v", waitPosition)
		if err := tm.MysqlDaemon.WaitMasterPos(ctx, pos); err != nil {
			return err
		}
	}

	log.Infof("ReloadSchema requested via RPC")
	return tm.QueryServiceControl.ReloadSchema(ctx)
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

	// get the db name from the tablet
	dbName := topoproto.TabletDbName(tm.Tablet())

	// apply the change
	scr, err := tm.MysqlDaemon.ApplySchemaChange(ctx, dbName, change)
	if err != nil {
		return nil, err
	}

	// and if it worked, reload the schema
	tm.ReloadSchema(ctx, "")
	return scr, nil
}
