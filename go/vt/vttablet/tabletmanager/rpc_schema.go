/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// GetSchema returns the schema.
func (agent *ActionAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return agent.MysqlDaemon.GetSchema(topoproto.TabletDbName(agent.Tablet()), tables, excludeTables, includeViews)
}

// ReloadSchema will reload the schema
// This doesn't need the action mutex because periodic schema reloads happen
// in the background anyway.
func (agent *ActionAgent) ReloadSchema(ctx context.Context, waitPosition string) error {
	if agent.DBConfigs.IsZero() {
		// we skip this for test instances that can't connect to the DB anyway
		return nil
	}

	if waitPosition != "" {
		pos, err := mysql.DecodePosition(waitPosition)
		if err != nil {
			return vterrors.Wrapf(err, "ReloadSchema: can't parse wait position (%q)", waitPosition)
		}
		log.Infof("ReloadSchema: waiting for replication position: %v", waitPosition)
		if err := agent.MysqlDaemon.WaitMasterPos(ctx, pos); err != nil {
			return err
		}
	}

	log.Infof("ReloadSchema requested via RPC")
	return agent.QueryServiceControl.ReloadSchema(ctx)
}

// PreflightSchema will try out the schema changes in "changes".
func (agent *ActionAgent) PreflightSchema(ctx context.Context, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if err := agent.lock(ctx); err != nil {
		return nil, err
	}
	defer agent.unlock()

	// get the db name from the tablet
	dbName := topoproto.TabletDbName(agent.Tablet())

	// and preflight the change
	return agent.MysqlDaemon.PreflightSchemaChange(dbName, changes)
}

// ApplySchema will apply a schema change
func (agent *ActionAgent) ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	if err := agent.lock(ctx); err != nil {
		return nil, err
	}
	defer agent.unlock()

	// get the db name from the tablet
	dbName := topoproto.TabletDbName(agent.Tablet())

	// apply the change
	scr, err := agent.MysqlDaemon.ApplySchemaChange(dbName, change)
	if err != nil {
		return nil, err
	}

	// and if it worked, reload the schema
	agent.ReloadSchema(ctx, "")
	return scr, nil
}
