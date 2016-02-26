// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// GetSchema returns the schema.
// Should be called under RPCWrap.
func (agent *ActionAgent) GetSchema(ctx context.Context, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return agent.MysqlDaemon.GetSchema(topoproto.TabletDbName(agent.Tablet()), tables, excludeTables, includeViews)
}

// ReloadSchema will reload the schema
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) ReloadSchema(ctx context.Context) {
	if agent.DBConfigs.IsZero() {
		// we skip this for test instances that can't connect to the DB anyway
		return
	}

	// This adds a dependency between tabletmanager and tabletserver,
	// so it's not ideal. But I (alainjobart) think it's better
	// to have up to date schema in vttablet.
	agent.QueryServiceControl.ReloadSchema()
}

// PreflightSchema will try out the schema change
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) PreflightSchema(ctx context.Context, change string) (*tmutils.SchemaChangeResult, error) {
	// get the db name from the tablet
	dbName := topoproto.TabletDbName(agent.Tablet())

	// and preflight the change
	return agent.MysqlDaemon.PreflightSchemaChange(dbName, change)
}

// ApplySchema will apply a schema change
// Should be called under RPCWrapLockAction.
func (agent *ActionAgent) ApplySchema(ctx context.Context, change *tmutils.SchemaChange) (*tmutils.SchemaChangeResult, error) {
	// get the db name from the tablet
	dbName := topoproto.TabletDbName(agent.Tablet())

	// apply the change
	scr, err := agent.MysqlDaemon.ApplySchemaChange(dbName, change)
	if err != nil {
		return nil, err
	}

	// and if it worked, reload the schema
	agent.ReloadSchema(ctx)
	return scr, nil
}
