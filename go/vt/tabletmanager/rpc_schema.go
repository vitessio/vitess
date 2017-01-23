// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
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
		pos, err := replication.DecodePosition(waitPosition)
		if err != nil {
			return fmt.Errorf("ReloadSchema: can't parse wait position (%q): %v", waitPosition, err)
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
