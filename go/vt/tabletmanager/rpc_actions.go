// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"regexp"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topotools"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the implementations of RPCAgent methods.
// Major groups of methods are broken out into files named "rpc_*.go".

// Ping makes sure RPCs work, and refreshes the tablet record.
func (agent *ActionAgent) Ping(ctx context.Context, args string) string {
	return args
}

// GetPermissions returns the db permissions.
func (agent *ActionAgent) GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error) {
	return mysqlctl.GetPermissions(agent.MysqlDaemon)
}

// SetReadOnly makes the mysql instance read-only or read-write.
func (agent *ActionAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	return agent.MysqlDaemon.SetReadOnly(rdonly)
}

// ChangeType changes the tablet type
func (agent *ActionAgent) ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	// change our type in the topology
	_, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, tabletType)
	if err != nil {
		return err
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "ChangeType"); err != nil {
		return err
	}

	// Let's see if we need to fix semi-sync acking.
	if err := agent.fixSemiSyncAndReplication(agent.Tablet().Type); err != nil {
		return fmt.Errorf("fixSemiSyncAndReplication failed, may not ack correctly: %v", err)
	}

	// and re-run health check
	agent.runHealthCheckLocked()
	return nil
}

// Sleep sleeps for the duration
func (agent *ActionAgent) Sleep(ctx context.Context, duration time.Duration) {
	if err := agent.lock(ctx); err != nil {
		// client gave up
		return
	}
	defer agent.unlock()

	time.Sleep(duration)
}

// ExecuteHook executes the provided hook locally, and returns the result.
func (agent *ActionAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	if err := agent.lock(ctx); err != nil {
		// client gave up
		return &hook.HookResult{}
	}
	defer agent.unlock()

	// Execute the hooks
	topotools.ConfigureTabletHook(hk, agent.TabletAlias)
	hr := hk.Execute()

	// We never know what the hook did, so let's refresh our state.
	if err := agent.refreshTablet(ctx, "ExecuteHook"); err != nil {
		log.Errorf("refreshTablet after ExecuteHook failed: %v", err)
	}

	return hr
}

// RefreshState reload the tablet record from the topo server.
func (agent *ActionAgent) RefreshState(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	return agent.refreshTablet(ctx, "RefreshState")
}

// RunHealthCheck will manually run the health check on the tablet.
func (agent *ActionAgent) RunHealthCheck(ctx context.Context) {
	agent.runHealthCheck()
}

// IgnoreHealthError sets the regexp for health check errors to ignore.
func (agent *ActionAgent) IgnoreHealthError(ctx context.Context, pattern string) error {
	var expr *regexp.Regexp
	if pattern != "" {
		var err error
		if expr, err = regexp.Compile(fmt.Sprintf("^%s$", pattern)); err != nil {
			return err
		}
	}
	agent.mutex.Lock()
	agent._ignoreHealthErrorExpr = expr
	agent.mutex.Unlock()
	return nil
}
