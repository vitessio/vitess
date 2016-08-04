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

// TODO(alainjobart): all the calls mention something like:
// Should be called under RPCWrap.
// Eventually, when all calls are going through RPCs, we'll refactor
// this so there is only one wrapper, and the extra stuff done by the
// RPCWrapXXX methods will be done internally. Until then, it's safer
// to have the comment.

// Ping makes sure RPCs work, and refreshes the tablet record.
// Should be called under RPCWrap.
func (agent *ActionAgent) Ping(ctx context.Context, args string) string {
	return args
}

// GetPermissions returns the db permissions.
// Should be called under RPCWrap.
func (agent *ActionAgent) GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error) {
	return mysqlctl.GetPermissions(agent.MysqlDaemon)
}

// SetReadOnly makes the mysql instance read-only or read-write.
func (agent *ActionAgent) SetReadOnly(ctx context.Context, rdonly bool) error {
	if err := agent.lockAndCheck(ctx); err != nil {
		return err
	}
	defer agent.actionMutex.Unlock()

	return agent.MysqlDaemon.SetReadOnly(rdonly)
}

// ChangeType changes the tablet type
func (agent *ActionAgent) ChangeType(ctx context.Context, tabletType topodatapb.TabletType) error {
	if err := agent.lockAndCheck(ctx); err != nil {
		return err
	}
	defer agent.actionMutex.Unlock()

	// change our type in the topology
	_, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, tabletType)
	if err != nil {
		return err
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "ChangeType"); err != nil {
		return err
	}

	// and re-run health check
	agent.runHealthCheckProtected()
	return nil
}

// Sleep sleeps for the duration
func (agent *ActionAgent) Sleep(ctx context.Context, duration time.Duration) {
	if err := agent.lockAndCheck(ctx); err != nil {
		// client gave up
		return
	}
	defer agent.actionMutex.Unlock()

	time.Sleep(duration)
}

// ExecuteHook executes the provided hook locally, and returns the result.
func (agent *ActionAgent) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	if err := agent.lockAndCheck(ctx); err != nil {
		// client gave up
		return &hook.HookResult{}
	}
	defer agent.actionMutex.Unlock()

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
	if err := agent.lockAndCheck(ctx); err != nil {
		return err
	}
	defer agent.actionMutex.Unlock()

	return agent.refreshTablet(ctx, "RefreshState")
}

// RunHealthCheck will manually run the health check on the tablet.
// Should be called under RPCWrap.
func (agent *ActionAgent) RunHealthCheck(ctx context.Context) {
	agent.runHealthCheck()
}

// IgnoreHealthError sets the regexp for health check errors to ignore.
// Should be called under RPCWrap.
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
