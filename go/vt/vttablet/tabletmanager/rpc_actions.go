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
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topotools"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// DBAction is used to tell ChangeTabletType whether to call SetReadOnly on change to
// PRIMARY tablet type
type DBAction int

// Allowed values for DBAction
const (
	DBActionNone = DBAction(iota)
	DBActionSetReadWrite
)

// SemiSyncAction is used to tell fixSemiSync whether to change the semi-sync
// settings or not.
type SemiSyncAction int

// Allowed values for SemiSyncAction
const (
	SemiSyncActionNone = SemiSyncAction(iota)
	SemiSyncActionSet
	SemiSyncActionUnset
)

// This file contains the implementations of RPCTM methods.
// Major groups of methods are broken out into files named "rpc_*.go".

// Ping makes sure RPCs work, and refreshes the tablet record.
func (tm *TabletManager) Ping(ctx context.Context, args string) string {
	return args
}

// GetPermissions returns the db permissions.
func (tm *TabletManager) GetPermissions(ctx context.Context) (*tabletmanagerdatapb.Permissions, error) {
	return mysqlctl.GetPermissions(tm.MysqlDaemon)
}

// SetReadOnly makes the mysql instance read-only or read-write.
func (tm *TabletManager) SetReadOnly(ctx context.Context, rdonly bool) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.MysqlDaemon.SetReadOnly(rdonly)
}

// ChangeType changes the tablet type
func (tm *TabletManager) ChangeType(ctx context.Context, tabletType topodatapb.TabletType, semiSync bool) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	semiSyncAction, err := tm.convertBoolToSemiSyncAction(semiSync)
	if err != nil {
		return err
	}

	return tm.changeTypeLocked(ctx, tabletType, DBActionNone, semiSyncAction)
}

// ChangeType changes the tablet type
func (tm *TabletManager) changeTypeLocked(ctx context.Context, tabletType topodatapb.TabletType, action DBAction, semiSync SemiSyncAction) error {
	// We don't want to allow multiple callers to claim a tablet as drained.
	if tabletType == topodatapb.TabletType_DRAINED && tm.Tablet().Type == topodatapb.TabletType_DRAINED {
		return fmt.Errorf("Tablet: %v, is already drained", tm.tabletAlias)
	}

	if err := tm.tmState.ChangeTabletType(ctx, tabletType, action); err != nil {
		return err
	}

	// Let's see if we need to fix semi-sync acking.
	if err := tm.fixSemiSyncAndReplication(tm.Tablet().Type, semiSync); err != nil {
		return vterrors.Wrap(err, "fixSemiSyncAndReplication failed, may not ack correctly")
	}
	return nil
}

// Sleep sleeps for the duration
func (tm *TabletManager) Sleep(ctx context.Context, duration time.Duration) {
	if err := tm.lock(ctx); err != nil {
		// client gave up
		return
	}
	defer tm.unlock()

	time.Sleep(duration)
}

// ExecuteHook executes the provided hook locally, and returns the result.
func (tm *TabletManager) ExecuteHook(ctx context.Context, hk *hook.Hook) *hook.HookResult {
	if err := tm.lock(ctx); err != nil {
		// client gave up
		return &hook.HookResult{}
	}
	defer tm.unlock()

	// Execute the hooks
	topotools.ConfigureTabletHook(hk, tm.tabletAlias)
	return hk.Execute()
}

// RefreshState reload the tablet record from the topo server.
func (tm *TabletManager) RefreshState(ctx context.Context) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.tmState.RefreshFromTopo(ctx)
}

// RunHealthCheck will manually run the health check on the tablet.
func (tm *TabletManager) RunHealthCheck(ctx context.Context) {
	tm.QueryServiceControl.BroadcastHealth()
}

func (tm *TabletManager) convertBoolToSemiSyncAction(semiSync bool) (SemiSyncAction, error) {
	semiSyncExtensionLoaded, err := tm.MysqlDaemon.SemiSyncExtensionLoaded()
	if err != nil {
		return SemiSyncActionNone, err
	}

	if semiSyncExtensionLoaded {
		if semiSync {
			return SemiSyncActionSet, nil
		} else {
			return SemiSyncActionUnset, nil
		}
	} else {
		if semiSync {
			return SemiSyncActionNone, vterrors.VT09013()
		} else {
			return SemiSyncActionNone, nil
		}
	}
}
