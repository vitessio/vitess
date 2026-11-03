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
	"log/slog"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topotools"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

// GetGlobalStatusVars returns the server's global status variables asked for.
// An empty/nil variable name parameter slice means you want all of them.
func (tm *TabletManager) GetGlobalStatusVars(ctx context.Context, variables []string) (map[string]string, error) {
	return tm.MysqlDaemon.GetGlobalStatusVars(ctx, variables)
}

// SetReadOnly makes the mysql instance read-only or read-write.
func (tm *TabletManager) SetReadOnly(ctx context.Context, rdonly bool) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()
	fences := tm.groupReplicationFence.snapshot()
	if !rdonly {
		if err := tm.checkGroupAllowsReadWrite(ctx); err != nil {
			return err
		}
		// Under a group replication policy that lists voters, MySQL takes writes only while its tablet
		// may serve as the primary of the shard's group (the serving invariant): Group Replication does
		// not make a primary writable on its own, nor may this RPC, for example PRS's recovery of a
		// partial promotion.
		reason, err := tm.groupReplicationServingDecision(ctx, nil, nil)
		if err != nil {
			return err
		}
		if reason != "" {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "refusing to make MySQL writable: %s", reason)
		}
	}
	superRo, err := tm.MysqlDaemon.IsSuperReadOnly(ctx)
	if err != nil {
		return err
	}

	if !rdonly && superRo {
		// If super read only is set, then we need to prepare the transactions before setting read_only OFF.
		// We need to redo the prepared transactions in read only mode using the dba user to ensure we don't lose them.
		// setting read_only OFF will also set super_read_only OFF if it was set.
		// If super read only is already off, then we probably called this function from PRS or some other place
		// because it is idempotent. We only need to redo prepared transactions the first time we transition from super read only
		// to read write.
		if err := tm.redoPreparedTransactionsAndSetReadWrite(ctx); err != nil {
			return err
		}
	} else if err := tm.MysqlDaemon.SetReadOnly(ctx, rdonly); err != nil {
		return err
	}
	if !rdonly && !tm.settleGroupReplicationFenceLocked(ctx, fences) {
		// The fence check fenced MySQL meanwhile, on a status that may be newer: MySQL is fenced again.
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "refusing to make MySQL writable: it was fenced with super_read_only meanwhile")
	}
	return nil
}

// ChangeTags changes the tablet tags
func (tm *TabletManager) ChangeTags(ctx context.Context, tabletTags map[string]string, replace bool) (map[string]string, error) {
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	tags := tm.tmState.Tablet().Tags
	if replace || len(tags) == 0 {
		tags = tabletTags
	} else {
		for key, val := range tabletTags {
			if val == "" {
				delete(tags, key)
				continue
			}
			tags[key] = val
		}
	}

	tm.tmState.ChangeTabletTags(ctx, tags)
	return tags, nil
}

// ChangeType changes the tablet type
func (tm *TabletManager) ChangeType(ctx context.Context, tabletType topodatapb.TabletType, semiSync bool) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	semiSyncAction, err := tm.convertBoolToSemiSyncAction(ctx, semiSync)
	if err != nil {
		return err
	}

	return tm.changeTypeLocked(ctx, tabletType, DBActionNone, semiSyncAction)
}

// changeTypeLocked changes the tablet type under a lock
func (tm *TabletManager) changeTypeLocked(ctx context.Context, tabletType topodatapb.TabletType, action DBAction, semiSync SemiSyncAction) error {
	return tm.changeTypeWithGroupRecordLocked(ctx, tabletType, action, semiSync, nil)
}

// changeTypeWithGroupRecordLocked is changeTypeLocked, with the shard's group record that the
// caller read a moment ago, if any, for the decision whether a PRIMARY tablet may serve.
func (tm *TabletManager) changeTypeWithGroupRecordLocked(ctx context.Context, tabletType topodatapb.TabletType, action DBAction, semiSync SemiSyncAction, rec *shardGroupRecord) error {
	// We don't want to allow multiple callers to claim a tablet as drained.
	if tabletType == topodatapb.TabletType_DRAINED && tm.Tablet().Type == topodatapb.TabletType_DRAINED {
		return fmt.Errorf("Tablet: %v, is already drained", tm.tabletAlias)
	}

	// settle is set when the tablet becomes a PRIMARY that may serve: MySQL takes writes from then
	// on, unless a fence was decided since the snapshot fences (see groupReplicationFence).
	settle, fences := false, uint64(0)
	if tabletType == topodatapb.TabletType_PRIMARY {
		if groupReplicationEnabled() {
			// Group Replication leaves the primary it elects super_read_only, and sets it again when
			// the election ends: decide once the election ended, so that MySQL stays writable.
			tm.waitForGroupElectionEnd(ctx)
		}
		// Only the group decides which member of a replication group is the primary, so a group
		// secondary cannot become PRIMARY. Check before the tablet record changes.
		if err := tm.checkGroupAllowsPrimary(ctx); err != nil {
			return vterrors.Wrapf(err, "cannot change the tablet type to PRIMARY")
		}
		// Under a group replication policy, the tablet becomes PRIMARY but only serves while
		// MySQL is the primary of the shard's recorded group with a majority of its voters, as
		// MySQL reports it now, under the action lock. Every promotion goes through here: the
		// sync loop's, PromoteReplica (PRS, ERS and VTOrc), InitPrimary, ReplicaWasPromoted and
		// ChangeType.
		//
		// The fence check may fence MySQL meanwhile, without the action lock: a fence decided
		// since the snapshot below stands (see groupReplicationFence).
		fence := &tm.groupReplicationFence
		fences = fence.snapshot()
		reason, status, err := tm.applyGroupReplicationServingDecisionLocked(ctx, rec)
		if err != nil {
			return vterrors.Wrapf(err, "cannot change the tablet type to PRIMARY")
		}
		if reason == "" && fence.decidedSince(fences) {
			reason = fence.lastReason()
			log.Warn("Group replication: MySQL was fenced while the tablet decided to serve as the primary", slog.String("reason", reason))
			if err := tm.tmState.SetGroupReplicationNotServing(ctx, reason); err != nil {
				return vterrors.Wrapf(err, "cannot change the tablet type to PRIMARY")
			}
		}
		switch {
		case reason != "":
			// MySQL stays read-only, as Group Replication left it or as the fence check made it: only a
			// decision that the tablet may serve makes it writable. The tablet becomes a PRIMARY that
			// does not serve, and serving again (serveAgain) makes MySQL writable, redoing the prepared
			// transactions that this promotion does not.
			if action == DBActionSetReadWrite {
				tm.groupReplicationRedoPending.Store(true)
			}
			action = DBActionNone
		case mysql.IsGroupPrimary(status) && action == DBActionNone:
			// Group Replication does not make the primary it elects writable (the member action
			// mysql_disable_super_read_only_if_primary is disabled), nor does a fence lift itself:
			// this decision does, unless MySQL is writable already.
			readOnly, err := tm.mysqlReadOnly(ctx)
			if err != nil {
				return vterrors.Wrapf(err, "cannot change the tablet type to PRIMARY")
			}
			if readOnly || fence.fenced.Load() {
				action = DBActionSetReadWrite
			}
		}
		if action == DBActionSetReadWrite {
			tm.groupReplicationRedoPending.Store(false)
		}
		settle = reason == ""
	}

	err := tm.tmState.ChangeTabletType(ctx, tabletType, action)
	if err == nil {
		// A new type ends a demotion (see groupReplicationDemoted).
		tm.groupReplicationDemoted.Store(false)
	}
	if settle {
		// MySQL may take writes now. If a fence was decided meanwhile, MySQL is fenced again, and
		// the tablet does not serve.
		tm.settleGroupReplicationFenceLocked(ctx, fences)
	}
	if err != nil {
		return err
	}

	// Let's see if we need to fix semi-sync acking.
	if err := tm.fixSemiSyncAndReplication(ctx, tm.Tablet().Type, semiSync); err != nil {
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

func (tm *TabletManager) convertBoolToSemiSyncAction(ctx context.Context, semiSync bool) (SemiSyncAction, error) {
	semiSyncExtensionLoaded, err := tm.MysqlDaemon.SemiSyncExtensionLoaded(ctx)
	if err != nil {
		return SemiSyncActionNone, err
	}

	switch semiSyncExtensionLoaded {
	case mysql.SemiSyncTypeSource, mysql.SemiSyncTypeMaster:
		if semiSync {
			return SemiSyncActionSet, nil
		} else {
			return SemiSyncActionUnset, nil
		}
	default:
		if semiSync {
			return SemiSyncActionNone, vterrors.VT09013()
		} else {
			return SemiSyncActionNone, nil
		}
	}
}
