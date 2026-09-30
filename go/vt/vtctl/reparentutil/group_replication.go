/*
Copyright 2026 The Vitess Authors.

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

package reparentutil

import (
	"context"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// fullStatusResult is the outcome of a FullStatus RPC on one tablet.
type fullStatusResult struct {
	tablet *topodatapb.Tablet
	status *replicationdatapb.FullStatus
	err    error
}

// groupStatus returns the Group Replication status of the tablet, or nil if the RPC failed
// or the tablet does not report one.
func (r *fullStatusResult) groupStatus() *replicationdatapb.GroupReplicationStatus {
	if r == nil || r.status == nil {
		return nil
	}
	return r.status.GroupReplicationStatus
}

// isActiveMember returns whether the tablet's MySQL is an ONLINE or RECOVERING member of a group.
func (r *fullStatusResult) isActiveMember() bool {
	return mysql.IsGroupMemberActive(r.groupStatus())
}

// isOnlineMember returns whether the tablet's MySQL is an ONLINE member of a group.
func (r *fullStatusResult) isOnlineMember() bool {
	gs := r.groupStatus()
	return mysql.IsGroupMemberActive(gs) && gs.MemberState == mysql.GroupMemberStateOnline
}

// isGroupPrimary returns whether the tablet's MySQL is the writable primary of a group with quorum.
func (r *fullStatusResult) isGroupPrimary() bool {
	return mysql.IsGroupPrimary(r.groupStatus())
}

// fetchFullStatuses runs FullStatus on the given tablets concurrently. Every RPC is bounded
// by timeout. The result has one entry per tablet, keyed by tablet alias; failed RPCs carry
// their error.
func fetchFullStatuses(ctx context.Context, tmc tmclient.TabletManagerClient, tablets []*topodatapb.Tablet, timeout time.Duration) map[string]*fullStatusResult {
	statusCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results = make(map[string]*fullStatusResult, len(tablets))
	)
	for _, tablet := range tablets {
		wg.Add(1)
		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			status, err := tmc.FullStatus(statusCtx, tablet)
			if err != nil {
				err = vterrors.Wrapf(err, "failed to get FullStatus of tablet %v", topoproto.TabletAliasString(tablet.Alias))
			}
			mu.Lock()
			defer mu.Unlock()
			results[topoproto.TabletAliasString(tablet.Alias)] = &fullStatusResult{tablet: tablet, status: status, err: err}
		}(tablet)
	}
	wg.Wait()
	return results
}

// fetchFullStatus runs FullStatus on one tablet, bounded by timeout.
func fetchFullStatus(ctx context.Context, tmc tmclient.TabletManagerClient, tablet *topodatapb.Tablet, timeout time.Duration) *fullStatusResult {
	return fetchFullStatuses(ctx, tmc, []*topodatapb.Tablet{tablet}, timeout)[topoproto.TabletAliasString(tablet.Alias)]
}

// memberIsOnlineInView returns whether the member with the given server_uuid is ONLINE in
// the membership view of the reporting member.
func memberIsOnlineInView(view *replicationdatapb.GroupReplicationStatus, serverUUID string) bool {
	if view == nil || serverUUID == "" {
		return false
	}
	for _, m := range view.Members {
		if m.MemberUuid == serverUUID {
			return m.State == mysql.GroupMemberStateOnline
		}
	}
	return false
}

// checkGroupReplicationPrimaryElect verifies that a planned reparent in a shard that runs
// Group Replication can promote the primary-elect: the elect must be an ONLINE member of the
// same group as the current primary. PromoteReplica then switches the group's primary with
// group_replication_set_as_primary; a tablet outside the group cannot be promoted while the
// group is active, because the group's members would keep following the old primary.
//
// A shard that never had a primary is initialized with InitPrimary, which bootstraps the
// group, so there is nothing to check. A shard whose primary is unknown can only promote an
// ONLINE member of a group that has quorum.
func checkGroupReplicationPrimaryElect(ctx context.Context, tmc tmclient.TabletManagerClient, shardInitialized bool, currentPrimary, primaryElect *topodatapb.Tablet) error {
	if primaryElect == nil || (currentPrimary != nil && topoproto.TabletAliasEqual(currentPrimary.Alias, primaryElect.Alias)) {
		return nil
	}
	if currentPrimary == nil {
		if !shardInitialized {
			return nil
		}
		electAlias := topoproto.TabletAliasString(primaryElect.Alias)
		res := fetchFullStatus(ctx, tmc, primaryElect, topo.RemoteOperationTimeout)
		if res.err != nil {
			return vterrors.Wrapf(res.err, "cannot verify the group replication membership of primary-elect %v", electAlias)
		}
		if !res.isOnlineMember() || !res.groupStatus().HasQuorum {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"primary-elect %v is not an ONLINE member of a replication group with quorum; the shard has no current primary to compare with", electAlias)
		}
		return nil
	}
	primaryAlias := topoproto.TabletAliasString(currentPrimary.Alias)
	electAlias := topoproto.TabletAliasString(primaryElect.Alias)

	statuses := fetchFullStatuses(ctx, tmc, []*topodatapb.Tablet{currentPrimary, primaryElect}, topo.RemoteOperationTimeout)
	primaryStatus, electStatus := statuses[primaryAlias], statuses[electAlias]
	if primaryStatus.err != nil {
		return vterrors.Wrapf(primaryStatus.err, "cannot verify the group replication membership of current primary %v", primaryAlias)
	}
	if electStatus.err != nil {
		return vterrors.Wrapf(electStatus.err, "cannot verify the group replication membership of primary-elect %v", electAlias)
	}

	primaryGroup := primaryStatus.groupStatus()
	if !primaryStatus.isActiveMember() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"current primary %v is not an active member of a replication group; the shard's group must be running before a planned reparent (see MigrateReplicationMode)", primaryAlias)
	}
	electGroup := electStatus.groupStatus()
	if !electStatus.isOnlineMember() {
		state := "not a member"
		if electGroup != nil && electGroup.PluginActive {
			state = electGroup.MemberState
		}
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not an ONLINE member of the replication group of current primary %v (state: %s); only an ONLINE member can be promoted while the group is active", electAlias, primaryAlias, state)
	}
	if electGroup.GroupName != primaryGroup.GroupName {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is a member of group %s, but current primary %v is a member of group %s", electAlias, electGroup.GroupName, primaryAlias, primaryGroup.GroupName)
	}
	if !memberIsOnlineInView(primaryGroup, electStatus.status.ServerUuid) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not an ONLINE member in the group view of current primary %v", electAlias, primaryAlias)
	}
	return nil
}
