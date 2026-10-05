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

package tabletmanager

import (
	"context"
	"log/slog"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// StartGroupReplication makes the tablet's MySQL join its shard's replication group, or
// bootstrap a new group, and waits until it is an ONLINE member.
//
// If MySQL is already an active member, it only completes the transition to Group Replication
// (see finishGroupJoinLocked) and waits for the member to be ONLINE, so the call can be
// retried. Bootstrapping an active member fails. Before joining, the default replication
// channel is stopped, because MySQL does not start Group Replication while it runs; if the
// join fails, it is restarted.
//
// A bootstrap creates a group of which this MySQL is the only member and the primary. The
// caller must hold the shard lock and must have verified that no member of the shard's group
// is active: bootstrapping while the group exists elsewhere splits the shard. A serving PRIMARY
// tablet pauses serving during the bootstrap (see pauseServingLocked). Right before MySQL's START,
// a bootstrap passes the checks that req asks for (see groupBootstrapChecks), and is refused with
// FAILED_PRECONDITION otherwise.
func (tm *TabletManager) StartGroupReplication(ctx context.Context, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
	bootstrap := req.GetBootstrap()
	log.Info("StartGroupReplication", slog.Bool("bootstrap", bootstrap), slog.String("required_gtid_set", req.GetRequiredGtidSet()),
		slog.String("intent_token", req.GetBootstrapIntentToken()), slog.String("expected_incarnation", req.GetExpectedIncarnation()))
	checks, err := newGroupBootstrapChecks(req)
	if err != nil {
		return nil, err
	}
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	if err := checkGroupReplicationEnabled(); err != nil {
		return nil, err
	}
	if !bootstrap {
		tm.refreshActiveGroupSeeds(ctx)
	}
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	if bootstrap {
		// A request whose bootstrap intent was superseded while it waited for the action lock changes
		// nothing: not the explicit stop of the rejoins, nor whether a PRIMARY tablet serves.
		if err := tm.checkGroupBootstrapIntentLocked(ctx, checks); err != nil {
			return nil, err
		}
	} else if err := tm.refuseJoinOnPrimaryLocked(ctx); err != nil {
		return nil, err
	}
	// An explicit start lifts a previous explicit stop. A bootstrap also keeps the sync loop from
	// starting a join while it runs: a join in progress makes MySQL refuse the bootstrap.
	tm.groupReplicationRejoinSuspended.Store(bootstrap)
	var pause *servingPause
	if bootstrap {
		defer tm.groupReplicationRejoinSuspended.Store(false)
		if err := tm.stopServingBeforeBootstrap(ctx); err != nil {
			return nil, err
		}
		// A PRIMARY tablet that still serves, as during a migration, pauses: MySQL refuses commits
		// for a moment while it bootstraps the group (see pauseServingLocked).
		if pause, err = tm.pauseServingLocked(ctx, groupReplicationBootstrapPause); err != nil {
			return nil, err
		}
	}
	_, err = tm.startGroupReplicationLocked(ctx, bootstrap, checks)
	pause.resume(ctx)
	if err != nil {
		return nil, err
	}
	return tm.waitForGroupMemberOnline(ctx)
}

// refreshActiveGroupSeeds reads which peers are active members of the shard's group, before a join
// that the StartGroupReplication RPC requests, so that MySQL contacts them first (see preferSeeds),
// as it does for the tablet's own joins, which check that first (checkLegitimateGroupToJoin). Right
// after a bootstrap, that is the bootstrapped member. The sorted seeds otherwise put first whichever
// peer's address sorts first, which may be a member whose own START is stuck: MySQL then waited for
// its group communication engine for 30s before it tried the next seed (the G12 chaos run). Peers
// that are not active members come after the active ones; none is left out, since the read may miss
// a member that is active. The read does not hold the action lock.
func (tm *TabletManager) refreshActiveGroupSeeds(ctx context.Context) {
	rec, err := tm.readShardGroupRecord(ctx, tm.groupReplicationTopo.lastRecord())
	if err != nil {
		log.Warn("Group replication: cannot read the shard record, the join contacts its seeds in their sorted order", slog.Any("error", err))
		return
	}
	if !tm.legitimateGroupActiveElsewhere(ctx, rec) {
		// No peer was seen active: forget the peers that were, a while ago.
		tm.groupReplicationPeers.setActiveSeeds(nil)
	}
}

// StopGroupReplication makes the tablet's MySQL leave its shard's replication group. MySQL
// stays read-only, unless it was the primary of the group and the tablet is PRIMARY: the tablet
// then makes it writable again, since it is no longer a member. The group replication sync loop
// does not make the tablet rejoin the group until StartGroupReplication or StartReplication is
// called.
func (tm *TabletManager) StopGroupReplication(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	log.Info("StopGroupReplication")
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}
	if err := checkGroupReplicationEnabled(); err != nil {
		return nil, err
	}
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	tm.groupReplicationRejoinSuspended.Store(true)
	return tm.stopGroupReplicationLocked(ctx)
}

// refuseJoinOnPrimaryLocked refuses, with FAILED_PRECONDITION, a join of MySQL into its group on a
// PRIMARY tablet under a group replication policy that lists voters, while MySQL is not an active
// member. Such a join is never intended: the migration only joins replicas and bootstraps the group
// on the primary, and VTOrc's GroupMemberNotOnline does not analyze a PRIMARY tablet. A PRIMARY
// tablet whose MySQL is out of its group is a stale primary, which the tablet demotes on its own
// first (demoteStalePrimary), and only then rejoins; a join started on it instead would hold the
// action lock for up to a minute, keeping that demotion from running, while the tablet still
// claims to be the shard's primary. Refusing changes nothing, and the join is retried once the
// tablet is a REPLICA. A MySQL that is already an active member only finishes its transition (see
// StartGroupReplication), which is harmless.
func (tm *TabletManager) refuseJoinOnPrimaryLocked(ctx context.Context) error {
	if tm.Tablet().Type != topodatapb.TabletType_PRIMARY {
		return nil
	}
	deadline := time.Now().Add(groupReplicationTopoReadTimeout)
	durability, err := tm.durabilityForGroupChange(ctx, deadline)
	if err != nil {
		return err
	}
	if !policy.IsGroupReplication(durability) {
		return nil
	}
	voters, err := tm.votersForGroupChange(ctx, deadline)
	if err != nil {
		return err
	}
	if len(voters) == 0 {
		return nil
	}
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return err
	}
	if mysql.IsGroupMemberActive(status) {
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
		"refusing to join the replication group on a PRIMARY tablet whose MySQL is %s: the tablet demotes itself to REPLICA first, and then rejoins the group",
		status.GetMemberState())
}
