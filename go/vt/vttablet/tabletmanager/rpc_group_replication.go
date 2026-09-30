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

	"vitess.io/vitess/go/vt/log"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
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
// is active: bootstrapping while the group exists elsewhere splits the shard.
func (tm *TabletManager) StartGroupReplication(ctx context.Context, bootstrap bool) (*replicationdatapb.GroupReplicationStatus, error) {
	log.Info("StartGroupReplication", slog.Bool("bootstrap", bootstrap))
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

	// An explicit start lifts a previous explicit stop. A bootstrap also keeps the sync loop from
	// starting a join while it runs: a join in progress makes MySQL refuse the bootstrap.
	tm.groupReplicationRejoinSuspended.Store(bootstrap)
	if bootstrap {
		defer tm.groupReplicationRejoinSuspended.Store(false)
	}
	if _, err := tm.startGroupReplicationLocked(ctx, bootstrap); err != nil {
		return nil, err
	}
	return tm.waitForGroupMemberOnline(ctx)
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
