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

package mysql

import (
	"math"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/vterrors"
)

// readGroupReplicationApplierStatus reads, in one round trip, what the replication lag tracker
// needs to know about a group member: whether the plugin is active, the member's own state, the
// size of its view and how many members of it are reachable (quorum), its view id, how many
// transactions wait to be certified or applied, and how long ago the oldest transaction that the
// applier is still working on was committed.
//
// The age is measured with the transactions' original commit timestamps, which the member that
// ran the transaction (the primary) records when the transaction is ready to commit, before it is
// certified, in its own clock: a clock skew between the primary and this member shifts the lag
// by the skew, as it does for heartbeat lag. The applier's workers report the transaction each of
// them is applying, and the coordinator the one it is scheduling; the oldest of them is the first
// transaction this member has not applied yet. Idle workers report an empty transaction and a
// zero timestamp, which UNIX_TIMESTAMP turns into 0.
const readGroupReplicationApplierStatus = "SELECT " +
	"(SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'group_replication') AS plugin_status, " +
	"(SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@global.server_uuid) AS member_state, " +
	"(SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_ID != '') AS members, " +
	"(SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_ID != '' AND MEMBER_STATE IN ('ONLINE', 'RECOVERING')) AS reachable_members, " +
	"(SELECT VIEW_ID FROM performance_schema.replication_group_member_stats WHERE MEMBER_ID = @@global.server_uuid) AS view_id, " +
	"(SELECT COUNT_TRANSACTIONS_IN_QUEUE + COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE FROM performance_schema.replication_group_member_stats WHERE MEMBER_ID = @@global.server_uuid) AS queued_transactions, " +
	"(SELECT UNIX_TIMESTAMP(NOW(6)) - MIN(NULLIF(UNIX_TIMESTAMP(ts), 0)) FROM (" +
	"SELECT APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP AS ts FROM performance_schema.replication_applier_status_by_worker " +
	"WHERE CHANNEL_NAME = 'group_replication_applier' AND APPLYING_TRANSACTION != '' " +
	"UNION ALL " +
	"SELECT PROCESSING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP FROM performance_schema.replication_applier_status_by_coordinator " +
	"WHERE CHANNEL_NAME = 'group_replication_applier' AND PROCESSING_TRANSACTION != ''" +
	") AS unapplied) AS applier_lag_seconds"

// GroupReplicationApplierStatus is a group member's own state and how far its applier is behind
// the transactions the group committed. It is what replication lag tracking reads on a member,
// which has no default replication channel.
type GroupReplicationApplierStatus struct {
	// PluginActive is set when the Group Replication plugin is loaded and active. The other
	// fields are only meaningful then.
	PluginActive bool
	// MemberState is the member's own state (ONLINE, RECOVERING, ...); OFFLINE when it is not
	// in a group.
	MemberState string
	// Members is the number of members in the member's view of its group, and ReachableMembers
	// how many of them it can reach (ONLINE or RECOVERING).
	Members, ReachableMembers int
	// ViewID is the member's current view of its group.
	ViewID string
	// QueuedTransactions is the number of transactions that the member received from the group
	// and has not applied yet: waiting for certification, or in the applier's queue, including
	// the ones being applied.
	QueuedTransactions int64
	// Applying is set when the applier is working on at least one transaction; OldestApplying
	// is then how long ago the oldest of them was committed by the member that ran it.
	Applying       bool
	OldestApplying time.Duration
}

// HasQuorum returns whether the member can reach a majority of its view.
func (s *GroupReplicationApplierStatus) HasQuorum() bool {
	return s != nil && s.Members > 0 && s.ReachableMembers > s.Members/2
}

// ApplierLag returns how far the member's applier is behind the group, and whether that is
// known. It is the age of the oldest transaction the applier is working on; with parallel
// workers, the oldest of them. It is 0 when no transaction waits. It is unknown for the instant
// at which transactions wait but none has reached a worker or the coordinator yet.
//
// It only measures transactions that the member received: a member that is cut off from its
// group receives nothing and reports 0. Whether the member is in its group must be checked
// separately.
func (s *GroupReplicationApplierStatus) ApplierLag() (time.Duration, bool) {
	switch {
	case s.Applying:
		// A primary whose clock is behind this member's makes recent transactions look like
		// they were committed in the future.
		return max(s.OldestApplying, 0), true
	case s.QueuedTransactions == 0:
		return 0, true
	}
	return 0, false
}

// GroupReplicationApplierStatus reads the member's state and its applier lag with a single query
// (see readGroupReplicationApplierStatus). PluginActive is false, and there is no error, when the
// Group Replication plugin is not loaded.
func (c *Conn) GroupReplicationApplierStatus() (*GroupReplicationApplierStatus, error) {
	qr, err := c.ExecuteFetch(readGroupReplicationApplierStatus, 1, true)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication applier status")
	}
	status := &GroupReplicationApplierStatus{MemberState: GroupMemberStateOffline}
	if len(qr.Rows) != 1 {
		return status, nil
	}
	row := qr.Named().Row()
	if row.AsString("plugin_status", "") != "ACTIVE" {
		return status, nil
	}
	status.PluginActive = true
	if state := row.AsString("member_state", ""); state != "" {
		status.MemberState = state
	}
	status.Members = int(row.AsInt64("members", 0))
	status.ReachableMembers = int(row.AsInt64("reachable_members", 0))
	status.ViewID = row.AsString("view_id", "")
	status.QueuedTransactions = row.AsInt64("queued_transactions", 0)
	if lag := row["applier_lag_seconds"]; !lag.IsNull() {
		seconds, err := strconv.ParseFloat(lag.ToString(), 64)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to parse the group replication applier lag %q", lag.ToString())
		}
		status.Applying = true
		status.OldestApplying = time.Duration(math.Round(seconds * float64(time.Second)))
	}
	return status, nil
}
