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

package repltracker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const testViewID = "17908682034375831:3"

// groupSecondary is the state of an ONLINE secondary of a group of three that it can reach, in
// view testViewID, whose applier is idle.
func groupSecondary() *mysql.GroupReplicationApplierStatus {
	return &mysql.GroupReplicationApplierStatus{
		PluginActive:     true,
		MemberState:      mysql.GroupMemberStateOnline,
		Members:          3,
		ReachableMembers: 3,
		ViewID:           testViewID,
	}
}

// newGroupMemberPoller returns a poller on a fake MySQL that has no default replication channel,
// like every member of a replication group, and whose group state is gs.
func newGroupMemberPoller(gs *mysql.GroupReplicationApplierStatus) (*poller, *mysqlctl.FakeMysqlDaemon) {
	mysqld := mysqlctl.NewFakeMysqlDaemon(nil)
	mysqld.ReplicationStatusError = mysql.ErrNotReplica
	mysqld.GroupReplicationApplier = gs
	p := &poller{}
	p.InitDBConfig(mysqld)
	return p, mysqld
}

// TestPollerReadsGroupSecondaryLagWithOneQuery reproduces polling mode on a Group Replication
// secondary: MySQL has no default channel, so SHOW REPLICA STATUS returns nothing, and the poller
// used to report "no replication status", which made every secondary of a group stop serving.
// The lag now comes from a single query of the group's applier state: the age of the oldest
// transaction being applied, measured with its original commit timestamp.
func TestPollerReadsGroupSecondaryLagWithOneQuery(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	cp := *db.ConnParams()
	mysqld := mysqlctl.NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS FOR CHANNEL ''", &sqltypes.Result{})
	db.AddQueryPattern(`SELECT \(SELECT PLUGIN_STATUS FROM information_schema\.PLUGINS .*`, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("plugin_status|member_state|members|reachable_members|view_id|queued_transactions|applier_lag_seconds",
			"varchar|varchar|int64|int64|varchar|uint64|decimal"),
		"ACTIVE|ONLINE|3|3|"+testViewID+"|4|2.500000"))

	p := &poller{}
	p.InitDBConfig(mysqld)
	p.SetGroupReplicationVerdict(true, testViewID)
	lag, err := p.Status()
	require.NoError(t, err)
	assert.Equal(t, 2500*time.Millisecond, lag)
}

// TestPollerGroupSecondaryLag checks the lag of a secondary that is ONLINE in the shard's
// legitimate group: the age of the oldest transaction its applier works on, 0 when nothing waits.
func TestPollerGroupSecondaryLag(t *testing.T) {
	gs := groupSecondary()
	p, mysqld := newGroupMemberPoller(gs)
	p.SetGroupReplicationVerdict(true, testViewID)

	lag, err := p.Status()
	require.NoError(t, err)
	assert.Zero(t, lag, "an idle applier with nothing queued is not behind")

	gs = groupSecondary()
	gs.QueuedTransactions = 12
	gs.Applying = true
	gs.OldestApplying = 3 * time.Second
	mysqld.SetGroupReplicationApplierStatus(gs)
	lag, err = p.Status()
	require.NoError(t, err)
	assert.Equal(t, 3*time.Second, lag)

	// A primary whose clock is behind this member's makes recent transactions look like they
	// were committed in the future.
	gs = groupSecondary()
	gs.QueuedTransactions = 1
	gs.Applying = true
	gs.OldestApplying = -200 * time.Millisecond
	mysqld.SetGroupReplicationApplierStatus(gs)
	lag, err = p.Status()
	require.NoError(t, err)
	assert.Zero(t, lag)
}

// TestPollerIdleMemberThatLeftItsGroup checks the critical case: a member that left its group
// (READ_ONLY exit state action: super_read_only, but still answering reads) receives nothing, so
// its applier is idle and has nothing queued. Its lag must grow from the last time it was healthy
// rather than read 0, so that the lag thresholds take it out of replica reads.
func TestPollerIdleMemberThatLeftItsGroup(t *testing.T) {
	for _, state := range []string{mysql.GroupMemberStateError, mysql.GroupMemberStateOffline, mysql.GroupMemberStateRecovering} {
		t.Run(state, func(t *testing.T) {
			p, mysqld := newGroupMemberPoller(groupSecondary())
			p.SetGroupReplicationVerdict(true, testViewID)
			lag, err := p.Status()
			require.NoError(t, err)
			require.Zero(t, lag)

			left := groupSecondary()
			left.MemberState = state
			left.Members, left.ReachableMembers = 1, 1
			mysqld.SetGroupReplicationApplierStatus(left)
			// The member was last found healthy a minute ago, with 2s of lag.
			p.timeRecorded = time.Now().Add(-time.Minute)
			p.lag = 2 * time.Second
			lag, err = p.Status()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, lag, time.Minute+2*time.Second)
			next, err := p.Status()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, next, lag, "the lag keeps growing while the member is out of its group")
		})
	}
}

// TestPollerGroupMemberWithoutQuorum checks that a member cut off from the majority of its group
// is not healthy as soon as MySQL reports the other members UNREACHABLE: it stays ONLINE in the
// same view, with an idle applier, until it leaves the group after the unreachable majority
// timeout, and the tablet manager's verdict may be up to one sync interval old.
func TestPollerGroupMemberWithoutQuorum(t *testing.T) {
	p, mysqld := newGroupMemberPoller(groupSecondary())
	p.SetGroupReplicationVerdict(true, testViewID)
	_, err := p.Status()
	require.NoError(t, err)

	cutOff := groupSecondary()
	cutOff.ReachableMembers = 1
	mysqld.SetGroupReplicationApplierStatus(cutOff)
	p.timeRecorded = time.Now().Add(-time.Minute)
	lag, err := p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
}

// TestPollerGroupMemberWithoutVoterMajority checks that a member whose group shrank below a
// majority of the shard's voters is not healthy. MySQL's own view quorum counts only the members
// still in the view, so only the tablet manager, which knows the voters, can tell.
func TestPollerGroupMemberWithoutVoterMajority(t *testing.T) {
	p, mysqld := newGroupMemberPoller(groupSecondary())
	p.SetGroupReplicationVerdict(true, testViewID)
	_, err := p.Status()
	require.NoError(t, err)

	shrunk := groupSecondary()
	shrunk.Members, shrunk.ReachableMembers = 1, 1
	shrunk.ViewID = "17908682034375831:4"
	mysqld.SetGroupReplicationApplierStatus(shrunk)
	p.SetGroupReplicationVerdict(false, shrunk.ViewID)
	p.timeRecorded = time.Now().Add(-time.Minute)
	lag, err := p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
}

// TestPollerStrayGroupIncarnation checks that a member alone in a group of a new incarnation, as
// a failed join formed in the chaos tests, is not healthy: it is ONLINE with quorum in its view of
// one, but that group does not hold the shard's transactions. Until the tablet manager checks the
// new view, its last verdict is about another incarnation, which does not count.
func TestPollerStrayGroupIncarnation(t *testing.T) {
	p, mysqld := newGroupMemberPoller(groupSecondary())
	p.SetGroupReplicationVerdict(true, testViewID)
	_, err := p.Status()
	require.NoError(t, err)

	stray := groupSecondary()
	stray.Members, stray.ReachableMembers = 1, 1
	stray.ViewID = "17907858161940982:1"
	mysqld.SetGroupReplicationApplierStatus(stray)
	p.timeRecorded = time.Now().Add(-time.Minute)
	lag, err := p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute, "a verdict about the shard's incarnation does not cover a new one")

	p.SetGroupReplicationVerdict(false, stray.ViewID)
	lag, err = p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
}

// TestPollerStaleGroupVerdict checks that a member is not trusted once the tablet manager stopped
// renewing its verdict, for example because its sync loop is stuck.
func TestPollerStaleGroupVerdict(t *testing.T) {
	p, _ := newGroupMemberPoller(groupSecondary())
	p.SetGroupReplicationVerdict(true, testViewID)
	_, err := p.Status()
	require.NoError(t, err)

	p.verdict.at = time.Now().Add(-groupReplicationVerdictMaxAge - time.Second)
	p.timeRecorded = time.Now().Add(-time.Minute)
	lag, err := p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
}

// TestPollerGroupMemberNeverHealthy checks that a member that was never found healthy reports an
// error, as a replica whose replication never ran does.
func TestPollerGroupMemberNeverHealthy(t *testing.T) {
	// No verdict from the tablet manager yet.
	p, mysqld := newGroupMemberPoller(groupSecondary())
	_, err := p.Status()
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	require.ErrorContains(t, err, "the tablet manager has not checked the group yet")

	left := groupSecondary()
	left.MemberState = mysql.GroupMemberStateOffline
	mysqld.SetGroupReplicationApplierStatus(left)
	p.SetGroupReplicationVerdict(false, "")
	_, err = p.Status()
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	assert.ErrorContains(t, err, "member state OFFLINE")
}

// sequenceMysqlDaemon returns the given group states one after the other.
type sequenceMysqlDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	statuses []*mysql.GroupReplicationApplierStatus
	calls    int
}

func (s *sequenceMysqlDaemon) GroupReplicationApplierStatus(ctx context.Context) (*mysql.GroupReplicationApplierStatus, error) {
	status := s.statuses[min(s.calls, len(s.statuses)-1)]
	s.calls++
	return status, nil
}

// TestPollerGroupLagUnknownForAnInstant checks that the instant at which transactions wait but
// none has reached the applier's workers yet (about 1 in 300 reads under load on MySQL 8.4) does
// not count as a period without a measurement: the poller looks again.
func TestPollerGroupLagUnknownForAnInstant(t *testing.T) {
	waiting := groupSecondary()
	waiting.QueuedTransactions = 3
	applying := groupSecondary()
	applying.QueuedTransactions = 3
	applying.Applying = true
	applying.OldestApplying = 40 * time.Millisecond
	fake := mysqlctl.NewFakeMysqlDaemon(nil)
	fake.ReplicationStatusError = mysql.ErrNotReplica
	mysqld := &sequenceMysqlDaemon{FakeMysqlDaemon: fake, statuses: []*mysql.GroupReplicationApplierStatus{waiting, applying}}
	p := &poller{}
	p.InitDBConfig(mysqld)
	p.SetGroupReplicationVerdict(true, testViewID)
	p.timeRecorded = time.Now().Add(-time.Minute)

	lag, err := p.Status()
	require.NoError(t, err)
	assert.Equal(t, 40*time.Millisecond, lag)
	assert.Equal(t, 2, mysqld.calls)

	// If it lasts, the lag is estimated from the last measurement, like an unknown
	// Seconds_Behind_Source.
	mysqld.statuses = []*mysql.GroupReplicationApplierStatus{waiting}
	mysqld.calls = 0
	p.timeRecorded = time.Now().Add(-time.Minute)
	lag, err = p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
}

// TestPollerAsyncReplicaIgnoresGroupState checks that a MySQL with a default replication channel
// keeps its lag from SHOW REPLICA STATUS, even with the Group Replication plugin active: an
// asynchronous replica of the group, or a tablet that is being converted to Group Replication.
func TestPollerAsyncReplicaIgnoresGroupState(t *testing.T) {
	gs := groupSecondary()
	gs.MemberState = mysql.GroupMemberStateOnline
	p, mysqld := newGroupMemberPoller(gs)
	p.SetGroupReplicationVerdict(false, testViewID)
	mysqld.ReplicationStatusError = nil
	mysqld.Replicating = true
	mysqld.IOThreadRunning = true
	mysqld.ReplicationLagSeconds = 7

	lag, err := p.Status()
	require.NoError(t, err)
	assert.Equal(t, 7*time.Second, lag)
}

// TestPollerWithoutGroupReplication checks that a MySQL without a default channel and without an
// active Group Replication plugin reports the same error as before Group Replication.
func TestPollerWithoutGroupReplication(t *testing.T) {
	p, mysqld := newGroupMemberPoller(&mysql.GroupReplicationApplierStatus{MemberState: mysql.GroupMemberStateOffline})
	_, err := p.Status()
	assert.Equal(t, mysql.ErrNotReplica, err)

	// The group state cannot be read: the same.
	mysqld.GroupReplicationApplierError = errors.New("performance_schema is disabled")
	_, err = p.Status()
	assert.Equal(t, mysql.ErrNotReplica, err)
}

// TestReplTrackerPrimaryOfGroup checks that a PRIMARY tablet, the group's primary, has no lag
// and does not query MySQL.
func TestReplTrackerPrimaryOfGroup(t *testing.T) {
	p, mysqld := newGroupMemberPoller(groupSecondary())
	mysqld.GroupReplicationApplierError = errors.New("must not be called")
	rt := &ReplTracker{mode: tabletenv.Polling, isPrimary: true, poller: p}
	lag, err := rt.Status()
	require.NoError(t, err)
	assert.Zero(t, lag)
}

// blockingMysqlDaemon blocks group state reads until their context ends while block is set, like
// MySQL while a START or STOP GROUP_REPLICATION runs on the member.
type blockingMysqlDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	block bool
	calls int
}

func (b *blockingMysqlDaemon) GroupReplicationApplierStatus(ctx context.Context) (*mysql.GroupReplicationApplierStatus, error) {
	b.calls++
	if b.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return b.FakeMysqlDaemon.GroupReplicationApplierStatus(ctx)
}

// TestPollerGroupStateUnreadable checks that a member whose group state cannot be read is not
// healthy, rather than reported with the error of a server that is not a replica, which would make
// it stop serving at once. MySQL blocks these reads while a join runs: VTOrc started one on the
// cut-off secondary of the G13 chaos scenario. The read is bounded, because the health check holds
// the query service's state lock meanwhile, and is not retried for a while.
func TestPollerGroupStateUnreadable(t *testing.T) {
	fake := mysqlctl.NewFakeMysqlDaemon(nil)
	fake.ReplicationStatusError = mysql.ErrNotReplica
	fake.GroupReplicationApplier = groupSecondary()
	mysqld := &blockingMysqlDaemon{FakeMysqlDaemon: fake}
	p := &poller{}
	p.InitDBConfig(mysqld)
	p.SetGroupReplicationVerdict(true, testViewID)
	_, err := p.Status()
	require.NoError(t, err)

	mysqld.block = true
	p.timeRecorded = time.Now().Add(-time.Minute)
	start := time.Now()
	lag, err := p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
	assert.Less(t, time.Since(start), 3*time.Second, "the read must be bounded")

	// Not read again for a while: still not healthy.
	mysqld.block = false
	calls := mysqld.calls
	lag, err = p.Status()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, time.Minute)
	assert.Equal(t, calls, mysqld.calls)

	p.groupReadFailed = time.Now().Add(-groupReplicationReadBackoff)
	p.SetGroupReplicationVerdict(true, testViewID)
	lag, err = p.Status()
	require.NoError(t, err)
	assert.Zero(t, lag)
}
