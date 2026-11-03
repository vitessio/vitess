/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var replicationLagSeconds = stats.NewGauge("replicationLagSec", "replication lag in seconds")

// groupReplicationVerdictMaxAge is how long the poller trusts the tablet manager's last verdict
// about the group membership of its MySQL. The tablet manager renews it on every run of its group
// replication sync loop (every second by default); an older verdict means that the loop is stuck
// or stopped, and the member is then treated as not healthy.
const groupReplicationVerdictMaxAge = 10 * time.Second

// groupReplicationReadTimeout bounds the read of a member's group state, which normally takes a
// millisecond. MySQL blocks these reads while a START or STOP GROUP_REPLICATION runs on the member,
// for example a join that cannot reach its group, which lasts up to a minute. The tablet's health
// check holds the query service's state lock during the read, and queries wait for that lock.
const groupReplicationReadTimeout = time.Second

// groupReplicationReadBackoff is how long the poller does not read the group state again after a
// read failed. The member is not healthy meanwhile.
const groupReplicationReadBackoff = 5 * time.Second

type poller struct {
	mysqld mysqlctl.MysqlDaemon

	mu           sync.Mutex
	lag          time.Duration
	timeRecorded time.Time
	// groupMember is set when MySQL was last found to have no default replication channel and
	// an active Group Replication plugin.
	groupMember bool
	// groupReadFailed is when a read of the group state last failed on a group member.
	groupReadFailed time.Time
	// groupUnhealthy is why the member was last found not healthy in its replication group,
	// empty while it is healthy. It is only used to log changes.
	groupUnhealthy string

	// verdictMu protects verdict. It is separate from mu, which Status holds while it queries
	// MySQL, so that the tablet manager never waits for a query.
	verdictMu sync.Mutex
	verdict   groupReplicationVerdict
}

// groupReplicationVerdict is the tablet manager's verdict about the group membership of its MySQL.
type groupReplicationVerdict struct {
	// healthy is set when MySQL was ONLINE in the shard's legitimate replication group, with a
	// majority of the shard's voters ONLINE in its view.
	healthy bool
	// viewID is the view of the group that the verdict is about.
	viewID string
	// at is when the poller received the verdict.
	at time.Time
}

func (p *poller) InitDBConfig(mysqld mysqlctl.MysqlDaemon) {
	p.mysqld = mysqld
}

// SetGroupReplicationVerdict records the tablet manager's verdict about the group membership of
// its MySQL: healthy is set when MySQL is ONLINE in the shard's legitimate replication group
// (the recorded incarnation, with a majority of the shard's voters ONLINE in its view), and viewID
// is the view the verdict is about.
func (p *poller) SetGroupReplicationVerdict(healthy bool, viewID string) {
	p.verdictMu.Lock()
	defer p.verdictMu.Unlock()
	p.verdict = groupReplicationVerdict{healthy: healthy, viewID: viewID, at: time.Now()}
}

func (p *poller) groupReplicationVerdict() groupReplicationVerdict {
	p.verdictMu.Lock()
	defer p.verdictMu.Unlock()
	return p.verdict
}

func (p *poller) Status() (time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := p.mysqld.ReplicationStatus(ctx)
	if errors.Is(err, mysql.ErrNotReplica) {
		// MySQL has no default replication channel. A member of a replication group has none:
		// it receives the group's transactions through the group's own channels.
		if lag, handled, grErr := p.groupReplicationStatus(ctx, err); handled {
			return lag, grErr
		}
	}
	if err != nil {
		return 0, err
	}
	p.groupMember = false

	// If replication is not currently running or we don't know what the lag is -- most commonly
	// because the replica mysqld is in the process of trying to start replicating from its source
	// but it hasn't yet reached the point where it can calculate the seconds_behind_source
	// value and it's thus NULL -- then we will estimate the lag ourselves using the last seen
	// value + the time elapsed since.
	if !status.Healthy() || status.ReplicationLagUnknown {
		if p.timeRecorded.IsZero() {
			return 0, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "replication is not running")
		}
		return time.Since(p.timeRecorded) + p.lag, nil
	}

	p.record(time.Duration(status.ReplicationLagSeconds) * time.Second)
	return p.lag, nil
}

// record records a lag that was just measured on a healthy replica.
func (p *poller) record(lag time.Duration) {
	p.lag = lag
	p.timeRecorded = time.Now()
	replicationLagSeconds.Set(int64(p.lag.Seconds()))
}

// groupReplicationStatus measures the lag of a MySQL that has no default replication channel, if
// it runs Group Replication. handled is false when the Group Replication plugin is not active, or
// its state cannot be read and MySQL was not known to be a group member: the caller then reports
// notReplica, as it did before Group Replication.
//
// The lag of a member that is ONLINE in the shard's legitimate group, with quorum, is how far
// its applier is behind the group (mysql.GroupReplicationApplierStatus.ApplierLag). Any other
// member receives nothing from the shard's group: an idle applier then says nothing about how
// stale the member is. It is reported like a replica whose replication stopped: the time since
// the member was last found healthy, plus the lag measured then, so that the tablet's unhealthy
// threshold and vtgate's replication lag thresholds take it out of replica reads. Without such a
// measurement, it is an error.
func (p *poller) groupReplicationStatus(ctx context.Context, notReplica error) (lag time.Duration, handled bool, err error) {
	if p.groupMember && time.Since(p.groupReadFailed) < groupReplicationReadBackoff {
		return p.groupMemberNotHealthy("its group replication state could not be read recently", notReplica)
	}
	gs, err := p.readGroupReplicationStatus(ctx)
	if err != nil {
		log.Warn("Replication lag: cannot read the group replication status", slog.Any("error", err))
		if !p.groupMember {
			return 0, false, nil
		}
		// A member whose state cannot be read is not known to receive the shard's transactions.
		p.groupReadFailed = time.Now()
		return p.groupMemberNotHealthy("its group replication state cannot be read: "+err.Error(), notReplica)
	}
	p.groupMember = gs.PluginActive
	if !gs.PluginActive {
		return 0, false, nil
	}
	unhealthy := p.groupMemberUnhealthy(gs)
	if unhealthy == "" {
		lag, known := gs.ApplierLag()
		if !known {
			// Transactions are waiting, but none has reached the applier's workers yet. This
			// lasts an instant; a second look almost always tells.
			if again, err := p.readGroupReplicationStatus(ctx); err == nil && again.PluginActive {
				gs = again
				unhealthy = p.groupMemberUnhealthy(gs)
				lag, known = gs.ApplierLag()
			}
		}
		if unhealthy == "" && known {
			p.logGroupHealth("")
			p.record(lag)
			return p.lag, true, nil
		}
		if unhealthy == "" {
			unhealthy = "the applier lag is unknown"
		}
	}
	return p.groupMemberNotHealthy(unhealthy, notReplica)
}

// readGroupReplicationStatus reads the member's group state, bounded by
// groupReplicationReadTimeout.
func (p *poller) readGroupReplicationStatus(ctx context.Context) (*mysql.GroupReplicationApplierStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, groupReplicationReadTimeout)
	defer cancel()
	return p.mysqld.GroupReplicationApplierStatus(ctx)
}

// groupMemberNotHealthy reports a member that is not healthy for the given reason: the time since
// it last was, plus the lag measured then, or an error if it never was.
func (p *poller) groupMemberNotHealthy(unhealthy string, notReplica error) (time.Duration, bool, error) {
	p.logGroupHealth(unhealthy)
	if p.timeRecorded.IsZero() {
		return 0, true, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "MySQL is not a healthy member of the shard's replication group: %s: %v", unhealthy, notReplica)
	}
	return time.Since(p.timeRecorded) + p.lag, true, nil
}

// groupMemberUnhealthy returns why the member does not receive the shard's transactions, or an
// empty string if it does: it is ONLINE, can reach a majority of its view, and the tablet manager
// recently found it in the shard's legitimate group, in the incarnation it is in now.
//
// MySQL's own state is read on every call, so that a member that left its group, or can no longer
// reach the majority of it, is noticed at once. Whether the group is the shard's legitimate group
// needs the shard record, which the tablet manager reads and judges on every run of its group
// replication sync loop: a stray group of a new incarnation, or a group that lost the majority of
// the shard's voters, has quorum in its own view.
func (p *poller) groupMemberUnhealthy(gs *mysql.GroupReplicationApplierStatus) string {
	if gs.MemberState != mysql.GroupMemberStateOnline {
		return "member state " + gs.MemberState
	}
	if !gs.HasQuorum() {
		return fmt.Sprintf("its view of the group has no quorum (%d of %d members reachable)", gs.ReachableMembers, gs.Members)
	}
	verdict := p.groupReplicationVerdict()
	switch {
	case verdict.at.IsZero():
		return "the tablet manager has not checked the group yet"
	case time.Since(verdict.at) > groupReplicationVerdictMaxAge:
		return fmt.Sprintf("the tablet manager last checked the group %v ago", time.Since(verdict.at).Round(time.Second))
	case !verdict.healthy:
		return "it is not in the shard's legitimate group with a majority of the voters"
	case groupIncarnation(verdict.viewID) != groupIncarnation(gs.ViewID):
		return fmt.Sprintf("its group incarnation changed (view %s, checked in view %s)", gs.ViewID, verdict.viewID)
	}
	return ""
}

// groupIncarnation returns the incarnation part of a group view id: the part before the ':'. It
// is the same for every view of a group, and every bootstrap creates a new one.
func groupIncarnation(viewID string) string {
	incarnation, _, _ := strings.Cut(viewID, ":")
	return incarnation
}

func (p *poller) logGroupHealth(unhealthy string) {
	if unhealthy == p.groupUnhealthy {
		return
	}
	if unhealthy == "" {
		log.Info("Replication lag: MySQL is a healthy member of the shard's replication group again")
	} else {
		log.Warn("Replication lag: MySQL is not a healthy member of the shard's replication group, reporting the lag since it last was", slog.String("reason", unhealthy))
	}
	p.groupUnhealthy = unhealthy
}
