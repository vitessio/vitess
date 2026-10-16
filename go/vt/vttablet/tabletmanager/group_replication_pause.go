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
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// The reasons for which a PRIMARY tablet pauses, for a change of its MySQL's replication that
// makes MySQL refuse commits for a moment.
const (
	// groupReplicationBootstrapPause: a migration bootstraps the group on the serving primary.
	// MySQL refuses a commit under way when Group Replication starts (errno 3100, its before_commit
	// hook), and is super_read_only for a few milliseconds before it is the group's writable primary.
	groupReplicationBootstrapPause = "bootstrapping the replication group"
	// groupReplicationLeavePause: the primary leaves its group, the last step of a migration back to
	// semi-sync. MySQL refuses commits while Group Replication stops, and is super_read_only after.
	groupReplicationLeavePause = "leaving the replication group"
)

var (
	// groupReplicationPauseNotice is how long a serving PRIMARY tablet reports that it does not
	// serve before it stops serving, for a planned pause (see pauseServingLocked).
	groupReplicationPauseNotice = 100 * time.Millisecond
	// groupReplicationPauseResumeTimeout bounds how long a paused PRIMARY tablet waits, once the
	// change of its MySQL ended, for MySQL to be writable before it serves again.
	groupReplicationPauseResumeTimeout = 5 * time.Second
)

// servingPause is a planned pause of a serving PRIMARY tablet (see pauseServingLocked).
type servingPause struct {
	tm         *TabletManager
	tabletType topodatapb.TabletType
	termStart  time.Time
	reason     string
	started    time.Time
}

// pauseServingLocked makes a serving PRIMARY tablet stop serving before a planned change of its
// MySQL that makes MySQL refuse commits for a moment, as PRS's DemotePrimary does: the tablet keeps
// its type and its primary term, so that vtgate buffers the writes, and resume serves again once
// MySQL is writable. It returns nil, and does nothing, if the tablet is not a serving PRIMARY. The
// caller holds the action lock, and must call resume once the change ended, whatever its outcome.
//
// vtgate does not send a request again to a tablet that refused it, even after its buffering: the
// tablet first reports that it does not serve while it still serves, for
// --group-replication-pause-notice, so that vtgate buffers the new writes instead of sending them,
// and only then refuses new queries and waits for those in flight. The change has not started, so
// the queries that arrive meanwhile run as before.
//
// While the pause lasts, the tablet does not serve as PRIMARY, whatever else changes its state
// (tmState.canServe).
func (tm *TabletManager) pauseServingLocked(ctx context.Context, reason string) (*servingPause, error) {
	tablet := tm.Tablet()
	if tablet.Type != topodatapb.TabletType_PRIMARY || !tm.QueryServiceControl.IsServing() {
		return nil, nil
	}
	p := &servingPause{
		tm:         tm,
		tabletType: tablet.Type,
		termStart:  protoutil.TimeFromProto(tablet.PrimaryTermStartTime).UTC(),
		reason:     reason,
		started:    time.Now(),
	}
	log.Info("The primary stops serving for a change of its replication", slog.String("reason", reason), slog.Duration("notice", groupReplicationPauseNotice))
	tm.QueryServiceControl.EnterLameduck()
	tm.QueryServiceControl.BroadcastHealth()
	notice := time.NewTimer(groupReplicationPauseNotice)
	defer notice.Stop()
	select {
	case <-ctx.Done():
		p.resume(ctx)
		return nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "the primary did not stop serving before %s: %v", reason, ctx.Err())
	case <-notice.C:
	}
	tm.tmState.setServingPause(reason)
	if err := tm.QueryServiceControl.SetServingType(p.tabletType, p.termStart, false, reason); err != nil {
		p.resume(ctx)
		return nil, vterrors.Wrapf(err, "failed to stop serving before %s", reason)
	}
	return p, nil
}

// resume ends a planned pause: once MySQL is writable, or groupReplicationPauseResumeTimeout
// passed, the tablet serves again with its primary term, unless it must not serve for its
// replication group (the serving invariant, see tmState.SetServingUnlessGroupReplicationNotServing).
// MySQL may still refuse writes if the change failed; the tablet then serves anyway, as a primary
// whose MySQL is read-only, which VTOrc repairs. vtgate's buffering ends when it sees the tablet
// serve again (see tabletserver's announceNotServingBeforeResuming). It does nothing on a nil pause.
func (p *servingPause) resume(ctx context.Context) {
	if p == nil {
		return
	}
	tm := p.tm
	if reason, _ := tm.tmState.GroupReplicationNotServingState(); reason == "" {
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), groupReplicationPauseResumeTimeout)
		err := tm.waitForWritablePrimary(waitCtx)
		cancel()
		if err != nil {
			log.Warn("The primary serves again although its MySQL does not take writes", slog.String("reason", p.reason), slog.Any("error", err))
		}
	}
	tm.tmState.clearServingPause()
	if err := tm.tmState.SetServingUnlessGroupReplicationNotServing(p.tabletType, p.termStart); err != nil {
		log.Error("The primary failed to serve again after a change of its replication", slog.String("reason", p.reason), slog.Any("error", err))
	}
	// A pause that ended during its notice, still serving, has not announced its end yet.
	tm.QueryServiceControl.BroadcastHealth()
	log.Info("The primary's pause ended", slog.String("reason", p.reason), slog.Duration("duration", time.Since(p.started)), slog.Bool("serving", tm.QueryServiceControl.IsServing()))
}

// waitForWritablePrimary waits until MySQL takes writes as a primary: read_only and
// super_read_only are off, and it is the primary of its group if it is an active member. Group
// Replication leaves the primary it elects super_read_only: once the election ended, the tablet's
// decision that it may serve makes MySQL writable (makeGroupPrimaryWritableLocked), or keeps it from
// serving, which ends the wait. The caller holds the action lock.
func (tm *TabletManager) waitForWritablePrimary(ctx context.Context) error {
	decided := false
	for {
		status, err := tm.groupReplicationStatus(ctx)
		if err != nil {
			return err
		}
		if mysql.IsGroupMemberActive(status) && !mysql.IsGroupPrimary(status) {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL is %s %s in replication group %s, not its primary", status.MemberState, status.MemberRole, status.GroupName)
		}
		superReadOnly, err := tm.MysqlDaemon.IsSuperReadOnly(ctx)
		if err != nil {
			return err
		}
		readOnly, err := tm.MysqlDaemon.IsReadOnly(ctx)
		if err != nil {
			return err
		}
		if !superReadOnly && !readOnly {
			return nil
		}
		if !decided && mysql.IsGroupPrimary(status) && !status.GetPrimaryElectionInProgress() {
			decided = true
			if err := tm.makeGroupPrimaryWritableLocked(ctx); err != nil {
				return err
			}
			if reason, _ := tm.tmState.GroupReplicationNotServingState(); reason != "" {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the primary does not serve: %s", reason)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "MySQL is still read-only (super_read_only=%v, read_only=%v): %v", superReadOnly, readOnly, ctx.Err())
		case <-time.After(groupReplicationPollInterval):
		}
	}
}
