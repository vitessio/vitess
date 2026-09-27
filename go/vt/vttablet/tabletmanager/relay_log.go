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
	"errors"
	"fmt"
	"log/slog"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// A semi-sync replica acknowledges a transaction once it is written to its relay log, before it
// is applied, and EmergencyReparentShard elects the new primary by received (relay log)
// position. Discarding the relay log of a replica therefore drops transactions the primary may
// have reported as committed; if the primary is gone before the replica fetches them again, they
// are lost. MySQL discards the relay log on CHANGE REPLICATION SOURCE when both replication
// threads are stopped, and on RESET REPLICA.

// replicationPreserveRelayLogs enables repointing replication without discarding the relay log,
// and the checks that refuse to discard it when that would lose transactions.
var replicationPreserveRelayLogs = true

func registerReplicationRelayLogFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &replicationPreserveRelayLogs, "replication-preserve-relay-logs", replicationPreserveRelayLogs,
		"Keep the relay log when repointing a MySQL 8.0+ replica that uses GTID auto-positioning: only the replication receiver is stopped and reconfigured while the applier keeps running, "+
			"so received but unapplied transactions (possibly acknowledged to a semi-sync primary) are not discarded. Operations that must discard the relay log proceed only if the replication source can send every discarded transaction again. "+
			"Set to false to restore the previous behavior of stopping both replication threads, which discards the relay log.")
}

func init() {
	servenv.OnParseFor("vtcombo", registerReplicationRelayLogFlags)
	servenv.OnParseFor("vttablet", registerReplicationRelayLogFlags)
}

// unappliedRelayLogGTIDs returns the GTIDs the replica has received into its relay log but not
// applied yet. ok is false when the positions are not MySQL GTID sets, in which case the relay
// log contents cannot be determined.
func unappliedRelayLogGTIDs(status replication.ReplicationStatus) (unapplied replication.Mysql56GTIDSet, ok bool) {
	received, ok := status.RelayLogPosition.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return nil, false
	}
	executed, ok := status.Position.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return nil, false
	}
	return received.Difference(executed), true
}

// replicationSource is what is known of the replication source a replica will replicate from:
// its tablet, its executed GTID position and its server UUID. The zero value means they are
// unknown.
type replicationSource struct {
	tablet   *topodatapb.Tablet
	position replication.Position
	uuid     replication.SID
}

// known reports whether the replication source is known.
func (source replicationSource) known() bool {
	return source.tablet != nil
}

// lacks returns the GTIDs in gtids that the source does not have.
//
// The source has its own transactions even when its executed position lacks them: a
// transaction with the source's server UUID originated on the source, which wrote it to its
// binary log before any replica received it. Replicas receive a transaction before the source
// commits it (a semi-sync source waits for their acknowledgement to commit), and the position may
// have been read before the replica's status, so it can lag behind what the replica received.
// A source that lost some of its own transactions (a crash without sync_binlog=1) reuses their
// GTIDs for new transactions, so the replica could not apply its copies without diverging anyway.
// The errant GTID check exempts the primary's own UUID for the same reason.
func (source replicationSource) lacks(gtids replication.Mysql56GTIDSet) replication.Mysql56GTIDSet {
	gtids = gtids.RemoveUUID(source.uuid)
	sourceGTIDs, ok := source.position.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return gtids
	}
	return gtids.Difference(sourceGTIDs)
}

// sourceGTIDPurged returns the GTIDs whose binary logs the replication source has purged. It
// cannot send those to a replica again.
func (tm *TabletManager) sourceGTIDPurged(ctx context.Context, source replicationSource) (replication.Mysql56GTIDSet, error) {
	status, err := tm.tmc.FullStatus(ctx, source.tablet)
	if err != nil {
		return nil, err
	}
	// A tablet whose disk is stalled returns a status without any MySQL data. A MySQL source
	// that collected its status always reports its purged GTIDs, even when it purged nothing.
	if status.DiskStalled {
		return nil, vterrors.New(vtrpc.Code_UNAVAILABLE, "the replication source's disk is stalled")
	}
	if status.GtidPurged == "" {
		return nil, vterrors.New(vtrpc.Code_UNAVAILABLE, "the replication source did not report its purged GTIDs")
	}
	purged, err := replication.DecodePosition(status.GtidPurged)
	if err != nil {
		return nil, err
	}
	gtids, ok := purged.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "the replication source's purged GTID set %s is not a MySQL GTID set", status.GtidPurged)
	}
	return gtids, nil
}

// checkRelayLogDiscard decides whether an operation that discards the relay log may proceed,
// given the replica's replication status, which must be final (the receiver stopped).
// Discarded transactions can only come back by fetching them again from the replication
// source, so it refuses with FAILED_PRECONDITION if the source the replica will replicate from
// lacks any of them, or has purged the binary logs that hold them. When the source is unknown,
// it logs the transactions that the source must still provide.
func (tm *TabletManager) checkRelayLogDiscard(ctx context.Context, status replication.ReplicationStatus, source replicationSource, operation string) error {
	if !replicationPreserveRelayLogs {
		return nil
	}
	unapplied, ok := unappliedRelayLogGTIDs(status)
	if !ok || unapplied.Empty() {
		return nil
	}
	if !source.known() {
		log.Warn(fmt.Sprintf("%s discards the relay log, which holds received but unapplied transactions %s. "+
			"The replication source must send them again; any it no longer has are lost.", operation, unapplied))
		return nil
	}
	if lost := source.lacks(unapplied); !lost.Empty() {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION,
			"refusing to %s: it discards the relay log, and the new replication source (executed %s) lacks these received but unapplied transactions, which would be lost: %s",
			operation, source.position, lost)
	}
	purged, err := tm.sourceGTIDPurged(ctx, source)
	if err != nil {
		return vterrors.Wrapf(err, "refusing to %s: it discards the relay log, which holds received but unapplied transactions %s, "+
			"and the replication source's purged GTIDs cannot be read to check that it can send them again", operation, unapplied)
	}
	if unsendable := unapplied.Difference(unapplied.Difference(purged)); !unsendable.Empty() {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION,
			"refusing to %s: it discards the relay log, and the new replication source has purged the binary logs holding these received but unapplied transactions, so it cannot send them again: %s",
			operation, unsendable)
	}
	log.Info(fmt.Sprintf("%s discards the relay log; its unapplied transactions %s will be fetched again from the replication source, which has them", operation, unapplied))
	return nil
}

// checkRelayLogDiscardBefore reads the replication status and applies checkRelayLogDiscard. If
// the status cannot be read, the relay log contents are unknown: it refuses when the source is
// known, and only logs that otherwise.
func (tm *TabletManager) checkRelayLogDiscardBefore(ctx context.Context, source replicationSource, operation string) error {
	if !replicationPreserveRelayLogs {
		return nil
	}
	status, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if errors.Is(err, mysql.ErrNotReplica) {
		return nil
	}
	if err != nil {
		if source.known() {
			return vterrors.Wrapf(err, "refusing to %s: it discards the relay log, and the replication status cannot be read to check what it holds", operation)
		}
		log.Warn(fmt.Sprintf("cannot read the replication status to check what %s discards from the relay log", operation), slog.Any("error", err))
		return nil
	}
	return tm.checkRelayLogDiscard(ctx, status, source, operation)
}

// repointReplication points replication at host:port, and starts it afterwards if
// startReplicationAfter is set. source is the new replication source.
//
// It first stops the receiver, so that the relay log contents are final when it checks them.
// For MySQL flavors with a receiver-only CHANGE REPLICATION SOURCE command, when replication is
// configured with GTID auto-positioning, it then changes only the receiver options while the
// applier runs, which makes MySQL keep the relay log (MySQL keeps it if at least one replication
// thread is running).
//
// Otherwise it reconfigures the whole channel, stopping both threads first, which discards the
// relay log: for first-time setup (there is no relay log to lose), for replication without MySQL
// GTIDs (the relay log contents are unknown), when the kill switch is off, and, after making sure
// the new source can send every unapplied relay log transaction again (refusing with
// FAILED_PRECONDITION otherwise), for other flavors, without auto-positioning, and when the
// applier is stopped and cannot be started, or stopped on an error.
//
// It refuses with FAILED_PRECONDITION when the replica received transactions of other servers
// that the new source lacks: applying them would introduce errant GTIDs, and discarding them would
// lose them. It stops the applier in that case, so the unapplied ones stay unapplied. SetReplicationSource's own
// errant GTID check covers the relay log and refuses most of those before it gets here; this check
// also covers what the receiver wrote until it stopped, and tablet startup, which checks only the
// executed GTIDs.
func (tm *TabletManager) repointReplication(ctx context.Context, host string, port int32, heartbeatInterval float64, source replicationSource, wasReplicating bool, startReplicationAfter bool) error {
	if !replicationPreserveRelayLogs {
		return tm.setReplicationSourceRecoverable(ctx, host, port, heartbeatInterval, replicationSource{}, wasReplicating, startReplicationAfter)
	}
	fullReconfiguration := func(stopReplicationBefore bool) error {
		return tm.setReplicationSourceRecoverable(ctx, host, port, heartbeatInterval, source, stopReplicationBefore, startReplicationAfter)
	}

	status, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if errors.Is(err, mysql.ErrNotReplica) {
		// Replication is not configured, so there is no relay log.
		return fullReconfiguration(wasReplicating)
	}
	if err != nil {
		return err
	}
	if _, ok := unappliedRelayLogGTIDs(status); !ok {
		// Without MySQL GTIDs, what the relay log holds cannot be checked.
		return fullReconfiguration(wasReplicating)
	}

	// Stop the receiver, so the relay log contents are final when they are checked.
	if err := tm.MysqlDaemon.StopIOThread(ctx); err != nil {
		return vterrors.Wrapf(err, "failed to stop the replication receiver")
	}
	status, err = tm.MysqlDaemon.ReplicationStatus(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the replication status after stopping the replication receiver")
	}
	unapplied, _ := unappliedRelayLogGTIDs(status)
	// Check everything the replica received, not only what is still unapplied: the applier
	// may have applied some of what arrived while the receiver was stopping.
	received, _ := status.RelayLogPosition.GTIDSet.(replication.Mysql56GTIDSet)
	if errant := source.lacks(received); !errant.Empty() {
		refusal := fmt.Sprintf("refusing to change the replication source: the replica received transactions %s that the new replication source (executed %s) lacks; "+
			"applying them would introduce errant GTIDs, and discarding the unapplied ones would lose them", errant, source.position)
		if status.SQLHealthy() {
			// Keep the unapplied ones unapplied. A stop that fails (e.g. times out on a
			// blocked applier) may still take effect later, after the applier applied more.
			if err := tm.MysqlDaemon.StopReplication(ctx, tm.hookExtraEnv()); err != nil {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "%s; stopping the replication applier to keep the unapplied ones unapplied failed: %s", refusal, err.Error())
			}
		}
		return vterrors.New(vtrpc.Code_FAILED_PRECONDITION, refusal)
	}

	const operation = "change the replication source"
	discardRelayLog := func() error {
		if err := tm.checkRelayLogDiscard(ctx, status, source, operation); err != nil {
			return err
		}
		return fullReconfiguration(true)
	}
	supported, err := tm.MysqlDaemon.SupportsReplicationSourceReceiverChange(ctx)
	if err != nil {
		return err
	}
	if !supported || !status.AutoPosition {
		// Only the full reconfiguration can change the source.
		return discardRelayLog()
	}
	if !status.SQLHealthy() {
		if unapplied.Empty() {
			// Nothing to lose: take the regular path, which also resets the applier.
			return fullReconfiguration(true)
		}
		// An applier that stopped on an error is not started: it stops again on the same
		// transaction within milliseconds, and if it does so before the change, MySQL discards
		// the relay log without the checks of the discard below.
		if startReplicationAfter && status.LastSQLError == "" {
			// Replication is going to run anyway: start the applier first, so the relay log
			// survives the change. If it fails to start, the relay log will be discarded.
			if err := tm.MysqlDaemon.StartSQLThread(ctx); err != nil {
				log.Warn("failed to start the replication applier to preserve the relay log", slog.Any("error", err))
			} else if status, err = tm.MysqlDaemon.ReplicationStatus(ctx); err != nil {
				return err
			}
		}
		if !status.SQLHealthy() {
			return discardRelayLog()
		}
	}

	if err := tm.MysqlDaemon.SetReplicationSourceReceiver(ctx, host, port, heartbeatInterval); err != nil {
		if !isRecoverableReplicationInitializationError(err) {
			return err
		}
		// The replication metadata is broken; the full reconfiguration resets it.
		log.Warn("Encountered recoverable replication initialization error while changing the replication receiver, reconfiguring replication",
			slog.String("source_host", host), slog.Int("source_port", int(port)), slog.Any("error", err))
		return discardRelayLog()
	}
	tm.verifyRelayLogKept(ctx, unapplied, source)

	if !startReplicationAfter {
		return nil
	}
	return tm.startReplicationRecoverable(ctx, source)
}

// verifyRelayLogKept logs an error if the replica no longer has transactions that were in its
// relay log before a receiver-only change, and the new source cannot send them again (it lacks
// them, or purged their binary logs). That happens if the applier stopped right before the
// change; no check before the change can prevent it, as keeping the relay log is the only way
// not to lose transactions the source cannot send again. A transaction that was only partially
// received is expected to be dropped (it is fetched again, and was never acknowledged).
func (tm *TabletManager) verifyRelayLogKept(ctx context.Context, unappliedBefore replication.Mysql56GTIDSet, source replicationSource) {
	if unappliedBefore.Empty() {
		return
	}
	status, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if err != nil {
		log.Warn("cannot read the replication status to verify that the relay log was kept", slog.Any("error", err))
		return
	}
	received, ok := status.RelayLogPosition.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return
	}
	executed, ok := status.Position.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return
	}
	missing := unappliedBefore.Difference(received).Difference(executed)
	if missing.Empty() {
		return
	}
	lost := source.lacks(missing)
	if !lost.Empty() {
		log.Error(fmt.Sprintf("changing the replication source discarded received but unapplied transactions %s from the relay log, and the new replication source "+
			"(executed %s) lacks %s. Unless that is a partially received transaction, acknowledged transactions may have been lost.", missing, source.position, lost))
		return
	}
	purged, err := tm.sourceGTIDPurged(ctx, source)
	if err != nil {
		log.Error(fmt.Sprintf("changing the replication source discarded received but unapplied transactions %s from the relay log, and the new replication source's "+
			"purged GTIDs cannot be read to check that it can send them again", missing), slog.Any("error", err))
		return
	}
	if unsendable := missing.Difference(missing.Difference(purged)); !unsendable.Empty() {
		log.Error(fmt.Sprintf("changing the replication source discarded received but unapplied transactions %s from the relay log, and the new replication source "+
			"has purged the binary logs holding %s, so it cannot send them again. The replica cannot continue replicating without them.", missing, unsendable))
		return
	}
	log.Warn(fmt.Sprintf("changing the replication source discarded received but unapplied transactions %s from the relay log; "+
		"the new replication source has them and will send them again", missing))
}
