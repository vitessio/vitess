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
)

// A semi-sync replica acknowledges a transaction once it is written to its relay log, before it
// is applied, and EmergencyReparentShard elects the new primary by received (relay log)
// position. Discarding the relay log of a replica therefore drops transactions the primary may
// have reported as committed; if the primary is gone before the replica fetches them again, they
// are lost. MySQL discards the relay log on CHANGE REPLICATION SOURCE when both replication
// threads are stopped, and on RESET REPLICA.
//
// Repointing replication (repointReplication) therefore never loses transactions: it keeps the
// relay log when it can, and discards it only when the new source has every transaction in it.
// Resetting replication (RESET REPLICA, and the full CHANGE REPLICATION SOURCE with both threads
// stopped) discards it unchecked, and is only used where that is needed: to set up replication,
// and to recover from broken replication metadata, which MySQL refuses to replicate with.

// replicationPreserveRelayLogs enables repointing replication without discarding the relay log.
var replicationPreserveRelayLogs = true

func registerReplicationRelayLogFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &replicationPreserveRelayLogs, "replication-preserve-relay-logs", replicationPreserveRelayLogs,
		"Keep the relay log when repointing a MySQL 8.0.26+ replica that uses GTID auto-positioning: only the replication receiver is stopped and reconfigured while the applier keeps running, "+
			"so received but unapplied transactions (possibly acknowledged to a semi-sync primary) are not discarded. A repoint whose replica received transactions of another server that the new source lacks fails, instead of applying or discarding them. "+
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
// its executed GTID position and its server UUID.
type replicationSource struct {
	position replication.Position
	uuid     replication.SID
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

// repointReplication points replication at host:port, and starts it afterwards if
// startReplicationAfter is set. source is the new replication source.
//
// For MySQL 8.0.26 and later with GTID auto-positioning, which is how Vitess configures
// replication, it stops the receiver, so that the relay log contents are final when it checks
// them, and changes only the receiver options while the applier runs, which makes MySQL keep the
// relay log (MySQL keeps it if at least one replication thread is running). A stopped applier is
// started first if replication is to run afterwards, also when it stopped on an error: if it
// stops again before the change, the relay log is discarded as below; otherwise it is kept,
// including a relay log the applier cannot read, until a later repoint finds the applier stopped.
//
// It refuses with FAILED_PRECONDITION when the replica received transactions of other servers
// that the new source lacks: applying them would introduce errant GTIDs, and discarding them would
// lose them. It stops the applier in that case, so the unapplied ones stay unapplied.
// SetReplicationSource's own errant GTID check covers the relay log and refuses most of those
// before it gets here; this check also covers what the receiver wrote until it stopped, and
// tablet startup, which checks only the executed GTIDs. Once it passes, the new source has every
// transaction the relay log holds, so discarding the relay log loses nothing.
//
// Otherwise it reconfigures the whole channel as before, which discards the relay log: when the
// applier cannot run (it is to stay stopped, or does not start or stops again), for first-time
// setup (there is no relay log), for replication that Vitess does not configure this way
// (without MySQL GTIDs, before MySQL 8.0.26, without auto-positioning), when the kill switch is
// off, and when the receiver-only change hits broken replication metadata, which only resetting
// replication repairs.
func (tm *TabletManager) repointReplication(ctx context.Context, host string, port int32, heartbeatInterval float64, source replicationSource, wasReplicating bool, startReplicationAfter bool) error {
	fullReconfiguration := func(stopReplicationBefore bool) error {
		return tm.setReplicationSourceRecoverable(ctx, host, port, heartbeatInterval, stopReplicationBefore, startReplicationAfter)
	}
	if !replicationPreserveRelayLogs {
		return fullReconfiguration(wasReplicating)
	}

	status, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if errors.Is(err, mysql.ErrNotReplica) {
		// Replication is not configured, so there is no relay log.
		return fullReconfiguration(wasReplicating)
	}
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the replication status before changing the replication source")
	}
	if _, ok := unappliedRelayLogGTIDs(status); !ok {
		// Without MySQL GTIDs, what the relay log holds cannot be checked.
		return fullReconfiguration(wasReplicating)
	}
	supported, err := tm.MysqlDaemon.SupportsReplicationSourceReceiverChange(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "failed to check whether the replication receiver can be changed alone")
	}
	if !supported || !status.AutoPosition {
		// Replication that Vitess does not configure this way, from MySQL before 8.0.26 or
		// without auto-positioning (Vitess always enables it), keeps the previous repoint.
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

	if !status.SQLHealthy() {
		// The receiver-only change keeps the relay log only while the applier runs. Replication is
		// going to run anyway, so start the applier first.
		if startReplicationAfter && !unapplied.Empty() {
			if err := tm.MysqlDaemon.StartSQLThread(ctx); err != nil {
				log.Warn("failed to start the replication applier to keep the relay log", slog.Any("error", err))
			} else if status, err = tm.MysqlDaemon.ReplicationStatus(ctx); err != nil {
				return vterrors.Wrapf(err, "failed to read the replication status after starting the replication applier; the replication receiver is stopped")
			}
		}
		if !status.SQLHealthy() {
			// The new source has every transaction the relay log holds (checked above).
			return fullReconfiguration(true)
		}
	}

	if err := tm.MysqlDaemon.SetReplicationSourceReceiver(ctx, host, port, heartbeatInterval); err != nil {
		if !isRecoverableReplicationInitializationError(err) {
			return vterrors.Wrapf(err, "failed to change the replication receiver; the replication receiver is stopped")
		}
		// The replication metadata is broken; only resetting replication repairs it.
		log.Warn("Encountered recoverable replication initialization error while changing the replication receiver, reconfiguring replication",
			slog.String("source_host", host), slog.Int("source_port", int(port)), slog.Any("error", err))
		return fullReconfiguration(true)
	}

	if !startReplicationAfter {
		return nil
	}
	return tm.startReplicationRecoverable(ctx)
}
