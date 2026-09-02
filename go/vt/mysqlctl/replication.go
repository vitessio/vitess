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

/*
Handle creating replicas and setting up the replication streams.
*/

package mysqlctl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// Queries used for RPCs
	getGlobalStatusQuery = "SELECT variable_name, variable_value FROM performance_schema.global_status"

	// superReadOnlyResetTimeout bounds the reset function returned by
	// SetSuperReadOnly.
	superReadOnlyResetTimeout = 1 * time.Minute
)

type (
	ResetSuperReadOnlyFunc func() error

	// SetSuperReadOnlyOption configures how SetSuperReadOnly runs.
	SetSuperReadOnlyOption func(*setSuperReadOnlyOptions)

	setSuperReadOnlyOptions struct {
		lockWaitTimeout time.Duration
	}

	// replicaShutdownState records the replica state that
	// prepareReplicaForShutdown changes, so that a shutdown which
	// subsequently fails (leaving mysqld running) can restore it.
	replicaShutdownState struct {
		startReceiver       bool
		startApplier        bool
		flushLogAtTrxCommit string
		syncBinlog          string
		syncRelayLog        string

		// The interrupted flags record a STOP statement that was issued but
		// did not return success (e.g. it was killed at the preparation
		// deadline, or hit rpl_stop_replica_timeout): the server-side stop may
		// then still be draining, and a restore must wait for it to settle --
		// a START issued while the thread is still draining is a no-op, and
		// the stop landing afterwards would leave replication stopped. The
		// wait is bounded (replicaRestorePendingStopSettlePasses): the stop is
		// only possibly pending, so one that never settles is eventually left
		// to external recovery.
		receiverStopInterrupted bool
		applierStopInterrupted  bool

		// cycleReceiver records a receiver stop that failed because stopping
		// the connection-failover monitor timed out
		// (SOURCE_CONNECTION_AUTO_FAILOVER, MySQL error 4011): the receiver
		// stop was never requested, so the receiver keeps running, but its
		// monitor may be left stopped. The restore must then cycle the
		// receiver -- stop it and start it again -- to bring both back.
		cycleReceiver bool
	}
)

const (
	// replicaRestorePollInterval is how often the post-failed-shutdown restore
	// re-reads the replication status while reconciling the replication threads.
	replicaRestorePollInterval = time.Second

	// replicaRestoreConnectTimeout is how long the post-failed-shutdown
	// restore keeps retrying while mysqld stays continuously unreachable: a
	// replica that just survived a failed shutdown can briefly refuse
	// connections, but one that stays unreachable this long is treated as
	// exiting after all.
	replicaRestoreConnectTimeout = time.Minute

	// replicaRestorePendingStopSettlePasses bounds how long the
	// post-failed-shutdown restore waits for an interrupted replication-thread
	// stop to settle once everything else has converged. An interrupted stop
	// is only possibly pending server-side -- the common CRServerLost case is
	// a statement the server never received -- so a stop that never lands must
	// not hold the restoration (and with it the shutdown locks and Close) for
	// the full restore deadline. Once the durability settings are restored and
	// the threads have reported the desired state for this many consecutive
	// passes, the restoration converges and leaves a stop that lands later to
	// external recovery (e.g. VTOrc).
	replicaRestorePendingStopSettlePasses = 30
)

// WithLockWaitTimeout sets the session lock_wait_timeout (rounded up to whole
// seconds) for the SET GLOBAL super_read_only statement, bounding how long it
// waits for metadata locks held by in-flight queries. By default the server's
// value is left untouched. A zero or negative timeout is the same as omitting
// the option: the server's value is left untouched and the wait is unbounded.
func WithLockWaitTimeout(timeout time.Duration) SetSuperReadOnlyOption {
	return func(options *setSuperReadOnlyOptions) {
		options.lockWaitTimeout = timeout
	}
}

// WaitForReplicationStart waits until the deadline for replication to start.
// This validates the current primary is correct and can be connected to.
func WaitForReplicationStart(ctx context.Context, mysqld MysqlDaemon, replicaStartDeadline int) (err error) {
	var replicaStatus replication.ReplicationStatus
	for range replicaStartDeadline {
		replicaStatus, err = mysqld.ReplicationStatus(ctx)
		if err != nil {
			return err
		}

		if replicaStatus.Running() {
			return nil
		}
		time.Sleep(time.Second)
	}
	errs := make([]string, 0, 2)
	if replicaStatus.LastSQLError != "" {
		errs = append(errs, "Last_SQL_Error: "+replicaStatus.LastSQLError)
	}
	if replicaStatus.LastIOError != "" {
		errs = append(errs, "Last_IO_Error: "+replicaStatus.LastIOError)
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

// StartReplication starts replication.
func (mysqld *Mysqld) StartReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StartReplicationCommand()}); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptionalContext(ctx)
}

// StartReplicationUntilAfter starts replication until replication has come to `targetPos`, then it stops replication
func (mysqld *Mysqld) StartReplicationUntilAfter(ctx context.Context, targetPos replication.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	queries := []string{conn.Conn.StartReplicationUntilAfterCommand(targetPos)}

	return mysqld.executeSuperQueryListConn(ctx, conn, queries)
}

// StartSQLThreadUntilAfter starts replication's SQL thread(s) until replication has come to `targetPos`, then it stops it
func (mysqld *Mysqld) StartSQLThreadUntilAfter(ctx context.Context, targetPos replication.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	queries := []string{conn.Conn.StartSQLThreadUntilAfterCommand(targetPos)}

	return mysqld.executeSuperQueryListConn(ctx, conn, queries)
}

// StopReplication stops replication.
func (mysqld *Mysqld) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptionalContext(ctx); err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StopReplicationCommand()})
}

// StopIOThread stops a replica's IO thread only.
func (mysqld *Mysqld) StopIOThread(ctx context.Context) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StopIOThreadCommand()})
}

// prepareReplicaForShutdown places a replica in a crash-safe state before
// shutdown. It calls onStateCaptured with the recorded pre-change state right
// before it starts mutating anything, so that a caller abandoning a
// timed-out preparation can tell whether a mutation may still land (and a
// restore is needed) or the preparation never got past its read-only probes.
//
// When inherited is non-nil, a retrying shutdown has taken over a pending
// restoration: inherited is the replica's true prior state, recorded before
// the first fence. Re-capturing here would read the half-restored state
// instead, so the read-only probes are skipped and the fence is re-applied
// directly on the inherited state. Skipping the probes also skips the role
// re-check, deliberately: the fence only tightens durability -- safe for any
// role, on a server this call is about to shut down -- and its thread stops
// fail harmlessly on a server that is no longer a replica, while the
// role-sensitive direction (relaxing the settings) stays guarded by the
// restore's per-pass probe. A role probe here would only narrow the promotion
// race while adding a failure mode of its own. The inherited state is
// published (and on failure returned) before anything that can fail: the
// previous restoration was cancelled when it was inherited, so this
// preparation owns the state now and must hand it back even when it never
// reaches mysqld -- otherwise a subsequently failed shutdown would have
// nothing to arm a replacement restoration from, and the replica would stay
// fenced with replication stopped.
//
// It uses a dedicated connection rather than the pools -- killed on ctx expiry
// by the context-aware executors -- so that a preparation hung in mysqld can
// never strand a pool slot: repeated failed shutdown attempts must not exhaust
// the DBA pool of a long-lived caller.
func (mysqld *Mysqld) prepareReplicaForShutdown(ctx context.Context, inherited *replicaShutdownState, onStateCaptured func(*replicaShutdownState)) (*replicaShutdownState, error) {
	var state *replicaShutdownState
	if inherited != nil {
		state = inherited
		onStateCaptured(state)
	}

	conn, err := mysqld.GetDbaConnection(ctx)
	if err != nil {
		return state, vterrors.Wrap(err, "failed to connect to MySQL before shutdown")
	}
	defer conn.Close()

	if inherited == nil {
		status, err := mysqld.showReplicationStatusDirectContext(ctx, conn)
		if err != nil {
			if errors.Is(err, mysql.ErrNotReplica) {
				return nil, nil
			}
			return nil, vterrors.Wrap(err, "failed to read replication status before shutdown")
		}

		// Record the state we are about to change so that a shutdown which
		// subsequently fails -- leaving mysqld running -- can restore it.
		qr, err := mysqld.executeFetchDirectContext(ctx, conn,
			"SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log")
		if err != nil {
			return nil, vterrors.Wrap(err, "failed to read the durability settings before shutdown")
		}
		if qr == nil || len(qr.Rows) != 1 || len(qr.Rows[0]) != 3 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"unexpected result reading the durability settings before shutdown: %+v", qr)
		}
		state = &replicaShutdownState{
			startReceiver: status.IOState == replication.ReplicationStateRunning ||
				status.IOState == replication.ReplicationStateConnecting,
			startApplier:        status.SQLState == replication.ReplicationStateRunning,
			flushLogAtTrxCommit: qr.Rows[0][0].ToString(),
			syncBinlog:          qr.Rows[0][1].ToString(),
			syncRelayLog:        qr.Rows[0][2].ToString(),
		}
		onStateCaptured(state)
	}

	// Restore full durability before shutdown: innodb_flush_log_at_trx_commit=1
	// and sync_binlog=1 re-enable per-commit InnoDB redo and binary log flushing
	// (both are often relaxed together to speed up replica catch-up), and
	// sync_relay_log=1 protects relay writes that race an interrupted receiver
	// stop. Those settings only govern commits from here on, so the flushes then
	// make the tails already written under the relaxed settings durable: FLUSH
	// ENGINE LOGS syncs the existing InnoDB redo, and rotating the binary and
	// relay logs syncs their current files. Every flush must be
	// NO_WRITE_TO_BINLOG: a binlogged FLUSH on a GTID server is assigned a
	// transaction from this replica's own UUID -- an errant GTID that later
	// blocks reparents and keeps the replica from rejoining. Stopping the
	// receiver and applier is then best effort.
	//
	// The fence statements are independent: each one narrows the crash-safety
	// gap on its own, so a failure in one -- e.g. a binary log rotation
	// failing on a full disk -- must not short-circuit the relay log flush or
	// the thread stops below, which close exactly the gap this fence exists
	// for. Failures are collected and surfaced together once everything has
	// been attempted.
	var fenceErrs []error
	for _, query := range []string{
		"SET GLOBAL innodb_flush_log_at_trx_commit = 1",
		"SET GLOBAL sync_binlog = 1",
		"SET GLOBAL sync_relay_log = 1",
		"FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS",
		"FLUSH NO_WRITE_TO_BINLOG BINARY LOGS",
		"FLUSH NO_WRITE_TO_BINLOG RELAY LOGS",
	} {
		if err := mysqld.executeSuperQueryListDirectContext(ctx, conn, []string{query}); err != nil {
			fenceErrs = append(fenceErrs, err)
		}
	}
	// Skip the thread stops for flavors without executable replication-thread
	// commands (see replicationThreadCommandAvailable) rather than issue a
	// statement that always fails.
	if stopReceiver := conn.StopIOThreadCommand(); replicationThreadCommandAvailable(stopReceiver) {
		if ctx.Err() != nil {
			// The preparation deadline has passed: do not dispatch a stop we
			// would then have to treat as possibly pending.
			log.Warn("skipping the replication receiver stop before shutdown; the preparation deadline has passed")
		} else if err := mysqld.executeSuperQueryListDirectContext(ctx, conn, []string{stopReceiver}); err != nil {
			var sqlErr *sqlerror.SQLError
			if errors.As(err, &sqlErr) && sqlErr.Number() == sqlerror.ERStopReplicaMonitorIOThreadTimeout {
				// Stopping the connection-failover monitor timed out before
				// the receiver stop was even requested: the receiver keeps
				// running, but its monitor may be left stopped. Record it so
				// a restore cycles the receiver to bring both back.
				state.cycleReceiver = true
			} else {
				state.receiverStopInterrupted = stopInterrupted(err)
			}
			log.Warn(
				"failed to stop the replication receiver before shutdown; the stop is best effort",
				slog.Any("error", err),
			)
		}
	}
	// Stopping the applier lets the (multi-threaded) worker queue drain to a
	// gap-free, position-consistent point, so an interrupted shutdown or crash
	// has less in-flight work to recover. Best effort and bounded by ctx: a
	// hung applier flush must not block shutdown.
	if stopApplier := conn.StopSQLThreadCommand(); replicationThreadCommandAvailable(stopApplier) {
		if ctx.Err() != nil {
			log.Warn("skipping the replication applier stop before shutdown; the preparation deadline has passed")
		} else if err := mysqld.executeSuperQueryListDirectContext(ctx, conn, []string{stopApplier}); err != nil {
			state.applierStopInterrupted = stopInterrupted(err)
			log.Warn(
				"failed to stop the replication applier before shutdown; the stop is best effort",
				slog.Any("error", err),
			)
		}
	}
	if len(fenceErrs) > 0 {
		// Return the state as well: some settings may have been changed before
		// a statement failed, and a failed shutdown should still restore them.
		return state, vterrors.Wrap(errors.Join(fenceErrs...), "failed to establish the crash-safety durability fence before shutdown")
	}
	return state, nil
}

// replicationThreadCommandAvailable reports whether a flavor-provided
// replication-thread command can actually be executed. Flavors that do not
// use classic replication threads return "" (e.g. MySQL Group Replication,
// whose members are managed by an external orchestrator), and the file
// position flavor for unmanaged servers returns mysql.UnsupportedCommand:
// neither is a statement worth sending, so the crash-safety preparation and
// restoration skip their thread stops and starts rather than issue queries
// that always fail.
func replicationThreadCommandAvailable(cmd string) bool {
	return cmd != "" && cmd != mysql.UnsupportedCommand
}

// stopInterrupted reports whether a failed replication-thread stop may have
// left its server-side stop pending: either the statement was cut short
// client-side (a context error from the bounded executors, whose kill does not
// cancel a stop the server already accepted), or the server itself reported
// that the stop timed out while remaining in effect (rpl_stop_replica_timeout,
// MySQL errors 1875/1876). Any other error is a definitive server response:
// the statement completed without leaving a stop pending.
func stopInterrupted(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if sqlErr, ok := errors.AsType[*sqlerror.SQLError](err); ok {
		switch sqlErr.Number() {
		case sqlerror.ERStopReplicaIOThreadTimeout, sqlerror.ERStopReplicaSQLThreadTimeout:
			// rpl_stop_replica_timeout fired: the stop remains in effect.
			return true
		case sqlerror.CRServerLost:
			// The statement was written but the response was lost: the server
			// may have accepted the stop and still be completing it. Its
			// sibling CRServerGone means the write itself failed -- the
			// statement was never delivered -- and stays definitive.
			return true
		}
	}
	return false
}

// showReplicationStatusDirectContext reads the replication status on a
// dedicated (non-pooled) connection, honoring ctx the way
// executeFetchDirectContext does: if ctx expires, the connection is killed so
// a hung status probe cannot outlive its caller unnoticed.
func (mysqld *Mysqld) showReplicationStatusDirectContext(ctx context.Context, conn *dbconnpool.DBConnection) (replication.ReplicationStatus, error) {
	// Fast fail if context is done.
	select {
	case <-ctx.Done():
		return replication.ReplicationStatus{}, ctx.Err()
	default:
	}

	// Execute asynchronously so we can select on both it and the context.
	var status replication.ReplicationStatus
	var executeErr error
	done := make(chan struct{})
	go func() {
		defer close(done)

		status, executeErr = conn.ShowReplicationStatus()
	}()

	select {
	case <-done:
		return status, executeErr
	case <-ctx.Done():
		// If both are done already, we may end up here anyway because select
		// chooses among multiple ready channels pseudorandomly.
		// Check the done channel and prefer that one if it's ready.
		select {
		case <-done:
			return status, executeErr
		default:
		}

		// The context expired or was canceled.
		// Try to kill the connection to effectively cancel the status read.
		connID := conn.ID()
		log.Info(fmt.Sprintf("Mysqld.showReplicationStatusDirectContext(): killing connID %v due to timeout", connID))
		if killErr := mysqld.killConnection(ctx, connID); killErr != nil {
			log.Warn(fmt.Sprintf("Mysqld.showReplicationStatusDirectContext(): failed to kill connID %v: %v", connID, killErr))
		}
		// Close the connection before waiting: if the server cannot service
		// the KILL (it is wedged), closing the socket is what unblocks the
		// in-flight status read client-side, so this wait stays bounded.
		conn.Close()
		<-done
		// It may have succeeded before we tried to kill it.
		if executeErr == nil {
			return status, executeErr
		}
		return replication.ReplicationStatus{}, ctx.Err()
	}
}

// restoreReplicaAfterFailedShutdown makes a best-effort attempt to undo what
// prepareReplicaForShutdown changed, for use when the subsequent shutdown
// failed and mysqld is still running: it restores the recorded durability
// settings and restarts whichever replication threads were running before.
//
// It uses a dedicated connection rather than the pools, so it keeps working
// after Close has closed them, and its START REPLICA serializes behind -- that
// is, waits out -- a server-side STOP REPLICA that is still draining, which is
// why callers must give it a generous deadline. That wait is bounded by
// settlePasses: an interrupted stop is only possibly pending, so once
// everything else has converged and the threads have held the desired state
// for that many consecutive passes, the restoration converges and leaves a
// stop that lands later to external recovery. All steps are best effort and
// retried within ctx: mysqld can be briefly unreachable right after the very
// failed shutdown that makes this restoration matter, so failed connections
// (and status reads on a connection the shutdown may have broken) are retried
// rather than abandoned. Only once mysqld has stayed continuously unreachable
// for connectTimeout is it treated as exiting after all -- with nothing left
// to restore -- so that a pending restoration cannot hold Close's bounded
// wait for the full restore deadline when mysqld is already gone.
//
// Role changes (promotion, RESET REPLICA ALL) do not serialize with this
// restoration, so every pass re-verifies that the server is still a replica
// BEFORE touching the durability settings, and the restoration ends the
// moment it is not: relaxed replica-catchup settings must not land on (or
// clobber the configuration of) a newly promoted primary. The window of a
// single in-flight SET racing the role change cannot be closed from this
// layer.
func (mysqld *Mysqld) restoreReplicaAfterFailedShutdown(ctx context.Context, state *replicaShutdownState, pollInterval, connectTimeout time.Duration, settlePasses int) {
	var conn *dbconnpool.DBConnection
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	var unreachableSince time.Time
	settingsRestored := false
	statusObserved := false
	settledPasses := 0
	pendingReceiver := state.receiverStopInterrupted
	pendingApplier := state.applierStopInterrupted
	cycleReceiver := state.cycleReceiver
	needReceiver := state.startReceiver
	needApplier := state.startApplier
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				switch {
				case cycleReceiver:
					log.Warn("gave up cycling the replication receiver after a failed shutdown; its connection-failover monitor may be left stopped")
				case statusObserved && !needReceiver && !needApplier && !settingsRestored:
					log.Warn("gave up restoring the durability settings after a failed shutdown; the replica keeps the safer full-durability settings")
				case statusObserved && !needReceiver && !needApplier:
					log.Warn("the replication threads are running as desired after a failed shutdown, but an interrupted stop may still be draining; if replication stops later it must be restarted externally (e.g. by VTOrc)")
				default:
					log.Warn(
						"gave up restoring the replica state after a failed shutdown",
						slog.Any("error", ctx.Err()),
					)
				}
				return
			case <-time.After(pollInterval):
			}
		}
		if conn == nil {
			c, err := mysqld.GetDbaConnection(ctx)
			if err != nil {
				if unreachableSince.IsZero() {
					unreachableSince = time.Now()
				} else if time.Since(unreachableSince) > connectTimeout {
					log.Warn(
						"mysqld stayed unreachable while restoring the replica state after a failed shutdown; treating it as exiting and giving up",
						slog.Any("error", err),
					)
					return
				}
				log.Warn(
					"failed to connect to MySQL to restore the replica state after a failed shutdown; retrying",
					slog.Any("error", err),
				)
				continue
			}
			unreachableSince = time.Time{}
			conn = c
		}
		// Verify the server is still a replica before every pass: a role
		// change does not serialize with this restoration, and a relaxed
		// durability SET must not land after one. The probe also resets a
		// connection a failed statement may have broken.
		status, err := mysqld.showReplicationStatusDirectContext(ctx, conn)
		if err != nil {
			if errors.Is(err, mysql.ErrNotReplica) {
				// The server stopped being a replica while we were restoring
				// (e.g. it was promoted, or its replication configuration was
				// reset): there are no threads left to reconcile, and its
				// durability settings are the new role's to manage.
				log.Warn("the server is no longer a replica; ending the replica state restoration after a failed shutdown without touching the durability settings")
				return
			}
			// The read may have failed because this connection broke:
			// reconnect for the next attempt.
			log.Warn(
				"failed to read the replication status while restoring the replica state after a failed shutdown; retrying",
				slog.Any("error", err),
			)
			conn.Close()
			conn = nil
			continue
		}
		statusObserved = true
		if !settingsRestored {
			// Only count the settings as restored once every SET succeeded, so
			// that a transient failure (e.g. on a connection the shutdown
			// broke) is retried like the thread restarts are; re-running an
			// already-applied SET is harmless.
			settingsRestored = true
			for _, query := range []string{
				"SET GLOBAL innodb_flush_log_at_trx_commit = " + state.flushLogAtTrxCommit,
				"SET GLOBAL sync_binlog = " + state.syncBinlog,
				"SET GLOBAL sync_relay_log = " + state.syncRelayLog,
			} {
				if _, err := mysqld.executeFetchDirectContext(ctx, conn, query); err != nil {
					settingsRestored = false
					log.Warn(
						"failed to restore a durability setting after a failed shutdown; retrying",
						slog.String("query", query),
						slog.Any("error", err),
					)
				}
			}
		}
		if !state.startReceiver && !state.startApplier {
			if settingsRestored {
				return
			}
			continue
		}
		// Reconcile the replication threads rather than fire a single START: an
		// interrupted STOP (killed at the preparation deadline, or one that hit
		// rpl_stop_replica_timeout) can still be draining server-side. A START
		// issued while its thread is still draining is a no-op, and the stop
		// landing afterwards would leave replication stopped -- so wait for any
		// interrupted stop to settle (observed as its thread reporting stopped),
		// start whatever should be running, and verify the result.
		receiverRunning := status.IOState == replication.ReplicationStateRunning ||
			status.IOState == replication.ReplicationStateConnecting
		applierRunning := status.SQLState == replication.ReplicationStateRunning
		// A thread observed stopped means any pending stop for it has settled,
		// and a receiver observed stopped needs no further cycling: restarting
		// it below brings its connection-failover monitor back with it.
		if !receiverRunning {
			pendingReceiver = false
			cycleReceiver = false
		}
		if !applierRunning {
			pendingApplier = false
		}
		needReceiver = state.startReceiver && !receiverRunning
		needApplier = state.startApplier && !applierRunning
		// Convergence also requires the durability settings: returning on the
		// threads alone would leave a transiently failed SET permanently at
		// the shutdown fence value (full per-commit syncing) on a live
		// replica -- the very regression this restoration undoes.
		if settingsRestored && !needReceiver && !needApplier && !pendingReceiver && !pendingApplier && !cycleReceiver {
			log.Warn("shutdown failed after replication was stopped to make the replica crash-safe; restored the previous replication state")
			return
		}
		// Only a possibly-pending stop is left. It may never land -- the
		// common CRServerLost case is a statement the server never received --
		// so once the threads have held the desired state for settlePasses
		// consecutive passes, converge rather than hold the restoration (and
		// with it the shutdown locks and Close) for the full restore deadline.
		// A stop that lands later stops replication visibly, and external
		// recovery (e.g. VTOrc) restarts it.
		if settingsRestored && !needReceiver && !needApplier && !cycleReceiver {
			settledPasses++
			if settledPasses >= settlePasses {
				log.Warn("the replication threads have held their desired state after a failed shutdown, but an interrupted stop never settled; ending the restoration -- if the stop lands later, replication must be restarted externally (e.g. by VTOrc)")
				return
			}
		} else {
			settledPasses = 0
		}
		if cycleReceiver {
			// The preparation's receiver stop failed because stopping the
			// connection-failover monitor timed out, leaving the receiver
			// running without its monitor. Stop the receiver -- retrying until
			// it takes -- so the restart below brings both back.
			if stopReceiver := conn.StopIOThreadCommand(); replicationThreadCommandAvailable(stopReceiver) {
				if _, err := mysqld.executeFetchDirectContext(ctx, conn, stopReceiver); err != nil {
					log.Warn(
						"failed to stop the replication receiver to restore its connection-failover monitor; retrying",
						slog.Any("error", err),
					)
				}
			} else {
				cycleReceiver = false
			}
		}
		if needReceiver || needApplier {
			var start string
			switch {
			case needReceiver && needApplier:
				start = conn.StartReplicationCommand()
			case needApplier:
				start = conn.StartSQLThreadCommand()
			case needReceiver:
				start = conn.StartIOThreadCommand()
			}
			if !replicationThreadCommandAvailable(start) {
				// Flavors without executable replication-thread commands have
				// nothing to start, matching the preparation's skipped stops
				// -- but only once the settings restore has also finished:
				// returning earlier would abandon a retry still in progress.
				if settingsRestored {
					return
				}
				continue
			}
			if _, err := mysqld.executeFetchDirectContext(ctx, conn, start); err != nil {
				log.Warn(
					"failed to restart replication after a failed shutdown; retrying",
					slog.Any("error", err),
				)
			}
		}
	}
}

// StopSQLThread stops a replica's SQL thread(s) only.
func (mysqld *Mysqld) StopSQLThread(ctx context.Context) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StopSQLThreadCommand()})
}

// RestartReplication stops, resets and starts replication.
func (mysqld *Mysqld) RestartReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptionalContext(ctx); err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, conn.Conn.RestartReplicationCommands()); err != nil {
		return err
	}

	h = hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptionalContext(ctx)
}

// GetMysqlPort returns mysql port
func (mysqld *Mysqld) GetMysqlPort(ctx context.Context) (int32, error) {
	// We can not use the connection pool here. This check runs very early
	// during MySQL startup when we still might be loading things like grants.
	// This means we need to use an isolated connection to avoid poisoning the
	// DBA connection pool for further queries.
	params, err := mysqld.dbcfgs.DbaConnector().MysqlParams()
	if err != nil {
		return 0, err
	}
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch("SHOW VARIABLES LIKE 'port'", 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		return 0, errors.New("no port variable in mysql")
	}
	utemp, err := qr.Rows[0][1].ToCastUint64()
	if err != nil {
		return 0, err
	}
	return int32(utemp), nil
}

// GetServerUUID returns mysql server uuid
func (mysqld *Mysqld) GetServerUUID(ctx context.Context) (string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return "", err
	}
	defer conn.Recycle()

	return conn.Conn.GetServerUUID()
}

// GetGlobalStatusVars returns the server's global status variables asked for.
// An empty/nil variable name parameter slice means you want all of them.
func (mysqld *Mysqld) GetGlobalStatusVars(ctx context.Context, variables []string) (map[string]string, error) {
	query := getGlobalStatusQuery
	if len(variables) != 0 {
		// The format specifier is for any optional predicates.
		statusBv, err := sqltypes.BuildBindVariable(variables)
		if err != nil {
			return nil, err
		}
		query, err = sqlparser.ParseAndBind(
			getGlobalStatusQuery+" WHERE variable_name IN %a",
			statusBv,
		)
		if err != nil {
			return nil, err
		}
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	finalRes := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		if len(row) != 2 {
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "incorrect number of fields in the row")
		}
		finalRes[row[0].ToString()] = row[1].ToString()
	}
	return finalRes, nil
}

// IsReadOnly return true if the instance is read only
func (mysqld *Mysqld) IsReadOnly(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW VARIABLES LIKE 'read_only'")
	if err != nil {
		return true, err
	}
	if len(qr.Rows) != 1 {
		return true, errors.New("no read_only variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}

// IsSuperReadOnly return true if the instance is super read only
func (mysqld *Mysqld) IsSuperReadOnly(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT @@global.super_read_only")
	if err != nil {
		return false, err
	}

	if len(qr.Rows) == 1 {
		sro := qr.Rows[0][0].ToString()
		if sro == "1" || sro == "ON" {
			return true, nil
		}
	}

	return false, nil
}

// SetReadOnly set/unset the read_only flag
func (mysqld *Mysqld) SetReadOnly(ctx context.Context, on bool) error {
	query := "SET GLOBAL read_only = "
	if on {
		query += "ON"
	} else {
		query += "OFF"
	}
	return mysqld.ExecuteSuperQuery(ctx, query)
}

// SetSuperReadOnly set/unset the super_read_only flag.
// Returns a function which is called to set super_read_only back to its original value.
func (mysqld *Mysqld) SetSuperReadOnly(ctx context.Context, on bool, opts ...SetSuperReadOnlyOption) (ResetSuperReadOnlyFunc, error) {
	var options setSuperReadOnlyOptions
	for _, opt := range opts {
		opt(&options)
	}

	superReadOnlyEnabled, err := mysqld.IsSuperReadOnly(ctx)
	if err != nil {
		return nil, err
	}

	// The reset function restores super_read_only to its original value, and
	// only exists when this call actually changes it. It can be used as a
	// defer by the caller.
	var resetFunc ResetSuperReadOnlyFunc
	if on != superReadOnlyEnabled {
		resetFunc = func() error {
			resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), superReadOnlyResetTimeout)
			defer cancel()
			return mysqld.execSetSuperReadOnly(resetCtx, superReadOnlyEnabled, setSuperReadOnlyOptions{})
		}
	}

	if err := mysqld.execSetSuperReadOnly(ctx, on, options); err != nil {
		return nil, err
	}

	return resetFunc, nil
}

// execSetSuperReadOnly runs the SET GLOBAL super_read_only statement, bounding
// how long it waits for metadata locks when options carries a lockWaitTimeout.
func (mysqld *Mysqld) execSetSuperReadOnly(ctx context.Context, on bool, options setSuperReadOnlyOptions) error {
	query := "SET GLOBAL super_read_only = "
	if on {
		query += "'ON'"
	} else {
		query += "'OFF'"
	}

	if options.lockWaitTimeout <= 0 {
		return mysqld.ExecuteSuperQuery(ctx, query)
	}

	// Pin a single connection so the session lock_wait_timeout applies to the
	// SET GLOBAL statement.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// lock_wait_timeout only supports whole seconds, so round up to keep
	// sub-second timeouts from truncating to 0.
	lockWaitTimeoutSeconds := int64(math.Ceil(options.lockWaitTimeout.Seconds()))
	setTimeoutQuery := fmt.Sprintf("SET SESSION lock_wait_timeout = %d", lockWaitTimeoutSeconds)
	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{setTimeoutQuery}); err != nil {
		// Some servers don't know lock_wait_timeout. Proceed without a
		// bound rather than return an error callers could mistake for
		// super_read_only being unknown.
		sqlErr, ok := errors.AsType[*sqlerror.SQLError](err)
		if !ok || sqlErr.Number() != sqlerror.ERUnknownSystemVariable {
			return err
		}

		log.Warn("server does not know about lock_wait_timeout, continuing without bounding the lock wait", slog.Any("error", err))

		return mysqld.executeSuperQueryListConn(ctx, conn, []string{query})
	}

	execErr := mysqld.executeSuperQueryListConn(ctx, conn, []string{query})
	if execErr != nil && ctx.Err() != nil {
		// The connection was interrupted mid-query, so it must not return to the pool.
		conn.Taint()
		return execErr
	}

	// Restore the session so the connection can return to the pool.
	restoreQuery := "SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"
	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{restoreQuery}); err != nil {
		log.Warn("failed to restore the session lock_wait_timeout, discarding the connection", slog.Any("error", err))
		conn.Taint()
	}

	return execErr
}

// WaitSourcePos lets replicas wait for the given replication position to
// be reached.
func (mysqld *Mysqld) WaitSourcePos(ctx context.Context, targetPos replication.Position) error {
	// Get a connection.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// First check if filePos flavored Position was passed in. If so, we
	// can't defer to the flavor in the connection, unless that flavor is
	// also filePos.
	if targetPos.MatchesFlavor(replication.FilePosFlavorID) {
		// If we are the primary, WaitUntilFilePosition will fail. But
		// position is most likely reached. So, check the position first.
		mpos, err := conn.Conn.PrimaryFilePosition()
		if err != nil {
			return vterrors.Wrapf(err, "WaitSourcePos: PrimaryFilePosition failed")
		}
		if mpos.AtLeast(targetPos) {
			return nil
		}
	} else {
		// If we are the primary, WaitUntilPosition will fail. But
		// position is most likely reached. So, check the position first.
		mpos, err := conn.Conn.PrimaryPosition()
		if err != nil {
			return vterrors.Wrapf(err, "WaitSourcePos: PrimaryPosition failed")
		}
		if mpos.AtLeast(targetPos) {
			return nil
		}
	}

	if err := conn.Conn.WaitUntilPosition(ctx, targetPos); err != nil {
		return vterrors.Wrapf(err, "WaitSourcePos failed")
	}
	return nil
}

func (mysqld *Mysqld) CatchupToGTID(ctx context.Context, targetPos replication.Position) error {
	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.Conn.CatchupToGTIDCommands(params, targetPos)
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ReplicationStatus returns the server replication status
func (mysqld *Mysqld) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}
	defer conn.Recycle()

	return conn.Conn.ShowReplicationStatus()
}

// PrimaryStatus returns the primary replication statuses
func (mysqld *Mysqld) PrimaryStatus(ctx context.Context) (replication.PrimaryStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	defer conn.Recycle()

	primaryStatus, err := conn.Conn.ShowPrimaryStatus()
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	primaryStatus.ServerUUID, err = conn.Conn.GetServerUUID()
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	return primaryStatus, nil
}

// GetGTIDPurged returns the gtid purged statuses
func (mysqld *Mysqld) GetGTIDPurged(ctx context.Context) (replication.Position, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.Position{}, err
	}
	defer conn.Recycle()

	return conn.Conn.GetGTIDPurged()
}

// PrimaryPosition returns the primary replication position.
func (mysqld *Mysqld) PrimaryPosition(ctx context.Context) (replication.Position, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.Position{}, err
	}
	defer conn.Recycle()

	return conn.Conn.PrimaryPosition()
}

// SetReplicationPosition sets the replication position at which the replica will resume
// when its replication is started.
func (mysqld *Mysqld) SetReplicationPosition(ctx context.Context, pos replication.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.Conn.SetReplicationPositionCommands(pos)
	log.Info(fmt.Sprintf("Executing commands to set replication position: %v", cmds))
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// SetReplicationSource makes the provided host / port the primary. It optionally
// stops replication before, and starts it after.
func (mysqld *Mysqld) SetReplicationSource(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	var cmds []string
	if stopReplicationBefore {
		cmds = append(cmds, conn.Conn.StopReplicationCommand())
	}
	smc := conn.Conn.SetReplicationSourceCommand(params, host, port, heartbeatInterval, int(replicationConnectRetry.Seconds()))
	cmds = append(cmds, smc)
	if startReplicationAfter {
		cmds = append(cmds, conn.Conn.StartReplicationCommand())
	}
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ResetReplication resets all replication for this host.
func (mysqld *Mysqld) ResetReplication(ctx context.Context) error {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()

	cmds := conn.Conn.ResetReplicationCommands()
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ResetReplicationParameters resets the replica replication parameters for this host.
func (mysqld *Mysqld) ResetReplicationParameters(ctx context.Context) error {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()

	cmds := conn.Conn.ResetReplicationParametersCommands()
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// +------+---------+---------------------+------+-------------+------+------------------------------------------------------------------+------------------+
// | Id   | User    | Host                | db   | Command     | Time | State                                                            | Info             |
// +------+---------+---------------------+------+-------------+------+------------------------------------------------------------------+------------------+
// | 9792 | vt_repl | host:port           | NULL | Binlog Dump |   54 | Has sent all binlog to replica; waiting for binlog to be updated | NULL             |
// | 9797 | vt_dba  | localhost           | NULL | Query       |    0 | NULL                                                             | show processlist |
// +------+---------+---------------------+------+-------------+------+------------------------------------------------------------------+------------------+
//
// Array indices for the results of SHOW PROCESSLIST.
const (
	colConnectionID = iota
	colUsername
	colClientAddr
	colDbName
	colCommand
)

const (
	// this is the command used by mysql replicas
	binlogDumpCommand = "Binlog Dump"
)

// FindReplicas gets IP addresses for all currently connected replicas.
func FindReplicas(ctx context.Context, mysqld MysqlDaemon) ([]string, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		// Check for prefix, since it could be "Binlog Dump GTID".
		if strings.HasPrefix(row[colCommand].ToString(), binlogDumpCommand) {
			host := row[colClientAddr].ToString()
			if host == "localhost" {
				// If we have a local binlog streamer, it will
				// show up as being connected
				// from 'localhost' through the local
				// socket. Ignore it.
				continue
			}
			host, _, err = netutil.SplitHostPort(host)
			if err != nil {
				return nil, fmt.Errorf("FindReplicas: malformed addr %v", err)
			}
			var ips []string
			ips, err = net.LookupHost(host)
			if err != nil {
				return nil, fmt.Errorf("FindReplicas: LookupHost failed %v", err)
			}
			addrs = append(addrs, ips...)
		}
	}

	return addrs, nil
}

// GetGTIDMode gets the GTID mode for the server
func (mysqld *Mysqld) GetGTIDMode(ctx context.Context) (string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return "", err
	}
	defer conn.Recycle()

	return conn.Conn.GetGTIDMode()
}

// FlushBinaryLogs is part of the MysqlDaemon interface.
func (mysqld *Mysqld) FlushBinaryLogs(ctx context.Context) (err error) {
	_, err = mysqld.FetchSuperQuery(ctx, "FLUSH BINARY LOGS")
	return err
}

// GetBinaryLogs is part of the MysqlDaemon interface.
func (mysqld *Mysqld) GetBinaryLogs(ctx context.Context) (binaryLogs []string, err error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return binaryLogs, err
	}
	for _, row := range qr.Rows {
		binaryLogs = append(binaryLogs, row[0].ToString())
	}
	return binaryLogs, err
}

// GetPreviousGTIDs is part of the MysqlDaemon interface.
func (mysqld *Mysqld) GetPreviousGTIDs(ctx context.Context, binlog string) (previousGtids string, err error) {
	query := fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' LIMIT 2", binlog)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return previousGtids, err
	}
	previousGtidsFound := false
	for _, row := range qr.Named().Rows {
		if row.AsString("Event_type", "") == "Previous_gtids" {
			previousGtids = row.AsString("Info", "")
			previousGtidsFound = true
		}
	}
	if !previousGtidsFound {
		return previousGtids, errors.New("GetPreviousGTIDs: previous GTIDs not found")
	}
	return previousGtids, nil
}

var ErrNoSemiSync = errors.New("semi-sync plugin not loaded")

func (mysqld *Mysqld) SemiSyncType(ctx context.Context) mysql.SemiSyncType {
	if mysqld.semiSyncType == mysql.SemiSyncTypeUnknown {
		mysqld.semiSyncType, _ = mysqld.SemiSyncExtensionLoaded(ctx)
	}
	return mysqld.semiSyncType
}

func (mysqld *Mysqld) enableSemiSyncQuery(ctx context.Context) (string, error) {
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		return "SET GLOBAL rpl_semi_sync_source_enabled = %v, GLOBAL rpl_semi_sync_replica_enabled = %v", nil
	case mysql.SemiSyncTypeMaster:
		return "SET GLOBAL rpl_semi_sync_master_enabled = %v, GLOBAL rpl_semi_sync_slave_enabled = %v", nil
	}
	return "", ErrNoSemiSync
}

func (mysqld *Mysqld) semiSyncReplicationStatusQuery(ctx context.Context) (string, error) {
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		return "SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", nil
	case mysql.SemiSyncTypeMaster:
		return "SHOW STATUS LIKE 'rpl_semi_sync_slave_status'", nil
	}
	return "", ErrNoSemiSync
}

// SetSemiSyncEnabled enables or disables semi-sync replication for
// primary and/or replica mode.
func (mysqld *Mysqld) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	log.Info(fmt.Sprintf("Setting semi-sync mode: primary=%v, replica=%v", primary, replica))

	// Convert bool to int.
	var p, s int
	if primary {
		p = 1
	}
	if replica {
		s = 1
	}

	query, err := mysqld.enableSemiSyncQuery(ctx)
	if err != nil {
		return err
	}
	err = mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf(query, p, s))
	if err != nil {
		return fmt.Errorf("can't set semi-sync mode: %v; make sure plugins are loaded in my.cnf", err)
	}
	return nil
}

// SemiSyncEnabled returns whether semi-sync is enabled for primary or replica.
// If the semi-sync plugin is not loaded, we assume semi-sync is disabled.
func (mysqld *Mysqld) SemiSyncEnabled(ctx context.Context) (primary, replica bool) {
	vars, err := mysqld.fetchVariables(ctx, "rpl_semi_sync_%_enabled")
	if err != nil {
		return false, false
	}
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		primary = vars["rpl_semi_sync_source_enabled"] == "ON"
		replica = vars["rpl_semi_sync_replica_enabled"] == "ON"
	case mysql.SemiSyncTypeMaster:
		primary = vars["rpl_semi_sync_master_enabled"] == "ON"
		replica = vars["rpl_semi_sync_slave_enabled"] == "ON"
	}
	return primary, replica
}

// SemiSyncStatus returns the current status of semi-sync for primary and replica.
func (mysqld *Mysqld) SemiSyncStatus(ctx context.Context) (primary, replica bool) {
	vars, err := mysqld.fetchStatuses(ctx, "Rpl_semi_sync_%_status")
	if err != nil {
		return false, false
	}
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		primary = vars["Rpl_semi_sync_source_status"] == "ON"
		replica = vars["Rpl_semi_sync_replica_status"] == "ON"
	case mysql.SemiSyncTypeMaster:
		primary = vars["Rpl_semi_sync_master_status"] == "ON"
		replica = vars["Rpl_semi_sync_slave_status"] == "ON"
	}
	return primary, replica
}

// SemiSyncReplicationStatus returns whether semi-sync is currently used by replication.
func (mysqld *Mysqld) SemiSyncReplicationStatus(ctx context.Context) (bool, error) {
	query, err := mysqld.semiSyncReplicationStatusQuery(ctx)
	if err != nil {
		return false, err
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 {
		return false, errors.New("no rpl_semi_sync_replica_status variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}

// SemiSyncExtensionLoaded returns whether semi-sync plugins are loaded.
func (mysqld *Mysqld) SemiSyncExtensionLoaded(ctx context.Context) (mysql.SemiSyncType, error) {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return mysql.SemiSyncTypeUnknown, connErr
	}
	defer conn.Recycle()

	return conn.Conn.SemiSyncExtensionLoaded()
}

func (mysqld *Mysqld) IsSemiSyncBlocked(ctx context.Context) (bool, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()

	// Execute the query to check if the primary is blocked on semi-sync.
	semiSyncWaitSessionsRead := "select variable_value from performance_schema.global_status where regexp_like(variable_name, 'Rpl_semi_sync_(source|master)_wait_sessions')"
	res, err := conn.Conn.ExecuteFetch(semiSyncWaitSessionsRead, 1, false)
	if err != nil {
		return false, err
	}
	// If we have no rows, then the primary doesn't have semi-sync enabled.
	// It then follows, that the primary isn't blocked :)
	if len(res.Rows) == 0 {
		return false, nil
	}

	// Read the status value and check if it is non-zero.
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return false, fmt.Errorf("unexpected number of rows received - %v", res.Rows)
	}
	value, err := res.Rows[0][0].ToCastInt64()
	return value != 0, err
}
