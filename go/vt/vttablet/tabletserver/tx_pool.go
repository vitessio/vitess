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

package tabletserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	txLogInterval = 1 * time.Minute
	beginWithCSRO = "start transaction with consistent snapshot, read only"
)

var txIsolations = map[querypb.ExecuteOptions_TransactionIsolation]string{
	querypb.ExecuteOptions_DEFAULT:                       "",
	querypb.ExecuteOptions_REPEATABLE_READ:               "repeatable read",
	querypb.ExecuteOptions_READ_COMMITTED:                "read committed",
	querypb.ExecuteOptions_READ_UNCOMMITTED:              "read uncommitted",
	querypb.ExecuteOptions_SERIALIZABLE:                  "serializable",
	querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY: "repeatable read",
}

var txAccessMode = map[querypb.ExecuteOptions_TransactionAccessMode]string{
	querypb.ExecuteOptions_CONSISTENT_SNAPSHOT: sqlparser.WithConsistentSnapshotStr,
	querypb.ExecuteOptions_READ_WRITE:          sqlparser.ReadWriteStr,
	querypb.ExecuteOptions_READ_ONLY:           sqlparser.ReadOnlyStr,
}

type (
	// TxPool does a lot of the transactional operations on StatefulConnections. It does not, with two exceptions,
	// concern itself with a connections life cycle. The two exceptions are Begin, which creates a new StatefulConnection,
	// and RollbackAndRelease, which does a Release after doing the rollback.
	TxPool struct {
		env     tabletenv.Env
		scp     *StatefulConnectionPool
		ticks   *timer.Timer
		limiter txlimiter.TxLimiter

		logMu   sync.Mutex
		lastLog time.Time
		txStats *servenv.TimingsWrapper
	}
)

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(env tabletenv.Env, limiter txlimiter.TxLimiter) *TxPool {
	config := env.Config()
	axp := &TxPool{
		env:     env,
		scp:     NewStatefulConnPool(env),
		ticks:   timer.NewTimer(txKillerTimeoutInterval(config)),
		limiter: limiter,
		txStats: env.Exporter().NewTimings("Transactions", "Transaction stats", "operation"),
	}
	// Careful: conns also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	env.Exporter().NewGaugeDurationFunc("OlapTransactionTimeout", "OLAP transaction timeout", func() time.Duration {
		return config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLAP)
	})
	env.Exporter().NewGaugeDurationFunc("TransactionTimeout", "Transaction timeout", func() time.Duration {
		return config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLTP)
	})
	return axp
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (tp *TxPool) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	tp.scp.Open(appParams, dbaParams, appDebugParams)
	if tp.ticks.Interval() > 0 {
		tp.ticks.Start(func() { tp.transactionKiller() })
	}
}

// Close closes the TxPool. A closed pool can be reopened.
func (tp *TxPool) Close() {
	tp.ticks.Stop()
	tp.scp.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (tp *TxPool) AdjustLastID(id int64) {
	tp.scp.AdjustLastID(id)
}

// Shutdown immediately rolls back all transactions that are not in use.
// In-use connections will be closed when they are unlocked (not in use).
func (tp *TxPool) Shutdown(ctx context.Context) {
	for _, v := range tp.scp.ShutdownAll() {
		tp.RollbackAndRelease(ctx, v)
	}
}

func (tp *TxPool) transactionKiller() {
	defer tp.env.LogError()
	for _, conn := range tp.scp.GetElapsedTimeout(vterrors.TxKillerRollback) {
		log.Warn(fmt.Sprintf("killing transaction (exceeded timeout: %v): %s", conn.timeout, conn.String(tp.env.Config().SanitizeLogMessages, tp.env.Environment().Parser())))
		switch {
		case conn.IsTainted():
			conn.Close()
			tp.env.Stats().KillCounters.Add("ReservedConnection", 1)
		case conn.IsInTransaction():
			_, err := conn.Exec(context.Background(), "rollback", 1, false, false /* keepConnOnTimeout */)
			if err != nil {
				conn.Close()
			}
			tp.env.Stats().KillCounters.Add("Transactions", 1)
		}
		// For logging, as transaction is killed as the connection is closed.
		if conn.IsTainted() && conn.IsInTransaction() {
			tp.env.Stats().KillCounters.Add("Transactions", 1)
		}
		if conn.IsInTransaction() {
			tp.txComplete(conn, tx.TxKill)
		}
		conn.Releasef("exceeded timeout: %v", conn.timeout)
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (tp *TxPool) WaitForEmpty() {
	tp.scp.WaitForEmpty()
}

// NewTxProps creates a new TxProperties struct
func (tp *TxPool) NewTxProps(immediateCaller *querypb.VTGateCallerID, effectiveCaller *vtrpcpb.CallerID, autocommit bool) *tx.Properties {
	return &tx.Properties{
		StartTime:       time.Now(),
		EffectiveCaller: effectiveCaller,
		ImmediateCaller: immediateCaller,
		Autocommit:      autocommit,
		Stats:           tp.txStats,
	}
}

// reservedKeepAlivePurpose is the purpose string KeepAliveReserved locks the
// connection with. GetAndLock recognizes it to briefly wait out a keepalive
// instead of failing the caller.
const reservedKeepAlivePurpose = "for reserved connection keepalive"

// GetAndLock fetches the connection associated to the connID and blocks it from concurrent use
// You must call Unlock on TxConnection once done.
func (tp *TxPool) GetAndLock(connID tx.ConnID, reason string) (*StatefulConnection, error) {
	conn, err := tp.scp.GetAndLock(connID, reason)
	// A keepalive holds the connection only for the microseconds it takes
	// to check and refresh it, and it never issues queries or waits on the
	// network — so a caller that collides with one briefly waits it out
	// rather than failing the client's query (or release) with an in-use
	// error. One retry is the norm; the deadline is deliberately generous
	// (the critical section is CPU-only, so it outlasts even a heavy
	// scheduler or GC stall) while still bounding the wait if something is
	// truly wedged.
	if err != nil && errors.Is(err, pools.ErrInUse) && strings.Contains(err.Error(), reservedKeepAlivePurpose) {
		deadline := time.Now().Add(1 * time.Second)
		for err != nil && time.Now().Before(deadline) &&
			errors.Is(err, pools.ErrInUse) && strings.Contains(err.Error(), reservedKeepAlivePurpose) {
			time.Sleep(100 * time.Microsecond)
			conn, err = tp.scp.GetAndLock(connID, reason)
		}
	}
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", connID, err)
	}
	return conn, nil
}

// KeepAliveReserved refreshes the idle timers of a reserved connection
// without executing anything on it — in particular, nothing is sent to the
// underlying MySQL connection, so mysqld's wait_timeout keeps counting only
// real user traffic. A connection that is currently in use counts as alive:
// its own activity refreshes the timers when it is unlocked. A connection
// that no longer exists returns the same "transaction %d: ..." error shape
// as GetAndLock so callers can recognize that it is gone.
func (tp *TxPool) KeepAliveReserved(reservedID tx.ConnID) error {
	conn, err := tp.scp.GetAndLock(reservedID, reservedKeepAlivePurpose)
	if err != nil {
		// The pool's in-use error means the connection is alive and busy —
		// exactly what a keepalive wants to hear.
		if errors.Is(err, pools.ErrInUse) {
			return nil
		}
		return vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", reservedID, err)
	}
	// On a platform without a real peer check (Windows), PeerCheck cannot tell a
	// live connection from one mysqld already closed. Refreshing anyway would pin
	// a dead connection in the pool forever, so instead return it without
	// extending its expiry: an idle connection is then reclaimed by the normal
	// tablet timeout, exactly as before this feature, rather than kept alive.
	// unlock(false) returns the connection to the pool without refreshing its
	// timers.
	if !conn.dbConn.Conn.PeerCheckSupported() {
		conn.unlock(false)
		return nil
	}
	// Detect a peer-closed socket (e.g. mysqld reclaimed the connection at
	// wait_timeout) with a non-blocking zero-byte read — nothing is sent,
	// so mysqld's idle timers are unaffected. A dead connection must be
	// released rather than refreshed: it occupies pool capacity, and
	// keeping its expiry fresh would pin the zombie until the session's
	// next command. The error carries the same "transaction %d: ended"
	// shape as other gone-connection errors so callers stop beating it.
	if err := conn.dbConn.Conn.PeerCheck(); err != nil {
		conn.ReleaseString("reserved connection keepalive found it dead")
		return vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: ended by the MySQL server (connection check: %v)", reservedID, err)
	}
	// Unlock refreshes the connection's timers for non-transactional
	// connections, which is the whole point of the keepalive.
	conn.Unlock()
	return nil
}

// Commit commits the transaction on the connection.
func (tp *TxPool) Commit(ctx context.Context, txConn *StatefulConnection) (string, error) {
	if !txConn.IsInTransaction() {
		return "", vterrors.New(vtrpcpb.Code_INTERNAL, "not in a transaction")
	}
	span, ctx := trace.NewSpan(ctx, "TxPool.Commit")
	defer span.Finish()
	defer tp.txComplete(txConn, tx.TxCommit)
	if txConn.TxProperties().Autocommit {
		return "", nil
	}

	if _, err := txConn.Exec(ctx, "commit", 1, false, false /* keepConnOnTimeout */); err != nil {
		txConn.Close()
		return "", err
	}
	return "commit", nil
}

// RollbackAndRelease rolls back the transaction on the specified connection, and releases the connection when done
func (tp *TxPool) RollbackAndRelease(ctx context.Context, txConn *StatefulConnection) {
	defer txConn.Release(tx.TxRollback)
	rollbackError := tp.Rollback(ctx, txConn)
	if rollbackError != nil {
		log.Error(fmt.Sprintf("tried to rollback, but failed with: %v", rollbackError.Error()))
	}
}

// Rollback rolls back the transaction on the specified connection.
func (tp *TxPool) Rollback(ctx context.Context, txConn *StatefulConnection) error {
	span, ctx := trace.NewSpan(ctx, "TxPool.Rollback")
	defer span.Finish()
	if txConn.IsClosed() || !txConn.IsInTransaction() {
		return nil
	}
	if txConn.TxProperties().Autocommit {
		tp.txComplete(txConn, tx.TxCommit)
		return nil
	}
	defer tp.txComplete(txConn, tx.TxRollback)
	if _, err := txConn.Exec(ctx, "rollback", 1, false, false /* keepConnOnTimeout */); err != nil {
		txConn.Close()
		return err
	}
	return nil
}

// Begin begins a transaction, and returns the associated connection and
// the statements (if any) executed to initiate the transaction. In autocommit
// mode the statement will be "".
// The connection returned is locked for the callee and its responsibility is to unlock the connection.
func (tp *TxPool) Begin(ctx context.Context, options *querypb.ExecuteOptions, readOnly bool, reservedID int64, setting *smartconnpool.Setting) (*StatefulConnection, string, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Begin")
	defer span.Finish()

	var conn *StatefulConnection
	var err error
	if reservedID != 0 {
		// Use the TxPool wrapper (not tp.scp directly) so a BEGIN on a
		// reserved connection that a keepalive touch is momentarily holding
		// waits the touch out instead of failing with an in-use error.
		conn, err = tp.GetAndLock(reservedID, "start transaction on reserve conn")
		if err != nil {
			return nil, "", "", err
		}
		// Update conn timeout.
		timeout := getTransactionTimeout(options, tp.env.Config(), options.GetWorkload())
		conn.SetTimeout(timeout)
	} else {
		immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
		effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)
		if !tp.limiter.Get(immediateCaller, effectiveCaller) {
			return nil, "", "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "per-user transaction pool connection limit exceeded")
		}
		conn, err = tp.createConn(ctx, options, setting)
		defer func() {
			if err != nil {
				// The transaction limiter frees transactions on rollback or commit. If we fail to create the transaction,
				// release immediately since there will be no rollback or commit.
				tp.limiter.Release(immediateCaller, effectiveCaller)
			}
		}()
	}
	if err != nil {
		return nil, "", "", err
	}
	sql, sessionStateChanges, err := tp.begin(ctx, options, readOnly, conn)
	if err != nil {
		conn.Close()
		conn.Release(tx.ConnInitFail)
		return nil, "", "", err
	}
	// If we have applied any settings on the connection, then we need to record the query
	// in case we need to redo the transaction because of a failure.
	if setting != nil {
		conn.TxProperties().RecordQueryDetail(setting.ApplyQuery(), nil)
	}
	return conn, sql, sessionStateChanges, nil
}

// getTransactionTimeout gets the smaller transaction timeout of either the timeout set in the options
// or the one configured for the current workload.
func getTransactionTimeout(options *querypb.ExecuteOptions, config *tabletenv.TabletConfig, workload querypb.ExecuteOptions_Workload) time.Duration {
	workloadTimeout := config.TxTimeoutForWorkload(workload)

	if options != nil && options.TransactionTimeout != nil {
		sessionTimeout := time.Duration(options.GetTransactionTimeout()) * time.Millisecond
		return smallerTimeout(sessionTimeout, workloadTimeout)
	}

	return workloadTimeout
}

func (tp *TxPool) begin(ctx context.Context, options *querypb.ExecuteOptions, readOnly bool, conn *StatefulConnection) (string, string, error) {
	immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
	effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)
	beginQueries, autocommit, sessionStateChanges, err := createTransaction(ctx, options, conn, readOnly)
	if err != nil {
		return "", "", err
	}
	conn.txProps = tp.NewTxProps(immediateCaller, effectiveCaller, autocommit)
	return beginQueries, sessionStateChanges, nil
}

func (tp *TxPool) createConn(ctx context.Context, options *querypb.ExecuteOptions, setting *smartconnpool.Setting) (*StatefulConnection, error) {
	conn, err := tp.scp.NewConn(ctx, options, setting)
	if err != nil {
		errCode := vterrors.Code(err)
		switch err {
		case smartconnpool.ErrCtxTimeout:
			tp.LogActive()
			err = vterrors.Errorf(errCode, "transaction pool aborting request due to already expired context")
		case smartconnpool.ErrTimeout:
			tp.LogActive()
			err = vterrors.Errorf(errCode, "transaction pool connection limit exceeded")
		case smartconnpool.ErrPoolWaiterCapReached:
			tp.LogActive()
			err = vterrors.Errorf(errCode, "transaction pool connection waiter cap exceeded")
		}
		return nil, err
	}
	return conn, nil
}

func createTransaction(
	ctx context.Context,
	options *querypb.ExecuteOptions,
	conn *StatefulConnection,
	readOnly bool,
) (beginQueries string, autocommitTransaction bool, sessionStateChanges string, err error) {
	switch options.GetTransactionIsolation() {
	case querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY:
		beginQueries, sessionStateChanges, err = handleConsistentSnapshotCase(ctx, conn)
		if err != nil {
			return "", false, "", err
		}
	case querypb.ExecuteOptions_AUTOCOMMIT:
		autocommitTransaction = true
	case querypb.ExecuteOptions_REPEATABLE_READ, querypb.ExecuteOptions_READ_COMMITTED, querypb.ExecuteOptions_READ_UNCOMMITTED,
		querypb.ExecuteOptions_SERIALIZABLE, querypb.ExecuteOptions_DEFAULT:
		isolationLevel := txIsolations[options.GetTransactionIsolation()]
		var execSQL string
		if isolationLevel != "" {
			execSQL, err = setIsolationLevel(ctx, conn, isolationLevel)
			if err != nil {
				return
			}
			beginQueries += execSQL
		}

		var beginSQL string
		beginSQL, err = createStartTxStmt(options, readOnly)
		if err != nil {
			return "", false, "", err
		}

		execSQL, sessionStateChanges, err = startTransaction(ctx, conn, beginSQL)
		if err != nil {
			return "", false, "", err
		}

		// Add the begin statement to the list of queries.
		beginQueries += execSQL
	default:
		return "", false, "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] don't know how to open a transaction of this type: %v", options.GetTransactionIsolation())
	}
	return
}

// createStartTxStmt - this method return the start transaction statement based on the TransactionAccessMode in options
// and the readOnly flag passed in.
// When readOnly is true, ReadWrite option should not have been passed, that will result in an error.
// If no option is passed, the default on the connection will be used by just execution "begin" statement.
func createStartTxStmt(options *querypb.ExecuteOptions, readOnly bool) (string, error) {
	// default statement.
	beginSQL := "begin"

	// generate the access mode string
	var modesStr strings.Builder
	// to know if read only is already added to modeStr
	// so that explicit addition of read only is not required in case of readOnly parameter is true.
	var readOnlyAdded bool
	for idx, accessMode := range options.GetTransactionAccessMode() {
		txMode, ok := txAccessMode[accessMode]
		if !ok {
			return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] transaction access mode not known of this type: %v", accessMode)
		}
		if readOnly && accessMode == querypb.ExecuteOptions_READ_WRITE {
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot start read write transaction on a read only tablet")
		}
		if accessMode == querypb.ExecuteOptions_READ_ONLY {
			readOnlyAdded = true
		}
		if idx == 0 {
			modesStr.WriteString(txMode)
			continue
		}
		modesStr.WriteString(", " + txMode)
	}

	if readOnly && !readOnlyAdded {
		if modesStr.Len() != 0 {
			modesStr.WriteString(", read only")
		} else {
			modesStr.WriteString("read only")
		}
	}
	if modesStr.Len() != 0 {
		beginSQL = "start transaction " + modesStr.String()
	}
	return beginSQL, nil
}

func handleConsistentSnapshotCase(ctx context.Context, conn *StatefulConnection) (beginSQL string, sessionStateChanges string, err error) {
	isolationLevel := txIsolations[querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY]

	execSQL, err := setIsolationLevel(ctx, conn, isolationLevel)
	if err != nil {
		return
	}
	beginSQL += execSQL

	execSQL, sessionStateChanges, err = startTransaction(ctx, conn, beginWithCSRO)
	if err != nil {
		return
	}
	beginSQL += execSQL
	return
}

func startTransaction(ctx context.Context, conn *StatefulConnection, transaction string) (string, string, error) {
	sessionStateChanges, err := conn.execWithRetry(ctx, transaction, 1, false)
	if err != nil {
		return "", "", err
	}
	return transaction, sessionStateChanges, nil
}

func setIsolationLevel(ctx context.Context, conn *StatefulConnection, level string) (string, error) {
	txQuery := "set transaction isolation level " + level
	if _, err := conn.execWithRetry(ctx, txQuery, 1, false); err != nil {
		return "", err
	}
	return txQuery + "; ", nil
}

// LogActive causes all existing transactions to be logged when they complete.
// The logging is throttled to no more than once every txLogInterval.
func (tp *TxPool) LogActive() {
	tp.logMu.Lock()
	defer tp.logMu.Unlock()
	if time.Since(tp.lastLog) < txLogInterval {
		return
	}
	tp.lastLog = time.Now()
	tp.scp.ForAllTxProperties(func(props *tx.Properties) {
		props.LogToFile = true
	})
}

func (tp *TxPool) txComplete(conn *StatefulConnection, reason tx.ReleaseReason) {
	conn.LogTransaction(reason)
	tp.limiter.Release(conn.TxProperties().ImmediateCaller, conn.TxProperties().EffectiveCaller)
	conn.CleanTxState()
}

func txKillerTimeoutInterval(config *tabletenv.TabletConfig) time.Duration {
	return smallerTimeout(
		config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLAP),
		config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLTP),
	) / 10
}
