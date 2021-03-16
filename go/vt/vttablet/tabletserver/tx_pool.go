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
	"sync"
	"time"

	"vitess.io/vitess/go/pools"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const txLogInterval = 1 * time.Minute

var txIsolations = map[querypb.ExecuteOptions_TransactionIsolation]queries{
	querypb.ExecuteOptions_DEFAULT:                       {setIsolationLevel: "", openTransaction: "begin"},
	querypb.ExecuteOptions_REPEATABLE_READ:               {setIsolationLevel: "REPEATABLE READ", openTransaction: "begin"},
	querypb.ExecuteOptions_READ_COMMITTED:                {setIsolationLevel: "READ COMMITTED", openTransaction: "begin"},
	querypb.ExecuteOptions_READ_UNCOMMITTED:              {setIsolationLevel: "READ UNCOMMITTED", openTransaction: "begin"},
	querypb.ExecuteOptions_SERIALIZABLE:                  {setIsolationLevel: "SERIALIZABLE", openTransaction: "begin"},
	querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY: {setIsolationLevel: "REPEATABLE READ", openTransaction: "start transaction with consistent snapshot, read only"},
}

type (
	// TxPool does a lot of the transactional operations on StatefulConnections. It does not, with two exceptions,
	// concern itself with a connections life cycle. The two exceptions are Begin, which creates a new StatefulConnection,
	// and RollbackAndRelease, which does a Release after doing the rollback.
	TxPool struct {
		env                tabletenv.Env
		scp                *StatefulConnectionPool
		transactionTimeout sync2.AtomicDuration
		ticks              *timer.Timer
		limiter            txlimiter.TxLimiter

		logMu   sync.Mutex
		lastLog time.Time
		txStats *servenv.TimingsWrapper
	}
	queries struct {
		setIsolationLevel string
		openTransaction   string
	}
)

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(env tabletenv.Env, limiter txlimiter.TxLimiter) *TxPool {
	config := env.Config()
	transactionTimeout := config.Oltp.TxTimeoutSeconds.Get()
	axp := &TxPool{
		env:                env,
		scp:                NewStatefulConnPool(env),
		transactionTimeout: sync2.NewAtomicDuration(transactionTimeout),
		ticks:              timer.NewTimer(transactionTimeout / 10),
		limiter:            limiter,
		txStats:            env.Exporter().NewTimings("Transactions", "Transaction stats", "operation"),
	}
	// Careful: conns also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	env.Exporter().NewGaugeDurationFunc("TransactionTimeout", "Transaction timeout", axp.transactionTimeout.Get)
	return axp
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (tp *TxPool) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	tp.scp.Open(appParams, dbaParams, appDebugParams)
	tp.ticks.Start(func() { tp.transactionKiller() })
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
	for _, conn := range tp.scp.GetOutdated(tp.Timeout(), "for tx killer rollback") {
		log.Warningf("killing transaction (exceeded timeout: %v): %s", tp.Timeout(), conn.String())
		switch {
		case conn.IsTainted():
			conn.Close()
			tp.env.Stats().KillCounters.Add("ReservedConnection", 1)
		case conn.IsInTransaction():
			_, err := conn.Exec(context.Background(), "rollback", 1, false)
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
		conn.Releasef("exceeded timeout: %v", tp.Timeout())
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (tp *TxPool) WaitForEmpty() {
	tp.scp.WaitForEmpty()
}

//NewTxProps creates a new TxProperties struct
func (tp *TxPool) NewTxProps(immediateCaller *querypb.VTGateCallerID, effectiveCaller *vtrpcpb.CallerID, autocommit bool) *tx.Properties {
	return &tx.Properties{
		StartTime:       time.Now(),
		EffectiveCaller: effectiveCaller,
		ImmediateCaller: immediateCaller,
		Autocommit:      autocommit,
		Stats:           tp.txStats,
	}
}

// GetAndLock fetches the connection associated to the connID and blocks it from concurrent use
// You must call Unlock on TxConnection once done.
func (tp *TxPool) GetAndLock(connID tx.ConnID, reason string) (*StatefulConnection, error) {
	conn, err := tp.scp.GetAndLock(connID, reason)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", connID, err)
	}
	return conn, nil
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

	if _, err := txConn.Exec(ctx, "commit", 1, false); err != nil {
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
		log.Errorf("tried to rollback, but failed with: %v", rollbackError.Error())
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
	if _, err := txConn.Exec(ctx, "rollback", 1, false); err != nil {
		txConn.Close()
		return err
	}
	return nil
}

// Begin begins a transaction, and returns the associated connection and
// the statements (if any) executed to initiate the transaction. In autocommit
// mode the statement will be "".
// The connection returned is locked for the callee and its responsibility is to unlock the connection.
func (tp *TxPool) Begin(ctx context.Context, options *querypb.ExecuteOptions, readOnly bool, reservedID int64, preQueries []string) (*StatefulConnection, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Begin")
	defer span.Finish()

	var conn *StatefulConnection
	var err error
	if reservedID != 0 {
		conn, err = tp.scp.GetAndLock(reservedID, "start transaction on reserve conn")
		if err != nil {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", reservedID, err)
		}
	} else {
		immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
		effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)
		if !tp.limiter.Get(immediateCaller, effectiveCaller) {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "per-user transaction pool connection limit exceeded")
		}
		conn, err = tp.createConn(ctx, options)
		defer func() {
			if err != nil {
				// The transaction limiter frees transactions on rollback or commit. If we fail to create the transaction,
				// release immediately since there will be no rollback or commit.
				tp.limiter.Release(immediateCaller, effectiveCaller)
			}
		}()
	}
	if err != nil {
		return nil, "", err
	}
	sql, err := tp.begin(ctx, options, readOnly, conn, preQueries)
	if err != nil {
		conn.Close()
		conn.Release(tx.ConnInitFail)
		return nil, "", err
	}
	return conn, sql, nil
}

func (tp *TxPool) begin(ctx context.Context, options *querypb.ExecuteOptions, readOnly bool, conn *StatefulConnection, preQueries []string) (string, error) {
	immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
	effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)
	beginQueries, autocommit, err := createTransaction(ctx, options, conn, readOnly, preQueries)
	if err != nil {
		return "", err
	}

	conn.txProps = tp.NewTxProps(immediateCaller, effectiveCaller, autocommit)

	return beginQueries, nil
}

func (tp *TxPool) createConn(ctx context.Context, options *querypb.ExecuteOptions) (*StatefulConnection, error) {
	conn, err := tp.scp.NewConn(ctx, options)
	if err != nil {
		switch err {
		case pools.ErrCtxTimeout:
			tp.LogActive()
			err = vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool aborting request due to already expired context")
		case pools.ErrTimeout:
			tp.LogActive()
			err = vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool connection limit exceeded")
		}
		return nil, err
	}
	return conn, nil
}

func createTransaction(ctx context.Context, options *querypb.ExecuteOptions, conn *StatefulConnection, readOnly bool, preQueries []string) (string, bool, error) {
	beginQueries := ""

	autocommitTransaction := false
	if queries, ok := txIsolations[options.GetTransactionIsolation()]; ok {
		if queries.setIsolationLevel != "" {
			txQuery := "set transaction isolation level " + queries.setIsolationLevel
			if err := conn.execWithRetry(ctx, txQuery, 1, false); err != nil {
				return "", false, vterrors.Wrap(err, txQuery)
			}
			beginQueries = queries.setIsolationLevel + "; "
		}
		beginSQL := queries.openTransaction
		if readOnly &&
			options.GetTransactionIsolation() != querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY {
			beginSQL = "start transaction read only"
		}
		if err := conn.execWithRetry(ctx, beginSQL, 1, false); err != nil {
			return "", false, vterrors.Wrap(err, beginSQL)
		}
		beginQueries = beginQueries + beginSQL
	} else if options.GetTransactionIsolation() == querypb.ExecuteOptions_AUTOCOMMIT {
		autocommitTransaction = true
	} else {
		return "", false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to open a transaction of this type: %v", options.GetTransactionIsolation())
	}

	for _, preQuery := range preQueries {
		if _, err := conn.Exec(ctx, preQuery, 1, false); err != nil {
			return "", false, vterrors.Wrap(err, preQuery)
		}
	}
	return beginQueries, autocommitTransaction, nil
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

// Timeout returns the transaction timeout.
func (tp *TxPool) Timeout() time.Duration {
	return tp.transactionTimeout.Get()
}

// SetTimeout sets the transaction timeout.
func (tp *TxPool) SetTimeout(timeout time.Duration) {
	tp.transactionTimeout.Set(timeout)
	tp.ticks.SetInterval(timeout / 10)
}

func (tp *TxPool) txComplete(conn *StatefulConnection, reason tx.ReleaseReason) {
	conn.LogTransaction(reason)
	tp.limiter.Release(conn.TxProperties().ImmediateCaller, conn.TxProperties().EffectiveCaller)
	conn.CleanTxState()
}
