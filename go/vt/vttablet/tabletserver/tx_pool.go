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
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/pools"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"golang.org/x/net/context"

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

// These consts identify how a transaction was resolved.
const (
	TxClose      = "close"
	TxCommit     = "commit"
	TxRollback   = "rollback"
	TxKill       = "kill"
	ConnInitFail = "initFail"
)

var txResolution = map[string]string{
	TxClose:      "closed",
	TxCommit:     "transaction committed",
	TxRollback:   "transaction rolled back",
	TxKill:       "kill",
	ConnInitFail: "initFail",
}

const txLogInterval = 1 * time.Minute

type queries struct {
	setIsolationLevel string
	openTransaction   string
}

var (
	txIsolations = map[querypb.ExecuteOptions_TransactionIsolation]queries{
		querypb.ExecuteOptions_DEFAULT:                       {setIsolationLevel: "", openTransaction: "begin"},
		querypb.ExecuteOptions_REPEATABLE_READ:               {setIsolationLevel: "REPEATABLE READ", openTransaction: "begin"},
		querypb.ExecuteOptions_READ_COMMITTED:                {setIsolationLevel: "READ COMMITTED", openTransaction: "begin"},
		querypb.ExecuteOptions_READ_UNCOMMITTED:              {setIsolationLevel: "READ UNCOMMITTED", openTransaction: "begin"},
		querypb.ExecuteOptions_SERIALIZABLE:                  {setIsolationLevel: "SERIALIZABLE", openTransaction: "begin"},
		querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY: {setIsolationLevel: "REPEATABLE READ", openTransaction: "start transaction with consistent snapshot, read only"},
	}
)

// TxPool is the transaction pool for the query service.
type TxPool struct {
	env tabletenv.Env

	activePool         *ActivePool
	transactionTimeout sync2.AtomicDuration
	ticks              *timer.Timer
	limiter            txlimiter.TxLimiter

	logMu   sync.Mutex
	lastLog time.Time
	txStats *servenv.TimingsWrapper
}

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(env tabletenv.Env, limiter txlimiter.TxLimiter) *TxPool {
	config := env.Config()
	transactionTimeout := time.Duration(config.Oltp.TxTimeoutSeconds * 1e9)
	axp := &TxPool{
		env:                env,
		activePool:         NewActivePool(env),
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
	tp.activePool.Open(appParams, dbaParams, appDebugParams)
	tp.ticks.Start(func() { tp.transactionKiller() })
}

// Close closes the TxPool. A closed pool can be reopened.
func (tp *TxPool) Close() {
	tp.ticks.Stop()
	tp.activePool.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (tp *TxPool) AdjustLastID(id int64) {
	tp.activePool.AdjustLastID(id)
}

// RollbackNonBusy rolls back all transactions that are not in use.
// Transactions can be in use for situations like executing statements
// or in prepared state.
func (tp *TxPool) RollbackNonBusy(ctx context.Context) {
	for _, v := range tp.activePool.GetOutdated(time.Duration(0), "for transition") {
		tp.LocalConclude(ctx, v)
	}
}

func (tp *TxPool) transactionKiller() {
	defer tp.env.LogError()
	for _, conn := range tp.activePool.GetOutdated(tp.Timeout(), "for tx killer rollback") {
		log.Warningf("killing transaction (exceeded timeout: %v): %s", tp.Timeout(), conn.String())
		tp.env.Stats().KillCounters.Add("Transactions", 1)
		conn.Close()
		conn.conclude(fmt.Sprintf("exceeded timeout: %v", tp.Timeout()))
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (tp *TxPool) WaitForEmpty() {
	tp.activePool.WaitForEmpty()
}

// Begin begins a transaction, and returns the associated transaction id and
// the statements (if any) executed to initiate the transaction. In autocommit
// mode the statement will be "".
//
// Subsequent statements can access the connection through the transaction id.
func (tp *TxPool) Begin(ctx context.Context, options *querypb.ExecuteOptions) (tx.ConnID, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Begin")
	defer span.Finish()
	beginQueries := ""

	immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
	effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)

	if !tp.limiter.Get(immediateCaller, effectiveCaller) {
		return 0, "", vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "per-user transaction pool connection limit exceeded")
	}

	txConn, err := tp.activePool.NewConn(ctx, options, callerid.GetPrincipal(effectiveCaller), callerid.GetUsername(immediateCaller), func(txConn *TxConnection) error {
		autocommitTransaction := false
		if queries, ok := txIsolations[options.GetTransactionIsolation()]; ok {
			if queries.setIsolationLevel != "" {
				if err := txConn.execWithRetry(ctx, "set transaction isolation level "+queries.setIsolationLevel, 1, false); err != nil {
					return err
				}
				beginQueries = queries.setIsolationLevel + "; "
			}
			if err := txConn.execWithRetry(ctx, queries.openTransaction, 1, false); err != nil {
				return err
			}
			beginQueries = beginQueries + queries.openTransaction
		} else if options.GetTransactionIsolation() == querypb.ExecuteOptions_AUTOCOMMIT {
			autocommitTransaction = true
		} else {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to open a transaction of this type: %v", options.GetTransactionIsolation())
		}
		txConn.TxProps = tp.NewTxProps(immediateCaller, effectiveCaller, autocommitTransaction)
		return nil
	})
	if err != nil {
		switch err {
		case pools.ErrCtxTimeout:
			tp.LogActive()
			err = vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool aborting request due to already expired context")
		case pools.ErrTimeout:
			tp.LogActive()
			err = vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool connection limit exceeded")
		}
		return 0, "", err
	}

	return txConn.TransactionID, beginQueries, nil
}

//NewTxProps creates a new TxProperties struct
func (tp *TxPool) NewTxProps(immediateCaller *querypb.VTGateCallerID, effectiveCaller *vtrpcpb.CallerID, autocommit bool) *TxProperties {
	return &TxProperties{
		StartTime:       time.Now(),
		EffectiveCaller: effectiveCaller,
		ImmediateCaller: immediateCaller,
		Autocommit:      autocommit,
		txStats:         tp.txStats,
	}
}

//Reserve will create a new reserved connection
func (tp *TxPool) Reserve(ctx context.Context, options *querypb.ExecuteOptions, setStatements []string) (int64, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Reserve")
	defer span.Finish()
	txConn, err := tp.activePool.NewConn(ctx, options, "", "", func(txConn *TxConnection) error {
		txConn.dbConn.Taint()
		for _, statement := range setStatements {
			_, err := txConn.Exec(ctx, statement, 10, false)
			if err != nil {
				return err
			}
		}
		txConn.reserved = true
		return nil
	})
	if err != nil {
		return 0, err
	}

	return txConn.TransactionID, nil
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on TxConnection once done.
func (tp *TxPool) Get(transactionID int64, reason string) (*TxConnection, error) {
	conn, err := tp.activePool.Get(transactionID, reason)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", transactionID, err)
	}
	return conn, nil
}

// Commit commits the specified transaction.
func (tp *TxPool) Commit(ctx context.Context, transactionID int64) (string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.Commit")
	defer span.Finish()
	conn, err := tp.Get(transactionID, "for commit")
	if err != nil {
		return "", err
	}
	return tp.LocalCommit(ctx, conn)
}

// Rollback rolls back the specified transaction.
func (tp *TxPool) Rollback(ctx context.Context, transactionID int64) error {
	span, ctx := trace.NewSpan(ctx, "TxPool.Rollback")
	defer span.Finish()

	conn, err := tp.Get(transactionID, "for rollback")
	if err != nil {
		return err
	}
	return tp.localRollback(ctx, conn)
}

// LocalBegin is equivalent to Begin->Get.
// It's used for executing transactions within a request. It's safe
// to always call LocalConclude at the end.
func (tp *TxPool) LocalBegin(ctx context.Context, options *querypb.ExecuteOptions) (*TxConnection, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.LocalBegin")
	defer span.Finish()

	transactionID, beginSQL, err := tp.Begin(ctx, options)
	if err != nil {
		return nil, "", err
	}
	conn, err := tp.Get(transactionID, "for local query")
	return conn, beginSQL, err
}

// LocalCommit is the commit function for LocalBegin.
func (tp *TxPool) LocalCommit(ctx context.Context, txConn *TxConnection) (string, error) {
	span, ctx := trace.NewSpan(ctx, "TxPool.LocalCommit")
	defer span.Finish()
	defer tp.txComplete(txConn, TxCommit)
	if txConn.TxProps.Autocommit {
		return "", nil
	}

	if _, err := txConn.Exec(ctx, "commit", 1, false); err != nil {
		txConn.Close()
		return "", err
	}
	return "commit", nil
}

// LocalConclude concludes a transaction started by LocalBegin.
// If the transaction was not previously concluded, it's rolled back.
func (tp *TxPool) LocalConclude(ctx context.Context, conn *TxConnection) {
	if conn.dbConn == nil {
		return
	}
	span, ctx := trace.NewSpan(ctx, "TxPool.LocalConclude")
	defer span.Finish()
	_ = tp.localRollback(ctx, conn)
}

func (tp *TxPool) localRollback(ctx context.Context, txConn *TxConnection) error {
	if txConn.TxProps.Autocommit {
		tp.txComplete(txConn, TxCommit)
		return nil
	}
	defer tp.txComplete(txConn, TxRollback)
	if _, err := txConn.Exec(ctx, "rollback", 1, false); err != nil {
		txConn.Close()
		return err
	}
	return nil
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
	tp.activePool.ForAllTxProperties(func(props *TxProperties) {
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

func (tp *TxPool) txComplete(conn *TxConnection, reason string) {
	tp.log(conn, txResolution[reason])
	if conn.reserved {
		conn.renewTxConnection()
	} else {
		conn.Release(reason)
	}
	tp.limiter.Release(conn.TxProps.ImmediateCaller, conn.TxProps.EffectiveCaller)
	conn.txClean()
}

func (tp *TxPool) log(txc *TxConnection, conclusion string) {
	if txc.TxProps == nil {
		return //Nothing to log as no transaction exists on this connection.
	}
	txc.TxProps.Conclusion = conclusion
	txc.TxProps.EndTime = time.Now()

	username := callerid.GetPrincipal(txc.TxProps.EffectiveCaller)
	if username == "" {
		username = callerid.GetUsername(txc.TxProps.ImmediateCaller)
	}
	duration := txc.TxProps.EndTime.Sub(txc.TxProps.StartTime)
	txc.env.Stats().UserTransactionCount.Add([]string{username, conclusion}, 1)
	txc.env.Stats().UserTransactionTimesNs.Add([]string{username, conclusion}, int64(duration))
	txc.TxProps.txStats.Add(conclusion, duration)
	if txc.TxProps.LogToFile {
		log.Infof("Logged transaction: %s", txc.String())
	}
	tabletenv.TxLogger.Send(txc)
}

//TxProperties contains all information that is related to the currently running
//transaction on the connection
type TxProperties struct {
	EffectiveCaller *vtrpcpb.CallerID
	ImmediateCaller *querypb.VTGateCallerID

	StartTime time.Time
	EndTime   time.Time

	Queries []string

	Autocommit bool
	Conclusion string

	LogToFile bool

	txStats *servenv.TimingsWrapper
}
