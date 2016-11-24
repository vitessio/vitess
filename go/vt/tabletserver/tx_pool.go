// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/callerid"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"golang.org/x/net/context"
)

// TxLogger can be used to enable logging of transactions.
// Call TxLogger.ServeLogs in your main program to enable logging.
// The log format can be inferred by looking at TxConnection.Format.
var TxLogger = streamlog.New("TxLog", 10)

// These consts identify how a transaction was resolved.
const (
	TxClose    = "close"
	TxCommit   = "commit"
	TxRollback = "rollback"
	TxPrepare  = "prepare"
	TxKill     = "kill"
)

const txLogInterval = time.Duration(1 * time.Minute)

// TxPool is the transaction pool for the query service.
type TxPool struct {
	pool              *ConnPool
	activePool        *pools.Numbered
	lastID            sync2.AtomicInt64
	timeout           sync2.AtomicDuration
	ticks             *timer.Timer
	txStats           *stats.Timings
	queryServiceStats *QueryServiceStats
	checker           MySQLChecker
	// Tracking culprits that cause tx pool full errors.
	logMu   sync.Mutex
	lastLog time.Time
}

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(
	name string,
	txStatsPrefix string,
	capacity int,
	timeout time.Duration,
	idleTimeout time.Duration,
	enablePublishStats bool,
	qStats *QueryServiceStats,
	checker MySQLChecker) *TxPool {

	txStatsName := ""
	if enablePublishStats {
		txStatsName = txStatsPrefix + "Transactions"
	}

	axp := &TxPool{
		pool:              NewConnPool(name, capacity, idleTimeout, enablePublishStats, qStats, checker),
		activePool:        pools.NewNumbered(),
		lastID:            sync2.NewAtomicInt64(time.Now().UnixNano()),
		timeout:           sync2.NewAtomicDuration(timeout),
		ticks:             timer.NewTimer(timeout / 10),
		txStats:           stats.NewTimings(txStatsName),
		checker:           checker,
		queryServiceStats: qStats,
	}
	// Careful: pool also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	if enablePublishStats {
		stats.Publish(name+"Timeout", stats.DurationFunc(axp.timeout.Get))
	}
	return axp
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (axp *TxPool) Open(appParams, dbaParams *sqldb.ConnParams) {
	log.Infof("Starting transaction id: %d", axp.lastID)
	axp.pool.Open(appParams, dbaParams)
	axp.ticks.Start(func() { axp.transactionKiller() })
}

// Close closes the TxPool. A closed pool can be reopened.
func (axp *TxPool) Close() {
	axp.ticks.Stop()
	for _, v := range axp.activePool.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction for shutdown: %s", conn.Format(nil))
		axp.queryServiceStats.InternalErrors.Add("StrayTransactions", 1)
		conn.Close()
		conn.conclude(TxClose)
	}
	axp.pool.Close()
}

// RollbackNonBusy rolls back all transactions that are not in use.
// Transactions can be in use for situations like executing statements
// or in prepared state.
func (axp *TxPool) RollbackNonBusy(ctx context.Context) {
	for _, v := range axp.activePool.GetOutdated(time.Duration(0), "for transition") {
		axp.LocalConclude(ctx, v.(*TxConnection))
	}
}

func (axp *TxPool) transactionKiller() {
	defer logError(axp.queryServiceStats)
	for _, v := range axp.activePool.GetOutdated(time.Duration(axp.Timeout()), "for rollback") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction (exceeded timeout: %v): %s", axp.Timeout(), conn.Format(nil))
		axp.queryServiceStats.KillStats.Add("Transactions", 1)
		conn.Close()
		conn.conclude(TxKill)
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (axp *TxPool) WaitForEmpty() {
	axp.activePool.WaitForEmpty()
}

// Begin begins a transaction, and returns the associated transaction id.
// Subsequent statements can access the connection through the transaction id.
func (axp *TxPool) Begin(ctx context.Context) (int64, error) {
	conn, err := axp.pool.Get(ctx)
	if err != nil {
		switch err {
		case ErrConnPoolClosed:
			return 0, err
		case pools.ErrTimeout:
			axp.LogActive()
			return 0, NewTabletError(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, "Transaction pool connection limit exceeded")
		}
		return 0, NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
	}
	if _, err := conn.Exec(ctx, "begin", 1, false); err != nil {
		conn.Recycle()
		return 0, NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	transactionID := axp.lastID.Add(1)
	axp.activePool.Register(
		transactionID,
		newTxConnection(
			conn,
			transactionID,
			axp,
			callerid.ImmediateCallerIDFromContext(ctx),
			callerid.EffectiveCallerIDFromContext(ctx),
		),
	)
	return transactionID, nil
}

// Commit commits the specified transaction.
func (axp *TxPool) Commit(ctx context.Context, transactionID int64) error {
	conn, err := axp.Get(transactionID, "for commit")
	if err != nil {
		return err
	}
	return axp.LocalCommit(ctx, conn)
}

// Rollback rolls back the specified transaction.
func (axp *TxPool) Rollback(ctx context.Context, transactionID int64) error {
	conn, err := axp.Get(transactionID, "for rollback")
	if err != nil {
		return err
	}
	return axp.localRollback(ctx, conn)
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on TxConnection once done.
func (axp *TxPool) Get(transactionID int64, reason string) (*TxConnection, error) {
	v, err := axp.activePool.Get(transactionID, reason)
	if err != nil {
		return nil, NewTabletError(vtrpcpb.ErrorCode_NOT_IN_TX, "Transaction %d: %v", transactionID, err)
	}
	return v.(*TxConnection), nil
}

// LocalBegin is equivalent to Begin->Get.
// It's used for executing transactions within a request. It's safe
// to always call LocalConclude at the end.
func (axp *TxPool) LocalBegin(ctx context.Context) (*TxConnection, error) {
	transactionID, err := axp.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return axp.Get(transactionID, "for local query")
}

// LocalCommit is the commit function for LocalBegin.
func (axp *TxPool) LocalCommit(ctx context.Context, conn *TxConnection) error {
	defer conn.conclude(TxCommit)
	axp.txStats.Add("Completed", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "commit", 1, false); err != nil {
		conn.Close()
		return NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return nil
}

// LocalConclude concludes a transaction started by LocalBegin.
// If the transaction was not previously concluded, it's rolled back.
func (axp *TxPool) LocalConclude(ctx context.Context, conn *TxConnection) {
	if conn.DBConn != nil {
		_ = axp.localRollback(ctx, conn)
	}
}

func (axp *TxPool) localRollback(ctx context.Context, conn *TxConnection) error {
	defer conn.conclude(TxRollback)
	axp.txStats.Add("Aborted", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "rollback", 1, false); err != nil {
		conn.Close()
		return NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return nil
}

// LogActive causes all existing transactions to be logged when they complete.
// The logging is throttled to no more than once every txLogInterval.
func (axp *TxPool) LogActive() {
	axp.logMu.Lock()
	defer axp.logMu.Unlock()
	if time.Now().Sub(axp.lastLog) < txLogInterval {
		return
	}
	axp.lastLog = time.Now()
	conns := axp.activePool.GetAll()
	for _, c := range conns {
		c.(*TxConnection).LogToFile.Set(1)
	}
}

// Timeout returns the transaction timeout.
func (axp *TxPool) Timeout() time.Duration {
	return axp.timeout.Get()
}

// SetTimeout sets the transaction timeout.
func (axp *TxPool) SetTimeout(timeout time.Duration) {
	axp.timeout.Set(timeout)
	axp.ticks.SetInterval(timeout / 10)
}

// TxConnection is meant for executing transactions. It can return itself to
// the tx pool correctly. It also does not retry statements if there
// are failures.
type TxConnection struct {
	*DBConn
	TransactionID     int64
	pool              *TxPool
	StartTime         time.Time
	EndTime           time.Time
	Queries           []string
	Conclusion        string
	LogToFile         sync2.AtomicInt32
	ImmediateCallerID *querypb.VTGateCallerID
	EffectiveCallerID *vtrpcpb.CallerID
}

func newTxConnection(conn *DBConn, transactionID int64, pool *TxPool, immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) *TxConnection {
	return &TxConnection{
		DBConn:            conn,
		TransactionID:     transactionID,
		pool:              pool,
		StartTime:         time.Now(),
		Queries:           make([]string, 0, 8),
		ImmediateCallerID: immediate,
		EffectiveCallerID: effective,
	}
}

// Exec executes the statement for the current transaction.
func (txc *TxConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	r, err := txc.DBConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if IsConnErr(err) {
			txc.pool.checker.CheckMySQL()
			return nil, NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
		}
		return nil, NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return r, nil
}

// Recycle returns the connection to the pool. The transaction remains
// active.
func (txc *TxConnection) Recycle() {
	if txc.IsClosed() {
		txc.conclude(TxClose)
	} else {
		txc.pool.activePool.Put(txc.TransactionID)
	}
}

// RecordQuery records the query against this transaction.
func (txc *TxConnection) RecordQuery(query string) {
	txc.Queries = append(txc.Queries, query)
}

func (txc *TxConnection) conclude(conclusion string) {
	txc.pool.activePool.Unregister(txc.TransactionID)
	txc.DBConn.Recycle()
	txc.DBConn = nil
	txc.log(conclusion)
}

func (txc *TxConnection) log(conclusion string) {
	txc.Conclusion = conclusion
	txc.EndTime = time.Now()

	username := callerid.GetPrincipal(txc.EffectiveCallerID)
	if username == "" {
		username = callerid.GetUsername(txc.ImmediateCallerID)
	}
	duration := txc.EndTime.Sub(txc.StartTime)
	txc.pool.queryServiceStats.UserTransactionCount.Add([]string{username, conclusion}, 1)
	txc.pool.queryServiceStats.UserTransactionTimesNs.Add([]string{username, conclusion}, int64(duration))
	if txc.LogToFile.Get() != 0 {
		log.Infof("Logged transaction: %s", txc.Format(nil))
	}
	TxLogger.Send(txc)
}

// EventTime returns the time the event was created.
func (txc *TxConnection) EventTime() time.Time {
	return txc.EndTime
}

// Format returns a printable version of the connection info.
func (txc *TxConnection) Format(params url.Values) string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		txc.TransactionID,
		callerid.GetPrincipal(txc.EffectiveCallerID),
		callerid.GetUsername(txc.ImmediateCallerID),
		txc.StartTime.Format(time.StampMicro),
		txc.EndTime.Format(time.StampMicro),
		txc.EndTime.Sub(txc.StartTime).Seconds(),
		txc.Conclusion,
		strings.Join(txc.Queries, ";"),
	)
}
