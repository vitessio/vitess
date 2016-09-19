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
	preparedPool      *TxPreparedPool
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

	// Set the prepared pool capacity to something lower than
	// tx pool capacity. Those spare connections are needed to
	// perform metadata state change operations. Without this,
	// the system can deadlock if all connections get moved to
	// the TxPreparedPool.
	prepCap := capacity - 2
	if prepCap < 0 {
		// A capacity of 0 means that Prepare will always fail.
		prepCap = 0
	}

	axp := &TxPool{
		pool:              NewConnPool(name, capacity, idleTimeout, enablePublishStats, qStats, checker),
		activePool:        pools.NewNumbered(),
		preparedPool:      NewTxPreparedPool(prepCap),
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

// WaitForEmpty waits until all active transactions are completed.
func (axp *TxPool) WaitForEmpty() {
	axp.activePool.WaitForEmpty()
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

// Begin begins a transaction, and returns the associated transaction id.
// Subsequent statements can access the connection through the transaction id.
func (axp *TxPool) Begin(ctx context.Context) (int64, error) {
	poolCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel func()
		poolCtx, cancel = context.WithDeadline(ctx, deadline.Add(-10*time.Millisecond))
		defer cancel()
	}
	conn, err := axp.pool.Get(poolCtx)
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
	conn, err := axp.Get(transactionID)
	if err != nil {
		return err
	}
	defer conn.conclude(TxCommit)
	axp.txStats.Add("Completed", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "commit", 1, false); err != nil {
		conn.Close()
		return NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return nil
}

// Rollback rolls back the specified transaction.
func (axp *TxPool) Rollback(ctx context.Context, transactionID int64) error {
	conn, err := axp.Get(transactionID)
	if err != nil {
		return err
	}
	defer conn.conclude(TxRollback)
	axp.txStats.Add("Aborted", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "rollback", 1, false); err != nil {
		conn.Close()
		return NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
	}
	return nil
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on TxConnection once done.
func (axp *TxPool) Get(transactionID int64) (*TxConnection, error) {
	v, err := axp.activePool.Get(transactionID, "for query")
	if err != nil {
		return nil, NewTabletError(vtrpcpb.ErrorCode_NOT_IN_TX, "Transaction %d: %v", transactionID, err)
	}
	return v.(*TxConnection), nil
}

// Prepare executes a Prepare on an active transaction and
// moves the connection to the preparedPool.
func (axp *TxPool) Prepare(ctx context.Context, transactionID int64, dtid string) error {
	// Remove the conn from active pool.
	v, err := axp.activePool.Get(transactionID, "for prepare")
	if err != nil {
		return NewTabletError(vtrpcpb.ErrorCode_NOT_IN_TX, "prepare failed for transaction %d: %v", transactionID, err)
	}
	txc := v.(*TxConnection)
	txc.pool.activePool.Unregister(txc.TransactionID)
	dbconn := txc.DBConn
	txc.DBConn = nil
	txc.log(TxPrepare)

	// Add conn to prepared pool. We need to call Put first to
	// ensure that we get a spot. If subsequent operations fail, we
	// have to "Get" it out.
	err = axp.preparedPool.Put(dbconn, dtid)
	if err != nil {
		// Always rollback a failed prepare.
		if _, err := dbconn.Exec(ctx, "rollback", 1, false); err != nil {
			dbconn.Close()
		}
		dbconn.Recycle()
		return NewTabletError(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, "prepare failed for transaction %d: %v", transactionID, err)
	}

	// TODO(sougou): Add statements to redo log.

	return nil
}

// CommitPrepared commits the prepared transaction.
func (axp *TxPool) CommitPrepared(ctx context.Context, dtid string) error {
	dbconn := axp.preparedPool.Get(dtid)
	if dbconn == nil {
		return nil
	}
	defer dbconn.Recycle()
	// TODO(sougou): Delete transaction metadata using dbconn.
	if _, err := dbconn.Exec(ctx, "commit", 1, false); err != nil {
		// TODO(sougou): Raise alerts and log info.
		dbconn.Close()
		return NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "commit-prepared failed for transaction %s: %v", dtid, err)
	}
	return nil
}

// RollbackPrepared rollsback the prepared transaction.
func (axp *TxPool) RollbackPrepared(ctx context.Context, dtid string) error {
	// TODO(sougou): Delete transaction metadata.
	dbconn := axp.preparedPool.Get(dtid)
	if dbconn == nil {
		return nil
	}
	defer dbconn.Recycle()
	if _, err := dbconn.Exec(ctx, "rollback", 1, false); err != nil {
		dbconn.Close()
		// No need to return an error. We've ensured that the transaction
		// can never be completed.
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
