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
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"golang.org/x/net/context"
)

/* Function naming convention:
UpperCaseFunctions() are thread safe, they can still panic on error
lowerCaseFunctions() are not thread safe
SafeFunctions() return os.Error instead of throwing exceptions
*/

// TxLogger can be used to enable logging of transactions.
// Call TxLogger.ServeLogs in your main program to enable logging.
// The log format can be inferred by looking at TxConnection.Format.
var TxLogger = streamlog.New("TxLog", 10)

// These consts identify how a transaction was resolved.
const (
	TxClose    = "close"
	TxCommit   = "commit"
	TxRollback = "rollback"
	TxKill     = "kill"
)

const txLogInterval = time.Duration(1 * time.Minute)

// TxPool is the transaction pool for the query service.
type TxPool struct {
	pool              *ConnPool
	activePool        *pools.Numbered
	lastID            sync2.AtomicInt64
	timeout           sync2.AtomicDuration
	poolTimeout       sync2.AtomicDuration
	ticks             *timer.Timer
	txStats           *stats.Timings
	queryServiceStats *QueryServiceStats
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
	poolTimeout time.Duration,
	idleTimeout time.Duration,
	enablePublishStats bool,
	qStats *QueryServiceStats) *TxPool {

	txStatsName := ""
	if enablePublishStats {
		txStatsName = txStatsPrefix + "Transactions"
	}

	axp := &TxPool{
		pool:              NewConnPool(name, capacity, idleTimeout, enablePublishStats, qStats),
		activePool:        pools.NewNumbered(),
		lastID:            sync2.NewAtomicInt64(time.Now().UnixNano()),
		timeout:           sync2.NewAtomicDuration(timeout),
		poolTimeout:       sync2.NewAtomicDuration(poolTimeout),
		ticks:             timer.NewTimer(timeout / 10),
		txStats:           stats.NewTimings(txStatsName),
		queryServiceStats: qStats,
	}
	// Careful: pool also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	if enablePublishStats {
		stats.Publish(name+"Timeout", stats.DurationFunc(axp.timeout.Get))
		stats.Publish(name+"PoolTimeout", stats.DurationFunc(axp.poolTimeout.Get))
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
		conn.discard(TxClose)
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
		conn.discard(TxKill)
	}
}

// Begin begins a transaction, and returns the associated transaction id.
// Subsequent statements can access the connection through the transaction id.
func (axp *TxPool) Begin(ctx context.Context) int64 {
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
			panic(err)
		case pools.ErrTimeout:
			axp.LogActive()
			panic(NewTabletError(ErrTxPoolFull, "Transaction pool connection limit exceeded"))
		}
		panic(NewTabletErrorSql(ErrFatal, err))
	}
	if _, err := conn.Exec(ctx, "begin", 1, false); err != nil {
		conn.Recycle()
		panic(NewTabletErrorSql(ErrFail, err))
	}
	transactionID := axp.lastID.Add(1)
	axp.activePool.Register(transactionID, newTxConnection(conn, transactionID, axp))
	return transactionID
}

// SafeCommit commits the specified transaction. Unlike other functions, it
// returns an error on failure instead of panic. The connection becomes free
// and can be reused in the future.
func (axp *TxPool) SafeCommit(ctx context.Context, transactionID int64) (invalidList map[string]DirtyKeys, err error) {
	defer handleError(&err, nil, axp.queryServiceStats)

	conn := axp.Get(transactionID)
	defer conn.discard(TxCommit)
	// Assign this upfront to make sure we always return the invalidList.
	invalidList = conn.dirtyTables
	axp.txStats.Add("Completed", time.Now().Sub(conn.StartTime))
	if _, fetchErr := conn.Exec(ctx, "commit", 1, false); fetchErr != nil {
		conn.Close()
		err = NewTabletErrorSql(ErrFail, fetchErr)
	}
	return
}

// Rollback rolls back the specified transaction.
func (axp *TxPool) Rollback(ctx context.Context, transactionID int64) {
	conn := axp.Get(transactionID)
	defer conn.discard(TxRollback)
	axp.txStats.Add("Aborted", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "rollback", 1, false); err != nil {
		conn.Close()
		panic(NewTabletErrorSql(ErrFail, err))
	}
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on TxConnection once done.
func (axp *TxPool) Get(transactionID int64) (conn *TxConnection) {
	v, err := axp.activePool.Get(transactionID, "for query")
	if err != nil {
		panic(NewTabletError(ErrNotInTx, "Transaction %d: %v", transactionID, err))
	}
	return v.(*TxConnection)
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

// SetPoolTimeout sets the wait time for the tx pool.
// TODO(sougou): move this to SqlQuery.
func (axp *TxPool) SetPoolTimeout(timeout time.Duration) {
	axp.poolTimeout.Set(timeout)
}

// PoolTimeout returns the wait time for the tx pool.
func (axp *TxPool) PoolTimeout() time.Duration {
	return axp.poolTimeout.Get()
}

// TxConnection is meant for executing transactions. It keeps track
// of dirty keys for rowcache invalidation. It can return itself to
// the tx pool correctly. It also does not retry statements if there
// are failures.
type TxConnection struct {
	*DBConn
	TransactionID int64
	pool          *TxPool
	inUse         bool
	StartTime     time.Time
	EndTime       time.Time
	dirtyTables   map[string]DirtyKeys
	Queries       []string
	Conclusion    string
	LogToFile     sync2.AtomicInt32
}

func newTxConnection(conn *DBConn, transactionID int64, pool *TxPool) *TxConnection {
	return &TxConnection{
		DBConn:        conn,
		TransactionID: transactionID,
		pool:          pool,
		StartTime:     time.Now(),
		dirtyTables:   make(map[string]DirtyKeys),
		Queries:       make([]string, 0, 8),
	}
}

// DirtyKeys returns the list of rowcache keys that became dirty
// during the transaction.
func (txc *TxConnection) DirtyKeys(tableName string) DirtyKeys {
	if list, ok := txc.dirtyTables[tableName]; ok {
		return list
	}
	list := make(DirtyKeys)
	txc.dirtyTables[tableName] = list
	return list
}

// Exec executes the statement for the current transaction.
func (txc *TxConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	r, err := txc.DBConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if IsConnErr(err) {
			go checkMySQL()
			return nil, NewTabletErrorSql(ErrFatal, err)
		}
		return nil, NewTabletErrorSql(ErrFail, err)
	}
	return r, nil
}

// Recycle returns the connection to the pool. The transaction remains
// active.
func (txc *TxConnection) Recycle() {
	if txc.IsClosed() {
		txc.discard(TxClose)
	} else {
		txc.pool.activePool.Put(txc.TransactionID)
	}
}

// RecordQuery records the query against this transaction.
func (txc *TxConnection) RecordQuery(query string) {
	txc.Queries = append(txc.Queries, query)
}

func (txc *TxConnection) discard(conclusion string) {
	txc.Conclusion = conclusion
	txc.EndTime = time.Now()
	txc.pool.activePool.Unregister(txc.TransactionID)
	txc.DBConn.Recycle()
	// Ensure PoolConnection won't be accessed after Recycle.
	txc.DBConn = nil
	if txc.LogToFile.Get() != 0 {
		log.Infof("Logged transaction: %s", txc.Format(nil))
	}
	TxLogger.Send(txc)
}

// Format returns a printable version of the connection info.
func (txc *TxConnection) Format(params url.Values) string {
	return fmt.Sprintf(
		"%v\t%v\t%v\t%.6f\t%v\t%v\t\n",
		txc.TransactionID,
		txc.StartTime.Format(time.StampMicro),
		txc.EndTime.Format(time.StampMicro),
		txc.EndTime.Sub(txc.StartTime).Seconds(),
		txc.Conclusion,
		strings.Join(txc.Queries, ";"),
	)
}

// DirtyKeys provides a cache-like interface, where
// it just adds keys to its likst as Delete gets called.
type DirtyKeys map[string]bool

// Delete just keeps track of what needs to be deleted
func (dk DirtyKeys) Delete(key string) {
	dk[key] = true
}
