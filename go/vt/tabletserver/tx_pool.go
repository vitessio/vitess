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
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
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

var (
	BEGIN    = "begin"
	COMMIT   = "commit"
	ROLLBACK = "rollback"
)

const (
	TX_CLOSE    = "close"
	TX_COMMIT   = "commit"
	TX_ROLLBACK = "rollback"
	TX_KILL     = "kill"
)

const txLogInterval = time.Duration(1 * time.Minute)

type TxPool struct {
	pool        *ConnPool
	activePool  *pools.Numbered
	lastId      sync2.AtomicInt64
	timeout     sync2.AtomicDuration
	poolTimeout sync2.AtomicDuration
	ticks       *timer.Timer
	txStats     *stats.Timings

	// Tracking culprits that cause tx pool full errors.
	logMu   sync.Mutex
	lastLog time.Time
}

func NewTxPool(name string, capacity int, timeout, poolTimeout, idleTimeout time.Duration) *TxPool {
	axp := &TxPool{
		pool:        NewConnPool(name, capacity, idleTimeout),
		activePool:  pools.NewNumbered(),
		lastId:      sync2.AtomicInt64(time.Now().UnixNano()),
		timeout:     sync2.AtomicDuration(timeout),
		poolTimeout: sync2.AtomicDuration(poolTimeout),
		ticks:       timer.NewTimer(timeout / 10),
		txStats:     stats.NewTimings("Transactions"),
	}
	// Careful: pool also exports name+"xxx" vars,
	// but we know it doesn't export Timeout.
	stats.Publish(name+"Timeout", stats.DurationFunc(axp.timeout.Get))
	stats.Publish(name+"PoolTimeout", stats.DurationFunc(axp.poolTimeout.Get))
	return axp
}

func (axp *TxPool) Open(appParams, dbaParams *mysql.ConnectionParams) {
	log.Infof("Starting transaction id: %d", axp.lastId)
	axp.pool.Open(appParams, dbaParams)
	axp.ticks.Start(func() { axp.TransactionKiller() })
}

func (axp *TxPool) Close() {
	axp.ticks.Stop()
	for _, v := range axp.activePool.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction for shutdown: %s", conn.Format(nil))
		internalErrors.Add("StrayTransactions", 1)
		conn.Close()
		conn.discard(TX_CLOSE)
	}
	axp.pool.Close()
}

func (axp *TxPool) WaitForEmpty() {
	axp.activePool.WaitForEmpty()
}

func (axp *TxPool) TransactionKiller() {
	defer logError()
	for _, v := range axp.activePool.GetOutdated(time.Duration(axp.Timeout()), "for rollback") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction (exceeded timeout: %v): %s", axp.Timeout(), conn.Format(nil))
		killStats.Add("Transactions", 1)
		conn.Close()
		conn.discard(TX_KILL)
	}
}

func (axp *TxPool) Begin() int64 {
	conn, err := axp.pool.Get(axp.poolTimeout.Get())
	if err != nil {
		switch err {
		case ErrConnPoolClosed:
			panic(connPoolClosedErr)
		case pools.TIMEOUT_ERR:
			axp.LogActive()
			panic(NewTabletError(ErrTxPoolFull, "Transaction pool connection limit exceeded"))
		}
		panic(NewTabletErrorSql(ErrFatal, err))
	}
	// TODO(sougou): Use deadline from context here.
	if _, err := conn.Exec(BEGIN, 1, false, NewDeadline(1*time.Minute)); err != nil {
		conn.Recycle()
		panic(NewTabletErrorSql(ErrFail, err))
	}
	transactionId := axp.lastId.Add(1)
	axp.activePool.Register(transactionId, newTxConnection(conn, transactionId, axp))
	return transactionId
}

func (axp *TxPool) SafeCommit(transactionId int64) (invalidList map[string]DirtyKeys, err error) {
	defer handleError(&err, nil)

	conn := axp.Get(transactionId)
	defer conn.discard(TX_COMMIT)
	// Assign this upfront to make sure we always return the invalidList.
	invalidList = conn.dirtyTables
	axp.txStats.Add("Completed", time.Now().Sub(conn.StartTime))
	// TODO(sougou): Use deadline from context here.
	if _, fetchErr := conn.Exec(COMMIT, 1, false, NewDeadline(1*time.Minute)); fetchErr != nil {
		conn.Close()
		err = NewTabletErrorSql(ErrFail, fetchErr)
	}
	return
}

func (axp *TxPool) Rollback(transactionId int64) {
	conn := axp.Get(transactionId)
	defer conn.discard(TX_ROLLBACK)
	axp.txStats.Add("Aborted", time.Now().Sub(conn.StartTime))
	// TODO(sougou): Use deadline from context here.
	if _, err := conn.Exec(ROLLBACK, 1, false, NewDeadline(1*time.Minute)); err != nil {
		conn.Close()
		panic(NewTabletErrorSql(ErrFail, err))
	}
}

// You must call Recycle on TxConnection once done.
func (axp *TxPool) Get(transactionId int64) (conn *TxConnection) {
	v, err := axp.activePool.Get(transactionId, "for query")
	if err != nil {
		panic(NewTabletError(ErrNotInTx, "Transaction %d: %v", transactionId, err))
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

func (axp *TxPool) Timeout() time.Duration {
	return axp.timeout.Get()
}

func (axp *TxPool) SetTimeout(timeout time.Duration) {
	axp.timeout.Set(timeout)
	axp.ticks.SetInterval(timeout / 10)
}

func (axp *TxPool) SetPoolTimeout(timeout time.Duration) {
	axp.poolTimeout.Set(timeout)
}

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

func newTxConnection(conn *DBConn, transactionId int64, pool *TxPool) *TxConnection {
	return &TxConnection{
		DBConn:        conn,
		TransactionID: transactionId,
		pool:          pool,
		StartTime:     time.Now(),
		dirtyTables:   make(map[string]DirtyKeys),
		Queries:       make([]string, 0, 8),
	}
}

func (txc *TxConnection) DirtyKeys(tableName string) DirtyKeys {
	if list, ok := txc.dirtyTables[tableName]; ok {
		return list
	}
	list := make(DirtyKeys)
	txc.dirtyTables[tableName] = list
	return list
}

func (txc *TxConnection) Exec(query string, maxrows int, wantfields bool, deadline Deadline) (*proto.QueryResult, error) {
	r, err := txc.DBConn.execOnce(query, maxrows, wantfields, deadline)
	if err != nil {
		if IsConnErr(err) {
			go checkMySQL()
			return nil, NewTabletErrorSql(ErrFatal, err)
		}
		return nil, NewTabletErrorSql(ErrFail, err)
	}
	return r, nil
}

func (txc *TxConnection) Recycle() {
	if txc.IsClosed() {
		txc.discard(TX_CLOSE)
	} else {
		txc.pool.activePool.Put(txc.TransactionID)
	}
}

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

type DirtyKeys map[string]bool

// Delete just keeps track of what needs to be deleted
func (dk DirtyKeys) Delete(key string) {
	dk[key] = true
}
