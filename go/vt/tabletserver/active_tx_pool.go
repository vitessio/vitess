// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	log "github.com/golang/glog"
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

type ActiveTxPool struct {
	pool            *pools.Numbered
	lastId          sync2.AtomicInt64
	timeout         sync2.AtomicDuration
	ticks           *timer.Timer
	txStats         *stats.Timings
	completionStats *stats.Timings
}

func NewActiveTxPool(name string, timeout time.Duration) *ActiveTxPool {
	axp := &ActiveTxPool{
		pool:            pools.NewNumbered(),
		lastId:          sync2.AtomicInt64(time.Now().UnixNano()),
		timeout:         sync2.AtomicDuration(timeout),
		ticks:           timer.NewTimer(timeout / 10),
		txStats:         stats.NewTimings("Transactions"),
		completionStats: stats.NewTimings("TransactionCompletion"),
	}
	stats.Publish(name+"Size", stats.IntFunc(axp.pool.Size))
	stats.Publish(
		name+"Timeout",
		stats.DurationFunc(func() time.Duration { return axp.timeout.Get() }),
	)
	return axp
}

func (axp *ActiveTxPool) Open() {
	log.Infof("Starting transaction id: %d", axp.lastId)
	axp.ticks.Start(func() { axp.TransactionKiller() })
}

func (axp *ActiveTxPool) Close() {
	axp.ticks.Stop()
	for _, v := range axp.pool.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*TxConnection)
		conn.Close()
		conn.discard(TX_CLOSE)
	}
}

func (axp *ActiveTxPool) WaitForEmpty() {
	axp.pool.WaitForEmpty()
}

func (axp *ActiveTxPool) TransactionKiller() {
	for _, v := range axp.pool.GetOutdated(time.Duration(axp.Timeout()), "for rollback") {
		conn := v.(*TxConnection)
		log.Infof("killing transaction %d: %#v", conn.transactionId, conn.queries)
		killStats.Add("Transactions", 1)
		conn.Close()
		conn.discard(TX_KILL)
	}
}

func (axp *ActiveTxPool) SafeBegin(conn PoolConnection) (transactionId int64, err error) {
	defer handleError(&err, nil)
	if _, err := conn.ExecuteFetch(BEGIN, 1, false); err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}
	transactionId = axp.lastId.Add(1)
	axp.pool.Register(transactionId, newTxConnection(conn, transactionId, axp))
	return transactionId, nil
}

func (axp *ActiveTxPool) SafeCommit(transactionId int64) (invalidList map[string]DirtyKeys, err error) {
	defer handleError(&err, nil)
	conn := axp.Get(transactionId)
	defer conn.discard(TX_COMMIT)
	axp.txStats.Add("Completed", time.Now().Sub(conn.startTime))
	defer axp.completionStats.Record("Commit", time.Now())
	if _, err = conn.ExecuteFetch(COMMIT, 1, false); err != nil {
		conn.Close()
		return conn.dirtyTables, NewTabletErrorSql(FAIL, err)
	}
	return conn.dirtyTables, nil
}

func (axp *ActiveTxPool) Rollback(transactionId int64) {
	conn := axp.Get(transactionId)
	defer conn.discard(TX_ROLLBACK)
	axp.txStats.Add("Aborted", time.Now().Sub(conn.startTime))
	defer axp.completionStats.Record("Rollback", time.Now())
	if _, err := conn.ExecuteFetch(ROLLBACK, 1, false); err != nil {
		conn.Close()
		panic(NewTabletErrorSql(FAIL, err))
	}
}

// You must call Recycle on TxConnection once done.
func (axp *ActiveTxPool) Get(transactionId int64) (conn *TxConnection) {
	v, err := axp.pool.Get(transactionId, "for query")
	if err != nil {
		panic(NewTabletError(NOT_IN_TX, "Transaction %d: %v", transactionId, err))
	}
	return v.(*TxConnection)
}

func (axp *ActiveTxPool) Timeout() time.Duration {
	return axp.timeout.Get()
}

func (axp *ActiveTxPool) SetTimeout(timeout time.Duration) {
	axp.timeout.Set(timeout)
	axp.ticks.SetInterval(timeout / 10)
}

func (axp *ActiveTxPool) StatsJSON() string {
	s, t := axp.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Timeout\": %v}", s, int64(t))
}

func (axp *ActiveTxPool) Stats() (size int64, timeout time.Duration) {
	return axp.pool.Size(), axp.Timeout()
}

type TxConnection struct {
	PoolConnection
	transactionId int64
	pool          *ActiveTxPool
	inUse         bool
	startTime     time.Time
	endTime       time.Time
	dirtyTables   map[string]DirtyKeys
	queries       []string
	conclusion    string
}

func newTxConnection(conn PoolConnection, transactionId int64, pool *ActiveTxPool) *TxConnection {
	return &TxConnection{
		PoolConnection: conn,
		transactionId:  transactionId,
		pool:           pool,
		startTime:      time.Now(),
		dirtyTables:    make(map[string]DirtyKeys),
		queries:        make([]string, 0, 8),
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

func (txc *TxConnection) Recycle() {
	if txc.IsClosed() {
		txc.discard(TX_CLOSE)
	} else {
		txc.pool.pool.Put(txc.transactionId)
	}
}

func (txc *TxConnection) RecordQuery(query string) {
	txc.queries = append(txc.queries, query)
}

func (txc *TxConnection) discard(conclusion string) {
	txc.conclusion = conclusion
	txc.endTime = time.Now()
	txc.pool.pool.Unregister(txc.transactionId)
	txc.PoolConnection.Recycle()
	// Ensure PoolConnection won't be accessed after Recycle.
	txc.PoolConnection = nil
	TxLogger.Send(txc)
}

const txTimeLayout = "2006-01-02 15:04:05.9999"

func (txc *TxConnection) Format(params url.Values) string {
	return fmt.Sprintf(
		"%v\t%v\t%v\t%.6f\t%v\t%v\t\n",
		txc.transactionId,
		txc.startTime.Format(txTimeLayout),
		txc.endTime.Format(txTimeLayout),
		txc.endTime.Sub(txc.startTime).Seconds(),
		txc.conclusion,
		strings.Join(txc.queries, ";"),
	)
}

type DirtyKeys map[string]bool

// Delete just keeps track of what needs to be deleted
func (dk DirtyKeys) Delete(key string) bool {
	dk[key] = true
	return true
}
