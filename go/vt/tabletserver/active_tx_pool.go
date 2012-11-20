// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/pools"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/timer"
	"fmt"
	"sync/atomic"
	"time"
)

/* Function naming convention:
UpperCaseFunctions() are thread safe, they can still panic on error
lowerCaseFunctions() are not thread safe
SafeFunctions() return os.Error instead of throwing exceptions
*/

var (
	BEGIN    = []byte("begin")
	COMMIT   = []byte("commit")
	ROLLBACK = []byte("rollback")
)

type ActiveTxPool struct {
	pool    *pools.Numbered
	lastId  int64
	timeout int64
	ticks   *timer.Timer
	txStats *stats.Timings
}

func NewActiveTxPool(timeout time.Duration) *ActiveTxPool {
	return &ActiveTxPool{
		pool:    pools.NewNumbered(),
		lastId:  time.Now().UnixNano(),
		timeout: int64(timeout),
		ticks:   timer.NewTimer(timeout / 10),
		txStats: stats.NewTimings("Transactions"),
	}
}

func (self *ActiveTxPool) Open() {
	relog.Info("Starting transaction id: %d", self.lastId)
	self.ticks.Start(func() { self.TransactionKiller() })
}

func (self *ActiveTxPool) Close() {
	self.ticks.Stop()
	for _, v := range self.pool.GetTimedout(time.Duration(0)) {
		conn := v.(*TxConnection)
		conn.Close()
		conn.discard()
	}
}

func (self *ActiveTxPool) WaitForEmpty() {
	self.pool.WaitForEmpty()
}

func (self *ActiveTxPool) TransactionKiller() {
	for _, v := range self.pool.GetTimedout(time.Duration(self.Timeout())) {
		conn := v.(*TxConnection)
		relog.Info("killing transaction %d", conn.transactionId)
		killStats.Add("Transactions", 1)
		conn.Close()
		conn.discard()
	}
}

func (self *ActiveTxPool) SafeBegin(conn PoolConnection) (transactionId int64, err error) {
	defer handleError(&err, nil)
	if _, err := conn.ExecuteFetch(BEGIN, 1, false); err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}
	transactionId = atomic.AddInt64(&self.lastId, 1)
	self.pool.Register(transactionId, newTxConnection(conn, transactionId, self))
	return transactionId, nil
}

func (self *ActiveTxPool) SafeCommit(transactionId int64) (invalidList map[string]DirtyKeys, err error) {
	defer handleError(&err, nil)
	conn := self.Get(transactionId)
	defer conn.discard()
	self.txStats.Add("Completed", time.Now().Sub(conn.startTime))
	if _, err = conn.ExecuteFetch(COMMIT, 1, false); err != nil {
		conn.Close()
	}
	return conn.dirtyTables, err
}

func (self *ActiveTxPool) Rollback(transactionId int64) {
	conn := self.Get(transactionId)
	defer conn.discard()
	self.txStats.Add("Aborted", time.Now().Sub(conn.startTime))
	if _, err := conn.ExecuteFetch(ROLLBACK, 1, false); err != nil {
		conn.Close()
		panic(NewTabletErrorSql(FAIL, err))
	}
}

// You must call Recycle on TxConnection once done.
func (self *ActiveTxPool) Get(transactionId int64) (conn *TxConnection) {
	v, err := self.pool.Get(transactionId)
	if err != nil {
		panic(NewTabletError(FAIL, "Transaction %d: %v", transactionId, err))
	}
	return v.(*TxConnection)
}

func (self *ActiveTxPool) Timeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&self.timeout))
}

func (self *ActiveTxPool) SetTimeout(timeout time.Duration) {
	atomic.StoreInt64(&self.timeout, int64(timeout))
	self.ticks.SetInterval(timeout / 10)
}

func (self *ActiveTxPool) StatsJSON() string {
	s, t := self.Stats()
	return fmt.Sprintf("{\"Size\": %v, \"Timeout\": %v}", s, int64(t))
}

func (self *ActiveTxPool) Stats() (size int, timeout time.Duration) {
	return self.pool.Stats(), self.Timeout()
}

type TxConnection struct {
	PoolConnection
	transactionId int64
	pool          *ActiveTxPool
	inUse         bool
	startTime     time.Time
	dirtyTables   map[string]DirtyKeys
}

func newTxConnection(conn PoolConnection, transactionId int64, pool *ActiveTxPool) *TxConnection {
	return &TxConnection{
		PoolConnection: conn,
		transactionId:  transactionId,
		pool:           pool,
		startTime:      time.Now(),
		dirtyTables:    make(map[string]DirtyKeys),
	}
}

func (self *TxConnection) DirtyKeys(tableName string) DirtyKeys {
	if list, ok := self.dirtyTables[tableName]; ok {
		return list
	}
	list := make(DirtyKeys)
	self.dirtyTables[tableName] = list
	return list
}

func (self *TxConnection) Recycle() {
	if self.IsClosed() {
		self.discard()
	} else {
		self.pool.pool.Put(self.transactionId)
	}
}

func (self *TxConnection) discard() {
	self.pool.pool.Unregister(self.transactionId)
	self.PoolConnection.Recycle()
}

type DirtyKeys map[string]bool

// Delete just keeps track of what needs to be deleted
func (self DirtyKeys) Delete(key string) bool {
	self[key] = true
	return true
}
