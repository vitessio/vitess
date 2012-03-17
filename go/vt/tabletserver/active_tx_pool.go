/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
	go self.TransactionKiller()
}

func (self *ActiveTxPool) Close() {
	self.ticks.Close()
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
	for self.ticks.Next() {
		for _, v := range self.pool.GetTimedout(time.Duration(self.Timeout())) {
			conn := v.(*TxConnection)
			relog.Info("killing transaction %d", conn.transactionId)
			killStats.Add("Transactions", 1)
			conn.Close()
			conn.discard()
		}
	}
}

func (self *ActiveTxPool) SafeBegin(conn PoolConnection) (transactionId int64, err error) {
	defer handleError(&err)
	if _, err := conn.ExecuteFetch(BEGIN, 10000); err != nil {
		panic(NewTabletErrorSql(FAIL, err))
	}
	transactionId = atomic.AddInt64(&self.lastId, 1)
	self.pool.Register(transactionId, newTxConnection(conn, transactionId, self))
	return transactionId, nil
}

// An unpleasant dependency to SchemaInfo. Avoiding it makes the code worse
func (self *ActiveTxPool) Commit(transactionId int64, schemaInfo *SchemaInfo) {
	conn := self.Get(transactionId)
	defer conn.discard()
	self.txStats.Add("Completed", time.Now().Sub(conn.startTime))
	defer func() {
		for tableName, invalidList := range conn.dirtyTables {
			tableInfo := schemaInfo.GetTable(tableName)
			for key := range invalidList {
				tableInfo.RowCache.Delete(key)
			}
			schemaInfo.Put(tableInfo)
		}
	}()
	if _, err := conn.ExecuteFetch(COMMIT, 10000); err != nil {
		conn.Close()
		panic(NewTabletErrorSql(FAIL, err))
	}
}

func (self *ActiveTxPool) Rollback(transactionId int64) {
	conn := self.Get(transactionId)
	defer conn.discard()
	self.txStats.Add("Aborted", time.Now().Sub(conn.startTime))
	if _, err := conn.ExecuteFetch(ROLLBACK, 10000); err != nil {
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
	return fmt.Sprintf("{\"Size\": %v, \"Timeout\": %v}", s, float64(t)/1e9)
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
