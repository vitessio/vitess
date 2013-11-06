// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
)

type ReservedPool struct {
	pool        *pools.Numbered
	lastId      sync2.AtomicInt64
	connFactory CreateConnectionFunc
}

func NewReservedPool(name string) *ReservedPool {
	rp := &ReservedPool{pool: pools.NewNumbered(), lastId: 1}
	stats.Publish(name+"Size", stats.IntFunc(rp.pool.Size))
	return rp
}

func (rp *ReservedPool) Open(connFactory CreateConnectionFunc) {
	rp.connFactory = connFactory
}

func (rp *ReservedPool) Close() {
	for _, v := range rp.pool.GetOutdated(time.Duration(0), "for close") {
		conn := v.(*reservedConnection)
		conn.Close()
		rp.pool.Unregister(conn.connectionId)
	}
}

func (rp *ReservedPool) CreateConnection() (connectionId int64) {
	conn, err := rp.connFactory()
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	connectionId = rp.lastId.Add(1)
	rconn := &reservedConnection{DBConnection: conn, connectionId: connectionId, pool: rp}
	rp.pool.Register(connectionId, rconn)
	return connectionId
}

func (rp *ReservedPool) CloseConnection(connectionId int64) {
	conn := rp.Get(connectionId).(*reservedConnection)
	conn.Close()
	rp.pool.Unregister(connectionId)
}

// You must call Recycle on the PoolConnection once done.
func (rp *ReservedPool) Get(connectionId int64) PoolConnection {
	v, err := rp.pool.Get(connectionId, "for query")
	if err != nil {
		panic(NewTabletError(FAIL, "Error getting connection %d: %v", connectionId, err))
	}
	return v.(*reservedConnection)
}

func (rp *ReservedPool) StatsJSON() string {
	return rp.pool.StatsJSON()
}

type reservedConnection struct {
	*DBConnection
	connectionId int64
	pool         *ReservedPool
	inUse        bool
}

func (pr *reservedConnection) Recycle() {
	if pr.IsClosed() {
		pr.pool.pool.Unregister(pr.connectionId)
	} else {
		pr.pool.pool.Put(pr.connectionId)
	}
}
