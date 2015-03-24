// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconnpool

import (
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
)

// PooledDBConnection re-exposes DBConnection as a PoolConnection
type PooledDBConnection struct {
	*DBConnection
	info       *sqldb.ConnParams
	mysqlStats *stats.Timings
	pool       *ConnectionPool
}

// Recycle implements PoolConnection's Recycle
func (pc *PooledDBConnection) Recycle() {
	if pc.IsClosed() {
		pc.pool.Put(nil)
	} else {
		pc.pool.Put(pc)
	}
}

// Reconnect replaces the existing underlying connection
// with a new one.
func (pc *PooledDBConnection) Reconnect() error {
	pc.DBConnection.Close()
	newConn, err := NewDBConnection(pc.info, pc.mysqlStats)
	if err != nil {
		return err
	}
	pc.DBConnection = newConn
	return nil
}

// DBConnectionCreator is the wrapper function to use to create a pool
// of DBConnection objects.
//
// For instance:
// mysqlStats := stats.NewTimings("Mysql")
// pool := dbconnpool.NewConnectionPool("name", 10, 30*time.Second)
// pool.Open(dbconnpool.DBConnectionCreator(info, mysqlStats))
// ...
// conn, err := pool.Get()
// ...
func DBConnectionCreator(info *sqldb.ConnParams, mysqlStats *stats.Timings) CreateConnectionFunc {
	return func(pool *ConnectionPool) (PoolConnection, error) {
		c, err := NewDBConnection(info, mysqlStats)
		if err != nil {
			return nil, err
		}
		return &PooledDBConnection{
			DBConnection: c,
			info:         info,
			mysqlStats:   mysqlStats,
			pool:         pool,
		}, nil
	}
}
