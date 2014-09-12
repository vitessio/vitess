// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconnpool

import (
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/stats"
)

// PooledDBConnection re-exposes DBConnection as a PoolConnection
type PooledDBConnection struct {
	*DBConnection
	pool *ConnectionPool
}

// Recycle implements PoolConnection's Recycle
func (pc *PooledDBConnection) Recycle() {
	if pc.IsClosed() {
		pc.pool.Put(nil)
	} else {
		pc.pool.Put(pc)
	}
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
func DBConnectionCreator(info *mysql.ConnectionParams, mysqlStats *stats.Timings) CreateConnectionFunc {
	return func(pool *ConnectionPool) (PoolConnection, error) {
		c, err := NewDBConnection(info, mysqlStats)
		if err != nil {
			return nil, err
		}
		return &PooledDBConnection{c, pool}, nil
	}
}
