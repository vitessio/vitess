/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dbconnpool

import (
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/stats"
)

// PooledDBConnection re-exposes DBConnection as a PoolConnection
type PooledDBConnection struct {
	*DBConnection
	info       *mysql.ConnParams
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
func DBConnectionCreator(info *mysql.ConnParams, mysqlStats *stats.Timings) CreateConnectionFunc {
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
