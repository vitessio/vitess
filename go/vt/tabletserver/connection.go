// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/stats"
	"time"
)

var mysqlStats *stats.Timings
var QueryLogger *relog.Logger

func init() {
	mysqlStats = stats.NewTimings("MySQL")
}

type PoolConnection interface {
	ExecuteFetch(query []byte, maxrows int, wantfields bool) (*QueryResult, error)
	Id() int64
	Close()
	IsClosed() bool
	Recycle()
}

type CreateConnectionFunc func() (connection *DBConnection, err error)

// DBConnection re-exposes mysql.Connection with some wrapping.
type DBConnection struct {
	*mysql.Connection
}

func (self *DBConnection) ExecuteFetch(query []byte, maxrows int, wantfields bool) (*QueryResult, error) {
	start := time.Now()
	if QueryLogger != nil {
		QueryLogger.Info("%s", query)
	}
	mqr, err := self.Connection.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		mysqlStats.Record("Exec", start)
		if sqlErr, ok := err.(*mysql.SqlError); ok {
			if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
				self.Close()
			}
			if sqlErr.Number() == 1317 { // Query was interrupted
				self.Close()
			}
		}
		return nil, err
	}
	mysqlStats.Record("Exec", start)
	qr := QueryResult(*mqr)
	return &qr, nil
}

// CreateConnection returns a connection for running user queries. No DDL.
func CreateConnection(socketPath, dbName string) (*DBConnection, error) {
	info := mysql.ConnectionParams{
		Host:       "localhost",
		UnixSocket: socketPath,
		Uname:      "vt_app",
		Dbname:     dbName,
		Charset:    "utf8",
	}
	c, err := mysql.Connect(info)
	return &DBConnection{c}, err
}

// ConnectionCreator creates a closure that wraps CreateConnection
func ConnectionCreator(socketPath, dbName string) CreateConnectionFunc {
	return func() (connection *DBConnection, err error) {
		return CreateConnection(socketPath, dbName)
	}
}

/* CreateSuperConnection retuns a connection for doing DDLs and maintenence operations
where you need full control over mysql.
*/
func CreateSuperConnection(socketPath, dbName string) (*DBConnection, error) {
	info := mysql.ConnectionParams{
		Host:       "localhost",
		UnixSocket: socketPath,
		Uname:      "vt_dba",
		Dbname:     dbName,
		Charset:    "utf8",
	}
	c, err := mysql.Connect(info)
	return &DBConnection{c}, err
}

// SuperConnectionCreator is a closure that wraps CreateSuperConnection
func SuperConnectionCreator(socketPath, dbName string) CreateConnectionFunc {
	return func() (connection *DBConnection, err error) {
		return CreateSuperConnection(socketPath, dbName)
	}
}

func CreateGenericConnection(info mysql.ConnectionParams) (*DBConnection, error) {
	c, err := mysql.Connect(info)
	return &DBConnection{c}, err
}

func GenericConnectionCreator(info mysql.ConnectionParams) CreateConnectionFunc {
	return func() (connection *DBConnection, err error) {
		return CreateGenericConnection(info)
	}
}
