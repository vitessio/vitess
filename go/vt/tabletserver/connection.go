// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"strings"
	"time"

	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/stats"
)

var mysqlStats *stats.Timings
var QueryLogger *relog.Logger

func init() {
	mysqlStats = stats.NewTimings("MySQL")
}

type PoolConnection interface {
	ExecuteFetch(query []byte, maxrows int, wantfields bool) (*proto.QueryResult, error)
	ExecuteStreamFetch(query []byte, callback func(interface{}) error, streamBufferSize int) error
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

func (conn *DBConnection) handleError(err error) {
	if sqlErr, ok := err.(*mysql.SqlError); ok {
		if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
			conn.Close()
		}
		if sqlErr.Number() == 1317 { // Query was interrupted
			conn.Close()
		}
	}
}

func (dbc *DBConnection) ExecuteFetch(query []byte, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	start := time.Now()
	if QueryLogger != nil {
		QueryLogger.Info("%s", query)
	}
	mqr, err := dbc.Connection.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		mysqlStats.Record("Exec", start)
		dbc.handleError(err)
		return nil, err
	}
	mysqlStats.Record("Exec", start)
	qr := proto.QueryResult(*mqr)
	return &qr, nil
}

func (conn *DBConnection) ExecuteStreamFetch(query []byte, callback func(interface{}) error, streamBufferSize int) error {
	start := time.Now()
	if QueryLogger != nil {
		QueryLogger.Info("%s", query)
	}

	err := conn.Connection.ExecuteStreamFetch(query)
	if err != nil {
		mysqlStats.Record("ExecStream", start)
		conn.handleError(err)
		return err
	}
	defer conn.CloseResult()

	// first call the callback with the fields
	err = callback(&proto.QueryResult{Fields: conn.Fields()})
	if err != nil {
		return err
	}

	// then get all the rows, sending them as we reach a decent packet size
	qr := &proto.QueryResult{}
	byteCount := 0
	for {
		row, err := conn.FetchNext()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		qr.Rows = append(qr.Rows, row)
		for _, s := range row {
			byteCount += len(s.Raw())
		}

		if byteCount >= streamBufferSize {
			err = callback(qr)
			if err != nil {
				return err
			}
			qr = &proto.QueryResult{}
			byteCount = 0
		}
	}

	if len(qr.Rows) > 0 {
		err = callback(qr)
		if err != nil {
			return err
		}
	}

	return nil
}

var getModeSql = []byte("select @@global.sql_mode")

func (conn *DBConnection) VerifyStrict() bool {
	qr, err := conn.ExecuteFetch(getModeSql, 2, false)
	if err != nil {
		return false
	}
	if len(qr.Rows) == 0 {
		return false
	}
	return strings.Contains(qr.Rows[0][0].String(), "STRICT_TRANS_TABLES")
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
