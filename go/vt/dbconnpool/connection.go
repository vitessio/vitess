// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconnpool

import (
	"strings"
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
)

// DBConnection re-exposes mysql.Connection with some wrapping to implement
// most of PoolConnection interface, except Recycle. That way it can be used
// by itself. (Recycle needs to know about the Pool).
type DBConnection struct {
	*mysql.Connection
	mysqlStats *stats.Timings
}

func (dbc *DBConnection) handleError(err error) {
	if sqlErr, ok := err.(*mysql.SqlError); ok {
		if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
			dbc.Close()
		}
		if sqlErr.Number() == 1317 { // Query was interrupted
			dbc.Close()
		}
	}
}

// ExecuteFetch is part of PoolConnection interface.
func (dbc *DBConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	start := time.Now()
	mqr, err := dbc.Connection.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		dbc.mysqlStats.Record("Exec", start)
		dbc.handleError(err)
		return nil, err
	}
	dbc.mysqlStats.Record("Exec", start)
	qr := proto.QueryResult(*mqr)
	return &qr, nil
}

// ExecuteStreamFetch is part of PoolConnection interface.
func (dbc *DBConnection) ExecuteStreamFetch(query string, callback func(*proto.QueryResult) error, streamBufferSize int) error {
	start := time.Now()

	err := dbc.Connection.ExecuteStreamFetch(query)
	if err != nil {
		dbc.mysqlStats.Record("ExecStream", start)
		dbc.handleError(err)
		return err
	}
	defer dbc.CloseResult()

	// first call the callback with the fields
	err = callback(&proto.QueryResult{Fields: dbc.Fields()})
	if err != nil {
		return err
	}

	// then get all the rows, sending them as we reach a decent packet size
	// start with a pre-allocated array of 256 rows capacity
	qr := &proto.QueryResult{Rows: make([][]sqltypes.Value, 0, 256)}
	byteCount := 0
	for {
		row, err := dbc.FetchNext()
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
			// empty the rows so we start over, but we keep the
			// same capacity
			qr.Rows = qr.Rows[:0]
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

var getModeSql = "select @@global.sql_mode"

// VerifyStrict is a helper method to verify mysql is running with
// sql_mode = STRICT_TRANS_TABLES.
func (dbc *DBConnection) VerifyStrict() bool {
	qr, err := dbc.ExecuteFetch(getModeSql, 2, false)
	if err != nil {
		return false
	}
	if len(qr.Rows) == 0 {
		return false
	}
	return strings.Contains(qr.Rows[0][0].String(), "STRICT_TRANS_TABLES")
}

// NewDBConnection returns a new DBConnection based on the ConnectionParams
// and will use the provided stats to collect timing.
func NewDBConnection(info *mysql.ConnectionParams, mysqlStats *stats.Timings) (*DBConnection, error) {
	params, err := dbconfigs.MysqlParams(info)
	if err != nil {
		return nil, err
	}
	c, err := mysql.Connect(params)
	return &DBConnection{c, mysqlStats}, err
}
