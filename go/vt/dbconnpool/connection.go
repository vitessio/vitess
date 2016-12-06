// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbconnpool

import (
	"fmt"
	"strings"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
)

// DBConnection re-exposes sqldb.Conn with some wrapping to implement
// most of PoolConnection interface, except Recycle. That way it can be used
// by itself. (Recycle needs to know about the Pool).
type DBConnection struct {
	sqldb.Conn
	mysqlStats *stats.Timings
}

func (dbc *DBConnection) handleError(err error) {
	if sqlErr, ok := err.(*sqldb.SQLError); ok {
		if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
			dbc.Close()
		}
		if sqlErr.Number() == 1317 { // Query was interrupted
			dbc.Close()
		}
	}
}

// ExecuteFetch is part of PoolConnection interface.
func (dbc *DBConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	defer dbc.mysqlStats.Record("Exec", time.Now())
	mqr, err := dbc.Conn.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		dbc.handleError(err)
		return nil, err
	}
	return mqr, nil
}

// ExecuteStreamFetch is part of PoolConnection interface.
func (dbc *DBConnection) ExecuteStreamFetch(query string, callback func(*sqltypes.Result) error, streamBufferSize int) error {
	defer dbc.mysqlStats.Record("ExecStream", time.Now())

	err := dbc.Conn.ExecuteStreamFetch(query)
	if err != nil {
		dbc.handleError(err)
		return err
	}
	defer dbc.CloseResult()

	// first call the callback with the fields
	flds, err := dbc.Fields()
	if err != nil {
		return err
	}
	err = callback(&sqltypes.Result{Fields: flds})
	if err != nil {
		return fmt.Errorf("stream send error: %v", err)
	}

	// then get all the rows, sending them as we reach a decent packet size
	// start with a pre-allocated array of 256 rows capacity
	qr := &sqltypes.Result{Rows: make([][]sqltypes.Value, 0, 256)}
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
			byteCount += s.Len()
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

var (
	getModeSQL    = "select @@global.sql_mode"
	getAutocommit = "select @@autocommit"
)

// VerifyMode is a helper method to verify mysql is running with
// sql_mode = STRICT_TRANS_TABLES and autocommit=ON.
func (dbc *DBConnection) VerifyMode() error {
	qr, err := dbc.ExecuteFetch(getModeSQL, 2, false)
	if err != nil {
		return fmt.Errorf("could not verify mode: %v", err)
	}
	if len(qr.Rows) != 1 {
		return fmt.Errorf("incorrect rowcount received for %s: %d", getModeSQL, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].String(), "STRICT_TRANS_TABLES") {
		return fmt.Errorf("require sql_mode to be STRICT_TRANS_TABLES: got %s", qr.Rows[0][0].String())
	}
	qr, err = dbc.ExecuteFetch(getAutocommit, 2, false)
	if err != nil {
		return fmt.Errorf("could not verify mode: %v", err)
	}
	if len(qr.Rows) != 1 {
		return fmt.Errorf("incorrect rowcount received for %s: %d", getAutocommit, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].String(), "1") {
		return fmt.Errorf("require autocommit to be 1: got %s", qr.Rows[0][0].String())
	}
	return nil
}

// NewDBConnection returns a new DBConnection based on the ConnParams
// and will use the provided stats to collect timing.
func NewDBConnection(info *sqldb.ConnParams, mysqlStats *stats.Timings) (*DBConnection, error) {
	params, err := dbconfigs.WithCredentials(info)
	if err != nil {
		return nil, err
	}
	c, err := sqldb.Connect(params)
	return &DBConnection{c, mysqlStats}, err
}
