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
	"fmt"
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
