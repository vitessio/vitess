/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// DBConnection re-exposes mysql.Conn with some wrapping to implement
// most of PoolConnection interface, except Recycle. That way it can be used
// by itself. (Recycle needs to know about the Pool).
type DBConnection struct {
	*mysql.Conn
}

// NewDBConnection returns a new DBConnection based on the ConnParams
// and will use the provided stats to collect timing.
func NewDBConnection(ctx context.Context, info dbconfigs.Connector) (*DBConnection, error) {
	c, err := info.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &DBConnection{Conn: c}, nil
}

// ExecuteFetch overwrites mysql.Conn.ExecuteFetch.
func (dbc *DBConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	mqr, err := dbc.Conn.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		dbc.handleError(err)
		return nil, err
	}
	return mqr, nil
}

// ExecuteStreamFetch overwrites mysql.Conn.ExecuteStreamFetch.
func (dbc *DBConnection) ExecuteStreamFetch(query string, callback func(*sqltypes.Result) error, alloc func() *sqltypes.Result, streamBufferSize int) error {

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
	qr := alloc()
	byteCount := 0
	for {
		row, result, err := dbc.FetchNext(nil)
		if err != nil {
			dbc.handleError(err)
			return err
		}
		if result != nil {
			qr.Info = result.Info
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

			qr = alloc()
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

func (dbc *DBConnection) handleError(err error) {
	if mysql.IsConnErr(err) {
		dbc.Close()
	}
}
