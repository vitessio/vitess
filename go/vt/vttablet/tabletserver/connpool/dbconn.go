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

package connpool

import (
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"golang.org/x/net/context"
)

// BinlogFormat is used for for specifying the binlog format.
type BinlogFormat int

// The following constants specify the possible binlog format values.
const (
	BinlogFormatStatement BinlogFormat = iota
	BinlogFormatRow
	BinlogFormatMixed
)

// DBConn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type DBConn struct {
	conn    *dbconnpool.DBConnection
	info    *mysql.ConnParams
	pool    *Pool
	current sync2.AtomicString
}

// NewDBConn creates a new DBConn. It triggers a CheckMySQL if creation fails.
func NewDBConn(
	cp *Pool,
	appParams *mysql.ConnParams) (*DBConn, error) {
	c, err := dbconnpool.NewDBConnection(appParams, tabletenv.MySQLStats)
	if err != nil {
		cp.checker.CheckMySQL()
		return nil, err
	}
	return &DBConn{
		conn: c,
		info: appParams,
		pool: cp,
	}, nil
}

// NewDBConnNoPool creates a new DBConn without a pool.
func NewDBConnNoPool(params *mysql.ConnParams) (*DBConn, error) {
	c, err := dbconnpool.NewDBConnection(params, tabletenv.MySQLStats)
	if err != nil {
		return nil, err
	}
	return &DBConn{
		conn: c,
		info: params,
		pool: nil,
	}, nil
}

// Exec executes the specified query. If there is a connection error, it will reconnect
// and retry. A failed reconnect will trigger a CheckMySQL.
func (dbc *DBConn) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("DBConn.Exec")
	defer span.Finish()

	for attempt := 1; attempt <= 2; attempt++ {
		r, err := dbc.execOnce(ctx, query, maxrows, wantfields)
		switch {
		case err == nil:
			return r, nil
		case !mysql.IsConnErr(err):
			// MySQL error that isn't due to a connection issue
			return nil, err
		case attempt == 2:
			// If the MySQL connection is bad, we assume that there is nothing wrong with
			// the query itself, and retrying it might succeed. The MySQL connection might
			// fix itself, or the query could succeed on a different VtTablet.
			return nil, err
		}

		// Connection error. Try to reconnect.
		if reconnectErr := dbc.reconnect(); reconnectErr != nil {
			// Reconnect failed.
			dbc.pool.checker.CheckMySQL()
			// Return the error of the reconnect and not the original connection error.
			// NOTE: We return a tryable error code here.
			return nil, reconnectErr
		}

		// Reconnect succeeded. Retry query at second attempt.
	}
	panic("unreachable")
}

func (dbc *DBConn) execOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	dbc.current.Set(query)
	defer dbc.current.Set("")

	done, wg := dbc.setDeadline(ctx)
	if done != nil {
		defer func() {
			close(done)
			wg.Wait()
		}()
	}
	// Uncomment this line for manual testing.
	// defer time.Sleep(20 * time.Second)
	return dbc.conn.ExecuteFetch(query, maxrows, wantfields)
}

// ExecOnce executes the specified query, but does not retry on connection errors.
func (dbc *DBConn) ExecOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	return dbc.execOnce(ctx, query, maxrows, wantfields)
}

// Stream executes the query and streams the results.
func (dbc *DBConn) Stream(ctx context.Context, query string, callback func(*sqltypes.Result) error, streamBufferSize int, includedFields querypb.ExecuteOptions_IncludedFields) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("DBConn.Stream")
	defer span.Finish()

	for attempt := 1; attempt <= 2; attempt++ {
		resultSent := false
		err := dbc.streamOnce(
			ctx,
			query,
			func(r *sqltypes.Result) error {
				if !resultSent {
					resultSent = true
					r = r.StripMetadata(includedFields)
				}
				return callback(r)
			},
			streamBufferSize,
		)
		switch {
		case err == nil:
			return nil
		case !mysql.IsConnErr(err) || resultSent || attempt == 2:
			// MySQL error that isn't due to a connection issue
			return err
		}
		err2 := dbc.reconnect()
		if err2 != nil {
			dbc.pool.checker.CheckMySQL()
			return err
		}
	}
	panic("unreachable")
}

func (dbc *DBConn) streamOnce(ctx context.Context, query string, callback func(*sqltypes.Result) error, streamBufferSize int) error {
	dbc.current.Set(query)
	defer dbc.current.Set("")

	done, wg := dbc.setDeadline(ctx)
	if done != nil {
		defer func() {
			close(done)
			wg.Wait()
		}()
	}
	return dbc.conn.ExecuteStreamFetch(query, callback, streamBufferSize)
}

var (
	getModeSQL    = "select @@global.sql_mode"
	getAutocommit = "select @@autocommit"
	showBinlog    = "show variables like 'binlog_format'"
)

// VerifyMode is a helper method to verify mysql is running with
// sql_mode = STRICT_TRANS_TABLES and autocommit=ON. It also returns
// the current binlog format.
func (dbc *DBConn) VerifyMode(strictTransTables bool) (BinlogFormat, error) {
	if strictTransTables {
		qr, err := dbc.conn.ExecuteFetch(getModeSQL, 2, false)
		if err != nil {
			return 0, fmt.Errorf("could not verify mode: %v", err)
		}
		if len(qr.Rows) != 1 {
			return 0, fmt.Errorf("incorrect rowcount received for %s: %d", getModeSQL, len(qr.Rows))
		}
		if !strings.Contains(qr.Rows[0][0].ToString(), "STRICT_TRANS_TABLES") {
			return 0, fmt.Errorf("require sql_mode to be STRICT_TRANS_TABLES: got '%s'", qr.Rows[0][0].ToString())
		}
	}
	qr, err := dbc.conn.ExecuteFetch(getAutocommit, 2, false)
	if err != nil {
		return 0, fmt.Errorf("could not verify mode: %v", err)
	}
	if len(qr.Rows) != 1 {
		return 0, fmt.Errorf("incorrect rowcount received for %s: %d", getAutocommit, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].ToString(), "1") {
		return 0, fmt.Errorf("require autocommit to be 1: got %s", qr.Rows[0][0].ToString())
	}
	qr, err = dbc.conn.ExecuteFetch(showBinlog, 10, false)
	if err != nil {
		return 0, fmt.Errorf("could not fetch binlog format: %v", err)
	}
	if len(qr.Rows) != 1 {
		return 0, fmt.Errorf("incorrect rowcount received for %s: %d", showBinlog, len(qr.Rows))
	}
	if len(qr.Rows[0]) != 2 {
		return 0, fmt.Errorf("incorrect column count received for %s: %d", showBinlog, len(qr.Rows[0]))
	}
	switch qr.Rows[0][1].ToString() {
	case "STATEMENT":
		return BinlogFormatStatement, nil
	case "ROW":
		return BinlogFormatRow, nil
	case "MIXED":
		return BinlogFormatMixed, nil
	}
	return 0, fmt.Errorf("unexpected binlog format for %s: %s", showBinlog, qr.Rows[0][1].ToString())
}

// Close closes the DBConn.
func (dbc *DBConn) Close() {
	dbc.conn.Close()
}

// IsClosed returns true if DBConn is closed.
func (dbc *DBConn) IsClosed() bool {
	return dbc.conn.IsClosed()
}

// Recycle returns the DBConn to the pool.
func (dbc *DBConn) Recycle() {
	switch {
	case dbc.pool == nil:
		dbc.Close()
	case dbc.conn.IsClosed():
		dbc.pool.Put(nil)
	default:
		dbc.pool.Put(dbc)
	}
}

// Kill kills the currently executing query both on MySQL side
// and on the connection side. If no query is executing, it's a no-op.
// Kill will also not kill a query more than once.
func (dbc *DBConn) Kill(reason string) error {
	tabletenv.KillStats.Add("Queries", 1)
	log.Infof("Due to %s, killing query %s", reason, dbc.Current())
	killConn, err := dbc.pool.dbaPool.Get(context.TODO())
	if err != nil {
		log.Warningf("Failed to get conn from dba pool: %v", err)
		return err
	}
	defer killConn.Recycle()
	sql := fmt.Sprintf("kill %d", dbc.conn.ID())
	_, err = killConn.ExecuteFetch(sql, 10000, false)
	if err != nil {
		log.Errorf("Could not kill query %s: %v", dbc.Current(), err)
		return err
	}
	return nil
}

// Current returns the currently executing query.
func (dbc *DBConn) Current() string {
	return dbc.current.Get()
}

// ID returns the connection id.
func (dbc *DBConn) ID() int64 {
	return dbc.conn.ID()
}

func (dbc *DBConn) reconnect() error {
	dbc.conn.Close()
	newConn, err := dbconnpool.NewDBConnection(dbc.info, tabletenv.MySQLStats)
	if err != nil {
		return err
	}
	dbc.conn = newConn
	return nil
}

// setDeadline starts a goroutine that will kill the currently executing query
// if the deadline is exceeded. It returns a channel and a waitgroup. After the
// query is done executing, the caller is required to close the done channel
// and wait for the waitgroup to make sure that the necessary cleanup is done.
func (dbc *DBConn) setDeadline(ctx context.Context) (chan bool, *sync.WaitGroup) {
	if ctx.Done() == nil {
		return nil, nil
	}
	done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		select {
		case <-ctx.Done():
			dbc.Kill(ctx.Err().Error())
		case <-done:
			return
		}
		elapsed := time.Now().Sub(startTime)

		// Give 2x the elapsed time and some buffer as grace period
		// for the query to get killed.
		tmr2 := time.NewTimer(2*elapsed + 5*time.Second)
		defer tmr2.Stop()
		select {
		case <-tmr2.C:
			tabletenv.InternalErrors.Add("HungQuery", 1)
			log.Warningf("Query may be hung: %s", dbc.Current())
		case <-done:
			return
		}
		<-done
		log.Warningf("Hung query returned")
	}()
	return done, &wg
}
