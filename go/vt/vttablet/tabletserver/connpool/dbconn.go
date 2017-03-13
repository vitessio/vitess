// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package connpool

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"golang.org/x/net/context"
)

// DBConn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type DBConn struct {
	conn    *dbconnpool.DBConnection
	info    *sqldb.ConnParams
	pool    *Pool
	current sync2.AtomicString
}

// NewDBConn creates a new DBConn. It triggers a CheckMySQL if creation fails.
func NewDBConn(
	cp *Pool,
	appParams,
	dbaParams *sqldb.ConnParams) (*DBConn, error) {
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
		case !mysqlconn.IsConnErr(err):
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
		case !mysqlconn.IsConnErr(err) || resultSent || attempt == 2:
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

// VerifyMode returns an error if the connection mode is incorrect.
func (dbc *DBConn) VerifyMode() error {
	return dbc.conn.VerifyMode()
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
	if dbc.conn.IsClosed() {
		dbc.pool.Put(nil)
	} else {
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
