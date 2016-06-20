// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"golang.org/x/net/context"
)

// DBConn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type DBConn struct {
	conn              *dbconnpool.DBConnection
	info              *sqldb.ConnParams
	pool              *ConnPool
	queryServiceStats *QueryServiceStats
	current           sync2.AtomicString
}

// NewDBConn creates a new DBConn. It triggers a CheckMySQL if creation fails.
func NewDBConn(
	cp *ConnPool,
	appParams,
	dbaParams *sqldb.ConnParams,
	qStats *QueryServiceStats) (*DBConn, error) {
	c, err := dbconnpool.NewDBConnection(appParams, qStats.MySQLStats)
	if err != nil {
		cp.checker.CheckMySQL()
		return nil, err
	}
	return &DBConn{
		conn:              c,
		info:              appParams,
		pool:              cp,
		queryServiceStats: qStats,
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
		case !IsConnErr(err):
			// MySQL error that isn't due to a connection issue
			return nil, NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err)
		case attempt == 2:
			// If the MySQL connection is bad, we assume that there is nothing wrong with
			// the query itself, and retrying it might succeed. The MySQL connection might
			// fix itself, or the query could succeed on a different VtTablet.
			return nil, NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
		}
		err2 := dbc.reconnect()
		if err2 != nil {
			dbc.pool.checker.CheckMySQL()
			return nil, NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
		}
	}
	panic("unreachable")
}

func (dbc *DBConn) execOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	dbc.current.Set(query)
	defer dbc.current.Set("")

	done := dbc.setDeadline(ctx)
	if done != nil {
		defer close(done)
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
func (dbc *DBConn) Stream(ctx context.Context, query string, callback func(*sqltypes.Result) error, streamBufferSize int) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("DBConn.Stream")
	defer span.Finish()

	for attempt := 1; attempt <= 2; attempt++ {
		resultSent := false
		err := dbc.streamOnce(
			ctx,
			query,
			func(r *sqltypes.Result) error {
				resultSent = true
				return callback(r)
			},
			streamBufferSize,
		)
		switch {
		case err == nil:
			return nil
		case !IsConnErr(err) || resultSent || attempt == 2:
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

	done := dbc.setDeadline(ctx)
	if done != nil {
		defer close(done)
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
	dbc.queryServiceStats.KillStats.Add("Queries", 1)
	log.Infof("Due to %s, killing query %s", reason, dbc.Current())
	killConn, err := dbc.pool.dbaPool.Get(context.TODO())
	if err != nil {
		log.Warningf("Failed to get conn from dba pool: %v", err)
		// TODO(aaijazi): Find the right error code for an internal error that we don't want to retry
		return NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "Failed to get conn from dba pool: %v", err)
	}
	defer killConn.Recycle()
	sql := fmt.Sprintf("kill %d", dbc.conn.ID())
	_, err = killConn.ExecuteFetch(sql, 10000, false)
	if err != nil {
		log.Errorf("Could not kill query %s: %v", dbc.Current(), err)
		// TODO(aaijazi): Find the right error code for an internal error that we don't want to retry
		return NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "Could not kill query %s: %v", dbc.Current(), err)
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
	newConn, err := dbconnpool.NewDBConnection(dbc.info, dbc.queryServiceStats.MySQLStats)
	if err != nil {
		return err
	}
	dbc.conn = newConn
	return nil
}

func (dbc *DBConn) setDeadline(ctx context.Context) chan bool {
	if ctx.Done() == nil {
		return nil
	}
	done := make(chan bool)
	go func() {
		startTime := time.Now()
		select {
		case <-ctx.Done():
			// There is a possibility that the query returned very fast,
			// which will cause ctx to get canceled. Check for this condition.
			select {
			case <-done:
				return
			default:
			}
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
			dbc.queryServiceStats.InternalErrors.Add("HungQuery", 1)
			log.Warningf("Query may be hung: %s", dbc.Current())
		case <-done:
			return
		}
		<-done
		log.Warningf("Hung query returned")
	}()
	return done
}
