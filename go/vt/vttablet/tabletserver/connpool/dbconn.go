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

package connpool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const defaultKillTimeout = 5 * time.Second

// Conn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type Conn struct {
	conn    *dbconnpool.DBConnection
	setting *smartconnpool.Setting

	env     tabletenv.Env
	dbaPool *dbconnpool.ConnectionPool
	stats   *tabletenv.Stats
	current atomic.Pointer[string]

	// err will be set if a query is killed through a Kill.
	errmu sync.Mutex
	err   error

	killTimeout time.Duration
}

// NewConnection creates a new DBConn. It triggers a CheckMySQL if creation fails.
func newPooledConn(ctx context.Context, pool *Pool, appParams dbconfigs.Connector) (*Conn, error) {
	start := time.Now()
	defer pool.env.Stats().MySQLTimings.Record("Connect", start)

	c, err := dbconnpool.NewDBConnection(ctx, appParams)
	if err != nil {
		pool.env.Stats().MySQLTimings.Record("ConnectError", start)
		pool.env.CheckMySQL()
		return nil, err
	}
	db := &Conn{
		conn:        c,
		env:         pool.env,
		stats:       pool.env.Stats(),
		dbaPool:     pool.dbaPool,
		killTimeout: defaultKillTimeout,
	}
	return db, nil
}

// NewConn creates a new Conn without a pool.
func NewConn(ctx context.Context, params dbconfigs.Connector, dbaPool *dbconnpool.ConnectionPool, setting *smartconnpool.Setting, env tabletenv.Env) (*Conn, error) {
	c, err := dbconnpool.NewDBConnection(ctx, params)
	if err != nil {
		return nil, err
	}
	dbconn := &Conn{
		conn:        c,
		dbaPool:     dbaPool,
		stats:       tabletenv.NewStats(servenv.NewExporter("Temp", "Tablet")),
		env:         env,
		killTimeout: defaultKillTimeout,
	}
	if setting == nil {
		return dbconn, nil
	}
	if err = dbconn.ApplySetting(ctx, setting); err != nil {
		dbconn.Close()
		return nil, err
	}
	return dbconn, nil
}

// Err returns an error if there was a client initiated error
// like a query kill.
func (dbc *Conn) Err() error {
	dbc.errmu.Lock()
	defer dbc.errmu.Unlock()
	return dbc.err
}

// Exec executes the specified query. If there is a connection error, it will reconnect
// and retry. A failed reconnect will trigger a CheckMySQL.
func (dbc *Conn) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	span, ctx := trace.NewSpan(ctx, "DBConn.Exec")
	defer span.Finish()

	for attempt := 1; attempt <= 2; attempt++ {
		r, err := dbc.execOnce(ctx, query, maxrows, wantfields)
		switch {
		case err == nil:
			// Success.
			return r, nil
		case sqlerror.IsConnLostDuringQuery(err):
			// Query probably killed. Don't retry.
			return nil, err
		case !sqlerror.IsConnErr(err):
			// Not a connection error. Don't retry.
			return nil, err
		case attempt == 2:
			// Reached the retry limit.
			return nil, err
		}

		// Conn error. Retry if context has not expired.
		select {
		case <-ctx.Done():
			return nil, err
		default:
		}

		if reconnectErr := dbc.Reconnect(ctx); reconnectErr != nil {
			dbc.env.CheckMySQL()
			// Return the error of the reconnect and not the original connection error.
			return nil, reconnectErr
		}

		// Reconnect succeeded. Retry query at second attempt.
	}
	panic("unreachable")
}

func (dbc *Conn) execOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	dbc.current.Store(&query)
	defer dbc.current.Store(nil)

	// Check if the context is already past its deadline before
	// trying to execute the query.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%v before execution started", err)
	}

	now := time.Now()
	defer dbc.stats.MySQLTimings.Record("Exec", now)

	type execResult struct {
		result *sqltypes.Result
		err    error
	}

	ch := make(chan execResult)
	go func() {
		result, err := dbc.conn.ExecuteFetch(query, maxrows, wantfields)
		ch <- execResult{result, err}
	}()

	select {
	case <-ctx.Done():
		killCtx, cancel := context.WithTimeout(context.Background(), dbc.killTimeout)
		defer cancel()

		_ = dbc.KillWithContext(killCtx, ctx.Err().Error(), time.Since(now))
		return nil, dbc.Err()
	case r := <-ch:
		if dbcErr := dbc.Err(); dbcErr != nil {
			return nil, dbcErr
		}
		return r.result, r.err
	}
}

// ExecOnce executes the specified query, but does not retry on connection errors.
func (dbc *Conn) ExecOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	return dbc.execOnce(ctx, query, maxrows, wantfields)
}

// FetchNext returns the next result set.
func (dbc *Conn) FetchNext(ctx context.Context, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	// Check if the context is already past its deadline before
	// trying to fetch the next result.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%v before reading next result set", err)
	}
	res, _, _, err := dbc.conn.ReadQueryResult(maxrows, wantfields)
	if err != nil {
		return nil, err
	}
	return res, err

}

// Stream executes the query and streams the results.
func (dbc *Conn) Stream(ctx context.Context, query string, callback func(*sqltypes.Result) error, alloc func() *sqltypes.Result, streamBufferSize int, includedFields querypb.ExecuteOptions_IncludedFields) error {
	span, ctx := trace.NewSpan(ctx, "DBConn.Stream")
	trace.AnnotateSQL(span, sqlparser.Preview(query))
	defer span.Finish()

	resultSent := false
	for attempt := 1; attempt <= 2; attempt++ {
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
			alloc,
			streamBufferSize,
		)
		switch {
		case err == nil:
			// Success.
			return nil
		case sqlerror.IsConnLostDuringQuery(err):
			// Query probably killed. Don't retry.
			return err
		case !sqlerror.IsConnErr(err):
			// Not a connection error. Don't retry.
			return err
		case attempt == 2:
			// Reached the retry limit.
			return err
		case resultSent:
			// Don't retry if streaming has started.
			return err
		}

		// Conn error. Retry if context has not expired.
		if ctx.Err() != nil {
			return err
		}
		if reconnectErr := dbc.Reconnect(ctx); reconnectErr != nil {
			dbc.env.CheckMySQL()
			// Return the error of the reconnect and not the original connection error.
			return reconnectErr
		}
	}
	panic("unreachable")
}

func (dbc *Conn) streamOnce(ctx context.Context, query string, callback func(*sqltypes.Result) error, alloc func() *sqltypes.Result, streamBufferSize int) error {
	dbc.current.Store(&query)
	defer dbc.current.Store(nil)

	now := time.Now()
	defer dbc.stats.MySQLTimings.Record("ExecStream", now)

	ch := make(chan error)
	go func() {
		ch <- dbc.conn.ExecuteStreamFetch(query, callback, alloc, streamBufferSize)
	}()

	select {
	case <-ctx.Done():
		killCtx, cancel := context.WithTimeout(context.Background(), dbc.killTimeout)
		defer cancel()

		_ = dbc.KillWithContext(killCtx, ctx.Err().Error(), time.Since(now))
		return dbc.Err()
	case err := <-ch:
		if dbcErr := dbc.Err(); dbcErr != nil {
			return dbcErr
		}
		return err
	}
}

// StreamOnce executes the query and streams the results. But, does not retry on connection errors.
func (dbc *Conn) StreamOnce(ctx context.Context, query string, callback func(*sqltypes.Result) error, alloc func() *sqltypes.Result, streamBufferSize int, includedFields querypb.ExecuteOptions_IncludedFields) error {
	resultSent := false
	return dbc.streamOnce(
		ctx,
		query,
		func(r *sqltypes.Result) error {
			if !resultSent {
				resultSent = true
				r = r.StripMetadata(includedFields)
			}
			return callback(r)
		},
		alloc,
		streamBufferSize,
	)
}

var (
	getModeSQL    = "select @@global.sql_mode"
	getAutocommit = "select @@autocommit"
	getAutoIsNull = "select @@sql_auto_is_null"
)

// VerifyMode is a helper method to verify mysql is running with
// sql_mode = STRICT_TRANS_TABLES or STRICT_ALL_TABLES and autocommit=ON.
func (dbc *Conn) VerifyMode(strictTransTables bool) error {
	if strictTransTables {
		qr, err := dbc.conn.ExecuteFetch(getModeSQL, 2, false)
		if err != nil {
			return err
		}
		if len(qr.Rows) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "incorrect rowcount received for %s: %d", getModeSQL, len(qr.Rows))
		}
		sqlMode := qr.Rows[0][0].ToString()
		if !(strings.Contains(sqlMode, "STRICT_TRANS_TABLES") || strings.Contains(sqlMode, "STRICT_ALL_TABLES")) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "require sql_mode to be STRICT_TRANS_TABLES or STRICT_ALL_TABLES: got '%s'", qr.Rows[0][0].ToString())
		}
	}
	qr, err := dbc.conn.ExecuteFetch(getAutocommit, 2, false)
	if err != nil {
		return err
	}
	if len(qr.Rows) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "incorrect rowcount received for %s: %d", getAutocommit, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].ToString(), "1") {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "require autocommit to be 1: got %s", qr.Rows[0][0].ToString())
	}
	qr, err = dbc.conn.ExecuteFetch(getAutoIsNull, 2, false)
	if err != nil {
		return err
	}
	if len(qr.Rows) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "incorrect rowcount received for %s: %d", getAutoIsNull, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].ToString(), "0") {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "require sql_auto_is_null to be 0: got %s", qr.Rows[0][0].ToString())
	}
	return nil
}

// Close closes the DBConn.
func (dbc *Conn) Close() {
	dbc.conn.Close()
}

// ApplySetting implements the pools.Resource interface.
func (dbc *Conn) ApplySetting(ctx context.Context, setting *smartconnpool.Setting) error {
	if _, err := dbc.execOnce(ctx, setting.ApplyQuery(), 1, false); err != nil {
		return err
	}
	dbc.setting = setting
	return nil
}

// ResetSetting implements the pools.Resource interface.
func (dbc *Conn) ResetSetting(ctx context.Context) error {
	if _, err := dbc.execOnce(ctx, dbc.setting.ResetQuery(), 1, false); err != nil {
		return err
	}
	dbc.setting = nil
	return nil
}

func (dbc *Conn) Setting() *smartconnpool.Setting {
	return dbc.setting
}

// IsClosed returns true if DBConn is closed.
func (dbc *Conn) IsClosed() bool {
	return dbc.conn.IsClosed()
}

// Kill wraps KillWithContext using context.Background.
func (dbc *Conn) Kill(reason string, elapsed time.Duration) error {
	return dbc.KillWithContext(context.Background(), reason, elapsed)
}

// KillWithContext kills the currently executing query both on MySQL side
// and on the connection side. If no query is executing, it's a no-op.
// Kill will also not kill a query more than once.
func (dbc *Conn) KillWithContext(ctx context.Context, reason string, elapsed time.Duration) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	dbc.stats.KillCounters.Add("Queries", 1)
	log.Infof("Due to %s, elapsed time: %v, killing query ID %v %s", reason, elapsed, dbc.conn.ID(), dbc.CurrentForLogging())

	// Client side action. Set error and close connection.
	dbc.errmu.Lock()
	dbc.err = vterrors.Errorf(vtrpcpb.Code_CANCELED, "(errno 2013) due to %s, elapsed time: %v, killing query ID %v", reason, elapsed, dbc.conn.ID())
	dbc.errmu.Unlock()
	dbc.conn.Close()

	// Server side action. Kill the session.
	killConn, err := dbc.dbaPool.Get(ctx)
	if err != nil {
		log.Warningf("Failed to get conn from dba pool: %v", err)
		return err
	}
	defer killConn.Recycle()

	ch := make(chan error)
	sql := fmt.Sprintf("kill %d", dbc.conn.ID())
	go func() {
		_, err := killConn.Conn.ExecuteFetch(sql, -1, false)
		ch <- err
	}()

	select {
	case <-ctx.Done():
		killConn.Close()

		dbc.stats.InternalErrors.Add("HungQuery", 1)
		log.Warningf("Query may be hung: %s", dbc.CurrentForLogging())

		return context.Cause(ctx)
	case err := <-ch:
		if err != nil {
			log.Errorf("Could not kill query ID %v %s: %v", dbc.conn.ID(), dbc.CurrentForLogging(), err)
			return err
		}
		return nil
	}
}

// Current returns the currently executing query.
func (dbc *Conn) Current() string {
	if q := dbc.current.Load(); q != nil {
		return *q
	}
	return ""
}

// ID returns the connection id.
func (dbc *Conn) ID() int64 {
	return dbc.conn.ID()
}

// BaseShowTables returns a query that shows tables
func (dbc *Conn) BaseShowTables() string {
	return dbc.conn.BaseShowTables()
}

// BaseShowTablesWithSizes returns a query that shows tables and their sizes
func (dbc *Conn) BaseShowTablesWithSizes() string {
	return dbc.conn.BaseShowTablesWithSizes()
}

func (dbc *Conn) ConnCheck(ctx context.Context) error {
	if err := dbc.conn.ConnCheck(); err != nil {
		return dbc.Reconnect(ctx)
	}
	return nil
}

func (dbc *Conn) Reconnect(ctx context.Context) error {
	err := dbc.conn.Reconnect(ctx)
	if err != nil {
		return err
	}
	if dbc.setting != nil {
		err = dbc.applySameSetting(ctx)
		if err != nil {
			return err
		}
	}
	dbc.errmu.Lock()
	dbc.err = nil
	dbc.errmu.Unlock()
	return nil
}

// CurrentForLogging applies transformations to the query making it suitable to log.
// It applies sanitization rules based on tablet settings and limits the max length of
// queries.
func (dbc *Conn) CurrentForLogging() string {
	var queryToLog string
	if dbc.env != nil && dbc.env.Config() != nil && !dbc.env.Config().SanitizeLogMessages {
		queryToLog = dbc.Current()
	} else {
		queryToLog, _ = dbc.env.Environment().Parser().RedactSQLQuery(dbc.Current())
	}
	return dbc.env.Environment().Parser().TruncateForLog(queryToLog)
}

func (dbc *Conn) applySameSetting(ctx context.Context) (err error) {
	_, err = dbc.execOnce(ctx, dbc.setting.ApplyQuery(), 1, false)
	return
}
