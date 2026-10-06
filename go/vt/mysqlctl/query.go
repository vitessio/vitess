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

package mysqlctl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
)

// getPoolReconnect gets a connection from a pool, tests it, and reconnects if
// the connection is lost.
func getPoolReconnect(ctx context.Context, pool *dbconnpool.ConnectionPool) (*dbconnpool.PooledDBConnection, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return conn, err
	}
	// Run a test query to see if this connection is still good.
	if _, err := conn.Conn.ExecuteFetch("SELECT 1", 1, false); err != nil {
		// If we get a connection error, try to reconnect.
		if sqlErr, ok := err.(*sqlerror.SQLError); ok && (sqlErr.Number() == sqlerror.CRServerGone || sqlErr.Number() == sqlerror.CRServerLost) {
			if err := conn.Conn.Reconnect(ctx); err != nil {
				conn.Recycle()
				return nil, err
			}
			return conn, nil
		}
		conn.Recycle()
		return nil, err
	}
	return conn, nil
}

// ExecuteSuperQuery allows the user to execute a query as a super user.
func (mysqld *Mysqld) ExecuteSuperQuery(ctx context.Context, query string) error {
	return mysqld.ExecuteSuperQueryList(ctx, []string{query})
}

// ExecuteSuperQueryList alows the user to execute queries as a super user.
func (mysqld *Mysqld) ExecuteSuperQueryList(ctx context.Context, queryList []string) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, queryList)
}

// comSetOption describes the capability exchange in the log line that says
// what a timeout killed.
const comSetOption = "COM_SET_OPTION"

// killGraceTimeout is how long a killed exchange is given to come back on its
// own before the connection is closed to interrupt it. A variable so that tests
// do not have to wait it out.
var killGraceTimeout = 5 * time.Second

// ExecuteSuperQueryListMulti is ExecuteSuperQueryList for queries that were
// written by an operator, where a single entry may hold several statements
// separated by a semicolon.
//
// The connection is discarded afterwards instead of returning to the pool, like
// ExecuteSuperQueryListTainted does: it can send a batch, which nothing else
// drawing on the pool expects, and operator-supplied SQL may have changed
// session state (e.g. sql_mode) that must not leak into pooled connections.
// MySQL parses the statements, so an entry holding a compound statement such as
// CREATE PROCEDURE, whose body carries semicolons of its own, means what it
// says.
func (mysqld *Mysqld) ExecuteSuperQueryListMulti(ctx context.Context, queryList []string) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	// A closed connection is discarded upon Recycle rather than reused.
	defer conn.Recycle()
	defer conn.Close()

	// The exchange is a blocking write and read of its own, so it is bounded the
	// way the queries below are: a server that takes the command and never
	// answers must not outlast the caller that is waiting on this.
	if err := mysqld.executeWithContext(ctx, conn, comSetOption, func() error {
		return conn.Conn.SetMultiStatements(true)
	}); err != nil {
		return vterrors.Wrapf(err, "failed to enable multi statement support")
	}

	return executeQueryList(queryList, "ExecuteFetchMultiDrain", func(query string) error {
		return mysqld.executeWithContext(ctx, conn, query, func() error {
			return conn.Conn.ExecuteFetchMultiDrain(query)
		})
	})
}

// ExecuteSuperQueryListTainted executes queries as a super user like
// ExecuteSuperQueryList, but discards the connection afterwards instead of
// returning it to the pool. Use it for operator-supplied SQL, whose session
// state changes (e.g. sql_mode) must not leak into pooled connections.
func (mysqld *Mysqld) ExecuteSuperQueryListTainted(ctx context.Context, queryList []string) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	// A closed connection is discarded upon Recycle rather than reused.
	defer conn.Recycle()
	defer conn.Close()

	return mysqld.executeSuperQueryListConn(ctx, conn, queryList)
}

// executeQueryList runs each query through exec, stopping at the first failure.
// name says what exec calls, for the error that failure returns.
func executeQueryList(queryList []string, name string, exec func(query string) error) error {
	const LogQueryLengthLimit = 200
	for _, query := range queryList {
		log.Infof("exec %s", limitString(redactPassword(query), LogQueryLengthLimit))
		if err := exec(query); err != nil {
			log.Errorf("%s(%v) failed: %v", name, redactPassword(query), redactPassword(err.Error()))
			return fmt.Errorf("%s(%v) failed: %v", name, redactPassword(query), redactPassword(err.Error()))
		}
	}
	return nil
}

func limitString(s string, limit int) string {
	if len(s) > limit {
		return s[:limit]
	}
	return s
}

func (mysqld *Mysqld) executeSuperQueryListConn(ctx context.Context, conn *dbconnpool.PooledDBConnection, queryList []string) error {
	return executeQueryList(queryList, "ExecuteFetch", func(query string) error {
		_, err := mysqld.executeFetchContext(ctx, conn, query, 10000, false)
		return err
	})
}

// FetchSuperQuery returns the results of executing a query as a super user.
func (mysqld *Mysqld) FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error) {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Recycle()
	qr, err := mysqld.executeFetchContext(ctx, conn, query, 10000, true)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// executeFetchContext calls ExecuteFetch() on the given connection,
// while respecting Context deadline and cancellation.
func (mysqld *Mysqld) executeFetchContext(ctx context.Context, conn *dbconnpool.PooledDBConnection, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	err := mysqld.executeWithContext(ctx, conn, query, func() error {
		var executeErr error
		qr, executeErr = conn.Conn.ExecuteFetch(query, maxrows, wantfields)
		return executeErr
	})
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// executeWithContext runs exec on the given connection while respecting Context
// deadline and cancellation, killing the connection to cancel the query if the
// context is done first. query is only used to describe what was killed.
func (mysqld *Mysqld) executeWithContext(ctx context.Context, conn *dbconnpool.PooledDBConnection, query string, exec func() error) error {
	// Fast fail if context is done.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Execute asynchronously so we can select on both it and the context.
	var executeErr error
	done := make(chan struct{})
	go func() {
		defer close(done)

		executeErr = exec()
	}()

	// Wait for either the query or the context to be done.
	select {
	case <-done:
		return executeErr
	case <-ctx.Done():
		// If both are done already, we may end up here anyway because select
		// chooses among multiple ready channels pseudorandomly.
		// Check the done channel and prefer that one if it's ready.
		select {
		case <-done:
			return executeErr
		default:
		}

		// The context expired or was canceled.
		// Try to kill the connection to effectively cancel the query.
		connID := conn.Conn.ID()
		log.Infof("Mysqld.executeWithContext(): killing connID %v due to timeout of query: %v", connID, query)
		if killErr := mysqld.killConnection(connID); killErr != nil {
			// Log it, but go ahead and wait for the query anyway.
			log.Warningf("Mysqld.executeWithContext(): failed to kill connID %v: %v", connID, killErr)
		}
		// Waiting for exec() to come back is not enough on its own: a kill that
		// was not delivered leaves a read the server never answers, and nothing
		// else interrupts that. Closing our end of the connection does, so that
		// is what bounds this in the end.
		select {
		case <-done:
		case <-time.After(killGraceTimeout):
			log.Warningf("Mysqld.executeWithContext(): connID %v did not come back after the kill, closing the connection to interrupt it", connID)
			conn.Close()
			<-done
		}
		// Close the connection. Upon Recycle() it will be thrown out.
		conn.Close()
		// The query may have succeeded before we tried to kill it.
		// If it had returned because we canceled it, then executeErr would be an
		// error like "MySQL has gone away".
		if executeErr == nil {
			return nil
		}
		return ctx.Err()
	}
}

// killConnection issues a MySQL KILL command for the given connection ID.
func (mysqld *Mysqld) killConnection(connID int64) error {
	// There's no other interface that both types of connection implement.
	// We only care about one method anyway.
	var killConn interface {
		ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
	}

	// Get another connection with which to kill.
	// Use background context because the caller's context is likely expired,
	// which is the reason we're being asked to kill the connection.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if poolConn, connErr := getPoolReconnect(ctx, mysqld.dbaPool); connErr == nil {
		// We got a pool connection.
		defer poolConn.Recycle()
		killConn = poolConn.Conn
	} else {
		// We couldn't get a connection from the pool.
		// It might be because the connection pool is exhausted,
		// because some connections need to be killed!
		// Try to open a new connection without the pool.
		conn, connErr := mysqld.GetDbaConnection(ctx)
		if connErr != nil {
			return connErr
		}
		defer conn.Close()
		killConn = conn
	}

	_, err := killConn.ExecuteFetch(fmt.Sprintf("kill %d", connID), 10000, false)
	return err
}

// fetchVariables returns a map from MySQL variable names to variable value
// for variables that match the given pattern.
func (mysqld *Mysqld) fetchVariables(ctx context.Context, pattern string) (map[string]string, error) {
	query := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", pattern)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(qr.Fields) != 2 {
		return nil, fmt.Errorf("query %#v returned %d columns, expected 2", query, len(qr.Fields))
	}
	varMap := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		varMap[row[0].ToString()] = row[1].ToString()
	}
	return varMap, nil
}

// fetchStatuses returns a map from MySQL status names to status value
// for variables that match the given pattern.
func (mysqld *Mysqld) fetchStatuses(ctx context.Context, pattern string) (map[string]string, error) {
	query := fmt.Sprintf("SHOW STATUS LIKE '%s'", pattern)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(qr.Fields) != 2 {
		return nil, fmt.Errorf("query %#v returned %d columns, expected 2", query, len(qr.Fields))
	}
	varMap := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		varMap[row[0].ToString()] = row[1].ToString()
	}
	return varMap, nil
}

// ExecuteSuperQuery allows the user to execute a query as a super user.
func (mysqld *Mysqld) AcquireGlobalReadLock(ctx context.Context) error {
	if mysqld.lockConn != nil {
		return errors.New("lock already acquired")
	}

	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}

	err = mysqld.executeSuperQueryListConn(ctx, conn, []string{"FLUSH TABLES WITH READ LOCK"})
	if err != nil {
		conn.Recycle()
		return err
	}

	mysqld.lockConn = conn
	return nil
}

func (mysqld *Mysqld) ReleaseGlobalReadLock(ctx context.Context) error {
	if mysqld.lockConn == nil {
		return errors.New("no read locks acquired yet")
	}

	err := mysqld.executeSuperQueryListConn(ctx, mysqld.lockConn, []string{"UNLOCK TABLES"})
	if err != nil {
		return err
	}

	mysqld.lockConn.Recycle()
	mysqld.lockConn = nil
	return nil
}

const (
	sourcePasswordStart = "  SOURCE_PASSWORD = '"
	sourcePasswordEnd   = "',\n"
	masterPasswordStart = "  MASTER_PASSWORD = '"
	masterPasswordEnd   = "',\n"
	passwordStart       = " PASSWORD = '"
	passwordEnd         = "'"
)

func redactPassword(input string) string {
	i := strings.Index(input, sourcePasswordStart)
	// We have primary password in the query, try to redact it
	if i != -1 {
		j := strings.Index(input[i+len(sourcePasswordStart):], sourcePasswordEnd)
		if j == -1 {
			return input
		}
		input = input[:i+len(sourcePasswordStart)] + strings.Repeat("*", 4) + input[i+len(masterPasswordStart)+j:]
	}

	i = strings.Index(input, masterPasswordStart)
	// We have primary password in the query, try to redact it
	if i != -1 {
		j := strings.Index(input[i+len(masterPasswordStart):], masterPasswordEnd)
		if j == -1 {
			return input
		}
		input = input[:i+len(masterPasswordStart)] + strings.Repeat("*", 4) + input[i+len(masterPasswordStart)+j:]
	}
	// We also check if we have any password keyword in the query
	i = strings.Index(input, passwordStart)
	if i == -1 {
		return input
	}
	j := strings.Index(input[i+len(passwordStart):], passwordEnd)
	if j == -1 {
		return input
	}
	return input[:i+len(passwordStart)] + strings.Repeat("*", 4) + input[i+len(passwordStart)+j:]
}
