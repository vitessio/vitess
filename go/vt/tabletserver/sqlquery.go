// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"
)

// Allowed state transitions:
// StateNotServing -> StateInitializing -> StateServing/StateNotServing,
// StateServing -> StateShuttingTx
// StateShuttingTx -> StateShuttingQueries
// StateShuttingQueries -> StateNotServing
const (
	// StateNotServing is the not serving state.
	StateNotServing = iota
	// StateInitializing is the initializing state.
	// This is a transient state. It's only informational.
	StateInitializing
	// StateServing is the serving state.
	// All operations are allowed.
	StateServing
	// StateShuttingTx means that the query service is shutting
	// down and has disallowed new transactions.
	// New queries are still allowed as long as they
	// are part of an existing transaction. We remain in this state
	// until all existing transactions are completed.
	StateShuttingTx
	// StateShuttingQueries comes after StateShuttingTx.
	// It means that the query service has disallowed
	// new queries. We enter this state after all existing
	// transactions have completed. We remain in this
	// state until all existing queries are completed.
	// The next state after this is StateNotServing.
	StateShuttingQueries
)

// stateName names every state. The number of elements must
// match the number of states.
var stateName = []string{
	"NOT_SERVING",
	"INITIALIZING",
	"SERVING",
	"SHUTTING_TX",
	"SHUTTING_QUERIES",
}

var (
	// RPCErrorOnlyInReply is the flag to control how errors will be sent over RPCs for all queryservice implementations.
	RPCErrorOnlyInReply = flag.Bool("rpc-error-only-in-reply", false, "if true, supported RPC calls will only return errors as part of the RPC server response")
)

// SqlQuery implements the RPC interface for the query service.
type SqlQuery struct {
	config Config
	// mu is used to access state. It's also used to ensure
	// that state does not change out of StateServing or StateShuttingTx
	// while we do requests.Add.
	// At the time of shut down, once we change the state to
	// StateShuttingQueries, no new requests will be honored.
	// At this time, it's safe to perform requests.Wait outside
	// the lock. Once the wait completes, we can transition
	// to StateNotServing.
	mu       sync.Mutex
	state    int64
	requests sync.WaitGroup

	// The following variables should only be accessed within
	// the context of a startRequest-endRequest.
	qe        *QueryEngine
	sessionID int64
	dbconfig  *dbconfigs.DBConfig
}

// NewSqlQuery creates an instance of SqlQuery. Only one instance
// of SqlQuery can be created per process.
func NewSqlQuery(config Config) *SqlQuery {
	sq := &SqlQuery{
		config: config,
	}
	sq.qe = NewQueryEngine(config)
	if config.EnablePublishStats {
		stats.Publish(config.StatsPrefix+"TabletState", stats.IntFunc(func() int64 {
			sq.mu.Lock()
			state := sq.state
			sq.mu.Unlock()
			return state
		}))
		stats.Publish(config.StatsPrefix+"TabletStateName", stats.StringFunc(sq.GetState))
	}
	return sq
}

// GetState returns the name of the current SqlQuery state.
func (sq *SqlQuery) GetState() string {
	sq.mu.Lock()
	name := stateName[sq.state]
	sq.mu.Unlock()
	return name
}

// setState changes the state and logs the event.
// It requires the caller to hold a lock on mu.
func (sq *SqlQuery) setState(state int64) {
	log.Infof("SqlQuery state: %v -> %v", stateName[sq.state], stateName[state])
	sq.state = state
}

// allowQueries starts the query service.
// If the state is other than StateServing or StateNotServing, it fails.
// If allowQuery succeeds, the resulting state is StateServing.
// Otherwise, it reverts back to StateNotServing.
// While allowQuery is running, the state is set to StateInitializing.
// If waitForMysql is set to true, allowQueries will not return
// until it's able to connect to mysql.
// No other operations are allowed when allowQueries is running.
func (sq *SqlQuery) allowQueries(dbconfigs *dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld mysqlctl.MysqlDaemon) (err error) {
	sq.mu.Lock()
	if sq.state == StateServing {
		sq.mu.Unlock()
		return nil
	}
	if sq.state != StateNotServing {
		state := sq.state
		sq.mu.Unlock()
		return NewTabletError(ErrFatal, "cannot start query service, current state: %s", state)
	}
	// state is StateNotServing
	sq.setState(StateInitializing)
	sq.mu.Unlock()

	c, err := dbconnpool.NewDBConnection(&dbconfigs.App.ConnParams, sq.qe.queryServiceStats.MySQLStats)
	if err != nil {
		log.Infof("allowQueries failed: %v", err)
		sq.mu.Lock()
		sq.setState(StateNotServing)
		sq.mu.Unlock()
		return err
	}
	c.Close()

	defer func() {
		state := int64(StateServing)
		if x := recover(); x != nil {
			err = x.(*TabletError)
			log.Errorf("Could not start query service: %v", err)
			sq.qe.Close()
			state = StateNotServing
		}
		sq.mu.Lock()
		sq.setState(state)
		sq.mu.Unlock()
	}()

	sq.qe.Open(dbconfigs, schemaOverrides, mysqld)
	sq.dbconfig = &dbconfigs.App
	sq.sessionID = Rand()
	log.Infof("Session id: %d", sq.sessionID)
	return nil
}

// disallowQueries shuts down the query service if it's StateServing.
// It first transitions to StateShuttingTx, then waits for existing
// transactions to complete. During this state, no new
// transactions or queries are allowed. However, existing
// transactions can still receive queries.
// Then, it transitions to StateShuttingQueries to wait for existing
// queries to complete. In this state no new requests are allowed.
// Once all queries are done, it shuts down the query engine
// and marks the state as StateNotServing.
func (sq *SqlQuery) disallowQueries() {
	// Setup a time bomb at 10x query timeout. If this function
	// takes too long, it's better to crash.
	done := make(chan struct{})
	defer close(done)
	go func() {
		qt := sq.qe.queryTimeout.Get()
		if qt == 0 {
			return
		}
		tmr := time.NewTimer(10 * qt)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Fatal("disallowQueries took too long. Crashing")
		case <-done:
		}
	}()

	// StateServing -> StateShuttingTx
	sq.mu.Lock()
	if sq.state != StateServing {
		sq.mu.Unlock()
		return
	}
	sq.setState(StateShuttingTx)
	sq.mu.Unlock()
	sq.qe.WaitForTxEmpty()

	// StateShuttingTx -> StateShuttingQueries
	sq.mu.Lock()
	sq.setState(StateShuttingQueries)
	sq.mu.Unlock()
	// Terminate all streaming queries
	sq.qe.streamQList.TerminateAll()
	// Wait for outstanding requests to finish.
	sq.requests.Wait()

	defer func() {
		// StateShuttingQueries -> StateNotServing
		sq.mu.Lock()
		sq.setState(StateNotServing)
		sq.mu.Unlock()
	}()
	log.Infof("Stopping query service. Session id: %d", sq.sessionID)
	sq.qe.Close()
	sq.sessionID = 0
	sq.dbconfig = &dbconfigs.DBConfig{}
}

// checkMySQL returns true if we can connect to MySQL.
// The function returns false only if the query service is running
// and we're unable to make a connection.
func (sq *SqlQuery) checkMySQL() bool {
	if err := sq.startRequest(0, true, false); err != nil {
		return true
	}
	defer sq.endRequest()
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Checking MySQL, unexpected error: %v", x)
		}
	}()
	return sq.qe.CheckMySQL()
}

// GetSessionId returns a sessionInfo response if the state is StateServing.
func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	if err := sq.startRequest(0, true, false); err != nil {
		return err
	}
	defer sq.endRequest()

	if sessionParams.Keyspace != sq.dbconfig.Keyspace {
		return NewTabletError(ErrFatal, "Keyspace mismatch, expecting %v, received %v", sq.dbconfig.Keyspace, sessionParams.Keyspace)
	}
	if strings.ToLower(sessionParams.Shard) != strings.ToLower(sq.dbconfig.Shard) {
		return NewTabletError(ErrFatal, "Shard mismatch, expecting %v, received %v", sq.dbconfig.Shard, sessionParams.Shard)
	}
	sessionInfo.SessionId = sq.sessionID
	return nil
}

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (sq *SqlQuery) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	logStats := newSqlQueryStats("Begin", ctx)
	logStats.OriginalSql = "begin"
	defer handleError(&err, logStats, sq.qe.queryServiceStats)

	if err = sq.startRequest(session.SessionId, false, false); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, sq.qe.txPool.PoolTimeout())
	defer func() {
		sq.qe.queryServiceStats.QueryStats.Record("BEGIN", time.Now())
		cancel()
		sq.endRequest()
	}()

	txInfo.TransactionId = sq.qe.txPool.Begin(ctx)
	logStats.TransactionID = txInfo.TransactionId
	return nil
}

// Commit commits the specified transaction.
func (sq *SqlQuery) Commit(ctx context.Context, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Commit", ctx)
	logStats.OriginalSql = "commit"
	logStats.TransactionID = session.TransactionId
	defer handleError(&err, logStats, sq.qe.queryServiceStats)

	if err = sq.startRequest(session.SessionId, false, true); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, sq.qe.queryTimeout.Get())
	defer func() {
		sq.qe.queryServiceStats.QueryStats.Record("COMMIT", time.Now())
		cancel()
		sq.endRequest()
	}()

	sq.qe.Commit(ctx, logStats, session.TransactionId)
	return nil
}

// Rollback rollsback the specified transaction.
func (sq *SqlQuery) Rollback(ctx context.Context, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Rollback", ctx)
	logStats.OriginalSql = "rollback"
	logStats.TransactionID = session.TransactionId
	defer handleError(&err, logStats, sq.qe.queryServiceStats)

	if err = sq.startRequest(session.SessionId, false, true); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, sq.qe.queryTimeout.Get())
	defer func() {
		sq.qe.queryServiceStats.QueryStats.Record("ROLLBACK", time.Now())
		cancel()
		sq.endRequest()
	}()

	sq.qe.txPool.Rollback(ctx, session.TransactionId)
	return nil
}

// handleExecError handles panics during query execution and sets
// the supplied error return value.
func (sq *SqlQuery) handleExecError(query *proto.Query, err *error, logStats *SQLQueryStats) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic for %v:\n%v\n%s", query, x, tb.Stack(4))
			*err = NewTabletError(ErrFail, "%v: uncaught panic for %v", x, query)
			sq.qe.queryServiceStats.InternalErrors.Add("Panic", 1)
			return
		}
		if sq.config.TerseErrors && terr.SqlError != 0 {
			*err = fmt.Errorf("%s(errno %d) during query: %s", terr.Prefix(), terr.SqlError, query.Sql)
		} else {
			*err = terr
		}
		terr.RecordStats(sq.qe.queryServiceStats)
		// suppress these errors in logs
		if terr.ErrorType == ErrRetry || terr.ErrorType == ErrTxPoolFull || terr.SqlError == mysql.ErrDupEntry {
			return
		}
		if terr.ErrorType == ErrFatal {
			log.Errorf("%v: %v", terr, query)
		} else {
			log.Warningf("%v: %v", terr, query)
		}
	}
	if logStats != nil {
		logStats.Error = *err
		logStats.Send()
	}
}

// Execute executes the query and returns the result as response.
func (sq *SqlQuery) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
	logStats := newSqlQueryStats("Execute", ctx)
	defer sq.handleExecError(query, &err, logStats)

	allowShutdown := (query.TransactionId != 0)
	if err = sq.startRequest(query.SessionId, false, allowShutdown); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, sq.qe.queryTimeout.Get())
	defer func() {
		cancel()
		sq.endRequest()
	}()

	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	qre := &QueryExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          sq.qe.schemaInfo.GetPlan(ctx, logStats, query.Sql),
		ctx:           ctx,
		logStats:      logStats,
		qe:            sq.qe,
	}
	*reply = *qre.Execute()
	return nil
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) (err error) {
	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(ErrFail, "Transactions not supported with streaming")
	}

	logStats := newSqlQueryStats("StreamExecute", ctx)
	defer sq.handleExecError(query, &err, logStats)

	if err = sq.startRequest(query.SessionId, false, false); err != nil {
		return err
	}
	defer sq.endRequest()

	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	qre := &QueryExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          sq.qe.schemaInfo.GetStreamPlan(query.Sql),
		ctx:           ctx,
		logStats:      logStats,
		qe:            sq.qe,
	}
	qre.Stream(sendReply)
	return nil
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can also begin
// its own transaction, in which case it's expected to commit it also.
func (sq *SqlQuery) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	if len(queryList.Queries) == 0 {
		return NewTabletError(ErrFail, "Empty query list")
	}

	allowShutdown := (queryList.TransactionId != 0)
	if err = sq.startRequest(queryList.SessionId, false, allowShutdown); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleError(&err, nil, sq.qe.queryServiceStats)

	beginCalled := false
	session := proto.Session{
		TransactionId: queryList.TransactionId,
		SessionId:     queryList.SessionId,
	}
	reply.List = make([]mproto.QueryResult, 0, len(queryList.Queries))
	for _, bound := range queryList.Queries {
		trimmed := strings.ToLower(strings.Trim(bound.Sql, " \t\r\n"))
		switch trimmed {
		case "begin":
			if session.TransactionId != 0 {
				panic(NewTabletError(ErrFail, "Nested transactions disallowed"))
			}
			var txInfo proto.TransactionInfo
			if err = sq.Begin(ctx, &session, &txInfo); err != nil {
				return err
			}
			session.TransactionId = txInfo.TransactionId
			beginCalled = true
			reply.List = append(reply.List, mproto.QueryResult{})
		case "commit":
			if !beginCalled {
				panic(NewTabletError(ErrFail, "Cannot commit without begin"))
			}
			if err = sq.Commit(ctx, &session); err != nil {
				return err
			}
			session.TransactionId = 0
			beginCalled = false
			reply.List = append(reply.List, mproto.QueryResult{})
		default:
			query := proto.Query{
				Sql:           bound.Sql,
				BindVariables: bound.BindVariables,
				TransactionId: session.TransactionId,
				SessionId:     session.SessionId,
			}
			var localReply mproto.QueryResult
			if err = sq.Execute(ctx, &query, &localReply); err != nil {
				if beginCalled {
					sq.Rollback(ctx, &session)
				}
				return err
			}
			reply.List = append(reply.List, localReply)
		}
	}
	if beginCalled {
		sq.Rollback(ctx, &session)
		panic(NewTabletError(ErrFail, "begin called with no commit"))
	}
	return nil
}

// SplitQuery splits a BoundQuery into smaller queries that return a subset of rows from the original query.
func (sq *SqlQuery) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	logStats := newSqlQueryStats("SplitQuery", ctx)
	defer handleError(&err, logStats, sq.qe.queryServiceStats)
	if err = sq.startRequest(req.SessionID, false, false); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, sq.qe.queryTimeout.Get())
	defer func() {
		cancel()
		sq.endRequest()
	}()

	splitter := NewQuerySplitter(&(req.Query), req.SplitColumn, req.SplitCount, sq.qe.schemaInfo)
	err = splitter.validateQuery()
	if err != nil {
		return NewTabletError(ErrFail, "splitQuery: query validation error: %s, request: %#v", err, req)
	}

	qre := &QueryExecutor{
		ctx:      ctx,
		logStats: logStats,
		qe:       sq.qe,
	}
	conn := qre.getConn(sq.qe.connPool)
	defer conn.Recycle()
	// TODO: For fetching MinMax, include where clauses on the
	// primary key, if any, in the original query which might give a narrower
	// range of split column to work with.
	minMaxSql := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v", splitter.splitColumn, splitter.splitColumn, splitter.tableName)
	splitColumnMinMax := qre.execSQL(conn, minMaxSql, true)
	reply.Queries, err = splitter.split(splitColumnMinMax)
	if err != nil {
		return NewTabletError(ErrFail, "splitQuery: query split error: %s, request: %#v", err, req)
	}
	return nil
}

// HandlePanic is part of the queryservice.QueryService interface
func (sq *SqlQuery) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// startRequest validates the current state and sessionID and registers
// the request (a waitgroup) as started. Every startRequest requires one
// and only one corresponding endRequest. When the service shuts down,
// disallowQueries will wait on this waitgroup to ensure that there are
// no requests in flight.
// ignoreSession is passed in as true for valid internal requests that don't have a session id.
func (sq *SqlQuery) startRequest(sessionID int64, ignoreSession, allowShutdown bool) (err error) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.state == StateServing {
		goto verifySession
	}
	if allowShutdown && sq.state == StateShuttingTx {
		goto verifySession
	}
	return NewTabletError(ErrRetry, "operation not allowed in state %s", stateName[sq.state])

verifySession:
	if ignoreSession {
		goto ok
	}
	if sessionID == 0 || sessionID != sq.sessionID {
		return NewTabletError(ErrRetry, "Invalid session Id %v", sessionID)
	}

ok:
	sq.requests.Add(1)
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (sq *SqlQuery) endRequest() {
	sq.requests.Done()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Rand generates a pseudo-random int64 number.
func Rand() int64 {
	return rand.Int63()
}

// withTimeout returns a context based on whether the timeout is 0 or not.
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
