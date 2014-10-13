// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

const (
	// Allowed state transitions:
	// NOT_SERVING -> INITIALIZING -> SERVING/NOT_SERVING,
	// SERVING -> SHUTTING_TX
	// SHUTTING_TX -> SHUTTING_QUERIES
	// SHUTTING_QUERIES -> NOT_SERVING
	//
	// NOT_SERVING: The query service is not serving queries.
	NOT_SERVING = iota
	// INITIALIZING: The query service is tyring to get to the SERVING state.
	// This is a transient state. It's only informational.
	INITIALIZING
	// SERVING: Query service is running. Everything is allowed.
	SERVING
	// SHUTTING_TX: Query service is shutting down and has disallowed
	// new transactions. New queries are still allowed as long as they
	// are part of an existing transaction. We remain in this state
	// until all existing transactions are completed.
	SHUTTING_TX
	// SHUTTING_QUERIES: Query service is shutting down and has disallowed
	// new queries. This state follows SHUTTING_TX. We enter this state
	// after all existing transactions have completed. We remain in this
	// state until all existing queries are completed. The next state
	// after this is NOT_SERVING.
	SHUTTING_QUERIES
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

// SqlQuery implements the RPC interface for the query service.
type SqlQuery struct {
	// mu is used to manage state transitions.
	// Obtain a write lock to change state.
	// Obtain a read lock to prevent state from changing.
	// If you want to know the current value of state and
	// don't care that it changes after you've read it,
	// you can perform a lockless atomic read.
	mu       sync.RWMutex
	requests sync.WaitGroup
	state    sync2.AtomicInt64

	qe        *QueryEngine
	sessionId int64
	dbconfig  *dbconfigs.DBConfig
	mysqld    *mysqlctl.Mysqld
}

// NewSqlQuery creates an instance of SqlQuery. Only one instance
// of SqlQuery can be created per process.
func NewSqlQuery(config Config) *SqlQuery {
	sq := &SqlQuery{}
	sq.qe = NewQueryEngine(config)
	stats.PublishJSONFunc("Voltron", sq.statsJSON)
	stats.Publish("TabletState", stats.IntFunc(sq.state.Get))
	stats.Publish("TabletStateName", stats.StringFunc(sq.GetState))
	return sq
}

// GetState returns the name of the current SqlQuery state.
func (sq *SqlQuery) GetState() string {
	return stateName[sq.state.Get()]
}

// setState changes the state and logs the event.
// It requires the caller to hold a lock on mu.
func (sq *SqlQuery) setState(state int64) {
	log.Infof("SqlQuery state: %v -> %v", sq.GetState(), stateName[state])
	sq.state.Set(state)
}

// allowQueries starts the query service.
// If the state is anything other than NOT_SERVING, it fails.
// If allowQuery succeeds, the resulting state is SERVING.
// Otherwise, it reverts back to NOT_SERVING.
// While allowQuery is running, the state is set to INITIALIZING.
// If waitForMysql is set to true, allowQueries will not return
// until it's able to connect to mysql.
// No other operations are allowed when allowQueries is running.
func (sq *SqlQuery) allowQueries(dbconfig *dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules, mysqld *mysqlctl.Mysqld, waitForMysql bool) (err error) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.state.Get() != NOT_SERVING {
		terr := NewTabletError(FATAL, "cannot start query service, current state: %s", sq.GetState())
		return terr
	}
	// state is NOT_SERVING
	sq.setState(INITIALIZING)

	if waitForMysql {
		waitTime := time.Second
		for {
			c, err := dbconnpool.NewDBConnection(&dbconfig.ConnectionParams, mysqlStats)
			if err == nil {
				c.Close()
				break
			}
			log.Warningf("mysql.Connect() error, retrying in %v: %v", waitTime, err)
			time.Sleep(waitTime)
			// Cap at 32 seconds
			if waitTime < 30*time.Second {
				waitTime = waitTime * 2
			}
		}
	}

	defer func() {
		if x := recover(); x != nil {
			err = x.(*TabletError)
			log.Errorf("Could not start query service: %v", err)
			sq.qe.Close()
			sq.setState(NOT_SERVING)
			return
		}
		sq.setState(SERVING)
	}()

	sq.qe.Open(dbconfig, schemaOverrides, qrs, mysqld)
	sq.dbconfig = dbconfig
	sq.mysqld = mysqld
	sq.sessionId = Rand()
	log.Infof("Session id: %d", sq.sessionId)
	return nil
}

// disallowQueries shuts down the query service if it's SERVING.
// It first transitions to SHUTTING_TX, then waits for existing
// transactions to complete. During this state, no new
// transactions or queries are allowed. However, existing
// transactions can still receive queries.
// Then, it transitions to SHUTTING_QUERIES to wait for existing
// queries to complete. In this state no new requests are allowed.
// Once all queries are done, it shuts down the query engine
// and marks the state as NOT_SERVING.
func (sq *SqlQuery) disallowQueries() {
	// SERVING -> SHUTTING_TX
	sq.mu.Lock()
	if sq.state.Get() != SERVING {
		sq.mu.Unlock()
		return
	}
	sq.setState(SHUTTING_TX)
	sq.mu.Unlock()
	// Don't hold lock while waiting.
	sq.qe.WaitForTxEmpty()

	// SHUTTING_TX -> SHUTTING_QUERIES
	sq.mu.Lock()
	sq.setState(SHUTTING_QUERIES)
	sq.mu.Unlock()
	// Terminate all streaming queries
	sq.qe.streamQList.TerminateAll()
	// Don't hold lock while waiting.
	sq.requests.Wait()

	// SHUTTING_QUERIES -> NOT_SERVING
	sq.mu.Lock()
	defer func() {
		sq.setState(NOT_SERVING)
		sq.mu.Unlock()
	}()
	log.Infof("Stopping query service: %d", sq.sessionId)
	sq.qe.Close()
	sq.sessionId = 0
	sq.dbconfig = &dbconfigs.DBConfig{}
}

// GetSessionId returns a sessionInfo response if the state is SERVING.
func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	// We perform a lockless read of state because we don't care if it changes
	// after we check its value.
	if sq.state.Get() != SERVING {
		return NewTabletError(RETRY, "Query server is in %s state", sq.GetState())
	}
	// state was SERVING
	if sessionParams.Keyspace != sq.dbconfig.Keyspace {
		return NewTabletError(FATAL, "Keyspace mismatch, expecting %v, received %v", sq.dbconfig.Keyspace, sessionParams.Keyspace)
	}
	if strings.ToLower(sessionParams.Shard) != strings.ToLower(sq.dbconfig.Shard) {
		return NewTabletError(FATAL, "Shard mismatch, expecting %v, received %v", sq.dbconfig.Shard, sessionParams.Shard)
	}
	sessionInfo.SessionId = sq.sessionId
	return nil
}

// Begin starts a new transaction. This is allowed only if the state is SERVING.
func (sq *SqlQuery) Begin(context context.Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	logStats := newSqlQueryStats("Begin", context)
	logStats.OriginalSql = "begin"
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	defer handleError(&err, logStats)
	if sq.state.Get() != SERVING {
		return NewTabletError(RETRY, "cannot begin transaction in state %s", sq.GetState())
	}
	// state is SERVING
	if session.SessionId == 0 || session.SessionId != sq.sessionId {
		return NewTabletError(RETRY, "Invalid session Id %v", session.SessionId)
	}
	defer queryStats.Record("BEGIN", time.Now())
	txInfo.TransactionId = sq.qe.activeTxPool.Begin()
	logStats.TransactionID = txInfo.TransactionId
	return nil
}

// Commit commits the specified transaction.
func (sq *SqlQuery) Commit(context context.Context, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Commit", context)
	logStats.OriginalSql = "commit"
	logStats.TransactionID = session.TransactionId
	if err = sq.startRequest(session.SessionId, true); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleError(&err, logStats)

	Commit(logStats, sq.qe, session.TransactionId)
	return nil
}

// Rollback rollsback the specified transaction.
func (sq *SqlQuery) Rollback(context context.Context, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Rollback", context)
	logStats.OriginalSql = "rollback"
	logStats.TransactionID = session.TransactionId
	if err = sq.startRequest(session.SessionId, true); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleError(&err, logStats)
	defer queryStats.Record("ROLLBACK", time.Now())
	sq.qe.activeTxPool.Rollback(session.TransactionId)
	return nil
}

// handleExecError handles panics during query execution and sets
// the supplied error return value.
func handleExecError(query *proto.Query, err *error, logStats *SQLQueryStats) {
	if logStats != nil {
		logStats.Send()
	}
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic for %v:\n%v\n%s", query, x, tb.Stack(4))
			*err = NewTabletError(FAIL, "%v: uncaught panic for %v", x, query)
			internalErrors.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats()
		// suppress these errors in logs
		if terr.ErrorType == RETRY || terr.ErrorType == TX_POOL_FULL || terr.SqlError == mysql.DUP_ENTRY {
			return
		}
		if terr.ErrorType == FATAL {
			log.Errorf("%v: %v", terr, query)
		} else {
			log.Warningf("%v: %v", terr, query)
		}
	}
}

// Execute executes the query and returns the result as response.
func (sq *SqlQuery) Execute(context context.Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
	logStats := newSqlQueryStats("Execute", context)
	allowShutdown := (query.TransactionId != 0)
	if err = sq.startRequest(query.SessionId, allowShutdown); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleExecError(query, &err, logStats)

	// TODO(sougou): Change usage such that we don't have to do this.
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	qre := &QueryExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          sq.qe.schemaInfo.GetPlan(logStats, query.Sql),
		RequestContext: RequestContext{
			ctx:      context,
			logStats: logStats,
			qe:       sq.qe,
		},
	}
	*reply = *qre.Execute()
	return nil
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (sq *SqlQuery) StreamExecute(context context.Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) (err error) {
	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(FAIL, "Transactions not supported with streaming")
	}

	logStats := newSqlQueryStats("StreamExecute", context)
	if err = sq.startRequest(query.SessionId, false); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleExecError(query, &err, logStats)

	// TODO(sougou): Change usage such that we don't have to do this.
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	qre := &QueryExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          sq.qe.schemaInfo.GetStreamPlan(query.Sql),
		RequestContext: RequestContext{
			ctx:      context,
			logStats: logStats,
			qe:       sq.qe,
		},
	}
	qre.Stream(sendReply)
	return nil
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can also begin
// its own transaction, in which case it's expected to commit it also.
func (sq *SqlQuery) ExecuteBatch(context context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	if len(queryList.Queries) == 0 {
		return NewTabletError(FAIL, "Empty query list")
	}

	allowShutdown := (queryList.TransactionId != 0)
	if err = sq.startRequest(queryList.SessionId, allowShutdown); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleError(&err, nil)

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
				panic(NewTabletError(FAIL, "Nested transactions disallowed"))
			}
			var txInfo proto.TransactionInfo
			if err = sq.Begin(context, &session, &txInfo); err != nil {
				return err
			}
			session.TransactionId = txInfo.TransactionId
			beginCalled = true
			reply.List = append(reply.List, mproto.QueryResult{})
		case "commit":
			if !beginCalled {
				panic(NewTabletError(FAIL, "Cannot commit without begin"))
			}
			if err = sq.Commit(context, &session); err != nil {
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
			if err = sq.Execute(context, &query, &localReply); err != nil {
				if beginCalled {
					sq.Rollback(context, &session)
				}
				return err
			}
			reply.List = append(reply.List, localReply)
		}
	}
	if beginCalled {
		sq.Rollback(context, &session)
		panic(NewTabletError(FAIL, "begin called with no commit"))
	}
	return nil
}

// SplitQuery splits a BoundQuery into smaller queries that return a subset of rows from the original query.
func (sq *SqlQuery) SplitQuery(context context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	logStats := newSqlQueryStats("SplitQuery", context)
	var err error
	// TODO(sougou/anandhenry): Add session validation.
	defer handleError(&err, logStats)

	splitter := NewQuerySplitter(&(req.Query), req.SplitCount, sq.qe.schemaInfo)
	err = splitter.validateQuery()
	if err != nil {
		return NewTabletError(FAIL, "query validation error: %s", err)
	}
	// Partial initialization or QueryExecutor is enough to call execSQL
	requestContext := RequestContext{
		ctx:      context,
		logStats: logStats,
		qe:       sq.qe,
	}
	conn := getOrPanic(sq.qe.connPool)
	// TODO: For fetching pkMinMax, include where clauses on the
	// primary key, if any, in the original query which might give a narrower
	// range of PKs to work with.
	minMaxSql := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v", splitter.pkCol, splitter.pkCol, splitter.tableName)
	pkMinMax := requestContext.execSQL(conn, minMaxSql, true)
	reply.Queries = splitter.split(pkMinMax)
	return nil
}

// startRequest validates the current state and sessionId and registers
// the request (a waitgroup) as started. Every startRequest requires one
// and only one corresponding endRequest. When the service shuts down,
// disallowQueries will wait on this waitgroup to ensure that there are
// no requests in flight.
func (sq *SqlQuery) startRequest(sessionId int64, allowShutdown bool) (err error) {
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	st := sq.state.Get()
	if st == SERVING {
		goto verifySession
	}
	if allowShutdown && st == SHUTTING_TX {
		goto verifySession
	}
	return NewTabletError(RETRY, "operation not allowed in state %s", sq.GetState())

verifySession:
	if sessionId == 0 || sessionId != sq.sessionId {
		return NewTabletError(RETRY, "Invalid session Id %v", sessionId)
	}
	sq.requests.Add(1)
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (sq *SqlQuery) endRequest() {
	sq.requests.Done()
}

// statsJSON is used to export SqlQuery status variables into expvar.
func (sq *SqlQuery) statsJSON() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\n \"State\": \"%v\",", stateName[sq.state.Get()])
	fmt.Fprintf(buf, "\n \"CachePool\": %v,", sq.qe.cachePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"QueryCache\": %v,", sq.qe.schemaInfo.queries.StatsJSON())
	fmt.Fprintf(buf, "\n \"ConnPool\": %v,", sq.qe.connPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"StreamConnPool\": %v,", sq.qe.streamConnPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActiveTxPool\": %v,", sq.qe.activeTxPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"QueryTimeout\": %v,", int64(sq.qe.queryTimeout.Get()))
	fmt.Fprintf(buf, "\n \"MaxResultSize\": %v,", sq.qe.maxResultSize.Get())
	fmt.Fprintf(buf, "\n \"StreamBufferSize\": %v", sq.qe.streamBufferSize.Get())
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Rand generates a pseudo-random int64 number.
func Rand() int64 {
	return rand.Int63()
}
