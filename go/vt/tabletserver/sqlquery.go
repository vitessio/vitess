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

	pb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	// StateNotConnected is the state where tabletserver is not
	// connected to an underlying mysql instance.
	StateNotConnected = iota
	// StateNotServing is the state where tabletserver is connected
	// to an underlying mysql instance, but is not serving queries.
	StateNotServing
	// StateServing is where queries are allowed.
	StateServing
	// StateTransitioning is a transient state indicating that
	// the tabletserver is tranisitioning to a new state.
	StateTransitioning
	// StateShuttingDown is a transient state indicating that
	// the tabletserver is shutting down. This state differs from
	// StateTransitioning because we allow queries for transactions
	// that are still in flight.
	StateShuttingDown
)

// stateName names every state. The number of elements must
// match the number of states. Names can overlap.
var stateName = []string{
	"NOT_SERVING",
	"NOT_SERVING",
	"SERVING",
	"NOT_SERVING",
	"SHUTTING_DOWN",
}

var (
	// RPCErrorOnlyInReply is the flag to control how errors will be sent over RPCs for all queryservice implementations.
	RPCErrorOnlyInReply = flag.Bool("rpc-error-only-in-reply", false, "if true, supported RPC calls will only return errors as part of the RPC server response")
)

// SqlQuery implements the RPC interface for the query service.
type SqlQuery struct {
	config Config
	// mu is used to access state. The lock should only be held
	// for short periods. For longer periods, you have to transition
	// the state to a transient value and release the lock.
	// Once the operation is complete, you can then transition
	// the state back to a stable value.
	// Only the function that moved the state to a transient one is
	// allowed to change it to a stable value.
	mu       sync.Mutex
	state    int64
	requests sync.WaitGroup

	// The following variables should be initialized only once
	// before starting the tabletserver. For backward compatibility,
	// we temporarily allow them to be changed until the migration
	// to the new API is complete.
	target          *pb.Target
	dbconfigs       *dbconfigs.DBConfigs
	schemaOverrides []SchemaOverride
	mysqld          mysqlctl.MysqlDaemon

	// The following variables should only be accessed within
	// the context of a startRequest-endRequest.
	qe          *QueryEngine
	invalidator *RowcacheInvalidator
	sessionID   int64

	// streamHealthMutex protects all the following fields
	streamHealthMutex        sync.Mutex
	streamHealthIndex        int
	streamHealthMap          map[int]chan<- *pb.StreamHealthResponse
	lastStreamHealthResponse *pb.StreamHealthResponse
}

// NewSqlQuery creates an instance of SqlQuery. Only one instance
// of SqlQuery can be created per process.
func NewSqlQuery(config Config) *SqlQuery {
	sq := &SqlQuery{
		config:          config,
		streamHealthMap: make(map[int]chan<- *pb.StreamHealthResponse),
		sessionID:       Rand(),
	}
	sq.qe = NewQueryEngine(config)
	sq.invalidator = NewRowcacheInvalidator(config.StatsPrefix, sq.qe, config.EnablePublishStats)
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

// InitDBConfig inititalizes the db config variables for SqlQuery. You must call this function before
// calling StartService or SetServingType.
func (sq *SqlQuery) InitDBConfig(target *pb.Target, dbconfigs *dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld mysqlctl.MysqlDaemon) error {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.state != StateNotConnected {
		return NewTabletError(ErrFatal, vtrpc.ErrorCode_INTERNAL_ERROR, "InitDBConfig failed, current state: %d", sq.state)
	}
	sq.target = target
	sq.dbconfigs = dbconfigs
	sq.schemaOverrides = schemaOverrides
	sq.mysqld = mysqld
	return nil
}

// StartService starts the query service. It returns an
// error if the state is anything other than StateNotConnected.
// If it succeeds, the resulting state is StateServing.
// Otherwise, it reverts back to StateNotConnected.
func (sq *SqlQuery) StartService(target *pb.Target, dbconfigs *dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld mysqlctl.MysqlDaemon) (err error) {
	sq.mu.Lock()
	if sq.state != StateNotConnected {
		state := sq.state
		sq.mu.Unlock()
		return NewTabletError(ErrFatal, vtrpc.ErrorCode_INTERNAL_ERROR, "cannot start tabletserver, current state: %d", state)
	}

	// Same as InitDBConfig
	sq.target = target
	sq.dbconfigs = dbconfigs
	sq.schemaOverrides = schemaOverrides
	sq.mysqld = mysqld

	sq.setState(StateTransitioning)
	sq.mu.Unlock()

	return sq.fullStart()
}

const (
	actionNone = iota
	actionFullStart
	actionServeNewType
	actionGracefulStop
)

// SetServingType changes the serving type of the tabletserver. It starts or
// stops internal services as deemed necessary.
func (sq *SqlQuery) SetServingType(tabletType topodata.TabletType, serving bool) error {
	action, err := sq.decideAction(tabletType, serving)
	if err != nil {
		return err
	}
	switch action {
	case actionNone:
		return nil
	case actionFullStart:
		return sq.fullStart()
	case actionServeNewType:
		return sq.serveNewType()
	case actionGracefulStop:
		sq.gracefulStop()
		return nil
	}
	panic("unreachable")
}

func (sq *SqlQuery) decideAction(tabletType topodata.TabletType, serving bool) (action int, err error) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.target == nil {
		return actionNone, NewTabletError(ErrFatal, vtrpc.ErrorCode_INTERNAL_ERROR, "cannot SetServingType if existing target is nil")
	}
	sq.target.TabletType = tabletType
	switch sq.state {
	case StateNotConnected:
		if serving {
			sq.setState(StateTransitioning)
			return actionFullStart, nil
		}
	case StateNotServing:
		if serving {
			sq.setState(StateTransitioning)
			return actionServeNewType, nil
		}
	case StateServing:
		if !serving {
			sq.setState(StateShuttingDown)
			return actionGracefulStop, nil
		}
		sq.setState(StateTransitioning)
		return actionServeNewType, nil
	case StateTransitioning, StateShuttingDown:
		return actionNone, NewTabletError(ErrFatal, vtrpc.ErrorCode_INTERNAL_ERROR, "cannot SetServingType, current state: %s", sq.state)
	default:
		panic("uncreachable")
	}
	return actionNone, nil
}

func (sq *SqlQuery) fullStart() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Could not start tabletserver: %v", x)
			sq.qe.Close()
			sq.mu.Lock()
			sq.setState(StateNotConnected)
			sq.mu.Unlock()
			err = x.(error)
		}
	}()

	c, err := dbconnpool.NewDBConnection(&sq.dbconfigs.App.ConnParams, sq.qe.queryServiceStats.MySQLStats)
	if err != nil {
		panic(err)
	}
	c.Close()

	sq.qe.Open(sq.dbconfigs, sq.schemaOverrides)
	return sq.serveNewType()
}

func (sq *SqlQuery) serveNewType() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Could not start tabletserver: %v", x)
			sq.qe.Close()
			sq.mu.Lock()
			sq.setState(StateNotConnected)
			sq.mu.Unlock()
			err = x.(error)
		}
	}()

	if needInvalidator(sq.target, sq.dbconfigs) {
		sq.invalidator.Open(sq.dbconfigs.App.DbName, sq.mysqld)
	} else {
		sq.invalidator.Close()
	}
	sq.sessionID = Rand()
	log.Infof("Session id: %d", sq.sessionID)
	sq.mu.Lock()
	sq.setState(StateServing)
	sq.mu.Unlock()
	return nil
}

// needInvalidator returns true if the rowcache invalidator needs to be enabled.
func needInvalidator(target *pb.Target, dbconfigs *dbconfigs.DBConfigs) bool {
	if !dbconfigs.App.EnableRowcache {
		return false
	}
	if target == nil {
		return dbconfigs.App.EnableInvalidator
	}
	return target.TabletType != topodata.TabletType_MASTER
}

func (sq *SqlQuery) gracefulStop() {
	defer close(sq.setTimeBomb())

	sq.qe.WaitForTxEmpty()
	sq.qe.streamQList.TerminateAll()
	sq.requests.Wait()
	sq.mu.Lock()
	sq.setState(StateNotServing)
	sq.mu.Unlock()
}

// StopService shuts down the tabletserver to the uninitialized state.
// It first transitions to StateShuttingDown, then waits for existing
// transactions to complete. Once all transactions are resolved, it shuts
// down the rest of the services nad transitions to StateNotConnected.
func (sq *SqlQuery) StopService() {
	defer close(sq.setTimeBomb())

	sq.mu.Lock()
	if sq.state != StateServing && sq.state != StateNotServing {
		sq.mu.Unlock()
		return
	}
	sq.setState(StateShuttingDown)
	sq.mu.Unlock()

	// Same as gracefulStop.
	log.Infof("Executing graceful transition to NotServing")
	sq.qe.WaitForTxEmpty()
	sq.qe.streamQList.TerminateAll()
	sq.requests.Wait()

	defer func() {
		sq.mu.Lock()
		sq.setState(StateNotConnected)
		sq.mu.Unlock()
	}()
	log.Infof("Shutting down query service")

	sq.invalidator.Close()
	sq.qe.Close()
	sq.sessionID = Rand()
}

func (sq *SqlQuery) setTimeBomb() chan struct{} {
	done := make(chan struct{})
	go func() {
		qt := sq.qe.queryTimeout.Get()
		if qt == 0 {
			return
		}
		tmr := time.NewTimer(10 * qt)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Fatal("Shutdown took too long. Crashing")
		case <-done:
		}
	}()
	return done
}

// CheckMySQL returns true if we can connect to MySQL.
// The function returns false only if the query service is
// in StateServing or StateNotServing.
func (sq *SqlQuery) CheckMySQL() bool {
	sq.mu.Lock()
	switch sq.state {
	case StateServing:
		// Prevent transition out of this state by
		// reserving a request.
		sq.requests.Add(1)
		defer sq.requests.Done()
	case StateNotServing:
		// Prevent transition out of this state by
		// temporarily switching to StateTransitioning.
		sq.setState(StateTransitioning)
		defer func() {
			sq.mu.Lock()
			sq.setState(StateNotServing)
			sq.mu.Unlock()
		}()
	default:
		sq.mu.Unlock()
		return true
	}
	sq.mu.Unlock()

	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Checking MySQL, unexpected error: %v", x)
		}
	}()
	return sq.qe.CheckMySQL()
}

// GetSessionId returns a sessionInfo response if the state is StateServing.
func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.state != StateServing {
		return NewTabletError(ErrRetry, vtrpc.ErrorCode_QUERY_NOT_SERVED, "operation not allowed in state %s", stateName[sq.state])
	}
	if sessionParams.Keyspace != sq.dbconfigs.App.Keyspace {
		return NewTabletError(ErrFatal, vtrpc.ErrorCode_INTERNAL_ERROR, "Keyspace mismatch, expecting %v, received %v", sq.dbconfigs.App.Keyspace, sessionParams.Keyspace)
	}
	if strings.ToLower(sessionParams.Shard) != strings.ToLower(sq.dbconfigs.App.Shard) {
		return NewTabletError(ErrFatal, vtrpc.ErrorCode_INTERNAL_ERROR, "Shard mismatch, expecting %v, received %v", sq.dbconfigs.App.Shard, sessionParams.Shard)
	}
	sessionInfo.SessionId = sq.sessionID
	return nil
}

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (sq *SqlQuery) Begin(ctx context.Context, target *pb.Target, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	logStats := newSqlQueryStats("Begin", ctx)
	logStats.OriginalSql = "begin"
	defer handleError(&err, logStats, sq.qe.queryServiceStats)

	if err = sq.startRequest(target, session.SessionId, false); err != nil {
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
func (sq *SqlQuery) Commit(ctx context.Context, target *pb.Target, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Commit", ctx)
	logStats.OriginalSql = "commit"
	logStats.TransactionID = session.TransactionId
	defer handleError(&err, logStats, sq.qe.queryServiceStats)

	if err = sq.startRequest(target, session.SessionId, true); err != nil {
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
func (sq *SqlQuery) Rollback(ctx context.Context, target *pb.Target, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Rollback", ctx)
	logStats.OriginalSql = "rollback"
	logStats.TransactionID = session.TransactionId
	defer handleError(&err, logStats, sq.qe.queryServiceStats)

	if err = sq.startRequest(target, session.SessionId, true); err != nil {
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
		*err = sq.handleExecErrorNoPanic(query, x, logStats)
	}
	if logStats != nil {
		logStats.Error = *err
		logStats.Send()
	}
}

func (sq *SqlQuery) handleExecErrorNoPanic(query *proto.Query, err interface{}, logStats *SQLQueryStats) error {
	terr, ok := err.(*TabletError)
	if !ok {
		log.Errorf("Uncaught panic for %v:\n%v\n%s", query, err, tb.Stack(4))
		sq.qe.queryServiceStats.InternalErrors.Add("Panic", 1)
		return NewTabletError(ErrFail, vtrpc.ErrorCode_UNKNOWN_ERROR, "%v: uncaught panic for %v", err, query)
	}
	var myError error
	if sq.config.TerseErrors && terr.SQLError != 0 && len(query.BindVariables) != 0 {
		myError = fmt.Errorf("%s(errno %d) during query: %s", terr.Prefix(), terr.SQLError, query.Sql)
	} else {
		myError = terr
	}
	terr.RecordStats(sq.qe.queryServiceStats)

	logMethod := log.Warningf
	// Suppress or demote some errors in logs
	switch terr.ErrorType {
	case ErrRetry, ErrTxPoolFull:
		return myError
	case ErrFatal:
		logMethod = log.Errorf
	}
	// We want to suppress/demote some MySQL error codes (regardless of the ErrorType)
	switch terr.SQLError {
	case mysql.ErrDupEntry:
		return myError
	case mysql.ErrLockWaitTimeout, mysql.ErrLockDeadlock, mysql.ErrDataTooLong, mysql.ErrDataOutOfRange:
		logMethod = log.Infof
	case 0:
		if strings.Contains(terr.Error(), "Row count exceeded") {
			logMethod = log.Infof
		}
	}
	logMethod("%v: %v", terr, query)
	return myError
}

// Execute executes the query and returns the result as response.
func (sq *SqlQuery) Execute(ctx context.Context, target *pb.Target, query *proto.Query, reply *mproto.QueryResult) (err error) {
	logStats := newSqlQueryStats("Execute", ctx)
	defer sq.handleExecError(query, &err, logStats)

	allowShutdown := (query.TransactionId != 0)
	if err = sq.startRequest(target, query.SessionId, allowShutdown); err != nil {
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
	result, err := qre.Execute()
	if err != nil {
		return sq.handleExecErrorNoPanic(query, err, logStats)
	}
	*reply = *result
	return nil
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (sq *SqlQuery) StreamExecute(ctx context.Context, target *pb.Target, query *proto.Query, sendReply func(*mproto.QueryResult) error) (err error) {
	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(ErrFail, vtrpc.ErrorCode_BAD_INPUT, "Transactions not supported with streaming")
	}

	logStats := newSqlQueryStats("StreamExecute", ctx)
	defer sq.handleExecError(query, &err, logStats)

	if err = sq.startRequest(target, query.SessionId, false); err != nil {
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
	err = qre.Stream(sendReply)
	if err != nil {
		return sq.handleExecErrorNoPanic(query, err, logStats)
	}
	return nil
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can be called with
// the AsTransaction flag which will execute all statements inside an independent
// transaction. If AsTransaction is true, TransactionId must be 0.
func (sq *SqlQuery) ExecuteBatch(ctx context.Context, target *pb.Target, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	if len(queryList.Queries) == 0 {
		return NewTabletError(ErrFail, vtrpc.ErrorCode_BAD_INPUT, "Empty query list")
	}
	if queryList.AsTransaction && queryList.TransactionId != 0 {
		return NewTabletError(ErrFail, vtrpc.ErrorCode_BAD_INPUT, "cannot start a new transaction in the scope of an existing one")
	}

	allowShutdown := (queryList.TransactionId != 0)
	if err = sq.startRequest(target, queryList.SessionId, allowShutdown); err != nil {
		return err
	}
	defer sq.endRequest()
	defer handleError(&err, nil, sq.qe.queryServiceStats)

	session := proto.Session{
		TransactionId: queryList.TransactionId,
		SessionId:     queryList.SessionId,
	}
	if queryList.AsTransaction {
		var txInfo proto.TransactionInfo
		if err = sq.Begin(ctx, target, &session, &txInfo); err != nil {
			return err
		}
		session.TransactionId = txInfo.TransactionId
		// If transaction was not committed by the end, it means
		// that there was an error, roll it back.
		defer func() {
			if session.TransactionId != 0 {
				sq.Rollback(ctx, target, &session)
			}
		}()
	}
	reply.List = make([]mproto.QueryResult, 0, len(queryList.Queries))
	for _, bound := range queryList.Queries {
		query := proto.Query{
			Sql:           bound.Sql,
			BindVariables: bound.BindVariables,
			TransactionId: session.TransactionId,
			SessionId:     session.SessionId,
		}
		var localReply mproto.QueryResult
		if err = sq.Execute(ctx, target, &query, &localReply); err != nil {
			return err
		}
		reply.List = append(reply.List, localReply)
	}
	if queryList.AsTransaction {
		if err = sq.Commit(ctx, target, &session); err != nil {
			session.TransactionId = 0
			return err
		}
		session.TransactionId = 0
	}
	return nil
}

// SplitQuery splits a BoundQuery into smaller queries that return a subset of rows from the original query.
func (sq *SqlQuery) SplitQuery(ctx context.Context, target *pb.Target, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	logStats := newSqlQueryStats("SplitQuery", ctx)
	defer handleError(&err, logStats, sq.qe.queryServiceStats)
	if err = sq.startRequest(target, req.SessionID, false); err != nil {
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
		return NewTabletError(ErrFail, vtrpc.ErrorCode_BAD_INPUT, "splitQuery: query validation error: %s, request: %#v", err, req)
	}

	qre := &QueryExecutor{
		ctx:      ctx,
		logStats: logStats,
		qe:       sq.qe,
	}
	columnType, err := getColumnType(qre, splitter.splitColumn, splitter.tableName)
	if err != nil {
		return err
	}
	var pkMinMax *mproto.QueryResult
	switch columnType {
	case mproto.VT_TINY, mproto.VT_SHORT, mproto.VT_LONG, mproto.VT_LONGLONG, mproto.VT_INT24, mproto.VT_FLOAT, mproto.VT_DOUBLE:
		pkMinMax, err = getColumnMinMax(qre, splitter.splitColumn, splitter.tableName)
		if err != nil {
			return err
		}
	}
	reply.Queries, err = splitter.split(columnType, pkMinMax)
	if err != nil {
		return NewTabletError(ErrFail, vtrpc.ErrorCode_BAD_INPUT, "splitQuery: query split error: %s, request: %#v", err, req)
	}
	return nil
}

// StreamHealthRegister is part of queryservice.QueryService interface
func (sq *SqlQuery) StreamHealthRegister(c chan<- *pb.StreamHealthResponse) (int, error) {
	sq.streamHealthMutex.Lock()
	defer sq.streamHealthMutex.Unlock()

	id := sq.streamHealthIndex
	sq.streamHealthIndex++
	sq.streamHealthMap[id] = c
	if sq.lastStreamHealthResponse != nil {
		c <- sq.lastStreamHealthResponse
	}
	return id, nil
}

// StreamHealthUnregister is part of queryservice.QueryService interface
func (sq *SqlQuery) StreamHealthUnregister(id int) error {
	sq.streamHealthMutex.Lock()
	defer sq.streamHealthMutex.Unlock()

	delete(sq.streamHealthMap, id)
	return nil
}

// HandlePanic is part of the queryservice.QueryService interface
func (sq *SqlQuery) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// BroadcastHealth will broadcast the current health to all listeners
func (sq *SqlQuery) BroadcastHealth(terTimestamp int64, stats *pb.RealtimeStats) {
	shr := &pb.StreamHealthResponse{
		Target: sq.target,
		TabletExternallyReparentedTimestamp: terTimestamp,
		RealtimeStats:                       stats,
	}

	sq.streamHealthMutex.Lock()
	defer sq.streamHealthMutex.Unlock()
	for _, c := range sq.streamHealthMap {
		// do not block on any write
		select {
		case c <- shr:
		default:
		}
	}
	sq.lastStreamHealthResponse = shr
}

// startRequest validates the current state and sessionID and registers
// the request (a waitgroup) as started. Every startRequest requires one
// and only one corresponding endRequest. When the service shuts down,
// StopService will wait on this waitgroup to ensure that there are
// no requests in flight.
func (sq *SqlQuery) startRequest(target *pb.Target, sessionID int64, allowShutdown bool) (err error) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.state == StateServing {
		goto verifySession
	}
	if allowShutdown && sq.state == StateShuttingDown {
		goto verifySession
	}
	return NewTabletError(ErrRetry, vtrpc.ErrorCode_QUERY_NOT_SERVED, "operation not allowed in state %s", stateName[sq.state])

verifySession:
	if target != nil && sq.target != nil {
		// a valid target can be used instead of a valid session
		if target.Keyspace != sq.target.Keyspace {
			return NewTabletError(ErrRetry, vtrpc.ErrorCode_QUERY_NOT_SERVED, "Invalid keyspace %v", target.Keyspace)
		}
		if target.Shard != sq.target.Shard {
			return NewTabletError(ErrRetry, vtrpc.ErrorCode_QUERY_NOT_SERVED, "Invalid shard %v", target.Shard)
		}
		if target.TabletType != sq.target.TabletType {
			return NewTabletError(ErrRetry, vtrpc.ErrorCode_QUERY_NOT_SERVED, "Invalid tablet type %v", target.TabletType)
		}
		goto ok
	}
	if sessionID != sq.sessionID {
		return NewTabletError(ErrRetry, vtrpc.ErrorCode_QUERY_NOT_SERVED, "Invalid session Id %v", sessionID)
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

func getColumnType(qre *QueryExecutor, columnName, tableName string) (int64, error) {
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return mproto.VT_NULL, err
	}
	defer conn.Recycle()
	// TODO(shengzhe): use AST to represent the query to avoid sql injection.
	// current code is safe because QuerySplitter.validateQuery is called before
	// calling this.
	query := fmt.Sprintf("SELECT %v FROM %v LIMIT 0", columnName, tableName)
	result, err := qre.execSQL(conn, query, true)
	if err != nil {
		return mproto.VT_NULL, err
	}
	if result == nil || len(result.Fields) != 1 {
		return mproto.VT_NULL, NewTabletError(ErrFail, vtrpc.ErrorCode_BAD_INPUT, "failed to get column type for column: %v, invalid result: %v", columnName, result)
	}
	return result.Fields[0].Type, nil
}

func getColumnMinMax(qre *QueryExecutor, columnName, tableName string) (*mproto.QueryResult, error) {
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	// TODO(shengzhe): use AST to represent the query to avoid sql injection.
	// current code is safe because QuerySplitter.validateQuery is called before
	// calling this.
	minMaxSQL := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v", columnName, columnName, tableName)
	return qre.execSQL(conn, minMaxSQL, true)
}
