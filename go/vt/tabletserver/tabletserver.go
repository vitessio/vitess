// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
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

// TabletServer implements the RPC interface for the query service.
type TabletServer struct {
	// config contains the original config values. TabletServer
	// contains variables that are derived from the original config
	// that can be subsequently changed. So, they may not always
	// correspond to the original values.
	config       Config
	QueryTimeout sync2.AtomicDuration
	BeginTimeout sync2.AtomicDuration

	// mu is used to access state. The lock should only be held
	// for short periods. For longer periods, you have to transition
	// the state to a transient value and release the lock.
	// Once the operation is complete, you can then transition
	// the state back to a stable value.
	// The lameduck mode causes tablet server to respond as unhealthy
	// for health checks. This does not affect how queries are served.
	// target specifies the primary target type, and also allow specifies
	// secondary types that should be additionally allowed.
	mu        sync.Mutex
	state     int64
	lameduck  sync2.AtomicInt32
	target    querypb.Target
	alsoAllow []topodatapb.TabletType
	requests  sync.WaitGroup
	begins    sync.WaitGroup

	// The following variables should be initialized only once
	// before starting the tabletserver. For backward compatibility,
	// we temporarily allow them to be changed until the migration
	// to the new API is complete.
	dbconfigs       dbconfigs.DBConfigs
	schemaOverrides []SchemaOverride
	mysqld          mysqlctl.MysqlDaemon

	// The following variables should only be accessed within
	// the context of a startRequest-endRequest.
	qe          *QueryEngine
	invalidator *RowcacheInvalidator
	sessionID   int64

	// checkMySQLThrottler is used to throttle the number of
	// requests sent to CheckMySQL.
	checkMySQLThrottler *sync2.Semaphore

	// streamHealthMutex protects all the following fields
	streamHealthMutex        sync.Mutex
	streamHealthIndex        int
	streamHealthMap          map[int]chan<- *querypb.StreamHealthResponse
	lastStreamHealthResponse *querypb.StreamHealthResponse

	// history records changes in state for display on the status page.
	// It has its own internal mutex.
	history *history.History
}

// RegisterFunction is a callback type to be called when we
// Register() a TabletServer
type RegisterFunction func(Controller)

// RegisterFunctions is a list of all the
// RegisterFunction that will be called upon
// Register() on a TabletServer
var RegisterFunctions []RegisterFunction

// MySQLChecker defines the CheckMySQL interface that lower
// level objects can use to call back into TabletServer.
type MySQLChecker interface {
	CheckMySQL()
}

// NewTabletServer creates an instance of TabletServer. Only one instance
// of TabletServer can be created per process.
func NewTabletServer(config Config) *TabletServer {
	tsv := &TabletServer{
		config:              config,
		QueryTimeout:        sync2.NewAtomicDuration(time.Duration(config.QueryTimeout * 1e9)),
		BeginTimeout:        sync2.NewAtomicDuration(time.Duration(config.TxPoolTimeout * 1e9)),
		checkMySQLThrottler: sync2.NewSemaphore(1, 0),
		streamHealthMap:     make(map[int]chan<- *querypb.StreamHealthResponse),
		sessionID:           Rand(),
		history:             history.New(10),
	}
	tsv.qe = NewQueryEngine(tsv, config)
	tsv.invalidator = NewRowcacheInvalidator(config.StatsPrefix, tsv, tsv.qe, config.EnablePublishStats)
	if config.EnablePublishStats {
		stats.Publish(config.StatsPrefix+"TabletState", stats.IntFunc(func() int64 {
			tsv.mu.Lock()
			state := tsv.state
			tsv.mu.Unlock()
			return state
		}))
		stats.Publish(config.StatsPrefix+"QueryTimeout", stats.DurationFunc(tsv.QueryTimeout.Get))
		stats.Publish(config.StatsPrefix+"BeginTimeout", stats.DurationFunc(tsv.BeginTimeout.Get))
		stats.Publish(config.StatsPrefix+"TabletStateName", stats.StringFunc(tsv.GetState))
	}
	return tsv
}

// Register prepares TabletServer for serving by calling
// all the registrations functions.
func (tsv *TabletServer) Register() {
	for _, f := range RegisterFunctions {
		f(tsv)
	}
	tsv.registerDebugHealthHandler()
	tsv.registerQueryzHandler()
	tsv.registerSchemazHandler()
	tsv.registerStreamQueryzHandlers()
}

// RegisterQueryRuleSource registers ruleSource for setting query rules.
func (tsv *TabletServer) RegisterQueryRuleSource(ruleSource string) {
	tsv.qe.schemaInfo.queryRuleSources.RegisterQueryRuleSource(ruleSource)
}

// UnRegisterQueryRuleSource unregisters ruleSource from query rules.
func (tsv *TabletServer) UnRegisterQueryRuleSource(ruleSource string) {
	tsv.qe.schemaInfo.queryRuleSources.UnRegisterQueryRuleSource(ruleSource)
}

// SetQueryRules sets the query rules for a registered ruleSource.
func (tsv *TabletServer) SetQueryRules(ruleSource string, qrs *QueryRules) error {
	err := tsv.qe.schemaInfo.queryRuleSources.SetRules(ruleSource, qrs)
	if err != nil {
		return err
	}
	tsv.qe.schemaInfo.ClearQueryPlanCache()
	return nil
}

// GetState returns the name of the current TabletServer state.
func (tsv *TabletServer) GetState() string {
	if tsv.lameduck.Get() != 0 {
		return "NOT_SERVING"
	}
	tsv.mu.Lock()
	name := stateName[tsv.state]
	tsv.mu.Unlock()
	return name
}

// setState changes the state and logs the event.
// It requires the caller to hold a lock on mu.
func (tsv *TabletServer) setState(state int64) {
	log.Infof("TabletServer state: %v -> %v", stateName[tsv.state], stateName[state])
	tsv.state = state
	tsv.history.Add(&historyRecord{
		Time:         time.Now(),
		ServingState: stateName[state],
		TabletType:   tsv.target.TabletType.String(),
	})
}

// transition obtains a lock and changes the state.
func (tsv *TabletServer) transition(newState int64) {
	tsv.mu.Lock()
	tsv.setState(newState)
	tsv.mu.Unlock()
}

// IsServing returns true if TabletServer is in SERVING state.
func (tsv *TabletServer) IsServing() bool {
	return tsv.GetState() == "SERVING"
}

// InitDBConfig inititalizes the db config variables for TabletServer. You must call this function before
// calling StartService or SetServingType.
func (tsv *TabletServer) InitDBConfig(target querypb.Target, dbconfigs dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld mysqlctl.MysqlDaemon) error {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state != StateNotConnected {
		return NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "InitDBConfig failed, current state: %d", tsv.state)
	}
	tsv.target = target
	tsv.dbconfigs = dbconfigs
	tsv.schemaOverrides = schemaOverrides
	tsv.mysqld = mysqld
	return nil
}

// StartService is a convenience function for InitDBConfig->SetServingType
// with serving=true.
func (tsv *TabletServer) StartService(target querypb.Target, dbconfigs dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld mysqlctl.MysqlDaemon) (err error) {
	// Save tablet type away to prevent data races
	tabletType := target.TabletType
	err = tsv.InitDBConfig(target, dbconfigs, schemaOverrides, mysqld)
	if err != nil {
		return err
	}
	return tsv.SetServingType(tabletType, true, nil)
}

// EnterLameduck causes tabletserver to enter the lameduck state. This
// state causes health checks to fail, but the behavior of tabletserver
// otherwise remains the same. Any subsequent calls to SetServingType will
// cause the tabletserver to exit this mode.
func (tsv *TabletServer) EnterLameduck() {
	tsv.lameduck.Set(1)
}

// ExitLameduck causes the tabletserver to exit the lameduck mode.
func (tsv *TabletServer) ExitLameduck() {
	tsv.lameduck.Set(0)
}

const (
	actionNone = iota
	actionFullStart
	actionServeNewType
	actionGracefulStop
)

// SetServingType changes the serving type of the tabletserver. It starts or
// stops internal services as deemed necessary. The tabletType determines the
// primary serving type, while alsoAllow specifies other tablet types that
// should also be honored for serving.
func (tsv *TabletServer) SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) error {
	defer tsv.ExitLameduck()

	action, err := tsv.decideAction(tabletType, serving, alsoAllow)
	if err != nil {
		return err
	}
	switch action {
	case actionNone:
		return nil
	case actionFullStart:
		return tsv.fullStart()
	case actionServeNewType:
		return tsv.serveNewType()
	case actionGracefulStop:
		tsv.gracefulStop()
		return nil
	}
	panic("unreachable")
}

func (tsv *TabletServer) decideAction(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (action int, err error) {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()

	tsv.alsoAllow = alsoAllow

	// Handle the case where the requested TabletType and serving state
	// match our current state. This avoids an unnecessary transition.
	// There's no similar shortcut if serving is false, because there
	// are different 'not serving' states that require different actions.
	if tsv.target.TabletType == tabletType {
		if serving && tsv.state == StateServing {
			// We're already in the desired state.
			return actionNone, nil
		}
	}
	tsv.target.TabletType = tabletType
	switch tsv.state {
	case StateNotConnected:
		if serving {
			tsv.setState(StateTransitioning)
			return actionFullStart, nil
		}
	case StateNotServing:
		if serving {
			tsv.setState(StateTransitioning)
			return actionServeNewType, nil
		}
	case StateServing:
		if !serving {
			tsv.setState(StateShuttingDown)
			return actionGracefulStop, nil
		}
		tsv.setState(StateTransitioning)
		return actionServeNewType, nil
	case StateTransitioning, StateShuttingDown:
		return actionNone, NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "cannot SetServingType, current state: %s", tsv.state)
	default:
		panic("uncreachable")
	}
	return actionNone, nil
}

func (tsv *TabletServer) fullStart() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Could not start tabletserver: %v", x)
			tsv.qe.Close()
			tsv.transition(StateNotConnected)
			err = x.(error)
		}
	}()

	c, err := dbconnpool.NewDBConnection(&tsv.dbconfigs.App.ConnParams, tsv.qe.queryServiceStats.MySQLStats)
	if err != nil {
		panic(err)
	}
	c.Close()

	tsv.qe.Open(tsv.dbconfigs, tsv.schemaOverrides)
	return tsv.serveNewType()
}

func (tsv *TabletServer) serveNewType() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Could not start tabletserver: %v", x)
			tsv.qe.Close()
			tsv.transition(StateNotConnected)
			err = x.(error)
		}
	}()

	if tsv.needInvalidator(tsv.target) {
		tsv.invalidator.Open(tsv.dbconfigs.App.DbName, tsv.mysqld)
	} else {
		tsv.invalidator.Close()
	}
	tsv.sessionID = Rand()
	log.Infof("Session id: %d", tsv.sessionID)
	tsv.transition(StateServing)
	return nil
}

// needInvalidator returns true if the rowcache invalidator needs to be enabled.
func (tsv *TabletServer) needInvalidator(target querypb.Target) bool {
	if !tsv.config.RowCache.Enabled {
		return false
	}
	return target.TabletType != topodatapb.TabletType_MASTER
}

func (tsv *TabletServer) gracefulStop() {
	defer close(tsv.setTimeBomb())
	tsv.waitForShutdown()
	tsv.transition(StateNotServing)
}

// StopService shuts down the tabletserver to the uninitialized state.
// It first transitions to StateShuttingDown, then waits for existing
// transactions to complete. Once all transactions are resolved, it shuts
// down the rest of the services nad transitions to StateNotConnected.
func (tsv *TabletServer) StopService() {
	defer close(tsv.setTimeBomb())
	defer logError(tsv.qe.queryServiceStats)

	tsv.mu.Lock()
	if tsv.state != StateServing && tsv.state != StateNotServing {
		tsv.mu.Unlock()
		return
	}
	tsv.setState(StateShuttingDown)
	tsv.mu.Unlock()

	log.Infof("Executing graceful transition to NotServing")
	tsv.waitForShutdown()

	defer func() {
		tsv.transition(StateNotConnected)
	}()
	log.Infof("Shutting down query service")

	tsv.invalidator.Close()
	tsv.qe.Close()
	tsv.sessionID = Rand()
}

func (tsv *TabletServer) waitForShutdown() {
	// Wait till begins have completed before waiting on tx pool.
	tsv.begins.Wait()
	tsv.qe.WaitForTxEmpty()
	tsv.qe.streamQList.TerminateAll()
	tsv.requests.Wait()
}

func (tsv *TabletServer) setTimeBomb() chan struct{} {
	done := make(chan struct{})
	go func() {
		qt := tsv.QueryTimeout.Get()
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

// IsHealthy returns nil if the query service is healthy (able to
// connect to the database and serving traffic) or an error explaining
// the unhealthiness otherwise.
func (tsv *TabletServer) IsHealthy() error {
	return tsv.Execute(
		context.Background(),
		nil,
		&proto.Query{
			Sql:       "select 1 from dual",
			SessionId: tsv.sessionID,
		},
		new(sqltypes.Result),
	)
}

// CheckMySQL initiates a check to see if MySQL is reachable.
// If not, it shuts down the query service. The check is rate-limited
// to no more than once per second.
func (tsv *TabletServer) CheckMySQL() {
	if !tsv.checkMySQLThrottler.TryAcquire() {
		return
	}
	go func() {
		defer func() {
			logError(tsv.qe.queryServiceStats)
			time.Sleep(1 * time.Second)
			tsv.checkMySQLThrottler.Release()
		}()
		if tsv.isMySQLReachable() {
			return
		}
		log.Info("Check MySQL failed. Shutting down query service")
		tsv.StopService()
	}()
}

// isMySQLReachable returns true if we can connect to MySQL.
// The function returns false only if the query service is
// in StateServing or StateNotServing.
func (tsv *TabletServer) isMySQLReachable() bool {
	tsv.mu.Lock()
	switch tsv.state {
	case StateServing:
		// Prevent transition out of this state by
		// reserving a request.
		tsv.requests.Add(1)
		defer tsv.requests.Done()
	case StateNotServing:
		// Prevent transition out of this state by
		// temporarily switching to StateTransitioning.
		tsv.setState(StateTransitioning)
		defer func() {
			tsv.transition(StateNotServing)
		}()
	default:
		tsv.mu.Unlock()
		return true
	}
	tsv.mu.Unlock()
	return tsv.qe.IsMySQLReachable()
}

// ReloadSchema reloads the schema.
// If the query service is not running, it's a no-op.
func (tsv *TabletServer) ReloadSchema() {
	defer logError(tsv.qe.queryServiceStats)
	tsv.qe.schemaInfo.triggerReload()
}

// ClearQueryPlanCache clears internal query plan cache
func (tsv *TabletServer) ClearQueryPlanCache() {
	tsv.qe.schemaInfo.ClearQueryPlanCache()
}

// QueryService returns the QueryService part of TabletServer.
func (tsv *TabletServer) QueryService() queryservice.QueryService {
	return tsv
}

// GetSessionId returns a sessionInfo response if the state is StateServing.
func (tsv *TabletServer) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state != StateServing {
		return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "operation not allowed in state %s", stateName[tsv.state])
	}
	if sessionParams.Keyspace != tsv.dbconfigs.App.Keyspace {
		return NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Keyspace mismatch, expecting %v, received %v", tsv.dbconfigs.App.Keyspace, sessionParams.Keyspace)
	}
	if strings.ToLower(sessionParams.Shard) != strings.ToLower(tsv.dbconfigs.App.Shard) {
		return NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Shard mismatch, expecting %v, received %v", tsv.dbconfigs.App.Shard, sessionParams.Shard)
	}
	sessionInfo.SessionId = tsv.sessionID
	return nil
}

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (tsv *TabletServer) Begin(ctx context.Context, target *querypb.Target, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	logStats := newLogStats("Begin", ctx)
	logStats.OriginalSQL = "begin"
	defer handleError(&err, logStats, tsv.qe.queryServiceStats)

	if err = tsv.startRequest(target, session.SessionId, true, false); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, tsv.BeginTimeout.Get())
	defer func() {
		tsv.qe.queryServiceStats.QueryStats.Record("BEGIN", time.Now())
		cancel()
		tsv.endRequest(true)
	}()

	txInfo.TransactionId = tsv.qe.txPool.Begin(ctx)
	logStats.TransactionID = txInfo.TransactionId
	return nil
}

// Commit commits the specified transaction.
func (tsv *TabletServer) Commit(ctx context.Context, target *querypb.Target, session *proto.Session) (err error) {
	logStats := newLogStats("Commit", ctx)
	logStats.OriginalSQL = "commit"
	logStats.TransactionID = session.TransactionId
	defer handleError(&err, logStats, tsv.qe.queryServiceStats)

	if err = tsv.startRequest(target, session.SessionId, false, true); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, tsv.QueryTimeout.Get())
	defer func() {
		tsv.qe.queryServiceStats.QueryStats.Record("COMMIT", time.Now())
		cancel()
		tsv.endRequest(false)
	}()

	tsv.qe.Commit(ctx, logStats, session.TransactionId)
	return nil
}

// Rollback rollsback the specified transaction.
func (tsv *TabletServer) Rollback(ctx context.Context, target *querypb.Target, session *proto.Session) (err error) {
	logStats := newLogStats("Rollback", ctx)
	logStats.OriginalSQL = "rollback"
	logStats.TransactionID = session.TransactionId
	defer handleError(&err, logStats, tsv.qe.queryServiceStats)

	if err = tsv.startRequest(target, session.SessionId, false, true); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, tsv.QueryTimeout.Get())
	defer func() {
		tsv.qe.queryServiceStats.QueryStats.Record("ROLLBACK", time.Now())
		cancel()
		tsv.endRequest(false)
	}()

	tsv.qe.txPool.Rollback(ctx, session.TransactionId)
	return nil
}

// handleExecError handles panics during query execution and sets
// the supplied error return value.
func (tsv *TabletServer) handleExecError(query *proto.Query, err *error, logStats *LogStats) {
	if x := recover(); x != nil {
		*err = tsv.handleExecErrorNoPanic(query, x, logStats)
	}
	if logStats != nil {
		logStats.Send()
	}
}

func (tsv *TabletServer) handleExecErrorNoPanic(query *proto.Query, err interface{}, logStats *LogStats) error {
	var terr *TabletError
	defer func() {
		if logStats != nil {
			logStats.Error = terr
		}
	}()
	terr, ok := err.(*TabletError)
	if !ok {
		log.Errorf("Uncaught panic for %v:\n%v\n%s", query, err, tb.Stack(4))
		tsv.qe.queryServiceStats.InternalErrors.Add("Panic", 1)
		terr = NewTabletError(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, "%v: uncaught panic for %v", err, query)
		return terr
	}
	var myError error
	if tsv.config.TerseErrors && terr.SQLError != 0 && len(query.BindVariables) != 0 {
		myError = &TabletError{
			ErrorType: terr.ErrorType,
			SQLError:  terr.SQLError,
			ErrorCode: terr.ErrorCode,
			Message:   fmt.Sprintf("(errno %d) during query: %s", terr.SQLError, query.Sql),
		}
	} else {
		myError = terr
	}
	terr.RecordStats(tsv.qe.queryServiceStats)

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
	case mysql.ErrLockWaitTimeout, mysql.ErrLockDeadlock, mysql.ErrDataTooLong,
		mysql.ErrDataOutOfRange, mysql.ErrBadNullError:
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
func (tsv *TabletServer) Execute(ctx context.Context, target *querypb.Target, query *proto.Query, reply *sqltypes.Result) (err error) {
	logStats := newLogStats("Execute", ctx)
	defer tsv.handleExecError(query, &err, logStats)

	allowShutdown := (query.TransactionId != 0)
	if err = tsv.startRequest(target, query.SessionId, false, allowShutdown); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, tsv.QueryTimeout.Get())
	defer func() {
		cancel()
		tsv.endRequest(false)
	}()

	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	qre := &QueryExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          tsv.qe.schemaInfo.GetPlan(ctx, logStats, query.Sql),
		ctx:           ctx,
		logStats:      logStats,
		qe:            tsv.qe,
	}
	result, err := qre.Execute()
	if err != nil {
		return tsv.handleExecErrorNoPanic(query, err, logStats)
	}
	*reply = *result
	return nil
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (tsv *TabletServer) StreamExecute(ctx context.Context, target *querypb.Target, query *proto.Query, sendReply func(*sqltypes.Result) error) (err error) {
	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "Transactions not supported with streaming")
	}

	logStats := newLogStats("StreamExecute", ctx)
	defer tsv.handleExecError(query, &err, logStats)

	if err = tsv.startRequest(target, query.SessionId, false, false); err != nil {
		return err
	}
	defer tsv.endRequest(false)

	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	stripTrailing(query)
	qre := &QueryExecutor{
		query:         query.Sql,
		bindVars:      query.BindVariables,
		transactionID: query.TransactionId,
		plan:          tsv.qe.schemaInfo.GetStreamPlan(query.Sql),
		ctx:           ctx,
		logStats:      logStats,
		qe:            tsv.qe,
	}
	err = qre.Stream(sendReply)
	if err != nil {
		return tsv.handleExecErrorNoPanic(query, err, logStats)
	}
	return nil
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can be called with
// the AsTransaction flag which will execute all statements inside an independent
// transaction. If AsTransaction is true, TransactionId must be 0.
func (tsv *TabletServer) ExecuteBatch(ctx context.Context, target *querypb.Target, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	if len(queryList.Queries) == 0 {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "Empty query list")
	}
	if queryList.AsTransaction && queryList.TransactionId != 0 {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "cannot start a new transaction in the scope of an existing one")
	}

	allowShutdown := (queryList.TransactionId != 0)
	if err = tsv.startRequest(target, queryList.SessionId, false, allowShutdown); err != nil {
		return err
	}
	defer tsv.endRequest(false)
	defer handleError(&err, nil, tsv.qe.queryServiceStats)

	session := proto.Session{
		TransactionId: queryList.TransactionId,
		SessionId:     queryList.SessionId,
	}
	if queryList.AsTransaction {
		var txInfo proto.TransactionInfo
		if err = tsv.Begin(ctx, target, &session, &txInfo); err != nil {
			return err
		}
		session.TransactionId = txInfo.TransactionId
		// If transaction was not committed by the end, it means
		// that there was an error, roll it back.
		defer func() {
			if session.TransactionId != 0 {
				tsv.Rollback(ctx, target, &session)
			}
		}()
	}
	reply.List = make([]sqltypes.Result, 0, len(queryList.Queries))
	for _, bound := range queryList.Queries {
		query := proto.Query{
			Sql:           bound.Sql,
			BindVariables: bound.BindVariables,
			TransactionId: session.TransactionId,
			SessionId:     session.SessionId,
		}
		var localReply sqltypes.Result
		if err = tsv.Execute(ctx, target, &query, &localReply); err != nil {
			return err
		}
		reply.List = append(reply.List, localReply)
	}
	if queryList.AsTransaction {
		if err = tsv.Commit(ctx, target, &session); err != nil {
			session.TransactionId = 0
			return err
		}
		session.TransactionId = 0
	}
	return nil
}

// SplitQuery splits a BoundQuery into smaller queries that return a subset of rows from the original query.
func (tsv *TabletServer) SplitQuery(ctx context.Context, target *querypb.Target, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	logStats := newLogStats("SplitQuery", ctx)
	defer handleError(&err, logStats, tsv.qe.queryServiceStats)
	if err = tsv.startRequest(target, req.SessionID, false, false); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, tsv.QueryTimeout.Get())
	defer func() {
		cancel()
		tsv.endRequest(false)
	}()

	splitter := NewQuerySplitter(&(req.Query), req.SplitColumn, req.SplitCount, tsv.qe.schemaInfo)
	err = splitter.validateQuery()
	if err != nil {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "splitQuery: query validation error: %s, request: %#v", err, req)
	}

	defer func(start time.Time) {
		addUserTableQueryStats(tsv.qe.queryServiceStats, ctx, splitter.tableName, "SplitQuery", int64(time.Now().Sub(start)))
	}(time.Now())

	qre := &QueryExecutor{
		ctx:      ctx,
		logStats: logStats,
		qe:       tsv.qe,
	}
	columnType, err := getColumnType(qre, splitter.splitColumn, splitter.tableName)
	if err != nil {
		return err
	}
	var pkMinMax *sqltypes.Result
	if sqltypes.IsIntegral(columnType) {
		pkMinMax, err = getColumnMinMax(qre, splitter.splitColumn, splitter.tableName)
		if err != nil {
			return err
		}
	}
	reply.Queries, err = splitter.split(columnType, pkMinMax)
	if err != nil {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "splitQuery: query split error: %s, request: %#v", err, req)
	}
	return nil
}

// StreamHealthRegister is part of queryservice.QueryService interface
func (tsv *TabletServer) StreamHealthRegister(c chan<- *querypb.StreamHealthResponse) (int, error) {
	tsv.streamHealthMutex.Lock()
	defer tsv.streamHealthMutex.Unlock()

	id := tsv.streamHealthIndex
	tsv.streamHealthIndex++
	tsv.streamHealthMap[id] = c
	if tsv.lastStreamHealthResponse != nil {
		c <- tsv.lastStreamHealthResponse
	}
	return id, nil
}

// StreamHealthUnregister is part of queryservice.QueryService interface
func (tsv *TabletServer) StreamHealthUnregister(id int) error {
	tsv.streamHealthMutex.Lock()
	defer tsv.streamHealthMutex.Unlock()

	delete(tsv.streamHealthMap, id)
	return nil
}

// HandlePanic is part of the queryservice.QueryService interface
func (tsv *TabletServer) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// BroadcastHealth will broadcast the current health to all listeners
func (tsv *TabletServer) BroadcastHealth(terTimestamp int64, stats *querypb.RealtimeStats) {
	tsv.mu.Lock()
	target := tsv.target
	tsv.mu.Unlock()
	shr := &querypb.StreamHealthResponse{
		Target:  &target,
		Serving: tsv.IsServing(),
		TabletExternallyReparentedTimestamp: terTimestamp,
		RealtimeStats:                       stats,
	}

	tsv.streamHealthMutex.Lock()
	defer tsv.streamHealthMutex.Unlock()
	for _, c := range tsv.streamHealthMap {
		// do not block on any write
		select {
		case c <- shr:
		default:
		}
	}
	tsv.lastStreamHealthResponse = shr
}

// startRequest validates the current state and sessionID and registers
// the request (a waitgroup) as started. Every startRequest requires one
// and only one corresponding endRequest. When the service shuts down,
// StopService will wait on this waitgroup to ensure that there are
// no requests in flight.
func (tsv *TabletServer) startRequest(target *querypb.Target, sessionID int64, isBegin, allowShutdown bool) (err error) {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state == StateServing {
		goto verifySession
	}
	if (isBegin || allowShutdown) && tsv.state == StateShuttingDown {
		goto verifySession
	}
	return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "operation not allowed in state %s", stateName[tsv.state])

verifySession:
	if target != nil {
		// a valid target can be used instead of a valid session
		if target.Keyspace != tsv.target.Keyspace {
			return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Invalid keyspace %v", target.Keyspace)
		}
		if target.Shard != tsv.target.Shard {
			return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Invalid shard %v", target.Shard)
		}
		if target.TabletType != tsv.target.TabletType {
			for _, otherType := range tsv.alsoAllow {
				if target.TabletType == otherType {
					goto ok
				}
			}
			return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Invalid tablet type: %v, want: %v or %v", target.TabletType, tsv.target.TabletType, tsv.alsoAllow)
		}
		goto ok
	}
	if sessionID != tsv.sessionID {
		return NewTabletError(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Invalid session Id %v", sessionID)
	}

ok:
	tsv.requests.Add(1)
	// If it's a begin, we should make the shutdown code
	// wait for the call to end before it waits for tx empty.
	if isBegin {
		tsv.begins.Add(1)
	}
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (tsv *TabletServer) endRequest(isBegin bool) {
	tsv.requests.Done()
	if isBegin {
		tsv.begins.Done()
	}
}

func (tsv *TabletServer) registerDebugHealthHandler() {
	http.HandleFunc("/debug/health", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
			acl.SendError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		if err := tsv.IsHealthy(); err != nil {
			w.Write([]byte("not ok"))
			return
		}
		w.Write([]byte("ok"))
	})
}

func (tsv *TabletServer) registerQueryzHandler() {
	http.HandleFunc("/queryz", func(w http.ResponseWriter, r *http.Request) {
		queryzHandler(tsv.qe.schemaInfo, w, r)
	})
}

func (tsv *TabletServer) registerStreamQueryzHandlers() {
	http.HandleFunc("/streamqueryz", func(w http.ResponseWriter, r *http.Request) {
		streamQueryzHandler(tsv.qe.streamQList, w, r)
	})
	http.HandleFunc("/streamqueryz/terminate", func(w http.ResponseWriter, r *http.Request) {
		streamQueryzTerminateHandler(tsv.qe.streamQList, w, r)
	})
}

func (tsv *TabletServer) registerSchemazHandler() {
	http.HandleFunc("/schemaz", func(w http.ResponseWriter, r *http.Request) {
		schemazHandler(tsv.qe.schemaInfo.GetSchema(), w, r)
	})
}

// SetPoolSize changes the pool size to the specified value.
func (tsv *TabletServer) SetPoolSize(val int) {
	tsv.qe.connPool.SetCapacity(val)
}

// PoolSize returns the pool size.
func (tsv *TabletServer) PoolSize() int {
	return int(tsv.qe.connPool.Capacity())
}

// SetStreamPoolSize changes the pool size to the specified value.
func (tsv *TabletServer) SetStreamPoolSize(val int) {
	tsv.qe.streamConnPool.SetCapacity(val)
}

// StreamPoolSize returns the pool size.
func (tsv *TabletServer) StreamPoolSize() int {
	return int(tsv.qe.streamConnPool.Capacity())
}

// SetTxPoolSize changes the tx pool size to the specified value.
func (tsv *TabletServer) SetTxPoolSize(val int) {
	tsv.qe.txPool.pool.SetCapacity(val)
}

// TxPoolSize returns the tx pool size.
func (tsv *TabletServer) TxPoolSize() int {
	return int(tsv.qe.txPool.pool.Capacity())
}

// SetTxTimeout changes the transaction timeout to the specified value.
func (tsv *TabletServer) SetTxTimeout(val time.Duration) {
	tsv.qe.txPool.SetTimeout(val)
}

// TxTimeout returns the transaction timeout.
func (tsv *TabletServer) TxTimeout() time.Duration {
	return tsv.qe.txPool.Timeout()
}

// SetQueryCacheCap changes the pool size to the specified value.
func (tsv *TabletServer) SetQueryCacheCap(val int) {
	tsv.qe.schemaInfo.SetQueryCacheCap(val)
}

// QueryCacheCap returns the pool size.
func (tsv *TabletServer) QueryCacheCap() int {
	return int(tsv.qe.schemaInfo.QueryCacheCap())
}

// SetStrictMode sets strict mode on or off.
func (tsv *TabletServer) SetStrictMode(strict bool) {
	if strict {
		tsv.qe.strictMode.Set(1)
	} else {
		tsv.qe.strictMode.Set(0)
	}
}

// SetAutoCommit sets autocommit on or off.
func (tsv *TabletServer) SetAutoCommit(auto bool) {
	if auto {
		tsv.qe.autoCommit.Set(1)
	} else {
		tsv.qe.autoCommit.Set(0)
	}
}

// SetMaxResultSize changes the max result size to the specified value.
func (tsv *TabletServer) SetMaxResultSize(val int) {
	tsv.qe.maxResultSize.Set(int64(val))
}

// MaxResultSize returns the max result size.
func (tsv *TabletServer) MaxResultSize() int {
	return int(tsv.qe.maxResultSize.Get())
}

// SetMaxDMLRows changes the max result size to the specified value.
func (tsv *TabletServer) SetMaxDMLRows(val int) {
	tsv.qe.maxDMLRows.Set(int64(val))
}

// MaxDMLRows returns the max result size.
func (tsv *TabletServer) MaxDMLRows() int {
	return int(tsv.qe.maxDMLRows.Get())
}

// SetSpotCheckRatio sets the spot check ration.
func (tsv *TabletServer) SetSpotCheckRatio(val float64) {
	tsv.qe.spotCheckFreq.Set(int64(val * spotCheckMultiplier))
}

// SpotCheckRatio returns the spot check ratio.
func (tsv *TabletServer) SpotCheckRatio() float64 {
	return float64(tsv.qe.spotCheckFreq.Get()) / spotCheckMultiplier
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

func getColumnType(qre *QueryExecutor, columnName, tableName string) (querypb.Type, error) {
	conn, err := qre.getConn(qre.qe.connPool)
	if err != nil {
		return sqltypes.Null, err
	}
	defer conn.Recycle()
	// TODO(shengzhe): use AST to represent the query to avoid sql injection.
	// current code is safe because QuerySplitter.validateQuery is called before
	// calling this.
	query := fmt.Sprintf("SELECT %v FROM %v LIMIT 0", columnName, tableName)
	result, err := qre.execSQL(conn, query, true)
	if err != nil {
		return sqltypes.Null, err
	}
	if result == nil || len(result.Fields) != 1 {
		return sqltypes.Null, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "failed to get column type for column: %v, invalid result: %v", columnName, result)
	}
	return result.Fields[0].Type, nil
}

func getColumnMinMax(qre *QueryExecutor, columnName, tableName string) (*sqltypes.Result, error) {
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
