// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/utils"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/messager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/splitquery"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/txserializer"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/txthrottler"

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
	// In order to achieve clean transitions, no requests are
	// allowed during this state.
	StateTransitioning
	// StateShuttingDown indicates that the tabletserver
	// is shutting down. In this state, we wait for outstanding
	// requests and transactions to conclude.
	StateShuttingDown
)

// logTxPoolFull is for throttling txpool full messages in the log.
var logTxPoolFull = logutil.NewThrottledLogger("TxPoolFull", 1*time.Minute)

var logComputeRowSerializerKey = logutil.NewThrottledLogger("ComputeRowSerializerKey", 1*time.Minute)

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
	QueryTimeout           sync2.AtomicDuration
	BeginTimeout           sync2.AtomicDuration
	TerseErrors            bool
	enableHotRowProtection bool

	// mu is used to access state. The lock should only be held
	// for short periods. For longer periods, you have to transition
	// the state to a transient value and release the lock.
	// Once the operation is complete, you can then transition
	// the state back to a stable value.
	// The lameduck mode causes tablet server to respond as unhealthy
	// for health checks. This does not affect how queries are served.
	// target specifies the primary target type, and also allow specifies
	// secondary types that should be additionally allowed.
	mu         sync.Mutex
	state      int64
	lameduck   sync2.AtomicInt32
	target     querypb.Target
	alsoAllow  []topodatapb.TabletType
	requests   sync.WaitGroup
	txRequests sync.WaitGroup

	// The following variables should be initialized only once
	// before starting the tabletserver.
	dbconfigs dbconfigs.DBConfigs
	mysqld    mysqlctl.MysqlDaemon

	// The following variables should only be accessed within
	// the context of a startRequest-endRequest.
	se               *schema.Engine
	qe               *QueryEngine
	te               *TxEngine
	messager         *messager.Engine
	watcher          *ReplicationWatcher
	updateStreamList *binlog.StreamList

	// checkMySQLThrottler is used to throttle the number of
	// requests sent to CheckMySQL.
	checkMySQLThrottler *sync2.Semaphore

	// txThrottler is used to throttle transactions based on the observed replication lag.
	txThrottler *txthrottler.TxThrottler
	topoServer  topo.Server

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

// NewServer creates a new TabletServer based on the command line flags.
func NewServer(topoServer topo.Server) *TabletServer {
	return NewTabletServer(tabletenv.Config, topoServer)
}

var tsOnce sync.Once

// NewTabletServerWithNilTopoServer is typically used in tests that don't need a topoSever
// member.
func NewTabletServerWithNilTopoServer(config tabletenv.TabletConfig) *TabletServer {
	return NewTabletServer(config, topo.Server{})
}

// NewTabletServer creates an instance of TabletServer. Only the first
// instance of TabletServer will expose its state variables.
func NewTabletServer(config tabletenv.TabletConfig, topoServer topo.Server) *TabletServer {
	tsv := &TabletServer{
		QueryTimeout:           sync2.NewAtomicDuration(time.Duration(config.QueryTimeout * 1e9)),
		BeginTimeout:           sync2.NewAtomicDuration(time.Duration(config.TxPoolTimeout * 1e9)),
		TerseErrors:            config.TerseErrors,
		enableHotRowProtection: config.EnableHotRowProtection || config.EnableHotRowProtectionDryRun,
		checkMySQLThrottler:    sync2.NewSemaphore(1, 0),
		streamHealthMap:        make(map[int]chan<- *querypb.StreamHealthResponse),
		history:                history.New(10),
		topoServer:             topoServer,
	}
	tsv.se = schema.NewEngine(tsv, config)
	tsv.qe = NewQueryEngine(tsv, tsv.se, config)
	tsv.te = NewTxEngine(tsv, config)
	tsv.txThrottler = txthrottler.CreateTxThrottlerFromTabletConfig(topoServer)
	tsv.messager = messager.NewEngine(tsv, tsv.se, config)
	tsv.watcher = NewReplicationWatcher(tsv.se, config)
	tsv.updateStreamList = &binlog.StreamList{}
	// FIXME(alainjobart) could we move this to the Register method below?
	// So that vtcombo doesn't even call it once, on the first tablet.
	// And we can remove the tsOnce variable.
	tsOnce.Do(func() {
		stats.Publish("TabletState", stats.IntFunc(func() int64 {
			tsv.mu.Lock()
			state := tsv.state
			tsv.mu.Unlock()
			return state
		}))
		stats.Publish("QueryTimeout", stats.DurationFunc(tsv.QueryTimeout.Get))
		stats.Publish("BeginTimeout", stats.DurationFunc(tsv.BeginTimeout.Get))
		stats.Publish("TabletStateName", stats.StringFunc(tsv.GetState))
	})
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
	tsv.registerStreamQueryzHandlers()
	tsv.registerTwopczHandler()
}

// RegisterQueryRuleSource registers ruleSource for setting query rules.
func (tsv *TabletServer) RegisterQueryRuleSource(ruleSource string) {
	tsv.qe.queryRuleSources.RegisterSource(ruleSource)
}

// UnRegisterQueryRuleSource unregisters ruleSource from query rules.
func (tsv *TabletServer) UnRegisterQueryRuleSource(ruleSource string) {
	tsv.qe.queryRuleSources.UnRegisterSource(ruleSource)
}

// SetQueryRules sets the query rules for a registered ruleSource.
func (tsv *TabletServer) SetQueryRules(ruleSource string, qrs *rules.Rules) error {
	err := tsv.qe.queryRuleSources.SetRules(ruleSource, qrs)
	if err != nil {
		return err
	}
	tsv.qe.ClearQueryPlanCache()
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
	log.Infof("TabletServer state: %s -> %s", stateName[tsv.state], stateName[state])
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
// calling SetServingType.
func (tsv *TabletServer) InitDBConfig(target querypb.Target, dbconfigs dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) error {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state != StateNotConnected {
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "InitDBConfig failed, current state: %s", stateName[tsv.state])
	}
	tsv.target = target
	tsv.dbconfigs = dbconfigs
	// Massage Dba so that it inherits the
	// App values but keeps the credentials.
	tsv.dbconfigs.Dba = dbconfigs.App
	if n, p := dbconfigs.Dba.Uname, dbconfigs.Dba.Pass; n != "" {
		tsv.dbconfigs.Dba.Uname = n
		tsv.dbconfigs.Dba.Pass = p
	}
	tsv.mysqld = mysqld
	return nil
}

// StartService is a convenience function for InitDBConfig->SetServingType
// with serving=true.
func (tsv *TabletServer) StartService(target querypb.Target, dbconfigs dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) (err error) {
	// Save tablet type away to prevent data races
	tabletType := target.TabletType
	err = tsv.InitDBConfig(target, dbconfigs, mysqld)
	if err != nil {
		return err
	}
	_ /* state changed */, err = tsv.SetServingType(tabletType, true, nil)
	return err
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
// Returns true if the state of QueryService or the tablet type changed.
func (tsv *TabletServer) SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	defer tsv.ExitLameduck()

	action, err := tsv.decideAction(tabletType, serving, alsoAllow)
	if err != nil {
		return false, err
	}
	switch action {
	case actionNone:
		return false, nil
	case actionFullStart:
		if err := tsv.fullStart(); err != nil {
			tsv.closeAll()
			return true, err
		}
		return true, nil
	case actionServeNewType:
		if err := tsv.serveNewType(); err != nil {
			tsv.closeAll()
			return true, err
		}
		return true, nil
	case actionGracefulStop:
		tsv.gracefulStop()
		return true, nil
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
		return actionNone, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot SetServingType, current state: %s", stateName[tsv.state])
	default:
		panic("unreachable")
	}
	return actionNone, nil
}

func (tsv *TabletServer) fullStart() (err error) {
	c, err := dbconnpool.NewDBConnection(&tsv.dbconfigs.App, tabletenv.MySQLStats)
	if err != nil {
		return err
	}
	c.Close()

	if err := tsv.se.Open(&tsv.dbconfigs.Dba); err != nil {
		return err
	}
	if err := tsv.qe.Open(tsv.dbconfigs); err != nil {
		return err
	}
	if err := tsv.te.Init(tsv.dbconfigs); err != nil {
		return err
	}
	tsv.updateStreamList.Init()
	return tsv.serveNewType()
}

func (tsv *TabletServer) serveNewType() (err error) {
	if tsv.target.TabletType == topodatapb.TabletType_MASTER {
		if err := tsv.txThrottler.Open(tsv.target.Keyspace, tsv.target.Shard); err != nil {
			return err
		}
		tsv.watcher.Close()
		tsv.te.Open(tsv.dbconfigs)
		tsv.messager.Open(tsv.dbconfigs)
	} else {
		tsv.messager.Close()
		// Wait for in-flight transactional requests to complete
		// before rolling back everything. In this state new
		// transactional requests are not allowed. So, we can
		// be sure that the tx pool won't change after the wait.
		tsv.txRequests.Wait()
		tsv.te.Close(true)
		tsv.watcher.Open(tsv.dbconfigs, tsv.mysqld)
		tsv.txThrottler.Close()
	}
	tsv.transition(StateServing)
	return nil
}

func (tsv *TabletServer) gracefulStop() {
	defer close(tsv.setTimeBomb())
	tsv.waitForShutdown()
	tsv.transition(StateNotServing)
}

// StopService shuts down the tabletserver to the uninitialized state.
// It first transitions to StateShuttingDown, then waits for active
// services to shut down. Then it shuts down QueryEngine. This function
// should be called before process termination, or if MySQL is unreachable.
// Under normal circumstances, SetServingType should be called, which will
// keep QueryEngine open.
func (tsv *TabletServer) StopService() {
	defer close(tsv.setTimeBomb())
	defer tabletenv.LogError()

	tsv.mu.Lock()
	if tsv.state != StateServing && tsv.state != StateNotServing {
		tsv.mu.Unlock()
		return
	}
	tsv.setState(StateShuttingDown)
	tsv.mu.Unlock()

	log.Infof("Executing complete shutdown.")
	tsv.waitForShutdown()
	tsv.qe.Close()
	tsv.se.Close()
	log.Infof("Shutdown complete.")
	tsv.transition(StateNotConnected)
}

func (tsv *TabletServer) waitForShutdown() {
	// Wait till txRequests have completed before waiting on tx pool.
	// During this state, new Begins are not allowed. After the wait,
	// we have the assurance that only non-begin transactional calls
	// will be allowed. They will enable the conclusion of outstanding
	// transactions.
	tsv.txRequests.Wait()
	tsv.messager.Close()
	tsv.te.Close(false)
	tsv.qe.streamQList.TerminateAll()
	tsv.updateStreamList.Stop()
	tsv.watcher.Close()
	tsv.requests.Wait()
	tsv.txThrottler.Close()
}

// closeAll is called if TabletServer fails to start.
// It forcibly shuts down everything.
func (tsv *TabletServer) closeAll() {
	tsv.messager.Close()
	tsv.te.Close(true)
	tsv.watcher.Close()
	tsv.updateStreamList.Stop()
	tsv.qe.Close()
	tsv.se.Close()
	tsv.txThrottler.Close()
	tsv.transition(StateNotConnected)
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
	_, err := tsv.Execute(
		tabletenv.LocalContext(),
		nil,
		"select 1 from dual",
		nil,
		0,
		nil,
	)
	return err
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
			tabletenv.LogError()
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
func (tsv *TabletServer) ReloadSchema(ctx context.Context) error {
	tsv.se.Reload(ctx)
	return nil
}

// ClearQueryPlanCache clears internal query plan cache
func (tsv *TabletServer) ClearQueryPlanCache() {
	// We should ideally bracket this with start & endErequest,
	// but query plan cache clearing is safe to call even if the
	// tabletserver is down.
	tsv.qe.ClearQueryPlanCache()
}

// QueryService returns the QueryService part of TabletServer.
func (tsv *TabletServer) QueryService() queryservice.QueryService {
	return tsv
}

// SchemaEngine returns the SchemaEngine part of TabletServer.
func (tsv *TabletServer) SchemaEngine() *schema.Engine {
	return tsv.se
}

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (tsv *TabletServer) Begin(ctx context.Context, target *querypb.Target) (transactionID int64, err error) {
	err = tsv.execRequest(
		ctx, tsv.BeginTimeout.Get(),
		"Begin", "begin", nil,
		target, true, false,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tabletenv.QueryStats.Record("BEGIN", time.Now())
			if tsv.txThrottler.Throttle() {
				// TODO(erez): I think this should be RESOURCE_EXHAUSTED.
				return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "Transaction throttled")
			}
			transactionID, err = tsv.te.txPool.Begin(ctx)
			logStats.TransactionID = transactionID
			return err
		},
	)
	return transactionID, err
}

// Commit commits the specified transaction.
func (tsv *TabletServer) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Commit", "commit", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tabletenv.QueryStats.Record("COMMIT", time.Now())
			logStats.TransactionID = transactionID
			return tsv.te.txPool.Commit(ctx, transactionID, tsv.messager)
		},
	)
}

// Rollback rollsback the specified transaction.
func (tsv *TabletServer) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Rollback", "rollback", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tabletenv.QueryStats.Record("ROLLBACK", time.Now())
			logStats.TransactionID = transactionID
			return tsv.te.txPool.Rollback(ctx, transactionID)
		},
	)
}

// Prepare prepares the specified transaction.
func (tsv *TabletServer) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Prepare", "prepare", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.Prepare(transactionID, dtid)
		},
	)
}

// CommitPrepared commits the prepared transaction.
func (tsv *TabletServer) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"CommitPrepared", "commit_prepared", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.CommitPrepared(dtid)
		},
	)
}

// RollbackPrepared commits the prepared transaction.
func (tsv *TabletServer) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"RollbackPrepared", "rollback_prepared", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.RollbackPrepared(dtid, originalID)
		},
	)
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (tsv *TabletServer) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"CreateTransaction", "create_transaction", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.CreateTransaction(dtid, participants)
		},
	)
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (tsv *TabletServer) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"StartCommit", "start_commit", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.StartCommit(transactionID, dtid)
		},
	)
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (tsv *TabletServer) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"SetRollback", "set_rollback", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.SetRollback(dtid, transactionID)
		},
	)
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (tsv *TabletServer) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"ConcludeTransaction", "conclude_transaction", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			return txe.ConcludeTransaction(dtid)
		},
	)
}

// ReadTransaction returns the metadata for the sepcified dtid.
func (tsv *TabletServer) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"ReadTransaction", "read_transaction", nil,
		target, true, true,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
				messager: tsv.messager,
			}
			metadata, err = txe.ReadTransaction(dtid)
			return err
		},
	)
	return metadata, err
}

// Execute executes the query and returns the result as response.
func (tsv *TabletServer) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (result *sqltypes.Result, err error) {
	allowOnShutdown := (transactionID != 0)
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Execute", sql, bindVariables,
		target, false, allowOnShutdown,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]interface{})
			}
			sql = stripTrailing(sql, bindVariables)
			plan, err := tsv.qe.GetPlan(ctx, logStats, sql)
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:         sql,
				bindVars:      bindVariables,
				transactionID: transactionID,
				plan:          plan,
				ctx:           ctx,
				logStats:      logStats,
				tsv:           tsv,
			}
			extras := tsv.watcher.ComputeExtras(options)
			result, err = qre.Execute()
			if err != nil {
				return err
			}
			result.Extras = extras
			result = result.StripMetadata(sqltypes.IncludeFieldsOrDefault(options))
			return nil
		},
	)
	return result, err
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (tsv *TabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (err error) {
	return tsv.execRequest(
		ctx, 0,
		"StreamExecute", sql, bindVariables,
		target, false, false,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]interface{})
			}
			sql = stripTrailing(sql, bindVariables)
			plan, err := tsv.qe.GetStreamPlan(sql)
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:    sql,
				bindVars: bindVariables,
				plan:     plan,
				ctx:      ctx,
				logStats: logStats,
				tsv:      tsv,
			}
			return qre.Stream(sqltypes.IncludeFieldsOrDefault(options), callback)
		},
	)
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can be called with
// the AsTransaction flag which will execute all statements inside an independent
// transaction. If AsTransaction is true, TransactionId must be 0.
func (tsv *TabletServer) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (results []sqltypes.Result, err error) {
	if len(queries) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Empty query list")
	}
	if asTransaction && transactionID != 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot start a new transaction in the scope of an existing one")
	}

	allowOnShutdown := (transactionID != 0)
	if err = tsv.startRequest(ctx, target, false, allowOnShutdown); err != nil {
		return nil, err
	}
	defer tsv.endRequest(false)
	defer tsv.handlePanicAndSendLogStats("batch", nil, &err, nil)

	if asTransaction {
		transactionID, err = tsv.Begin(ctx, target)
		if err != nil {
			return nil, tsv.convertAndLogError("batch", nil, err, nil)
		}
		// If transaction was not committed by the end, it means
		// that there was an error, roll it back.
		defer func() {
			if transactionID != 0 {
				tsv.Rollback(ctx, target, transactionID)
			}
		}()
	}
	results = make([]sqltypes.Result, 0, len(queries))
	for _, bound := range queries {
		localReply, err := tsv.Execute(ctx, target, bound.Sql, bound.BindVariables, transactionID, options)
		if err != nil {
			return nil, tsv.convertAndLogError("batch", nil, err, nil)
		}
		results = append(results, *localReply)
	}
	if asTransaction {
		if err = tsv.Commit(ctx, target, transactionID); err != nil {
			transactionID = 0
			return nil, tsv.convertAndLogError("batch", nil, err, nil)
		}
		transactionID = 0
	}
	return results, nil
}

// BeginExecute combines Begin and Execute.
func (tsv *TabletServer) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	if tsv.enableHotRowProtection {
		txDone, err := tsv.beginWaitForSameRangeTransactions(ctx, target, sql, bindVariables)
		if err != nil {
			return nil, 0, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	transactionID, err := tsv.Begin(ctx, target)
	if err != nil {
		return nil, 0, err
	}

	result, err := tsv.Execute(ctx, target, sql, bindVariables, transactionID, options)
	return result, transactionID, err
}

func (tsv *TabletServer) beginWaitForSameRangeTransactions(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}) (txserializer.DoneFunc, error) {
	// Serialize the creation of new transactions *if* the first
	// UPDATE or DELETE query has the same WHERE clause as a query which is
	// already running in a transaction (only other BeginExecute() calls are
	// considered). This avoids exhausting all txpool slots due to a hot row.
	//
	// Known Issue: There can be more than one transaction pool slot in use for
	// the same row because the next transaction is unblocked after this
	// BeginExecute() call is done and before Commit() on this transaction has
	// been called. Due to the additional MySQL locking, this should result into
	// two transaction pool slots per row at most. (This transaction pending on
	// COMMIT, the next one waiting for MySQL in BEGIN+EXECUTE.)
	var txDone txserializer.DoneFunc

	err := tsv.execRequest(
		// Use (potentially longer) -queryserver-config-query-timeout and not
		// -queryserver-config-txpool-timeout (defaults to 1s) to limit the waiting.
		ctx, tsv.QueryTimeout.Get(),
		"", "waitForSameRangeTransactions", nil,
		target, true /* isTx */, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			k, table := tsv.computeTxSerializerKey(ctx, logStats, sql, bindVariables)
			if k == "" {
				// Query is not subject to tx serialization/hot row protection.
				return nil
			}

			startTime := time.Now()
			done, waited, waitErr := tsv.qe.txSerializer.Wait(ctx, k, table)
			txDone = done
			if waited {
				tabletenv.WaitStats.Record("TxSerializer", startTime)
			}

			return waitErr
		})
	return txDone, err
}

// computeTxSerializerKey returns a unique string ("key") used to determine
// whether two queries would update the same row (range).
// Additionally, it returns the table name (needed for updating stats vars).
// It returns an empty string as key if the row (range) cannot be parsed from
// the query and bind variables or the table name is empty.
func (tsv *TabletServer) computeTxSerializerKey(ctx context.Context, logStats *tabletenv.LogStats, sql string, bindVariables map[string]interface{}) (string, string) {
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to get plan for query: %v err: %v", sql, err)
		return "", ""
	}

	if plan.PlanID != planbuilder.PlanDMLPK && plan.PlanID != planbuilder.PlanDMLSubquery {
		// Serialize only UPDATE or DELETE queries.
		return "", ""
	}

	tableName := plan.TableName()
	if tableName.IsEmpty() {
		// Do not serialize any queries without a table name.
		return "", ""
	}

	where, err := plan.WhereClause.GenerateQuery(bindVariables)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to substitute bind vars in where clause: %v query: %v bind vars: %v", err, sql, bindVariables)
		return "", ""
	}

	// Example: table1 where id = 1 and sub_id = 2
	key := fmt.Sprintf("%s%s", tableName, hack.String(where))
	return key, tableName.String()
}

// BeginExecuteBatch combines Begin and ExecuteBatch.
func (tsv *TabletServer) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	transactionID, err := tsv.Begin(ctx, target)
	if err != nil {
		return nil, 0, err
	}

	results, err := tsv.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, err
}

// MessageStream streams messages from the requested table.
func (tsv *TabletServer) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) (err error) {
	return tsv.execRequest(
		ctx, 0,
		"MessageStream", "stream", nil,
		target, false, false,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			// TODO(sougou): perform ACL checks.
			done, err := tsv.messager.Subscribe(name, func(r *sqltypes.Result) error {
				select {
				case <-ctx.Done():
					return io.EOF
				default:
				}
				return callback(r)
			})
			if err != nil {
				return err
			}
			<-done
			return nil
		},
	)
}

// MessageAck acks the list of messages for a given message table.
// It returns the number of messages successfully acked.
func (tsv *TabletServer) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	sids := make([]string, 0, len(ids))
	for _, val := range ids {
		v, err := sqltypes.BuildConverted(val.Type, val.Value)
		if err != nil {
			return 0, tsv.convertAndLogError("message_ack", nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid type: %v", err), nil)
		}
		sids = append(sids, v.String())
	}
	return tsv.execDML(ctx, target, func() (string, map[string]interface{}, error) {
		return tsv.messager.GenerateAckQuery(name, sids)
	})
}

// PostponeMessages postpones the list of messages for a given message table.
// It returns the number of messages successfully postponed.
func (tsv *TabletServer) PostponeMessages(ctx context.Context, target *querypb.Target, name string, ids []string) (count int64, err error) {
	return tsv.execDML(ctx, target, func() (string, map[string]interface{}, error) {
		return tsv.messager.GeneratePostponeQuery(name, ids)
	})
}

// PurgeMessages purges messages older than specified time in Unix Nanoseconds.
// It purges at most 500 messages. It returns the number of messages successfully purged.
func (tsv *TabletServer) PurgeMessages(ctx context.Context, target *querypb.Target, name string, timeCutoff int64) (count int64, err error) {
	return tsv.execDML(ctx, target, func() (string, map[string]interface{}, error) {
		return tsv.messager.GeneratePurgeQuery(name, timeCutoff)
	})
}

func (tsv *TabletServer) execDML(ctx context.Context, target *querypb.Target, queryGenerator func() (string, map[string]interface{}, error)) (count int64, err error) {
	if err = tsv.startRequest(ctx, target, true, false); err != nil {
		return 0, err
	}
	defer tsv.endRequest(true)
	defer tsv.handlePanicAndSendLogStats("ack", nil, &err, nil)

	query, bv, err := queryGenerator()
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}

	transactionID, err := tsv.Begin(ctx, target)
	if err != nil {
		return 0, err
	}
	// If transaction was not committed by the end, it means
	// that there was an error, roll it back.
	defer func() {
		if transactionID != 0 {
			tsv.Rollback(ctx, target, transactionID)
		}
	}()
	qr, err := tsv.Execute(ctx, target, query, bv, transactionID, nil)
	if err != nil {
		return 0, err
	}
	if err = tsv.Commit(ctx, target, transactionID); err != nil {
		transactionID = 0
		return 0, err
	}
	transactionID = 0
	return int64(qr.RowsAffected), nil
}

// SplitQuery splits a query + bind variables into smaller queries that return a
// subset of rows from the original query. This is the new version that supports multiple
// split columns and multiple split algortihms.
// See the documentation of SplitQueryRequest in proto/vtgate.proto for more details.
func (tsv *TabletServer) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) (splits []querytypes.QuerySplit, err error) {
	err = tsv.execRequest(
		ctx, 0,
		"SplitQuery", query.Sql, query.BindVariables,
		target, false, false,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			// SplitQuery using the Full Scan algorithm can take a while and
			// we don't expect too many of these queries to run concurrently.
			ciSplitColumns := make([]sqlparser.ColIdent, 0, len(splitColumns))
			for _, s := range splitColumns {
				ciSplitColumns = append(ciSplitColumns, sqlparser.NewColIdent(s))
			}

			if err := validateSplitQueryParameters(
				target,
				query,
				splitColumns,
				splitCount,
				numRowsPerQueryPart,
				algorithm,
			); err != nil {
				return err
			}
			schema := tsv.se.GetSchema()
			splitParams, err := createSplitParams(
				query, ciSplitColumns, splitCount, numRowsPerQueryPart, schema)
			if err != nil {
				return err
			}
			defer func(start time.Time) {
				splitTableName := splitParams.GetSplitTableName()
				tabletenv.RecordUserQuery(ctx, splitTableName, "SplitQuery", int64(time.Now().Sub(start)))
			}(time.Now())
			sqlExecuter, err := newSplitQuerySQLExecuter(ctx, logStats, tsv)
			if err != nil {
				return err
			}
			defer sqlExecuter.done()
			algorithmObject, err := createSplitQueryAlgorithmObject(algorithm, splitParams, sqlExecuter)
			if err != nil {
				return err
			}
			splits, err = splitquery.NewSplitter(splitParams, algorithmObject).Split()
			if err != nil {
				return err
			}
			return nil
		},
	)
	return splits, err
}

// execRequest performs verfications, sets up the necessary environments
// and calls the supplied function for executing the request.
func (tsv *TabletServer) execRequest(
	ctx context.Context, timeout time.Duration,
	requestName, sql string, bindVariables map[string]interface{},
	target *querypb.Target, isTx, allowOnShutdown bool,
	exec func(ctx context.Context, logStats *tabletenv.LogStats) error,
) (err error) {
	logStats := tabletenv.NewLogStats(ctx, requestName)
	logStats.Target = target
	logStats.OriginalSQL = sql
	logStats.BindVariables = bindVariables
	defer tsv.handlePanicAndSendLogStats(sql, bindVariables, &err, logStats)
	if err = tsv.startRequest(ctx, target, isTx, allowOnShutdown); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, timeout)
	defer func() {
		cancel()
		tsv.endRequest(isTx)
	}()

	err = exec(ctx, logStats)
	if err != nil {
		return tsv.convertAndLogError(sql, bindVariables, err, logStats)
	}
	return nil
}

func (tsv *TabletServer) handlePanicAndSendLogStats(
	sql string,
	bindVariables map[string]interface{},
	err *error,
	logStats *tabletenv.LogStats,
) {
	if x := recover(); x != nil {
		errorMessage := fmt.Sprintf(
			"Uncaught panic for %v:\n%v\n%s",
			querytypes.QueryAsString(sql, bindVariables),
			x,
			tb.Stack(4) /* Skip the last 4 boiler-plate frames. */)
		log.Errorf(errorMessage)
		terr := vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "%s", errorMessage)
		*err = terr
		tabletenv.InternalErrors.Add("Panic", 1)
		if logStats != nil {
			logStats.Error = terr
		}
	}
	// Examples where we don't send the log stats:
	// - ExecuteBatch() (logStats == nil)
	// - beginWaitForSameRangeTransactions() (Method == "")
	if logStats != nil && logStats.Method != "" {
		logStats.Send()
	}
}

func (tsv *TabletServer) convertAndLogError(sql string, bindVariables map[string]interface{}, err error, logStats *tabletenv.LogStats) error {
	if err == nil {
		return nil
	}
	err = tsv.convertError(sql, bindVariables, err)

	if logStats != nil {
		logStats.Error = err
	}
	errCode := vterrors.Code(err)
	tabletenv.ErrorStats.Add(errCode.String(), 1)

	logMethod := log.Errorf
	// Suppress or demote some errors in logs.
	switch errCode {
	case vtrpcpb.Code_FAILED_PRECONDITION, vtrpcpb.Code_ALREADY_EXISTS:
		return err
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		logMethod = logTxPoolFull.Errorf
	case vtrpcpb.Code_ABORTED:
		logMethod = log.Warningf
	case vtrpcpb.Code_INVALID_ARGUMENT, vtrpcpb.Code_DEADLINE_EXCEEDED:
		logMethod = log.Infof
	}
	logMethod("%v: %v", err, querytypes.QueryAsString(sql, bindVariables))
	return err
}

func (tsv *TabletServer) convertError(sql string, bindVariables map[string]interface{}, err error) error {
	sqlErr, ok := err.(*sqldb.SQLError)
	if !ok {
		return err
	}

	errCode := vterrors.Code(err)
	errstr := err.Error()
	errnum := sqlErr.Number()
	sqlState := sqlErr.SQLState()
	switch errnum {
	case mysqlconn.EROptionPreventsStatement:
		// Special-case this error code. It's probably because
		// there was a failover and there are old clients still connected.
		if strings.Contains(errstr, "read-only") {
			errCode = vtrpcpb.Code_FAILED_PRECONDITION
		}
	case 1227: // Google internal failover error code.
		if strings.Contains(errstr, "failover in progress") {
			errCode = vtrpcpb.Code_FAILED_PRECONDITION
		}
	case mysqlconn.ERDupEntry:
		errCode = vtrpcpb.Code_ALREADY_EXISTS
	case mysqlconn.ERDataTooLong, mysqlconn.ERDataOutOfRange, mysqlconn.ERBadNullError:
		errCode = vtrpcpb.Code_INVALID_ARGUMENT
	case mysqlconn.ERLockWaitTimeout:
		errCode = vtrpcpb.Code_DEADLINE_EXCEEDED
	case mysqlconn.ERLockDeadlock:
		// A deadlock rolls back the transaction.
		errCode = vtrpcpb.Code_ABORTED
	case mysqlconn.CRServerLost:
		// Query was killed.
		errCode = vtrpcpb.Code_DEADLINE_EXCEEDED
	case mysqlconn.CRServerGone, mysqlconn.ERServerShutdown:
		errCode = vtrpcpb.Code_UNAVAILABLE
	}

	// If TerseErrors is on, strip the error message returned by MySQL and only
	// keep the error number and sql state.
	// We assume that bind variable have PII, which are included in the MySQL
	// query and come back as part of the error message. Removing the MySQL
	// error helps us avoid leaking PII.
	// There are two exceptions:
	// 1. If no bind vars were specified, it's likely that the query was issued
	// by someone manually. So, we don't suppress the error.
	// 2. FAILED_PRECONDITION errors. These are caused when a failover is in progress.
	// If so, we don't want to suppress the error. This will allow VTGate to
	// detect and perform buffering during failovers.
	if tsv.TerseErrors && len(bindVariables) != 0 && errCode != vtrpcpb.Code_FAILED_PRECONDITION {
		errstr = fmt.Sprintf("(errno %d) (sqlstate %s) during query: %s", errnum, sqlState, sql)
	}
	return vterrors.New(errCode, errstr)
}

// validateSplitQueryParameters perform some validations on the SplitQuery parameters
// returns an error that can be returned to the user if a validation fails.
func validateSplitQueryParameters(
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) error {
	// Check that the caller requested a RDONLY tablet.
	// Since we're called by VTGate this should not normally be violated.
	if target.TabletType != topodatapb.TabletType_RDONLY {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"SplitQuery must be called with a RDONLY tablet. TableType passed is: %v",
			target.TabletType)
	}
	if numRowsPerQueryPart < 0 {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitQuery: numRowsPerQueryPart must be non-negative. Got: %v. SQL: %v",
			numRowsPerQueryPart,
			querytypes.QueryAsString(query.Sql, query.BindVariables))
	}
	if splitCount < 0 {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitQuery: splitCount must be non-negative. Got: %v. SQL: %v",
			splitCount,
			querytypes.QueryAsString(query.Sql, query.BindVariables))
	}
	if (splitCount == 0 && numRowsPerQueryPart == 0) ||
		(splitCount != 0 && numRowsPerQueryPart != 0) {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitQuery: exactly one of {numRowsPerQueryPart, splitCount} must be"+
				" non zero. Got: numRowsPerQueryPart=%v, splitCount=%v. SQL: %v",
			numRowsPerQueryPart,
			splitCount,
			querytypes.QueryAsString(query.Sql, query.BindVariables))
	}
	if algorithm != querypb.SplitQueryRequest_EQUAL_SPLITS &&
		algorithm != querypb.SplitQueryRequest_FULL_SCAN {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitquery: unsupported algorithm: %v. SQL: %v",
			algorithm,
			querytypes.QueryAsString(query.Sql, query.BindVariables))
	}
	return nil
}

func createSplitParams(
	query querytypes.BoundQuery,
	splitColumns []sqlparser.ColIdent,
	splitCount int64,
	numRowsPerQueryPart int64,
	schema map[string]*schema.Table,
) (*splitquery.SplitParams, error) {
	switch {
	case numRowsPerQueryPart != 0 && splitCount == 0:
		return splitquery.NewSplitParamsGivenNumRowsPerQueryPart(
			query, splitColumns, numRowsPerQueryPart, schema)
	case numRowsPerQueryPart == 0 && splitCount != 0:
		return splitquery.NewSplitParamsGivenSplitCount(
			query, splitColumns, splitCount, schema)
	default:
		panic(fmt.Errorf("Exactly one of {numRowsPerQueryPart, splitCount} must be"+
			" non zero. This should have already been caught by 'validateSplitQueryParameters' and "+
			" returned as an error. Got: numRowsPerQueryPart=%v, splitCount=%v. SQL: %v",
			numRowsPerQueryPart,
			splitCount,
			querytypes.QueryAsString(query.Sql, query.BindVariables)))
	}
}

// splitQuerySQLExecuter implements splitquery.SQLExecuterInterface and allows the splitquery
// package to send SQL statements to MySQL
type splitQuerySQLExecuter struct {
	queryExecutor *QueryExecutor
	conn          *connpool.DBConn
}

// Constructs a new splitQuerySQLExecuter object. The 'done' method must be called on
// the object after it's no longer used, to recycle the database connection.
func newSplitQuerySQLExecuter(
	ctx context.Context, logStats *tabletenv.LogStats, tsv *TabletServer,
) (*splitQuerySQLExecuter, error) {
	queryExecutor := &QueryExecutor{
		ctx:      ctx,
		logStats: logStats,
		tsv:      tsv,
	}
	result := &splitQuerySQLExecuter{
		queryExecutor: queryExecutor,
	}
	var err error
	result.conn, err = queryExecutor.getConn(queryExecutor.tsv.qe.conns)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (se *splitQuerySQLExecuter) done() {
	se.conn.Recycle()
}

// SQLExecute is part of the SQLExecuter interface.
func (se *splitQuerySQLExecuter) SQLExecute(
	sql string, bindVariables map[string]interface{},
) (*sqltypes.Result, error) {
	// We need to parse the query since we're dealing with bind-vars.
	// TODO(erez): Add an SQLExecute() to SQLExecuterInterface that gets a parsed query so that
	// we don't have to parse the query again here.
	ast, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("splitQuerySQLExecuter: parsing sql failed with: %v", err)
	}
	parsedQuery := sqlparser.GenerateParsedQuery(ast)

	// We clone "bindVariables" since fullFetch() changes it.
	return se.queryExecutor.dbConnFetch(
		se.conn,
		parsedQuery,
		utils.CloneBindVariables(bindVariables),
		nil,  /* buildStreamComment */
		true, /* wantfields */
	)
}

func createSplitQueryAlgorithmObject(
	algorithm querypb.SplitQueryRequest_Algorithm,
	splitParams *splitquery.SplitParams,
	sqlExecuter splitquery.SQLExecuter) (splitquery.SplitAlgorithmInterface, error) {

	switch algorithm {
	case querypb.SplitQueryRequest_FULL_SCAN:
		return splitquery.NewFullScanAlgorithm(splitParams, sqlExecuter)
	case querypb.SplitQueryRequest_EQUAL_SPLITS:
		return splitquery.NewEqualSplitsAlgorithm(splitParams, sqlExecuter)
	default:
		panic(fmt.Errorf("Unknown algorithm enum: %+v", algorithm))
	}
}

// StreamHealth streams the health status to callback.
// At the beginning, if TabletServer has a valid health
// state, that response is immediately sent.
func (tsv *TabletServer) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	tsv.streamHealthMutex.Lock()
	shr := tsv.lastStreamHealthResponse
	tsv.streamHealthMutex.Unlock()
	// Send current state immediately.
	if shr != nil {
		if err := callback(shr); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

	// Broadcast periodic updates.
	id, ch := tsv.streamHealthRegister()
	defer tsv.streamHealthUnregister(id)
	for {
		select {
		case <-ctx.Done():
			return nil
		case shr = <-ch:
		}
		if err := callback(shr); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (tsv *TabletServer) streamHealthRegister() (int, chan *querypb.StreamHealthResponse) {
	tsv.streamHealthMutex.Lock()
	defer tsv.streamHealthMutex.Unlock()

	id := tsv.streamHealthIndex
	tsv.streamHealthIndex++
	ch := make(chan *querypb.StreamHealthResponse, 10)
	tsv.streamHealthMap[id] = ch
	return id, ch
}

func (tsv *TabletServer) streamHealthUnregister(id int) {
	tsv.streamHealthMutex.Lock()
	defer tsv.streamHealthMutex.Unlock()
	delete(tsv.streamHealthMap, id)
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
		// Do not block on any write.
		select {
		case c <- shr:
		default:
		}
	}
	tsv.lastStreamHealthResponse = shr
}

// UpdateStream streams binlog events.
func (tsv *TabletServer) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	// Parse the position if needed.
	var p replication.Position
	var err error
	if timestamp == 0 {
		if position != "" {
			p, err = replication.DecodePosition(position)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot parse position: %v", err)
			}
		}
	} else if position != "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at most one of position and timestamp should be specified")
	}

	// Validate proper target is used.
	if err = tsv.startRequest(ctx, target, false, false); err != nil {
		return err
	}
	defer tsv.endRequest(false)

	s := binlog.NewEventStreamer(tsv.dbconfigs.App.DbName, tsv.mysqld, tsv.se, p, timestamp, callback)

	// Create a cancelable wrapping context.
	streamCtx, streamCancel := context.WithCancel(ctx)
	i := tsv.updateStreamList.Add(streamCancel)
	defer tsv.updateStreamList.Delete(i)

	// And stream with it.
	err = s.Stream(streamCtx)
	switch err {
	case mysqlctl.ErrBinlogUnavailable:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%v", err)
	case nil, io.EOF:
		return nil
	default:
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "%v", err)
	}
}

// HandlePanic is part of the queryservice.QueryService interface
func (tsv *TabletServer) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v\n. Stack-trace:\n%s", x, tb.Stack(4))
	}
}

// Close is a no-op.
func (tsv *TabletServer) Close(ctx context.Context) error {
	return nil
}

// startRequest validates the current state and target and registers
// the request (a waitgroup) as started. Every startRequest requires one
// and only one corresponding endRequest. When the service shuts down,
// StopService will wait on this waitgroup to ensure that there are
// no requests in flight. For transactional requests like begin, etc.,
// isTx must be set to true, which increments an additional waitgroup.
// During state transitions, this waitgroup will be checked to make
// sure that no such statements are in-flight while we resolve the tx pool.
func (tsv *TabletServer) startRequest(ctx context.Context, target *querypb.Target, isTx, allowOnShutdown bool) (err error) {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state == StateServing {
		goto verifyTarget
	}
	if allowOnShutdown && tsv.state == StateShuttingDown {
		goto verifyTarget
	}
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[tsv.state])

verifyTarget:
	if target != nil {
		// a valid target needs to be used
		switch {
		case target.Keyspace != tsv.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
		case target.Shard != tsv.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
		case isTx && tsv.target.TabletType != topodatapb.TabletType_MASTER:
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "transactional statement disallowed on non-master tablet: %v", tsv.target.TabletType)
		case target.TabletType != tsv.target.TabletType:
			for _, otherType := range tsv.alsoAllow {
				if target.TabletType == otherType {
					goto ok
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, tsv.target.TabletType, tsv.alsoAllow)
		}
	} else if !tabletenv.IsLocalContext(ctx) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	}

ok:
	tsv.requests.Add(1)
	// If it's a begin, we should make the shutdown code
	// wait for the call to end before it waits for tx empty.
	if isTx {
		tsv.txRequests.Add(1)
	}
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (tsv *TabletServer) endRequest(isTx bool) {
	tsv.requests.Done()
	if isTx {
		tsv.txRequests.Done()
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
		queryzHandler(tsv.qe, w, r)
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

func (tsv *TabletServer) registerTwopczHandler() {
	http.HandleFunc("/twopcz", func(w http.ResponseWriter, r *http.Request) {
		ctx := tabletenv.LocalContext()
		txe := &TxExecutor{
			ctx:      ctx,
			logStats: tabletenv.NewLogStats(ctx, "twopcz"),
			te:       tsv.te,
			messager: tsv.messager,
		}
		twopczHandler(txe, w, r)
	})
}

// SetPoolSize changes the pool size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetPoolSize(val int) {
	tsv.qe.conns.SetCapacity(val)
}

// PoolSize returns the pool size.
func (tsv *TabletServer) PoolSize() int {
	return int(tsv.qe.conns.Capacity())
}

// SetStreamPoolSize changes the pool size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetStreamPoolSize(val int) {
	tsv.qe.streamConns.SetCapacity(val)
}

// StreamPoolSize returns the pool size.
func (tsv *TabletServer) StreamPoolSize() int {
	return int(tsv.qe.streamConns.Capacity())
}

// SetTxPoolSize changes the tx pool size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetTxPoolSize(val int) {
	tsv.te.txPool.conns.SetCapacity(val)
}

// TxPoolSize returns the tx pool size.
func (tsv *TabletServer) TxPoolSize() int {
	return int(tsv.te.txPool.conns.Capacity())
}

// SetTxTimeout changes the transaction timeout to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetTxTimeout(val time.Duration) {
	tsv.te.txPool.SetTimeout(val)
}

// TxTimeout returns the transaction timeout.
func (tsv *TabletServer) TxTimeout() time.Duration {
	return tsv.te.txPool.Timeout()
}

// SetQueryCacheCap changes the pool size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetQueryCacheCap(val int) {
	tsv.qe.SetQueryCacheCap(val)
}

// QueryCacheCap returns the pool size.
func (tsv *TabletServer) QueryCacheCap() int {
	return int(tsv.qe.QueryCacheCap())
}

// SetStrictMode sets strict mode on or off.
// This only sets the mode for QueryEngine, but not
// for schema.Engine.
// This function should only be used for testing.
func (tsv *TabletServer) SetStrictMode(strict bool) {
	tsv.qe.strictMode.Set(strict)
}

// SetAutoCommit sets autocommit on or off.
// This function should only be used for testing.
func (tsv *TabletServer) SetAutoCommit(auto bool) {
	tsv.qe.autoCommit.Set(auto)
}

// SetMaxResultSize changes the max result size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetMaxResultSize(val int) {
	tsv.qe.maxResultSize.Set(int64(val))
}

// MaxResultSize returns the max result size.
func (tsv *TabletServer) MaxResultSize() int {
	return int(tsv.qe.maxResultSize.Get())
}

// SetMaxDMLRows changes the max result size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetMaxDMLRows(val int) {
	tsv.qe.maxDMLRows.Set(int64(val))
}

// MaxDMLRows returns the max result size.
func (tsv *TabletServer) MaxDMLRows() int {
	return int(tsv.qe.maxDMLRows.Get())
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Rand generates a pseudo-random int64 number.
func Rand() int64 {
	return rand.Int63()
}

// withTimeout returns a context based on the specified timeout.
// If the context is local or if timeout is 0, the
// original context is returned as is.
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 || tabletenv.IsLocalContext(ctx) {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
