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

package tabletserver

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/heartbeat"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/messager"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/splitquery"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txserializer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

// logPoolFull is for throttling transaction / query pool full messages in the log.
var logPoolFull = logutil.NewThrottledLogger("PoolFull", 1*time.Minute)

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

// stateDetail matches every state and optionally more information about the reason
// why the state is serving / not serving.
var stateDetail = []string{
	"Not Connected",
	"Not Serving",
	"",
	"Transitioning",
	"Shutting Down",
}

// stateInfo returns a string representation of the state and optional detail
// about the reason for the state transition
func stateInfo(state int64) string {
	if state == StateServing {
		return "SERVING"
	}
	return fmt.Sprintf("%s (%s)", stateName[state], stateDetail[state])
}

// TabletServer implements the RPC interface for the query service.
// TabletServer is initialized in the following sequence:
// NewTabletServer->InitDBConfig->SetServingType.
// Subcomponents of TabletServer are initialized using one of the
// following sequences:
// New->InitDBConfig->Init->Open, or New->InitDBConfig->Open.
// Essentially, InitDBConfig is a continuation of New. However,
// the db config is not initially available. For this reason,
// the initialization is done in two phases.
// Some subcomponents have Init functions. Such functions usually
// perform one-time initializations like creating metadata tables
// in the sidecar database. These functions must be idempotent.
// Open and Close can be called repeatedly during the lifetime of
// a subcomponent. These should also be idempotent.
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
	mu            sync.Mutex
	state         int64
	lameduck      sync2.AtomicInt32
	target        querypb.Target
	alsoAllow     []topodatapb.TabletType
	requests      sync.WaitGroup
	beginRequests sync.WaitGroup

	// The following variables should be initialized only once
	// before starting the tabletserver.
	dbconfigs *dbconfigs.DBConfigs

	// The following variables should only be accessed within
	// the context of a startRequest-endRequest.
	se               *schema.Engine
	qe               *QueryEngine
	te               *TxEngine
	hw               *heartbeat.Writer
	hr               *heartbeat.Reader
	messager         *messager.Engine
	watcher          *ReplicationWatcher
	updateStreamList *binlog.StreamList

	// checkMySQLThrottler is used to throttle the number of
	// requests sent to CheckMySQL.
	checkMySQLThrottler *sync2.Semaphore

	// txThrottler is used to throttle transactions based on the observed replication lag.
	txThrottler *txthrottler.TxThrottler
	topoServer  *topo.Server

	// streamHealthMutex protects all the following fields
	streamHealthMutex        sync.Mutex
	streamHealthIndex        int
	streamHealthMap          map[int]chan<- *querypb.StreamHealthResponse
	lastStreamHealthResponse *querypb.StreamHealthResponse

	// history records changes in state for display on the status page.
	// It has its own internal mutex.
	history *history.History

	// alias is used for identifying this tabletserver in healthcheck responses.
	alias topodatapb.TabletAlias
}

// RegisterFunction is a callback type to be called when we
// Register() a TabletServer
type RegisterFunction func(Controller)

// RegisterFunctions is a list of all the
// RegisterFunction that will be called upon
// Register() on a TabletServer
var RegisterFunctions []RegisterFunction

// NewServer creates a new TabletServer based on the command line flags.
func NewServer(topoServer *topo.Server, alias topodatapb.TabletAlias) *TabletServer {
	return NewTabletServer(tabletenv.Config, topoServer, alias)
}

type TxEngineStateController interface {
	// Stop will stop accepting any new transactions. If in RW mode, transactions are given
	// a chance to finish before being rolled back. If in RO mode, transactions are
	// immediately aborted.
	Stop() error

	// Will start accepting all transactions. If transitioning from RO mode, transactions
	// might need to be rolled back before new transactions can be accepts.
	AcceptReadWrite() error

	// Will start accepting read-only transactions, but not full read and write transactions.
	// If the engine is currently accepting full read and write transactions, they need to
	// given a chance to clean up before they are forcefully rolled back.
	AcceptReadOnly() error
}

var tsOnce sync.Once

// NewTabletServerWithNilTopoServer is typically used in tests that
// don't need a topoServer member.
func NewTabletServerWithNilTopoServer(config tabletenv.TabletConfig) *TabletServer {
	return NewTabletServer(config, nil, topodatapb.TabletAlias{})
}

// NewTabletServer creates an instance of TabletServer. Only the first
// instance of TabletServer will expose its state variables.
func NewTabletServer(config tabletenv.TabletConfig, topoServer *topo.Server, alias topodatapb.TabletAlias) *TabletServer {
	tsv := &TabletServer{
		QueryTimeout:           sync2.NewAtomicDuration(time.Duration(config.QueryTimeout * 1e9)),
		BeginTimeout:           sync2.NewAtomicDuration(time.Duration(config.TxPoolTimeout * 1e9)),
		TerseErrors:            config.TerseErrors,
		enableHotRowProtection: config.EnableHotRowProtection || config.EnableHotRowProtectionDryRun,
		checkMySQLThrottler:    sync2.NewSemaphore(1, 0),
		streamHealthMap:        make(map[int]chan<- *querypb.StreamHealthResponse),
		history:                history.New(10),
		topoServer:             topoServer,
		alias:                  alias,
	}
	tsv.se = schema.NewEngine(tsv, config)
	tsv.qe = NewQueryEngine(tsv, tsv.se, config)
	tsv.te = NewTxEngine(tsv, config)
	tsv.hw = heartbeat.NewWriter(tsv, alias, config)
	tsv.hr = heartbeat.NewReader(tsv, config)
	tsv.txThrottler = txthrottler.CreateTxThrottlerFromTabletConfig(topoServer)
	tsv.messager = messager.NewEngine(tsv, tsv.se, config)
	tsv.watcher = NewReplicationWatcher(tsv.se, config)
	tsv.updateStreamList = &binlog.StreamList{}
	// FIXME(alainjobart) could we move this to the Register method below?
	// So that vtcombo doesn't even call it once, on the first tablet.
	// And we can remove the tsOnce variable.
	tsOnce.Do(func() {
		stats.NewGaugeFunc("TabletState", "Tablet server state", func() int64 {
			tsv.mu.Lock()
			state := tsv.state
			tsv.mu.Unlock()
			return state
		})
		stats.Publish("TabletStateName", stats.StringFunc(tsv.GetState))

		// TabletServerState exports the same information as the above two stats (TabletState / TabletStateName),
		// but exported with TabletStateName as a label for Prometheus, which doesn't support exporting strings as stat values.
		stats.NewGaugesFuncWithMultiLabels("TabletServerState", "Tablet server state labeled by state name", []string{"name"}, func() map[string]int64 {
			return map[string]int64{tsv.GetState(): 1}
		})
		stats.NewGaugeDurationFunc("QueryTimeout", "Tablet server query timeout", tsv.QueryTimeout.Get)
		stats.NewGaugeDurationFunc("QueryPoolTimeout", "Tablet server timeout to get a connection from the query pool", tsv.qe.connTimeout.Get)
		stats.NewGaugeDurationFunc("BeginTimeout", "Tablet server begin timeout", tsv.BeginTimeout.Get)
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
	log.Infof("TabletServer state: %s -> %s", stateInfo(tsv.state), stateInfo(state))
	tsv.state = state
	tsv.history.Add(&historyRecord{
		Time:         time.Now(),
		ServingState: stateInfo(state),
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

// InitDBConfig initializes the db config variables for TabletServer. You must call this function before
// calling SetServingType.
func (tsv *TabletServer) InitDBConfig(target querypb.Target, dbcfgs *dbconfigs.DBConfigs) error {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state != StateNotConnected {
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "InitDBConfig failed, current state: %s", stateName[tsv.state])
	}
	tsv.target = target
	tsv.dbconfigs = dbcfgs

	tsv.se.InitDBConfig(tsv.dbconfigs)
	tsv.qe.InitDBConfig(tsv.dbconfigs)
	tsv.te.InitDBConfig(tsv.dbconfigs)
	tsv.hw.InitDBConfig(tsv.dbconfigs)
	tsv.hr.InitDBConfig(tsv.dbconfigs)
	tsv.messager.InitDBConfig(tsv.dbconfigs)
	tsv.watcher.InitDBConfig(tsv.dbconfigs)
	return nil
}

func (tsv *TabletServer) initACL(tableACLConfigFile string, enforceTableACLConfig bool) {
	// tabletacl.Init loads ACL from file if *tableACLConfig is not empty
	err := tableacl.Init(
		tableACLConfigFile,
		func() {
			tsv.ClearQueryPlanCache()
		},
	)
	if err != nil {
		log.Errorf("Fail to initialize Table ACL: %v", err)
		if enforceTableACLConfig {
			log.Exit("Need a valid initial Table ACL when enforce-tableacl-config is set, exiting.")
		}
	}
}

// InitACL loads the table ACL and sets up a SIGHUP handler for reloading it.
func (tsv *TabletServer) InitACL(tableACLConfigFile string, enforceTableACLConfig bool) {
	tsv.initACL(tableACLConfigFile, enforceTableACLConfig)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for {
			<-sigChan
			tsv.initACL(tableACLConfigFile, enforceTableACLConfig)
		}
	}()
}

// StartService is a convenience function for InitDBConfig->SetServingType
// with serving=true.
func (tsv *TabletServer) StartService(target querypb.Target, dbcfgs *dbconfigs.DBConfigs) (err error) {
	// Save tablet type away to prevent data races
	tabletType := target.TabletType
	err = tsv.InitDBConfig(target, dbcfgs)
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
	c, err := dbconnpool.NewDBConnection(tsv.dbconfigs.AppWithDB(), tabletenv.MySQLStats)
	if err != nil {
		log.Errorf("error creating db app connection: %v", err)
		return err
	}
	c.Close()

	if err := tsv.se.Open(); err != nil {
		return err
	}
	if err := tsv.qe.Open(); err != nil {
		return err
	}
	if err := tsv.te.Init(); err != nil {
		return err
	}
	if err := tsv.hw.Init(tsv.target); err != nil {
		return err
	}
	tsv.hr.Init(tsv.target)
	tsv.updateStreamList.Init()

	return tsv.serveNewType()
}

func (tsv *TabletServer) serveNewType() (err error) {
	// Wait for in-flight transactional requests to complete
	// before rolling back everything. In this state new
	// transactional requests are not allowed. So, we can
	// be sure that the tx pool won't change after the wait.
	tsv.beginRequests.Wait()

	if tsv.target.TabletType == topodatapb.TabletType_MASTER {
		tsv.te.AcceptReadWrite()
		if err := tsv.txThrottler.Open(tsv.target.Keyspace, tsv.target.Shard); err != nil {
			return err
		}
		tsv.watcher.Close()
		tsv.messager.Open()
		tsv.hr.Close()
		tsv.hw.Open()
	} else {
		tsv.te.AcceptReadOnly()
		tsv.messager.Close()
		tsv.hr.Open()
		tsv.hw.Close()
		tsv.watcher.Open()

		// Reset the sequences.
		tsv.se.MakeNonMaster()
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
	tsv.hw.Close()
	tsv.hr.Close()
	log.Infof("Shutdown complete.")
	tsv.transition(StateNotConnected)
}

func (tsv *TabletServer) waitForShutdown() {
	// Wait till beginRequests have completed before waiting on tx pool.
	// During this state, new Begins are not allowed. After the wait,
	// we have the assurance that only non-begin transactional calls
	// will be allowed. They will enable the conclusion of outstanding
	// transactions.
	tsv.beginRequests.Wait()
	tsv.messager.Close()
	tsv.te.Stop()
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
	tsv.hr.Close()
	tsv.hw.Close()
	tsv.te.CloseRudely()
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

// IsHealthy returns nil for non-serving types or if the query service is healthy (able to
// connect to the database and serving traffic), or an error explaining
// the unhealthiness otherwise.
func (tsv *TabletServer) IsHealthy() error {
	tsv.mu.Lock()
	tabletType := tsv.target.TabletType
	tsv.mu.Unlock()
	switch tabletType {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_BATCH, topodatapb.TabletType_EXPERIMENTAL:
		_, err := tsv.Execute(
			tabletenv.LocalContext(),
			nil,
			"/* health */ select 1 from dual",
			nil,
			0,
			nil,
		)
		return err
	default:
		return nil
	}
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
	return tsv.se.Reload(ctx)
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
func (tsv *TabletServer) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (transactionID int64, err error) {
	err = tsv.execRequest(
		ctx, tsv.BeginTimeout.Get(),
		"Begin", "begin", nil,
		target, options, true /* isBegin */, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tabletenv.QueryStats.Record("BEGIN", time.Now())
			if tsv.txThrottler.Throttle() {
				// TODO(erez): I think this should be RESOURCE_EXHAUSTED.
				return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "Transaction throttled")
			}
			transactionID, err = tsv.te.txPool.Begin(ctx, options)
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, true /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
		target, nil, false /* isBegin */, true, /* allowOnShutdown */
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
func (tsv *TabletServer) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (result *sqltypes.Result, err error) {
	allowOnShutdown := (transactionID != 0)
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Execute", sql, bindVariables,
		target, options, false /* isBegin */, allowOnShutdown,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]*querypb.BindVariable)
			}
			query, comments := sqlparser.SplitMarginComments(sql)
			plan, err := tsv.qe.GetPlan(ctx, logStats, query, skipQueryPlanCache(options))
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:          query,
				marginComments: comments,
				bindVars:       bindVariables,
				transactionID:  transactionID,
				options:        options,
				plan:           plan,
				ctx:            ctx,
				logStats:       logStats,
				tsv:            tsv,
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
func (tsv *TabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (err error) {
	return tsv.execRequest(
		ctx, 0,
		"StreamExecute", sql, bindVariables,
		target, options, false /* isBegin */, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]*querypb.BindVariable)
			}
			query, comments := sqlparser.SplitMarginComments(sql)
			plan, err := tsv.qe.GetStreamPlan(query)
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:          query,
				marginComments: comments,
				bindVars:       bindVariables,
				transactionID:  transactionID,
				options:        options,
				plan:           plan,
				ctx:            ctx,
				logStats:       logStats,
				tsv:            tsv,
			}
			return qre.Stream(callback)
		},
	)
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can be called with
// the AsTransaction flag which will execute all statements inside an independent
// transaction. If AsTransaction is true, TransactionId must be 0.
func (tsv *TabletServer) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (results []sqltypes.Result, err error) {
	if len(queries) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Empty query list")
	}
	if asTransaction && transactionID != 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot start a new transaction in the scope of an existing one")
	}

	if tsv.enableHotRowProtection && asTransaction {
		// Serialize transactions which target the same hot row range.
		// NOTE: We put this intentionally at this place *before* tsv.startRequest()
		// gets called below. Otherwise, the startRequest()/endRequest() section from
		// below would overlap with the startRequest()/endRequest() section executed
		// by tsv.beginWaitForSameRangeTransactions().
		txDone, err := tsv.beginWaitForSameRangeTransactions(ctx, target, options, queries[0].Sql, queries[0].BindVariables)
		if err != nil {
			return nil, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	allowOnShutdown := (transactionID != 0)
	// TODO(sougou): Convert startRequest/endRequest pattern to use wrapper
	// function tsv.execRequest() instead.
	// Note that below we always return "err" right away and do not call
	// tsv.convertAndLogError. That's because the methods which returned "err",
	// e.g. tsv.Execute(), already called that function and therefore already
	// converted and logged the error.
	if err = tsv.startRequest(ctx, target, false /* isBegin */, allowOnShutdown); err != nil {
		return nil, err
	}
	defer tsv.endRequest(false)
	defer tsv.handlePanicAndSendLogStats("batch", nil, nil)

	if asTransaction {
		transactionID, err = tsv.Begin(ctx, target, options)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		results = append(results, *localReply)
	}
	if asTransaction {
		if err = tsv.Commit(ctx, target, transactionID); err != nil {
			transactionID = 0
			return nil, err
		}
		transactionID = 0
	}
	return results, nil
}

// BeginExecute combines Begin and Execute.
func (tsv *TabletServer) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	if tsv.enableHotRowProtection {
		txDone, err := tsv.beginWaitForSameRangeTransactions(ctx, target, options, sql, bindVariables)
		if err != nil {
			return nil, 0, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	transactionID, err := tsv.Begin(ctx, target, options)
	if err != nil {
		return nil, 0, err
	}

	result, err := tsv.Execute(ctx, target, sql, bindVariables, transactionID, options)
	return result, transactionID, err
}

func (tsv *TabletServer) beginWaitForSameRangeTransactions(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions, sql string, bindVariables map[string]*querypb.BindVariable) (txserializer.DoneFunc, error) {
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
		target, options, true /* isBegin */, false, /* allowOnShutdown */
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
func (tsv *TabletServer) computeTxSerializerKey(ctx context.Context, logStats *tabletenv.LogStats, sql string, bindVariables map[string]*querypb.BindVariable) (string, string) {
	// Strip trailing comments so we don't pollute the query cache.
	sql, _ = sqlparser.SplitMarginComments(sql)
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql, false /* skipQueryPlanCache */)
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

	where, err := plan.WhereClause.GenerateQuery(bindVariables, nil)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to substitute bind vars in where clause: %v query: %v bind vars: %v", err, sql, bindVariables)
		return "", ""
	}

	// Example: table1 where id = 1 and sub_id = 2
	key := fmt.Sprintf("%s%s", tableName, hack.String(where))
	return key, tableName.String()
}

// BeginExecuteBatch combines Begin and ExecuteBatch.
func (tsv *TabletServer) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	// TODO(mberlin): Integrate hot row protection here as we did for BeginExecute()
	// and ExecuteBatch(asTransaction=true).
	transactionID, err := tsv.Begin(ctx, target, options)
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
		target, nil, false /* isBegin */, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			plan, err := tsv.qe.GetMessageStreamPlan(name)
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:    "stream from msg",
				plan:     plan,
				ctx:      ctx,
				logStats: logStats,
				tsv:      tsv,
			}
			return qre.MessageStream(callback)
		},
	)
}

// MessageAck acks the list of messages for a given message table.
// It returns the number of messages successfully acked.
func (tsv *TabletServer) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	sids := make([]string, 0, len(ids))
	for _, val := range ids {
		sids = append(sids, sqltypes.ProtoToValue(val).ToString())
	}
	count, err = tsv.execDML(ctx, target, func() (string, map[string]*querypb.BindVariable, error) {
		return tsv.messager.GenerateAckQuery(name, sids)
	})
	if err != nil {
		return 0, err
	}
	messager.MessageStats.Add([]string{name, "Acked"}, count)
	return count, nil
}

// PostponeMessages postpones the list of messages for a given message table.
// It returns the number of messages successfully postponed.
func (tsv *TabletServer) PostponeMessages(ctx context.Context, target *querypb.Target, name string, ids []string) (count int64, err error) {
	return tsv.execDML(ctx, target, func() (string, map[string]*querypb.BindVariable, error) {
		return tsv.messager.GeneratePostponeQuery(name, ids)
	})
}

// PurgeMessages purges messages older than specified time in Unix Nanoseconds.
// It purges at most 500 messages. It returns the number of messages successfully purged.
func (tsv *TabletServer) PurgeMessages(ctx context.Context, target *querypb.Target, name string, timeCutoff int64) (count int64, err error) {
	return tsv.execDML(ctx, target, func() (string, map[string]*querypb.BindVariable, error) {
		return tsv.messager.GeneratePurgeQuery(name, timeCutoff)
	})
}

func (tsv *TabletServer) execDML(ctx context.Context, target *querypb.Target, queryGenerator func() (string, map[string]*querypb.BindVariable, error)) (count int64, err error) {
	if err = tsv.startRequest(ctx, target, true /* isBegin */, false /* allowOnShutdown */); err != nil {
		return 0, err
	}
	defer tsv.endRequest(true)
	defer tsv.handlePanicAndSendLogStats("ack", nil, nil)

	query, bv, err := queryGenerator()
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}

	transactionID, err := tsv.Begin(ctx, target, nil)
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
	query *querypb.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) (splits []*querypb.QuerySplit, err error) {
	err = tsv.execRequest(
		ctx, 0,
		"SplitQuery", query.Sql, query.BindVariables,
		target, nil, false /* isBegin */, false, /* allowOnShutdown */
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

// execRequest performs verifications, sets up the necessary environments
// and calls the supplied function for executing the request.
func (tsv *TabletServer) execRequest(
	ctx context.Context, timeout time.Duration,
	requestName, sql string, bindVariables map[string]*querypb.BindVariable,
	target *querypb.Target, options *querypb.ExecuteOptions, isBegin, allowOnShutdown bool,
	exec func(ctx context.Context, logStats *tabletenv.LogStats) error,
) (err error) {
	logStats := tabletenv.NewLogStats(ctx, requestName)
	logStats.Target = target
	logStats.OriginalSQL = sql
	logStats.BindVariables = bindVariables
	defer tsv.handlePanicAndSendLogStats(sql, bindVariables, logStats)
	if err = tsv.startRequest(ctx, target, isBegin, allowOnShutdown); err != nil {
		return err
	}

	ctx, cancel := withTimeout(ctx, timeout, options)
	defer func() {
		cancel()
		tsv.endRequest(isBegin)
	}()

	err = exec(ctx, logStats)
	if err != nil {
		return tsv.convertAndLogError(ctx, sql, bindVariables, err, logStats)
	}
	return nil
}

func (tsv *TabletServer) handlePanicAndSendLogStats(
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	logStats *tabletenv.LogStats,
) {
	if x := recover(); x != nil {
		errorMessage := fmt.Sprintf(
			"Uncaught panic for %v:\n%v\n%s",
			queryAsString(sql, bindVariables),
			x,
			tb.Stack(4) /* Skip the last 4 boiler-plate frames. */)
		log.Errorf(errorMessage)
		terr := vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "%s", errorMessage)
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

func (tsv *TabletServer) convertAndLogError(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, err error, logStats *tabletenv.LogStats) error {
	if err == nil {
		return nil
	}

	errCode := convertErrorCode(err)
	tabletenv.ErrorStats.Add(errCode.String(), 1)

	callerID := ""
	cid := callerid.ImmediateCallerIDFromContext(ctx)
	if cid != nil {
		callerID = fmt.Sprintf(" (CallerID: %s)", cid.Username)
	}

	logMethod := tabletenv.Errorf
	// Suppress or demote some errors in logs.
	switch errCode {
	case vtrpcpb.Code_FAILED_PRECONDITION, vtrpcpb.Code_ALREADY_EXISTS:
		logMethod = nil
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		logMethod = logPoolFull.Errorf
	case vtrpcpb.Code_ABORTED:
		logMethod = tabletenv.Warningf
	case vtrpcpb.Code_INVALID_ARGUMENT, vtrpcpb.Code_DEADLINE_EXCEEDED:
		logMethod = tabletenv.Infof
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
	var message string
	sqlErr, ok := err.(*mysql.SQLError)
	if ok {
		sqlState := sqlErr.SQLState()
		errnum := sqlErr.Number()
		if tsv.TerseErrors && len(bindVariables) != 0 && errCode != vtrpcpb.Code_FAILED_PRECONDITION {
			err = vterrors.Errorf(errCode, "(errno %d) (sqlstate %s)%s: %s", errnum, sqlState, callerID, queryAsString(sql, nil))
			if logMethod != nil {
				message = fmt.Sprintf("%s (errno %d) (sqlstate %s)%s: %s", sqlErr.Message, errnum, sqlState, callerID, truncateSQLAndBindVars(sql, bindVariables))
			}
		} else {
			err = vterrors.Errorf(errCode, "%s (errno %d) (sqlstate %s)%s: %s", sqlErr.Message, errnum, sqlState, callerID, queryAsString(sql, bindVariables))
			if logMethod != nil {
				message = fmt.Sprintf("%s (errno %d) (sqlstate %s)%s: %s", sqlErr.Message, errnum, sqlState, callerID, truncateSQLAndBindVars(sql, bindVariables))
			}
		}
	} else {
		err = vterrors.Errorf(errCode, "%v%s", err.Error(), callerID)
		if tsv.TerseErrors && len(bindVariables) != 0 && errCode != vtrpcpb.Code_FAILED_PRECONDITION {
			if logMethod != nil {
				message = fmt.Sprintf("%v: %v", err, truncateSQLAndBindVars(sql, nil))
			}
		} else {
			if logMethod != nil {
				message = fmt.Sprintf("%v: %v", err, truncateSQLAndBindVars(sql, bindVariables))
			}
		}
	}

	if logMethod != nil {
		logMethod(message)
	}

	if logStats != nil {
		logStats.Error = err
	}

	return err
}

// truncateSQLAndBindVars calls TruncateForLog which:
//  splits off trailing comments, truncates the query, and re-adds the trailing comments
// appends quoted bindvar: value pairs in sorted order
// truncates the resulting string
func truncateSQLAndBindVars(sql string, bindVariables map[string]*querypb.BindVariable) string {
	truncatedQuery := sqlparser.TruncateForLog(sql)
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "BindVars: {")
	var keys []string
	for key := range bindVariables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var valString string
	for _, key := range keys {
		valString = fmt.Sprintf("%v", bindVariables[key])
		fmt.Fprintf(buf, "%s: %q", key, valString)
	}
	fmt.Fprintf(buf, "}")
	bv := string(buf.Bytes())
	maxLen := *sqlparser.TruncateErrLen
	if maxLen != 0 && len(bv) > maxLen {
		bv = bv[:maxLen-12] + " [TRUNCATED]"
	}
	return fmt.Sprintf("Sql: %q, %s", truncatedQuery, bv)
}

func convertErrorCode(err error) vtrpcpb.Code {
	errCode := vterrors.Code(err)
	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		return errCode
	}

	errstr := err.Error()
	errnum := sqlErr.Number()
	switch errnum {
	case mysql.ERNotSupportedYet:
		errCode = vtrpcpb.Code_UNIMPLEMENTED
	case mysql.ERDiskFull, mysql.EROutOfMemory, mysql.EROutOfSortMemory, mysql.ERConCount, mysql.EROutOfResources, mysql.ERRecordFileFull, mysql.ERHostIsBlocked,
		mysql.ERCantCreateThread, mysql.ERTooManyDelayedThreads, mysql.ERNetPacketTooLarge, mysql.ERTooManyUserConnections, mysql.ERLockTableFull, mysql.ERUserLimitReached, mysql.ERVitessMaxRowsExceeded:
		errCode = vtrpcpb.Code_RESOURCE_EXHAUSTED
	case mysql.ERLockWaitTimeout:
		errCode = vtrpcpb.Code_DEADLINE_EXCEEDED
	case mysql.CRServerGone, mysql.ERServerShutdown:
		errCode = vtrpcpb.Code_UNAVAILABLE
	case mysql.ERFormNotFound, mysql.ERKeyNotFound, mysql.ERBadFieldError, mysql.ERNoSuchThread, mysql.ERUnknownTable, mysql.ERCantFindUDF, mysql.ERNonExistingGrant,
		mysql.ERNoSuchTable, mysql.ERNonExistingTableGrant, mysql.ERKeyDoesNotExist:
		errCode = vtrpcpb.Code_NOT_FOUND
	case mysql.ERDBAccessDenied, mysql.ERAccessDeniedError, mysql.ERKillDenied, mysql.ERNoPermissionToCreateUsers:
		errCode = vtrpcpb.Code_PERMISSION_DENIED
	case mysql.ERNoDb, mysql.ERNoSuchIndex, mysql.ERCantDropFieldOrKey, mysql.ERTableNotLockedForWrite, mysql.ERTableNotLocked, mysql.ERTooBigSelect, mysql.ERNotAllowedCommand,
		mysql.ERTooLongString, mysql.ERDelayedInsertTableLocked, mysql.ERDupUnique, mysql.ERRequiresPrimaryKey, mysql.ERCantDoThisDuringAnTransaction, mysql.ERReadOnlyTransaction,
		mysql.ERCannotAddForeign, mysql.ERNoReferencedRow, mysql.ERRowIsReferenced, mysql.ERCantUpdateWithReadLock, mysql.ERNoDefault, mysql.EROperandColumns,
		mysql.ERSubqueryNo1Row, mysql.ERNonUpdateableTable, mysql.ERFeatureDisabled, mysql.ERDuplicatedValueInType, mysql.ERRowIsReferenced2,
		mysql.ErNoReferencedRow2:
		errCode = vtrpcpb.Code_FAILED_PRECONDITION
	case mysql.EROptionPreventsStatement:
		// Special-case this error code. It's probably because
		// there was a failover and there are old clients still connected.
		if strings.Contains(errstr, "read-only") {
			errCode = vtrpcpb.Code_FAILED_PRECONDITION
		}
	case mysql.ERTableExists, mysql.ERDupEntry, mysql.ERFileExists, mysql.ERUDFExists:
		errCode = vtrpcpb.Code_ALREADY_EXISTS
	case mysql.ERGotSignal, mysql.ERForcingClose, mysql.ERAbortingConnection, mysql.ERLockDeadlock:
		// For ERLockDeadlock, a deadlock rolls back the transaction.
		errCode = vtrpcpb.Code_ABORTED
	case mysql.ERUnknownComError, mysql.ERBadNullError, mysql.ERBadDb, mysql.ERBadTable, mysql.ERNonUniq, mysql.ERWrongFieldWithGroup, mysql.ERWrongGroupField,
		mysql.ERWrongSumSelect, mysql.ERWrongValueCount, mysql.ERTooLongIdent, mysql.ERDupFieldName, mysql.ERDupKeyName, mysql.ERWrongFieldSpec, mysql.ERParseError,
		mysql.EREmptyQuery, mysql.ERNonUniqTable, mysql.ERInvalidDefault, mysql.ERMultiplePriKey, mysql.ERTooManyKeys, mysql.ERTooManyKeyParts, mysql.ERTooLongKey,
		mysql.ERKeyColumnDoesNotExist, mysql.ERBlobUsedAsKey, mysql.ERTooBigFieldLength, mysql.ERWrongAutoKey, mysql.ERWrongFieldTerminators, mysql.ERBlobsAndNoTerminated,
		mysql.ERTextFileNotReadable, mysql.ERWrongSubKey, mysql.ERCantRemoveAllFields, mysql.ERUpdateTableUsed, mysql.ERNoTablesUsed, mysql.ERTooBigSet,
		mysql.ERBlobCantHaveDefault, mysql.ERWrongDbName, mysql.ERWrongTableName, mysql.ERUnknownProcedure, mysql.ERWrongParamCountToProcedure,
		mysql.ERWrongParametersToProcedure, mysql.ERFieldSpecifiedTwice, mysql.ERInvalidGroupFuncUse, mysql.ERTableMustHaveColumns, mysql.ERUnknownCharacterSet,
		mysql.ERTooManyTables, mysql.ERTooManyFields, mysql.ERTooBigRowSize, mysql.ERWrongOuterJoin, mysql.ERNullColumnInIndex, mysql.ERFunctionNotDefined,
		mysql.ERWrongValueCountOnRow, mysql.ERInvalidUseOfNull, mysql.ERRegexpError, mysql.ERMixOfGroupFuncAndFields, mysql.ERIllegalGrantForTable, mysql.ERSyntaxError,
		mysql.ERWrongColumnName, mysql.ERWrongKeyColumn, mysql.ERBlobKeyWithoutLength, mysql.ERPrimaryCantHaveNull, mysql.ERTooManyRows, mysql.ERUnknownSystemVariable,
		mysql.ERSetConstantsOnly, mysql.ERWrongArguments, mysql.ERWrongUsage, mysql.ERWrongNumberOfColumnsInSelect, mysql.ERDupArgument, mysql.ERLocalVariable,
		mysql.ERGlobalVariable, mysql.ERWrongValueForVar, mysql.ERWrongTypeForVar, mysql.ERVarCantBeRead, mysql.ERCantUseOptionHere, mysql.ERIncorrectGlobalLocalVar,
		mysql.ERWrongFKDef, mysql.ERKeyRefDoNotMatchTableRef, mysql.ERCyclicReference, mysql.ERCollationCharsetMismatch, mysql.ERCantAggregate2Collations,
		mysql.ERCantAggregate3Collations, mysql.ERCantAggregateNCollations, mysql.ERVariableIsNotStruct, mysql.ERUnknownCollation, mysql.ERWrongNameForIndex,
		mysql.ERWrongNameForCatalog, mysql.ERBadFTColumn, mysql.ERTruncatedWrongValue, mysql.ERTooMuchAutoTimestampCols, mysql.ERInvalidOnUpdate, mysql.ERUnknownTimeZone,
		mysql.ERInvalidCharacterString, mysql.ERIllegalReference, mysql.ERDerivedMustHaveAlias, mysql.ERTableNameNotAllowedHere, mysql.ERDataTooLong, mysql.ERDataOutOfRange,
		mysql.ERTruncatedWrongValueForField:
		errCode = vtrpcpb.Code_INVALID_ARGUMENT
	case mysql.ERSpecifiedAccessDenied:
		// This code is also utilized for Google internal failover error code.
		if strings.Contains(errstr, "failover in progress") {
			errCode = vtrpcpb.Code_FAILED_PRECONDITION
		} else {
			errCode = vtrpcpb.Code_PERMISSION_DENIED
		}
	case mysql.CRServerLost:
		// Query was killed.
		errCode = vtrpcpb.Code_DEADLINE_EXCEEDED
	}

	return errCode
}

// validateSplitQueryParameters perform some validations on the SplitQuery parameters
// returns an error that can be returned to the user if a validation fails.
func validateSplitQueryParameters(
	target *querypb.Target,
	query *querypb.BoundQuery,
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
			queryAsString(query.Sql, query.BindVariables))
	}
	if splitCount < 0 {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitQuery: splitCount must be non-negative. Got: %v. SQL: %v",
			splitCount,
			queryAsString(query.Sql, query.BindVariables))
	}
	if (splitCount == 0 && numRowsPerQueryPart == 0) ||
		(splitCount != 0 && numRowsPerQueryPart != 0) {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitQuery: exactly one of {numRowsPerQueryPart, splitCount} must be"+
				" non zero. Got: numRowsPerQueryPart=%v, splitCount=%v. SQL: %v",
			numRowsPerQueryPart,
			splitCount,
			queryAsString(query.Sql, query.BindVariables))
	}
	if algorithm != querypb.SplitQueryRequest_EQUAL_SPLITS &&
		algorithm != querypb.SplitQueryRequest_FULL_SCAN {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"splitquery: unsupported algorithm: %v. SQL: %v",
			algorithm,
			queryAsString(query.Sql, query.BindVariables))
	}
	return nil
}

func createSplitParams(
	query *querypb.BoundQuery,
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
			queryAsString(query.Sql, query.BindVariables)))
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
	result.conn, err = queryExecutor.getConn()
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
	sql string, bindVariables map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	// We need to parse the query since we're dealing with bind-vars.
	// TODO(erez): Add an SQLExecute() to SQLExecuterInterface that gets a parsed query so that
	// we don't have to parse the query again here.
	ast, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, vterrors.Wrap(err, "splitQuerySQLExecuter: parsing sql failed with")
	}
	parsedQuery := sqlparser.NewParsedQuery(ast)

	// We clone "bindVariables" since fullFetch() changes it.
	return se.queryExecutor.dbConnFetch(
		se.conn,
		parsedQuery,
		sqltypes.CopyBindVariables(bindVariables),
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
		Target:                              &target,
		TabletAlias:                         &tsv.alias,
		Serving:                             tsv.IsServing(),
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

// HeartbeatLag returns the current lag as calculated by the heartbeat
// package, if heartbeat is enabled. Otherwise returns 0.
func (tsv *TabletServer) HeartbeatLag() (time.Duration, error) {
	return tsv.hr.GetLatest()
}

// TopoServer returns the topo server.
func (tsv *TabletServer) TopoServer() *topo.Server {
	return tsv.topoServer
}

// UpdateStream streams binlog events.
func (tsv *TabletServer) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	// Parse the position if needed.
	var p mysql.Position
	var err error
	if timestamp == 0 {
		if position != "" {
			p, err = mysql.DecodePosition(position)
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot parse position: %v", err)
			}
		}
	} else if position != "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at most one of position and timestamp should be specified")
	}

	// Validate proper target is used.
	if err = tsv.startRequest(ctx, target, false /* isBegin */, false /* allowOnShutdown */); err != nil {
		return err
	}
	defer tsv.endRequest(false)

	s := binlog.NewEventStreamer(tsv.dbconfigs.DbaWithDB(), tsv.se, p, timestamp, callback)

	// Create a cancelable wrapping context.
	streamCtx, streamCancel := context.WithCancel(ctx)
	i := tsv.updateStreamList.Add(streamCancel)
	defer tsv.updateStreamList.Delete(i)

	// And stream with it.
	err = s.Stream(streamCtx)
	switch err {
	case binlog.ErrBinlogUnavailable:
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
// the request (a waitgroup) as started. Every startRequest requires
// one and only one corresponding endRequest. When the service shuts
// down, StopService will wait on this waitgroup to ensure that there
// are no requests in flight. For begin requests, isBegin must be set
// to true, which increments an additional waitgroup.  During state
// transitions, this waitgroup will be checked to make sure that no
// such statements are in-flight while we resolve the tx pool.
func (tsv *TabletServer) startRequest(ctx context.Context, target *querypb.Target, isBegin, allowOnShutdown bool) (err error) {
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
	if isBegin {
		tsv.beginRequests.Add(1)
	}
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (tsv *TabletServer) endRequest(isBegin bool) {
	tsv.requests.Done()
	if isBegin {
		tsv.beginRequests.Done()
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
			http.Error(w, fmt.Sprintf("not ok: %v", err), http.StatusInternalServerError)
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

// SetQueryPlanCacheCap changes the pool size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetQueryPlanCacheCap(val int) {
	tsv.qe.SetQueryPlanCacheCap(val)
}

// QueryPlanCacheCap returns the pool size.
func (tsv *TabletServer) QueryPlanCacheCap() int {
	return int(tsv.qe.QueryPlanCacheCap())
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

// SetWarnResultSize changes the warn result size to the specified value.
// This function should only be used for testing.
func (tsv *TabletServer) SetWarnResultSize(val int) {
	tsv.qe.warnResultSize.Set(int64(val))
}

// WarnResultSize returns the warn result size.
func (tsv *TabletServer) WarnResultSize() int {
	return int(tsv.qe.warnResultSize.Get())
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

// SetPassthroughDMLs changes the setting to pass through all DMLs
// It should only be used for testing
func (tsv *TabletServer) SetPassthroughDMLs(val bool) {
	planbuilder.PassthroughDMLs = true
	tsv.qe.passthroughDMLs.Set(val)
}

// SetAllowUnsafeDMLs changes the setting to allow unsafe DML statements
// in SBR mode. It should be used only on initialization or for testing.
func (tsv *TabletServer) SetAllowUnsafeDMLs(val bool) {
	tsv.qe.allowUnsafeDMLs = val
}

// SetQueryPoolTimeout changes the timeout to get a connection from the
// query pool
// This function should only be used for testing.
func (tsv *TabletServer) SetQueryPoolTimeout(val time.Duration) {
	tsv.qe.connTimeout.Set(val)
}

// GetQueryPoolTimeout returns the timeout to get a connection from the
// query pool
// This function should only be used for testing.
func (tsv *TabletServer) GetQueryPoolTimeout() time.Duration {
	return tsv.qe.connTimeout.Get()
}

// SetQueryPoolWaiterCap changes the limit on the number of queries that can be
// waiting for a connection from the pool
// This function should only be used for testing.
func (tsv *TabletServer) SetQueryPoolWaiterCap(val int64) {
	tsv.qe.queryPoolWaiterCap.Set(val)
}

// GetQueryPoolWaiterCap returns the limit on the number of queries that can be
// waiting for a connection from the pool
// This function should only be used for testing.
func (tsv *TabletServer) GetQueryPoolWaiterCap() int64 {
	return tsv.qe.queryPoolWaiterCap.Get()
}

// SetTxPoolWaiterCap changes the limit on the number of queries that can be
// waiting for a connection from the pool
// This function should only be used for testing.
func (tsv *TabletServer) SetTxPoolWaiterCap(val int64) {
	tsv.te.txPool.waiterCap.Set(val)
}

// GetTxPoolWaiterCap returns the limit on the number of queries that can be
// waiting for a connection from the pool
// This function should only be used for testing.
func (tsv *TabletServer) GetTxPoolWaiterCap() int64 {
	return tsv.te.txPool.waiterCap.Get()
}

// SetConsolidatorEnabled (true) will enable the query consolidator.
// SetConsolidatorEnabled (false) will disable the query consolidator.
// This function should only be used for testing.
func (tsv *TabletServer) SetConsolidatorEnabled(enabled bool) {
	tsv.qe.enableConsolidator = enabled
}

// queryAsString returns a readable version of query+bind variables.
func queryAsString(sql string, bindVariables map[string]*querypb.BindVariable) string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "Sql: %q", sql)
	fmt.Fprintf(buf, ", BindVars: {")
	var keys []string
	for key := range bindVariables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var valString string
	for _, key := range keys {
		valString = fmt.Sprintf("%v", bindVariables[key])
		fmt.Fprintf(buf, "%s: %q", key, valString)
	}
	fmt.Fprintf(buf, "}")
	return string(buf.Bytes())
}

// withTimeout returns a context based on the specified timeout.
// If the context is local or if timeout is 0, the
// original context is returned as is.
func withTimeout(ctx context.Context, timeout time.Duration, options *querypb.ExecuteOptions) (context.Context, context.CancelFunc) {
	if timeout == 0 || options.GetWorkload() == querypb.ExecuteOptions_DBA || tabletenv.IsLocalContext(ctx) {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// skipQueryPlanCache returns true if the query plan should be cached
func skipQueryPlanCache(options *querypb.ExecuteOptions) bool {
	if options == nil {
		return false
	}
	return options.SkipQueryPlanCache
}
