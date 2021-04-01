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

package tabletserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/onlineddl"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/gc"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/messager"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/repltracker"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txserializer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
	"vitess.io/vitess/go/vt/vttablet/vexec"
)

// logPoolFull is for throttling transaction / query pool full messages in the log.
var logPoolFull = logutil.NewThrottledLogger("PoolFull", 1*time.Minute)

var logComputeRowSerializerKey = logutil.NewThrottledLogger("ComputeRowSerializerKey", 1*time.Minute)

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
	exporter               *servenv.Exporter
	config                 *tabletenv.TabletConfig
	stats                  *tabletenv.Stats
	QueryTimeout           sync2.AtomicDuration
	txTimeout              sync2.AtomicDuration
	TerseErrors            bool
	enableHotRowProtection bool
	topoServer             *topo.Server

	// These are sub-components of TabletServer.
	statelessql  *QueryList
	statefulql   *QueryList
	olapql       *QueryList
	se           *schema.Engine
	rt           *repltracker.ReplTracker
	vstreamer    *vstreamer.Engine
	tracker      *schema.Tracker
	watcher      *BinlogWatcher
	qe           *QueryEngine
	txThrottler  *txthrottler.TxThrottler
	te           *TxEngine
	messager     *messager.Engine
	hs           *healthStreamer
	lagThrottler *throttle.Throttler
	tableGC      *gc.TableGC

	// sm manages state transitions.
	sm                *stateManager
	onlineDDLExecutor *onlineddl.Executor

	// alias is used for identifying this tabletserver in healthcheck responses.
	alias topodatapb.TabletAlias
}

var _ queryservice.QueryService = (*TabletServer)(nil)

// RegisterFunctions is a list of all the
// RegisterFunction that will be called upon
// Register() on a TabletServer
var RegisterFunctions []func(Controller)

// NewServer creates a new TabletServer based on the command line flags.
func NewServer(name string, topoServer *topo.Server, alias topodatapb.TabletAlias) *TabletServer {
	return NewTabletServer(name, tabletenv.NewCurrentConfig(), topoServer, alias)
}

var (
	tsOnce        sync.Once
	srvTopoServer srvtopo.Server
)

// NewTabletServer creates an instance of TabletServer. Only the first
// instance of TabletServer will expose its state variables.
func NewTabletServer(name string, config *tabletenv.TabletConfig, topoServer *topo.Server, alias topodatapb.TabletAlias) *TabletServer {
	exporter := servenv.NewExporter(name, "Tablet")
	tsv := &TabletServer{
		exporter:               exporter,
		stats:                  tabletenv.NewStats(exporter),
		config:                 config,
		QueryTimeout:           sync2.NewAtomicDuration(config.Oltp.QueryTimeoutSeconds.Get()),
		txTimeout:              sync2.NewAtomicDuration(config.Oltp.TxTimeoutSeconds.Get()),
		TerseErrors:            config.TerseErrors,
		enableHotRowProtection: config.HotRowProtection.Mode != tabletenv.Disable,
		topoServer:             topoServer,
		alias:                  alias,
	}

	tsOnce.Do(func() { srvTopoServer = srvtopo.NewResilientServer(topoServer, "TabletSrvTopo") })

	tabletTypeFunc := func() topodatapb.TabletType {
		if tsv.sm == nil {
			return topodatapb.TabletType_UNKNOWN
		}
		return tsv.sm.Target().TabletType
	}

	tsv.statelessql = NewQueryList("oltp-stateless")
	tsv.statefulql = NewQueryList("oltp-stateful")
	tsv.olapql = NewQueryList("olap")
	tsv.lagThrottler = throttle.NewThrottler(tsv, topoServer, tabletTypeFunc)
	tsv.hs = newHealthStreamer(tsv, alias)
	tsv.se = schema.NewEngine(tsv)
	tsv.rt = repltracker.NewReplTracker(tsv, alias)
	tsv.vstreamer = vstreamer.NewEngine(tsv, srvTopoServer, tsv.se, tsv.lagThrottler, alias.Cell)
	tsv.tracker = schema.NewTracker(tsv, tsv.vstreamer, tsv.se)
	tsv.watcher = NewBinlogWatcher(tsv, tsv.vstreamer, tsv.config)
	tsv.qe = NewQueryEngine(tsv, tsv.se)
	tsv.txThrottler = txthrottler.NewTxThrottler(tsv.config, topoServer)
	tsv.te = NewTxEngine(tsv)
	tsv.messager = messager.NewEngine(tsv, tsv.se, tsv.vstreamer)

	tsv.onlineDDLExecutor = onlineddl.NewExecutor(tsv, alias, topoServer, tabletTypeFunc)
	tsv.tableGC = gc.NewTableGC(tsv, topoServer, tabletTypeFunc, tsv.lagThrottler)

	tsv.sm = &stateManager{
		statelessql: tsv.statelessql,
		statefulql:  tsv.statefulql,
		olapql:      tsv.olapql,
		hs:          tsv.hs,
		se:          tsv.se,
		rt:          tsv.rt,
		vstreamer:   tsv.vstreamer,
		tracker:     tsv.tracker,
		watcher:     tsv.watcher,
		qe:          tsv.qe,
		txThrottler: tsv.txThrottler,
		te:          tsv.te,
		messager:    tsv.messager,
		ddle:        tsv.onlineDDLExecutor,
		throttler:   tsv.lagThrottler,
		tableGC:     tsv.tableGC,
	}

	tsv.exporter.NewGaugeFunc("TabletState", "Tablet server state", func() int64 { return int64(tsv.sm.State()) })
	tsv.exporter.Publish("TabletStateName", stats.StringFunc(tsv.sm.IsServingString))

	// TabletServerState exports the same information as the above two stats (TabletState / TabletStateName),
	// but exported with TabletStateName as a label for Prometheus, which doesn't support exporting strings as stat values.
	tsv.exporter.NewGaugesFuncWithMultiLabels("TabletServerState", "Tablet server state labeled by state name", []string{"name"}, func() map[string]int64 {
		return map[string]int64{tsv.sm.IsServingString(): 1}
	})
	tsv.exporter.NewGaugeDurationFunc("QueryTimeout", "Tablet server query timeout", tsv.QueryTimeout.Get)

	tsv.registerHealthzHealthHandler()
	tsv.registerDebugHealthHandler()
	tsv.registerQueryzHandler()
	tsv.registerQueryListHandlers([]*QueryList{tsv.statelessql, tsv.statefulql, tsv.olapql})
	tsv.registerTwopczHandler()
	tsv.registerMigrationStatusHandler()
	tsv.registerThrottlerHandlers()
	tsv.registerDebugEnvHandler()

	return tsv
}

// InitDBConfig initializes the db config variables for TabletServer. You must call this function
// to complete the creation of TabletServer.
func (tsv *TabletServer) InitDBConfig(target querypb.Target, dbcfgs *dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) error {
	if tsv.sm.State() != StateNotConnected {
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "InitDBConfig failed, current state: %s", tsv.sm.IsServingString())
	}
	tsv.sm.Init(tsv, target)
	tsv.sm.target = target
	tsv.config.DB = dbcfgs

	tsv.se.InitDBConfig(tsv.config.DB.DbaWithDB())
	tsv.rt.InitDBConfig(target, mysqld)
	tsv.txThrottler.InitDBConfig(target)
	tsv.vstreamer.InitDBConfig(target.Keyspace)
	tsv.hs.InitDBConfig(target)
	tsv.onlineDDLExecutor.InitDBConfig(target.Keyspace, target.Shard, dbcfgs.DBName)
	tsv.lagThrottler.InitDBConfig(target.Keyspace, target.Shard)
	tsv.tableGC.InitDBConfig(target.Keyspace, target.Shard, dbcfgs.DBName)
	return nil
}

// Register prepares TabletServer for serving by calling
// all the registrations functions.
func (tsv *TabletServer) Register() {
	for _, f := range RegisterFunctions {
		f(tsv)
	}
}

// Exporter satisfies tabletenv.Env.
func (tsv *TabletServer) Exporter() *servenv.Exporter {
	return tsv.exporter
}

// Config satisfies tabletenv.Env.
func (tsv *TabletServer) Config() *tabletenv.TabletConfig {
	return tsv.config
}

// Stats satisfies tabletenv.Env.
func (tsv *TabletServer) Stats() *tabletenv.Stats {
	return tsv.stats
}

// LogError satisfies tabletenv.Env.
func (tsv *TabletServer) LogError() {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		tsv.stats.InternalErrors.Add("Panic", 1)
	}
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
func (tsv *TabletServer) InitACL(tableACLConfigFile string, enforceTableACLConfig bool, reloadACLConfigFileInterval time.Duration) {
	tsv.initACL(tableACLConfigFile, enforceTableACLConfig)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for range sigChan {
			tsv.initACL(tableACLConfigFile, enforceTableACLConfig)
		}
	}()

	if reloadACLConfigFileInterval != 0 {
		ticker := time.NewTicker(reloadACLConfigFileInterval)
		go func() {
			for range ticker.C {
				sigChan <- syscall.SIGHUP
			}
		}()
	}
}

// SetServingType changes the serving type of the tabletserver. It starts or
// stops internal services as deemed necessary.
// Returns true if the state of QueryService or the tablet type changed.
func (tsv *TabletServer) SetServingType(tabletType topodatapb.TabletType, terTimestamp time.Time, serving bool, reason string) error {
	state := StateNotServing
	if serving {
		state = StateServing
	}
	return tsv.sm.SetServingType(tabletType, terTimestamp, state, reason)
}

// StartService is a convenience function for InitDBConfig->SetServingType
// with serving=true.
func (tsv *TabletServer) StartService(target querypb.Target, dbcfgs *dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) error {
	if err := tsv.InitDBConfig(target, dbcfgs, mysqld); err != nil {
		return err
	}
	// StartService is only used for testing. So, we cheat by aggressively setting replication to healthy.
	return tsv.sm.SetServingType(target.TabletType, time.Time{}, StateServing, "")
}

// StopService shuts down the tabletserver to the uninitialized state.
// It first transitions to StateShuttingDown, then waits for active
// services to shut down. Then it shuts down the rest. This function
// should be called before process termination, or if MySQL is unreachable.
// Under normal circumstances, SetServingType should be called.
func (tsv *TabletServer) StopService() {
	tsv.sm.StopService()
}

// IsHealthy returns nil for non-serving types or if the query service is healthy (able to
// connect to the database and serving traffic), or an error explaining
// the unhealthiness otherwise.
func (tsv *TabletServer) IsHealthy() error {
	if tsv.IsServingType() {
		_, err := tsv.Execute(
			tabletenv.LocalContext(),
			nil,
			"/* health */ select 1 from dual",
			nil,
			0,
			0,
			nil,
		)
		return err
	}
	return nil
}

// IsServingType returns true if the tablet type is one that should be serving to be healthy, or false if the tablet type
// should not be serving in it's healthy state.
func (tsv *TabletServer) IsServingType() bool {
	switch tsv.sm.Target().TabletType {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_BATCH, topodatapb.TabletType_EXPERIMENTAL:
		return true
	default:
		return false
	}
}

// ReloadSchema reloads the schema.
func (tsv *TabletServer) ReloadSchema(ctx context.Context) error {
	return tsv.se.Reload(ctx)
}

// WaitForSchemaReset blocks the TabletServer until there's been at least `timeout` duration without
// any schema changes. This is useful for tests that need to wait for all the currently existing schema
// changes to finish being applied.
func (tsv *TabletServer) WaitForSchemaReset(timeout time.Duration) {
	onSchemaChange := make(chan struct{}, 1)
	tsv.se.RegisterNotifier("_tsv_wait", func(_ map[string]*schema.Table, _, _, _ []string) {
		onSchemaChange <- struct{}{}
	})
	defer tsv.se.UnregisterNotifier("_tsv_wait")

	after := time.NewTimer(timeout)
	defer after.Stop()

	for {
		select {
		case <-after.C:
			return
		case <-onSchemaChange:
			if !after.Stop() {
				<-after.C
			}
			after.Reset(timeout)
		}
	}
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

// OnlineDDLExecutor returns the onlineddl.Executor part of TabletServer.
func (tsv *TabletServer) OnlineDDLExecutor() vexec.Executor {
	return tsv.onlineDDLExecutor
}

// LagThrottler returns the throttle.Throttler part of TabletServer.
func (tsv *TabletServer) LagThrottler() *throttle.Throttler {
	return tsv.lagThrottler
}

// TableGC returns the tableDropper part of TabletServer.
func (tsv *TabletServer) TableGC() *gc.TableGC {
	return tsv.tableGC
}

// SchemaEngine returns the SchemaEngine part of TabletServer.
func (tsv *TabletServer) SchemaEngine() *schema.Engine {
	return tsv.se
}

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (tsv *TabletServer) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (transactionID int64, tablet *topodatapb.TabletAlias, err error) {
	return tsv.begin(ctx, target, nil, 0, options)
}

func (tsv *TabletServer) begin(ctx context.Context, target *querypb.Target, preQueries []string, reservedID int64, options *querypb.ExecuteOptions) (transactionID int64, tablet *topodatapb.TabletAlias, err error) {
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Begin", "begin", nil,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			startTime := time.Now()
			if tsv.txThrottler.Throttle() {
				return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Transaction throttled")
			}
			var beginSQL string
			transactionID, beginSQL, err = tsv.te.Begin(ctx, preQueries, reservedID, options)
			logStats.TransactionID = transactionID
			logStats.ReservedID = reservedID

			// Record the actual statements that were executed in the logStats.
			// If nothing was actually executed, don't count the operation in
			// the tablet metrics, and clear out the logStats Method so that
			// handlePanicAndSendLogStats doesn't log the no-op.
			logStats.OriginalSQL = beginSQL
			if beginSQL != "" {
				tsv.stats.QueryTimings.Record("BEGIN", startTime)
			} else {
				logStats.Method = ""
			}
			return err
		},
	)
	return transactionID, &tsv.alias, err
}

// Commit commits the specified transaction.
func (tsv *TabletServer) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (newReservedID int64, err error) {
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Commit", "commit", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			startTime := time.Now()
			logStats.TransactionID = transactionID

			var commitSQL string
			newReservedID, commitSQL, err = tsv.te.Commit(ctx, transactionID)
			if newReservedID > 0 {
				// commit executed on old reserved id.
				logStats.ReservedID = transactionID
			}

			// If nothing was actually executed, don't count the operation in
			// the tablet metrics, and clear out the logStats Method so that
			// handlePanicAndSendLogStats doesn't log the no-op.
			if commitSQL != "" {
				tsv.stats.QueryTimings.Record("COMMIT", startTime)
			} else {
				logStats.Method = ""
			}
			return err
		},
	)
	return newReservedID, err
}

// Rollback rollsback the specified transaction.
func (tsv *TabletServer) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (newReservedID int64, err error) {
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Rollback", "rollback", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("ROLLBACK", time.Now())
			logStats.TransactionID = transactionID
			newReservedID, err = tsv.te.Rollback(ctx, transactionID)
			if newReservedID > 0 {
				// rollback executed on old reserved id.
				logStats.ReservedID = transactionID
			}
			return err
		},
	)
	return newReservedID, err
}

// Prepare prepares the specified transaction.
func (tsv *TabletServer) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Prepare", "prepare", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
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
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
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
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
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
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
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
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
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
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
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
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
			}
			return txe.ConcludeTransaction(dtid)
		},
	)
}

// ReadTransaction returns the metadata for the specified dtid.
func (tsv *TabletServer) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"ReadTransaction", "read_transaction", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			txe := &TxExecutor{
				ctx:      ctx,
				logStats: logStats,
				te:       tsv.te,
			}
			metadata, err = txe.ReadTransaction(dtid)
			return err
		},
	)
	return metadata, err
}

// Execute executes the query and returns the result as response.
func (tsv *TabletServer) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (result *sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "TabletServer.Execute")
	trace.AnnotateSQL(span, sql)
	defer span.Finish()

	if transactionID != 0 && reservedID != 0 && transactionID != reservedID {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "transactionID and reserveID must match if both are non-zero")
	}

	allowOnShutdown := false
	timeout := tsv.QueryTimeout.Get()
	if transactionID != 0 {
		allowOnShutdown = true
		// Use the smaller of the two values (0 means infinity).
		// TODO(sougou): Assign deadlines to each transaction and set query timeout accordingly.
		timeout = smallerTimeout(timeout, tsv.txTimeout.Get())
	}
	err = tsv.execRequest(
		ctx, timeout,
		"Execute", sql, bindVariables,
		target, options, allowOnShutdown,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]*querypb.BindVariable)
			}
			query, comments := sqlparser.SplitMarginComments(sql)
			plan, err := tsv.qe.GetPlan(ctx, logStats, query, skipQueryPlanCache(options), reservedID != 0)
			if err != nil {
				return err
			}
			// If both the values are non-zero then by design they are same value. So, it is safe to overwrite.
			connID := reservedID
			if transactionID != 0 {
				connID = transactionID
			}
			logStats.ReservedID = reservedID
			logStats.TransactionID = transactionID
			qre := &QueryExecutor{
				query:          query,
				marginComments: comments,
				bindVars:       bindVariables,
				connID:         connID,
				options:        options,
				plan:           plan,
				ctx:            ctx,
				logStats:       logStats,
				tsv:            tsv,
				tabletType:     target.GetTabletType(),
			}
			result, err = qre.Execute()
			if err != nil {
				return err
			}
			result = result.StripMetadata(sqltypes.IncludeFieldsOrDefault(options))

			// Change database name in mysql output to the keyspace name
			if tsv.sm.target.Keyspace != tsv.config.DB.DBName && sqltypes.IncludeFieldsOrDefault(options) == querypb.ExecuteOptions_ALL {
				switch qre.plan.PlanID {
				case planbuilder.PlanSelect, planbuilder.PlanSelectImpossible:
					dbName := tsv.config.DB.DBName
					ksName := tsv.sm.target.Keyspace
					for _, f := range result.Fields {
						if f.Database == dbName {
							f.Database = ksName
						}
					}
				}
			}
			return nil
		},
	)
	return result, err
}

// smallerTimeout returns the smaller of the two timeouts.
// 0 is treated as infinity.
func smallerTimeout(t1, t2 time.Duration) time.Duration {
	if t1 == 0 {
		return t2
	}
	if t2 == 0 {
		return t1
	}
	if t1 < t2 {
		return t1
	}
	return t2
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (tsv *TabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (err error) {
	return tsv.execRequest(
		ctx, 0,
		"StreamExecute", sql, bindVariables,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]*querypb.BindVariable)
			}
			query, comments := sqlparser.SplitMarginComments(sql)
			// TODO: update the isReservedConn logic when StreamExecute supports reserved connections.
			plan, err := tsv.qe.GetStreamPlan(query, false /* isReservedConn */)
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:          query,
				marginComments: comments,
				bindVars:       bindVariables,
				connID:         transactionID,
				options:        options,
				plan:           plan,
				ctx:            ctx,
				logStats:       logStats,
				tsv:            tsv,
			}
			newCallback := func(result *sqltypes.Result) error {
				if sqltypes.IncludeFieldsOrDefault(options) == querypb.ExecuteOptions_ALL {
					// Change database name in mysql output to the keyspace name
					for _, f := range result.Fields {
						if f.Database != "" {
							f.Database = tsv.sm.target.Keyspace
						}
					}
				}
				return callback(result)
			}
			return qre.Stream(newCallback)
		},
	)
}

// ExecuteBatch executes a group of queries and returns their results as a list.
// ExecuteBatch can be called for an existing transaction, or it can be called with
// the AsTransaction flag which will execute all statements inside an independent
// transaction. If AsTransaction is true, TransactionId must be 0.
// TODO(reserve-conn): Validate the use-case and Add support for reserve connection in ExecuteBatch
func (tsv *TabletServer) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (results []sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "TabletServer.ExecuteBatch")
	defer span.Finish()

	if len(queries) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Empty query list")
	}
	if asTransaction && transactionID != 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot start a new transaction in the scope of an existing one")
	}

	if tsv.enableHotRowProtection && asTransaction {
		// Serialize transactions which target the same hot row range.
		// NOTE: We put this intentionally at this place *before* StartRequest()
		// gets called below. Otherwise, the StartRequest()/EndRequest() section from
		// below would overlap with the StartRequest()/EndRequest() section executed
		// by tsv.beginWaitForSameRangeTransactions().
		txDone, err := tsv.beginWaitForSameRangeTransactions(ctx, target, options, queries[0].Sql, queries[0].BindVariables)
		if err != nil {
			return nil, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	allowOnShutdown := transactionID != 0
	// TODO(sougou): Convert StartRequest/EndRequest pattern to use wrapper
	// function tsv.execRequest() instead.
	// Note that below we always return "err" right away and do not call
	// tsv.convertAndLogError. That's because the methods which returned "err",
	// e.g. tsv.Execute(), already called that function and therefore already
	// converted and logged the error.
	if err = tsv.sm.StartRequest(ctx, target, allowOnShutdown); err != nil {
		return nil, err
	}
	defer tsv.sm.EndRequest()
	defer tsv.handlePanicAndSendLogStats("batch", nil, nil)

	if options == nil {
		options = &querypb.ExecuteOptions{}
	}

	// When all these conditions are met, we send the queries directly
	// to the MySQL without creating a transaction. This optimization
	// yields better throughput.
	// Setting ExecuteOptions_AUTOCOMMIT will get a connection out of the
	// pool without actually begin/commit the transaction.
	if (options.TransactionIsolation == querypb.ExecuteOptions_DEFAULT) &&
		asTransaction &&
		planbuilder.PassthroughDMLs {
		options.TransactionIsolation = querypb.ExecuteOptions_AUTOCOMMIT
	}

	if asTransaction {
		// We ignore the return alias because this transaction only exists in the scope of this call
		transactionID, _, err = tsv.Begin(ctx, target, options)
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
		localReply, err := tsv.Execute(ctx, target, bound.Sql, bound.BindVariables, transactionID, 0, options)
		if err != nil {
			return nil, err
		}
		results = append(results, *localReply)
	}
	if asTransaction {
		if _, err = tsv.Commit(ctx, target, transactionID); err != nil {
			transactionID = 0
			return nil, err
		}
		transactionID = 0
	}
	return results, nil
}

// BeginExecute combines Begin and Execute.
func (tsv *TabletServer) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {

	// Disable hot row protection in case of reserve connection.
	if tsv.enableHotRowProtection && reservedID == 0 {
		txDone, err := tsv.beginWaitForSameRangeTransactions(ctx, target, options, sql, bindVariables)
		if err != nil {
			return nil, 0, nil, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	transactionID, alias, err := tsv.begin(ctx, target, preQueries, reservedID, options)
	if err != nil {
		return nil, 0, nil, err
	}

	result, err := tsv.Execute(ctx, target, sql, bindVariables, transactionID, reservedID, options)
	return result, transactionID, alias, err
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
		target, options, false, /* allowOnShutdown */
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
				tsv.stats.WaitTimings.Record("TxSerializer", startTime)
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
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql, false /* skipQueryPlanCache */, false /* isReservedConn */)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to get plan for query: %v err: %v", sql, err)
		return "", ""
	}

	switch plan.PlanID {
	// Serialize only UPDATE or DELETE queries.
	case planbuilder.PlanUpdate, planbuilder.PlanUpdateLimit,
		planbuilder.PlanDelete, planbuilder.PlanDeleteLimit:
	default:
		return "", ""
	}

	tableName := plan.TableName()
	if tableName.IsEmpty() || plan.WhereClause == nil {
		// Do not serialize any queries without table name or where clause
		return "", ""
	}

	where, err := plan.WhereClause.GenerateQuery(bindVariables, nil)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to substitute bind vars in where clause: %v query: %v bind vars: %v", err, sql, bindVariables)
		return "", ""
	}

	// Example: table1 where id = 1 and sub_id = 2
	key := fmt.Sprintf("%s%s", tableName, where)
	return key, tableName.String()
}

// BeginExecuteBatch combines Begin and ExecuteBatch.
func (tsv *TabletServer) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	// TODO(mberlin): Integrate hot row protection here as we did for BeginExecute()
	// and ExecuteBatch(asTransaction=true).
	transactionID, alias, err := tsv.Begin(ctx, target, options)
	if err != nil {
		return nil, 0, nil, err
	}

	results, err := tsv.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, alias, err
}

// MessageStream streams messages from the requested table.
func (tsv *TabletServer) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) (err error) {
	return tsv.execRequest(
		ctx, 0,
		"MessageStream", "stream", nil,
		target, nil, false, /* allowOnShutdown */
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
	if err = tsv.sm.StartRequest(ctx, target, false /* allowOnShutdown */); err != nil {
		return 0, err
	}
	defer tsv.sm.EndRequest()
	defer tsv.handlePanicAndSendLogStats("ack", nil, nil)

	query, bv, err := queryGenerator()
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}

	transactionID, _, err := tsv.Begin(ctx, target, nil)
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
	qr, err := tsv.Execute(ctx, target, query, bv, transactionID, 0, nil)
	if err != nil {
		return 0, err
	}
	if _, err = tsv.Commit(ctx, target, transactionID); err != nil {
		transactionID = 0
		return 0, err
	}
	transactionID = 0
	return int64(qr.RowsAffected), nil
}

// VStream streams VReplication events.
func (tsv *TabletServer) VStream(ctx context.Context, target *querypb.Target, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	if err := tsv.sm.VerifyTarget(ctx, target); err != nil {
		return err
	}
	return tsv.vstreamer.Stream(ctx, startPos, tablePKs, filter, send)
}

// VStreamRows streams rows from the specified starting point.
func (tsv *TabletServer) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	if err := tsv.sm.VerifyTarget(ctx, target); err != nil {
		return err
	}
	var row []sqltypes.Value
	if lastpk != nil {
		r := sqltypes.Proto3ToResult(lastpk)
		if len(r.Rows) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected lastpk input: %v", lastpk)
		}
		row = r.Rows[0]
	}
	return tsv.vstreamer.StreamRows(ctx, query, row, send)
}

// VStreamResults streams rows from the specified starting point.
func (tsv *TabletServer) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	if err := tsv.sm.VerifyTarget(ctx, target); err != nil {
		return err
	}
	return tsv.vstreamer.StreamResults(ctx, query, send)
}

//ReserveBeginExecute implements the QueryService interface
func (tsv *TabletServer) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {

	var connID int64
	var err error

	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"ReserveBegin", "begin", bindVariables,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RESERVE", time.Now())
			connID, err = tsv.te.ReserveBegin(ctx, options, preQueries)
			if err != nil {
				return err
			}
			logStats.TransactionID = connID
			logStats.ReservedID = connID
			return nil
		},
	)

	if err != nil {
		return nil, 0, 0, nil, err
	}

	result, err := tsv.Execute(ctx, target, sql, bindVariables, connID, connID, options)
	return result, connID, connID, &tsv.alias, err
}

//ReserveExecute implements the QueryService interface
func (tsv *TabletServer) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	var connID int64
	var err error

	err = tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Reserve", "", bindVariables,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RESERVE", time.Now())
			connID, err = tsv.te.Reserve(ctx, options, transactionID, preQueries)
			if err != nil {
				return err
			}
			logStats.TransactionID = transactionID
			logStats.ReservedID = connID
			return nil
		},
	)

	if err != nil {
		return nil, 0, nil, err
	}

	result, err := tsv.Execute(ctx, target, sql, bindVariables, connID, connID, options)
	return result, connID, &tsv.alias, err
}

//Release implements the QueryService interface
func (tsv *TabletServer) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	if reservedID == 0 && transactionID == 0 {
		return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "Connection Id and Transaction ID does not exists")
	}
	return tsv.execRequest(
		ctx, tsv.QueryTimeout.Get(),
		"Release", "", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RELEASE", time.Now())
			logStats.TransactionID = transactionID
			logStats.ReservedID = reservedID
			if reservedID != 0 {
				//Release to close the underlying connection.
				return tsv.te.Release(reservedID)
			}
			// Rollback to cleanup the transaction before returning to the pool.
			_, err := tsv.te.Rollback(ctx, transactionID)
			return err
		},
	)
}

// execRequest performs verifications, sets up the necessary environments
// and calls the supplied function for executing the request.
func (tsv *TabletServer) execRequest(
	ctx context.Context, timeout time.Duration,
	requestName, sql string, bindVariables map[string]*querypb.BindVariable,
	target *querypb.Target, options *querypb.ExecuteOptions, allowOnShutdown bool,
	exec func(ctx context.Context, logStats *tabletenv.LogStats) error,
) (err error) {
	span, ctx := trace.NewSpan(ctx, "TabletServer."+requestName)
	if options != nil {
		span.Annotate("isolation-level", options.TransactionIsolation)
	}
	trace.AnnotateSQL(span, sql)
	if target != nil {
		span.Annotate("cell", target.Cell)
		span.Annotate("shard", target.Shard)
		span.Annotate("keyspace", target.Keyspace)
	}
	defer span.Finish()

	logStats := tabletenv.NewLogStats(ctx, requestName)
	logStats.Target = target
	logStats.OriginalSQL = sql
	logStats.BindVariables = bindVariables
	defer tsv.handlePanicAndSendLogStats(sql, bindVariables, logStats)
	if err = tsv.sm.StartRequest(ctx, target, allowOnShutdown); err != nil {
		return err
	}

	ctx, cancel := withTimeout(ctx, timeout, options)
	defer func() {
		cancel()
		tsv.sm.EndRequest()
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
		tsv.stats.InternalErrors.Add("Panic", 1)
		if logStats != nil {
			logStats.Error = terr
		}
	}
	// Examples where we don't send the log stats:
	// - ExecuteBatch() (logStats == nil)
	// - beginWaitForSameRangeTransactions() (Method == "")
	// - Begin / Commit in autocommit mode
	if logStats != nil && logStats.Method != "" {
		logStats.Send()
	}
}

func (tsv *TabletServer) convertAndLogError(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, err error, logStats *tabletenv.LogStats) error {
	if err == nil {
		return nil
	}

	errCode := convertErrorCode(err)
	tsv.stats.ErrorCounters.Add(errCode.String(), 1)

	callerID := ""
	cid := callerid.ImmediateCallerIDFromContext(ctx)
	if cid != nil {
		callerID = fmt.Sprintf(" (CallerID: %s)", cid.Username)
	}

	logMethod := log.Errorf
	// Suppress or demote some errors in logs.
	switch errCode {
	case vtrpcpb.Code_FAILED_PRECONDITION, vtrpcpb.Code_ALREADY_EXISTS:
		logMethod = nil
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		logMethod = logPoolFull.Errorf
	case vtrpcpb.Code_ABORTED:
		logMethod = log.Warningf
	case vtrpcpb.Code_INVALID_ARGUMENT, vtrpcpb.Code_DEADLINE_EXCEEDED:
		logMethod = log.Infof
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
	bv := buf.String()
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
		mysql.ErNoReferencedRow2, mysql.ERWarnDataOutOfRange:
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
		errCode = vtrpcpb.Code_CANCELED
	}

	return errCode
}

// StreamHealth streams the health status to callback.
func (tsv *TabletServer) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return tsv.hs.Stream(ctx, callback)
}

// BroadcastHealth will broadcast the current health to all listeners
func (tsv *TabletServer) BroadcastHealth() {
	tsv.sm.Broadcast()
}

// EnterLameduck causes tabletserver to enter the lameduck state. This
// state causes health checks to fail, but the behavior of tabletserver
// otherwise remains the same. Any subsequent calls to SetServingType will
// cause the tabletserver to exit this mode.
func (tsv *TabletServer) EnterLameduck() {
	tsv.sm.EnterLameduck()
}

// ExitLameduck causes the tabletserver to exit the lameduck mode.
func (tsv *TabletServer) ExitLameduck() {
	tsv.sm.ExitLameduck()
}

// IsServing returns true if TabletServer is in SERVING state.
func (tsv *TabletServer) IsServing() bool {
	return tsv.sm.IsServing()
}

// CheckMySQL initiates a check to see if MySQL is reachable.
// If not, it shuts down the query service. The check is rate-limited
// to no more than once per second.
// The function satisfies tabletenv.Env.
func (tsv *TabletServer) CheckMySQL() {
	tsv.sm.CheckMySQL()
}

// TopoServer returns the topo server.
func (tsv *TabletServer) TopoServer() *topo.Server {
	return tsv.topoServer
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

var okMessage = []byte("ok\n")

// Health check
// Returns ok if we are in the desired serving state
func (tsv *TabletServer) registerHealthzHealthHandler() {
	tsv.exporter.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
			acl.SendError(w, err)
			return
		}
		if tsv.sm.wantState == StateServing && !tsv.sm.IsServing() {
			http.Error(w, "500 internal server error: vttablet is not serving", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%v", len(okMessage)))
		w.WriteHeader(http.StatusOK)
		w.Write(okMessage)
	})
}

// Query service health check
// Returns ok if a query can go all the way to database and back
func (tsv *TabletServer) registerDebugHealthHandler() {
	tsv.exporter.HandleFunc("/debug/health", func(w http.ResponseWriter, r *http.Request) {
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
	tsv.exporter.HandleFunc("/queryz", func(w http.ResponseWriter, r *http.Request) {
		queryzHandler(tsv.qe, w, r)
	})
}

func (tsv *TabletServer) registerQueryListHandlers(queryLists []*QueryList) {
	tsv.exporter.HandleFunc("/livequeryz/", func(w http.ResponseWriter, r *http.Request) {
		livequeryzHandler(queryLists, w, r)
	})
	tsv.exporter.HandleFunc("/livequeryz/terminate", func(w http.ResponseWriter, r *http.Request) {
		livequeryzTerminateHandler(queryLists, w, r)
	})
}

func (tsv *TabletServer) registerTwopczHandler() {
	tsv.exporter.HandleFunc("/twopcz", func(w http.ResponseWriter, r *http.Request) {
		ctx := tabletenv.LocalContext()
		txe := &TxExecutor{
			ctx:      ctx,
			logStats: tabletenv.NewLogStats(ctx, "twopcz"),
			te:       tsv.te,
		}
		twopczHandler(txe, w, r)
	})
}

func (tsv *TabletServer) registerMigrationStatusHandler() {
	tsv.exporter.HandleFunc("/schema-migration/report-status", func(w http.ResponseWriter, r *http.Request) {
		ctx := tabletenv.LocalContext()
		query := r.URL.Query()
		if err := tsv.onlineDDLExecutor.OnSchemaMigrationStatus(ctx, query.Get("uuid"), query.Get("status"), query.Get("dryrun"), query.Get("progress"), query.Get("eta")); err != nil {
			http.Error(w, fmt.Sprintf("not ok: %v", err), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})
}

// registerThrottlerCheckHandlers registers throttler "check" requests
func (tsv *TabletServer) registerThrottlerCheckHandlers() {
	handle := func(path string, checkType throttle.ThrottleCheckType) {
		tsv.exporter.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			ctx := tabletenv.LocalContext()
			remoteAddr := r.Header.Get("X-Forwarded-For")
			if remoteAddr == "" {
				remoteAddr = r.RemoteAddr
				remoteAddr = strings.Split(remoteAddr, ":")[0]
			}
			appName := r.URL.Query().Get("app")
			if appName == "" {
				appName = throttle.DefaultAppName
			}
			flags := &throttle.CheckFlags{
				LowPriority: (r.URL.Query().Get("p") == "low"),
			}
			checkResult := tsv.lagThrottler.CheckByType(ctx, appName, remoteAddr, flags, checkType)
			if checkResult.StatusCode == http.StatusNotFound && flags.OKIfNotExists {
				checkResult.StatusCode = http.StatusOK // 200
			}

			if r.Method == http.MethodGet {
				w.Header().Set("Content-Type", "application/json")
			}
			w.WriteHeader(checkResult.StatusCode)
			if r.Method == http.MethodGet {
				json.NewEncoder(w).Encode(checkResult)
			}
		})
	}
	handle("/throttler/check", throttle.ThrottleCheckPrimaryWrite)
	handle("/throttler/check-self", throttle.ThrottleCheckSelf)
}

// registerThrottlerStatusHandler registers a throttler "status" request
func (tsv *TabletServer) registerThrottlerStatusHandler() {
	tsv.exporter.HandleFunc("/throttler/status", func(w http.ResponseWriter, r *http.Request) {
		status := tsv.lagThrottler.Status()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})
}

// registerThrottlerThrottleAppHandler registers a throttler "throttle-app" request
func (tsv *TabletServer) registerThrottlerThrottleAppHandler() {
	tsv.exporter.HandleFunc("/throttler/throttle-app", func(w http.ResponseWriter, r *http.Request) {
		appName := r.URL.Query().Get("app")
		d, err := time.ParseDuration(r.URL.Query().Get("duration"))
		if err != nil {
			http.Error(w, fmt.Sprintf("not ok: %v", err), http.StatusInternalServerError)
			return
		}
		appThrottle := tsv.lagThrottler.ThrottleApp(appName, time.Now().Add(d), 1)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(appThrottle)
	})
	tsv.exporter.HandleFunc("/throttler/unthrottle-app", func(w http.ResponseWriter, r *http.Request) {
		appName := r.URL.Query().Get("app")
		appThrottle := tsv.lagThrottler.UnthrottleApp(appName)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(appThrottle)
	})
}

// registerThrottlerHandlers registers all throttler handlers
func (tsv *TabletServer) registerThrottlerHandlers() {
	tsv.registerThrottlerCheckHandlers()
	tsv.registerThrottlerStatusHandler()
	tsv.registerThrottlerThrottleAppHandler()
}

func (tsv *TabletServer) registerDebugEnvHandler() {
	tsv.exporter.HandleFunc("/debug/env", func(w http.ResponseWriter, r *http.Request) {
		debugEnvHandler(tsv, w, r)
	})
}

// EnableHeartbeat forces heartbeat to be on or off.
// Only to be used for testing.
func (tsv *TabletServer) EnableHeartbeat(enabled bool) {
	tsv.rt.EnableHeartbeat(enabled)
}

// EnableThrottler forces throttler to be on or off.
// When throttler is off, it responds to all check requests with HTTP 200 OK
// Only to be used for testing.
func (tsv *TabletServer) EnableThrottler(enabled bool) {
	tsv.Config().EnableLagThrottler = enabled
}

// SetTracking forces tracking to be on or off.
// Only to be used for testing.
func (tsv *TabletServer) SetTracking(enabled bool) {
	tsv.tracker.Enable(enabled)
}

// EnableHistorian forces historian to be on or off.
// Only to be used for testing.
func (tsv *TabletServer) EnableHistorian(enabled bool) {
	_ = tsv.se.EnableHistorian(enabled)
}

// SetPoolSize changes the pool size to the specified value.
func (tsv *TabletServer) SetPoolSize(val int) {
	if val <= 0 {
		return
	}
	tsv.qe.conns.SetCapacity(val)
}

// PoolSize returns the pool size.
func (tsv *TabletServer) PoolSize() int {
	return int(tsv.qe.conns.Capacity())
}

// SetStreamPoolSize changes the pool size to the specified value.
func (tsv *TabletServer) SetStreamPoolSize(val int) {
	tsv.qe.streamConns.SetCapacity(val)
}

// StreamPoolSize returns the pool size.
func (tsv *TabletServer) StreamPoolSize() int {
	return int(tsv.qe.streamConns.Capacity())
}

// SetTxPoolSize changes the tx pool size to the specified value.
func (tsv *TabletServer) SetTxPoolSize(val int) {
	tsv.te.txPool.scp.conns.SetCapacity(val)
}

// TxPoolSize returns the tx pool size.
func (tsv *TabletServer) TxPoolSize() int {
	return tsv.te.txPool.scp.Capacity()
}

// SetTxTimeout changes the transaction timeout to the specified value.
func (tsv *TabletServer) SetTxTimeout(val time.Duration) {
	tsv.te.txPool.SetTimeout(val)
	tsv.txTimeout.Set(val)
}

// TxTimeout returns the transaction timeout.
func (tsv *TabletServer) TxTimeout() time.Duration {
	return tsv.txTimeout.Get()
}

// SetQueryPlanCacheCap changes the pool size to the specified value.
func (tsv *TabletServer) SetQueryPlanCacheCap(val int) {
	tsv.qe.SetQueryPlanCacheCap(val)
}

// QueryPlanCacheCap returns the plan cache capacity
func (tsv *TabletServer) QueryPlanCacheCap() int {
	return tsv.qe.QueryPlanCacheCap()
}

// QueryPlanCacheLen returns the plan cache length
func (tsv *TabletServer) QueryPlanCacheLen() int {
	return tsv.qe.QueryPlanCacheLen()
}

// QueryPlanCacheWait waits until the query plan cache has processed all recent queries
func (tsv *TabletServer) QueryPlanCacheWait() {
	tsv.qe.plans.Wait()
}

// SetMaxResultSize changes the max result size to the specified value.
func (tsv *TabletServer) SetMaxResultSize(val int) {
	tsv.qe.maxResultSize.Set(int64(val))
}

// MaxResultSize returns the max result size.
func (tsv *TabletServer) MaxResultSize() int {
	return int(tsv.qe.maxResultSize.Get())
}

// SetWarnResultSize changes the warn result size to the specified value.
func (tsv *TabletServer) SetWarnResultSize(val int) {
	tsv.qe.warnResultSize.Set(int64(val))
}

// WarnResultSize returns the warn result size.
func (tsv *TabletServer) WarnResultSize() int {
	return int(tsv.qe.warnResultSize.Get())
}

// SetPassthroughDMLs changes the setting to pass through all DMLs
// It should only be used for testing
func (tsv *TabletServer) SetPassthroughDMLs(val bool) {
	planbuilder.PassthroughDMLs = val
}

// SetConsolidatorMode sets the consolidator mode.
func (tsv *TabletServer) SetConsolidatorMode(mode string) {
	switch mode {
	case tabletenv.NotOnMaster, tabletenv.Enable, tabletenv.Disable:
		tsv.qe.consolidatorMode.Set(mode)
	}
}

// ConsolidatorMode returns the consolidator mode.
func (tsv *TabletServer) ConsolidatorMode() string {
	return tsv.qe.consolidatorMode.Get()
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
	return buf.String()
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
	return options.SkipQueryPlanCache || options.HasCreatedTempTables
}
