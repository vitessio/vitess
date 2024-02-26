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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txserializer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
// perform one-time initializations and must be idempotent.
// Open and Close can be called repeatedly during the lifetime of
// a subcomponent. These should also be idempotent.
type TabletServer struct {
	exporter               *servenv.Exporter
	config                 *tabletenv.TabletConfig
	stats                  *tabletenv.Stats
	QueryTimeout           atomic.Int64
	TerseErrors            bool
	TruncateErrorLen       int
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
	txThrottler  txthrottler.TxThrottler
	te           *TxEngine
	messager     *messager.Engine
	hs           *healthStreamer
	lagThrottler *throttle.Throttler
	tableGC      *gc.TableGC

	// sm manages state transitions.
	sm                *stateManager
	onlineDDLExecutor *onlineddl.Executor

	// alias is used for identifying this tabletserver in healthcheck responses.
	alias *topodatapb.TabletAlias

	// This field is only stored for testing
	checkMysqlGaugeFunc *stats.GaugeFunc

	env *vtenv.Environment
}

var _ queryservice.QueryService = (*TabletServer)(nil)

// RegisterFunctions is a list of all the
// RegisterFunction that will be called upon
// Register() on a TabletServer
var RegisterFunctions []func(Controller)

// NewServer creates a new TabletServer based on the command line flags.
func NewServer(ctx context.Context, env *vtenv.Environment, name string, topoServer *topo.Server, alias *topodatapb.TabletAlias, srvTopoCounts *stats.CountersWithSingleLabel) *TabletServer {
	return NewTabletServer(ctx, env, name, tabletenv.NewCurrentConfig(), topoServer, alias, srvTopoCounts)
}

// NewTabletServer creates an instance of TabletServer. Only the first
// instance of TabletServer will expose its state variables.
func NewTabletServer(ctx context.Context, env *vtenv.Environment, name string, config *tabletenv.TabletConfig, topoServer *topo.Server, alias *topodatapb.TabletAlias, srvTopoCounts *stats.CountersWithSingleLabel) *TabletServer {
	exporter := servenv.NewExporter(name, "Tablet")
	tsv := &TabletServer{
		exporter:               exporter,
		stats:                  tabletenv.NewStats(exporter),
		config:                 config,
		TerseErrors:            config.TerseErrors,
		TruncateErrorLen:       config.TruncateErrorLen,
		enableHotRowProtection: config.HotRowProtection.Mode != tabletenv.Disable,
		topoServer:             topoServer,
		alias:                  alias.CloneVT(),
		env:                    env,
	}
	tsv.QueryTimeout.Store(config.Oltp.QueryTimeout.Nanoseconds())

	srvTopoServer := srvtopo.NewResilientServer(ctx, topoServer, srvTopoCounts)

	tabletTypeFunc := func() topodatapb.TabletType {
		if tsv.sm == nil || tsv.sm.Target() == nil {
			return topodatapb.TabletType_UNKNOWN
		}
		return tsv.sm.Target().TabletType
	}

	tsv.statelessql = NewQueryList("oltp-stateless", env.Parser())
	tsv.statefulql = NewQueryList("oltp-stateful", env.Parser())
	tsv.olapql = NewQueryList("olap", env.Parser())
	tsv.se = schema.NewEngine(tsv)
	tsv.hs = newHealthStreamer(tsv, alias, tsv.se)
	tsv.rt = repltracker.NewReplTracker(tsv, alias)
	tsv.lagThrottler = throttle.NewThrottler(tsv, srvTopoServer, topoServer, alias.Cell, tsv.rt.HeartbeatWriter(), tabletTypeFunc)
	tsv.vstreamer = vstreamer.NewEngine(tsv, srvTopoServer, tsv.se, tsv.lagThrottler, alias.Cell)
	tsv.tracker = schema.NewTracker(tsv, tsv.vstreamer, tsv.se)
	tsv.watcher = NewBinlogWatcher(tsv, tsv.vstreamer, tsv.config)
	tsv.qe = NewQueryEngine(tsv, tsv.se)
	tsv.txThrottler = txthrottler.NewTxThrottler(tsv, topoServer)
	tsv.te = NewTxEngine(tsv)
	tsv.messager = messager.NewEngine(tsv, tsv.se, tsv.vstreamer)

	tsv.tableGC = gc.NewTableGC(tsv, topoServer, tsv.lagThrottler)
	tsv.onlineDDLExecutor = onlineddl.NewExecutor(tsv, alias, topoServer, tsv.lagThrottler, tabletTypeFunc, tsv.onlineDDLExecutorToggleTableBuffer, tsv.tableGC.RequestChecks)

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
		rw:          newRequestsWaiter(),
	}

	tsv.exporter.NewGaugeFunc("TabletState", "Tablet server state", func() int64 { return int64(tsv.sm.State()) })
	tsv.checkMysqlGaugeFunc = tsv.exporter.NewGaugeFunc("CheckMySQLRunning", "Check MySQL operation currently in progress", tsv.sm.isCheckMySQLRunning)
	tsv.exporter.Publish("TabletStateName", stats.StringFunc(tsv.sm.IsServingString))

	// TabletServerState exports the same information as the above two stats (TabletState / TabletStateName),
	// but exported with TabletStateName as a label for Prometheus, which doesn't support exporting strings as stat values.
	tsv.exporter.NewGaugesFuncWithMultiLabels("TabletServerState", "Tablet server state labeled by state name", []string{"name"}, func() map[string]int64 {
		return map[string]int64{tsv.sm.IsServingString(): 1}
	})
	tsv.exporter.NewGaugeDurationFunc("QueryTimeout", "Tablet server query timeout", tsv.loadQueryTimeout)

	tsv.registerHealthzHealthHandler()
	tsv.registerDebugHealthHandler()
	tsv.registerQueryzHandler()
	tsv.registerQuerylogzHandler()
	tsv.registerTxlogzHandler()
	tsv.registerQueryListHandlers([]*QueryList{tsv.statelessql, tsv.statefulql, tsv.olapql})
	tsv.registerTwopczHandler()
	tsv.registerMigrationStatusHandler()
	tsv.registerThrottlerHandlers()
	tsv.registerDebugEnvHandler()

	return tsv
}

func (tsv *TabletServer) loadQueryTimeout() time.Duration {
	return time.Duration(tsv.QueryTimeout.Load())
}

// onlineDDLExecutorToggleTableBuffer is called by onlineDDLExecutor as a callback function. onlineDDLExecutor
// uses it to start/stop query buffering for a given table.
// It is onlineDDLExecutor's responsibility to make sure buffering is stopped after some definite amount of time.
// There are two layers to buffering/unbuffering:
//  1. the creation and destruction of a QueryRuleSource. The existence of such source affects query plan rules
//     for all new queries (see Execute() function and call to GetPlan())
//  2. affecting already existing rules: a Rule has a context.WithCancel, that is cancelled by onlineDDLExecutor
func (tsv *TabletServer) onlineDDLExecutorToggleTableBuffer(bufferingCtx context.Context, tableName string, timeout time.Duration, bufferQueries bool) {
	queryRuleSource := fmt.Sprintf("onlineddl/%s", tableName)

	if bufferQueries {
		tsv.RegisterQueryRuleSource(queryRuleSource)
		bufferRules := rules.New()

		bufferRules.Add(rules.NewBufferedTableQueryRule(bufferingCtx, tableName, timeout, "buffered for cut-over"))
		tsv.SetQueryRules(queryRuleSource, bufferRules)
	} else {
		tsv.UnRegisterQueryRuleSource(queryRuleSource) // new rules will not have buffering. Existing rules will be affected by bufferingContext.Done()
	}
}

// InitDBConfig initializes the db config variables for TabletServer. You must call this function
// to complete the creation of TabletServer.
func (tsv *TabletServer) InitDBConfig(target *querypb.Target, dbcfgs *dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) error {
	if tsv.sm.State() != StateNotConnected {
		return vterrors.NewErrorf(vtrpcpb.Code_UNAVAILABLE, vterrors.ServerNotAvailable, "Server isn't available")
	}
	tsv.sm.Init(tsv, target)
	tsv.sm.target = target.CloneVT()
	tsv.config.DB = dbcfgs

	tsv.se.InitDBConfig(tsv.config.DB.DbaWithDB())
	tsv.rt.InitDBConfig(target, mysqld)
	tsv.txThrottler.InitDBConfig(target)
	tsv.vstreamer.InitDBConfig(target.Keyspace, target.Shard)
	tsv.hs.InitDBConfig(target, tsv.config.DB.DbaWithDB())
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

// Environment satisfies tabletenv.Env.
func (tsv *TabletServer) Environment() *vtenv.Environment {
	return tsv.env
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
func (tsv *TabletServer) SetServingType(tabletType topodatapb.TabletType, ptsTimestamp time.Time, serving bool, reason string) error {
	state := StateNotServing
	if serving {
		state = StateServing
	}
	return tsv.sm.SetServingType(tabletType, ptsTimestamp, state, reason)
}

// StartService is a convenience function for InitDBConfig->SetServingType
// with serving=true.
func (tsv *TabletServer) StartService(target *querypb.Target, dbcfgs *dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) error {
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
	if topoproto.IsServingType(tsv.sm.Target().TabletType) {
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

// ReloadSchema reloads the schema.
func (tsv *TabletServer) ReloadSchema(ctx context.Context) error {
	return tsv.se.Reload(ctx)
}

// WaitForSchemaReset blocks the TabletServer until there's been at least `timeout` duration without
// any schema changes. This is useful for tests that need to wait for all the currently existing schema
// changes to finish being applied.
func (tsv *TabletServer) WaitForSchemaReset(timeout time.Duration) {
	onSchemaChange := make(chan struct{}, 1)
	tsv.se.RegisterNotifier("_tsv_wait", func(_ map[string]*schema.Table, _, _, _ []*schema.Table) {
		onSchemaChange <- struct{}{}
	}, true)
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

// LagThrottler returns the throttle.Throttler part of TabletServer.
func (tsv *TabletServer) LagThrottler() *throttle.Throttler {
	return tsv.lagThrottler
}

// TableGC returns the tableDropper part of TabletServer.
func (tsv *TabletServer) TableGC() *gc.TableGC {
	return tsv.tableGC
}

// TwoPCEngineWait waits until the TwoPC engine has been opened, and the redo read
func (tsv *TabletServer) TwoPCEngineWait() {
	tsv.te.twoPCReady.Wait()
}

// SchemaEngine returns the SchemaEngine part of TabletServer.
func (tsv *TabletServer) SchemaEngine() *schema.Engine {
	return tsv.se
}

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (tsv *TabletServer) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (state queryservice.TransactionState, err error) {
	return tsv.begin(ctx, target, nil, 0, nil, options)
}

func (tsv *TabletServer) begin(ctx context.Context, target *querypb.Target, savepointQueries []string, reservedID int64, settings []string, options *querypb.ExecuteOptions) (state queryservice.TransactionState, err error) {
	state.TabletAlias = tsv.alias
	err = tsv.execRequest(
		ctx, tsv.loadQueryTimeout(),
		"Begin", "begin", nil,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			startTime := time.Now()
			if tsv.txThrottler.Throttle(tsv.getPriorityFromOptions(options), options.GetWorkloadName()) {
				return errTxThrottled
			}
			var connSetting *smartconnpool.Setting
			if len(settings) > 0 {
				connSetting, err = tsv.qe.GetConnSetting(ctx, settings)
				if err != nil {
					return err
				}
			}
			transactionID, beginSQL, sessionStateChanges, err := tsv.te.Begin(ctx, savepointQueries, reservedID, connSetting, options)
			state.TransactionID = transactionID
			state.SessionStateChanges = sessionStateChanges
			logStats.TransactionID = transactionID
			logStats.ReservedID = reservedID

			// Record the actual statements that were executed in the logStats.
			// If nothing was actually executed, don't count the operation in
			// the tablet metrics, and clear out the logStats Method so that
			// handlePanicAndSendLogStats doesn't log the no-op.
			logStats.OriginalSQL = beginSQL
			if beginSQL != "" {
				tsv.stats.QueryTimings.Record("BEGIN", startTime)
				targetType, err := tsv.resolveTargetType(ctx, target)
				if err != nil {
					return err
				}
				tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), startTime)
			} else {
				logStats.Method = ""
			}
			return err
		},
	)
	return state, err
}

func (tsv *TabletServer) getPriorityFromOptions(options *querypb.ExecuteOptions) int {
	priority := tsv.config.TxThrottlerDefaultPriority
	if options == nil {
		return priority
	}
	if options.Priority == "" {
		return priority
	}

	optionsPriority, err := strconv.Atoi(options.Priority)
	// This should never error out, as the value for Priority has been validated in the vtgate already.
	// Still, handle it just to make sure.
	if err != nil {
		log.Errorf(
			"The value of the %s query directive could not be converted to integer, using the "+
				"default value. Error was: %s",
			sqlparser.DirectivePriority, priority, err)

		return priority
	}

	return optionsPriority
}

// resolveTargetType returns the appropriate target tablet type for a
// TabletServer request. If the caller has a local context then it's
// an internal request and the target is the local tablet's current
// target. If it's not a local context then there should always be a
// non-nil target specified.
func (tsv *TabletServer) resolveTargetType(ctx context.Context, target *querypb.Target) (topodatapb.TabletType, error) {
	if target != nil {
		return target.TabletType, nil
	}
	if !tabletenv.IsLocalContext(ctx) {
		return topodatapb.TabletType_UNKNOWN, ErrNoTarget
	}
	if tsv.sm.Target() == nil {
		return topodatapb.TabletType_UNKNOWN, nil // This is true, and does not block the request
	}
	return tsv.sm.Target().TabletType, nil
}

// Commit commits the specified transaction.
func (tsv *TabletServer) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (newReservedID int64, err error) {
	err = tsv.execRequest(
		ctx, tsv.loadQueryTimeout(),
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
				targetType, err := tsv.resolveTargetType(ctx, target)
				if err != nil {
					return err
				}
				tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), startTime)
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
		ctx, tsv.loadQueryTimeout(),
		"Rollback", "rollback", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("ROLLBACK", time.Now())
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			defer tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), time.Now())
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
		ctx, tsv.loadQueryTimeout(),
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
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	if transactionID != 0 && reservedID != 0 && transactionID != reservedID {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] transactionID and reserveID must match if both are non-zero")
	}

	return tsv.execute(ctx, target, sql, bindVariables, transactionID, reservedID, nil, options)
}

func (tsv *TabletServer) execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, settings []string, options *querypb.ExecuteOptions) (result *sqltypes.Result, err error) {
	allowOnShutdown := false
	timeout := tsv.loadQueryTimeout()
	if transactionID != 0 {
		allowOnShutdown = true
		// Execute calls happen for OLTP only, so we can directly fetch the
		// OLTP TX timeout.
		txTimeout := tsv.config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLTP)
		// Use the smaller of the two values (0 means infinity).
		// TODO(sougou): Assign deadlines to each transaction and set query timeout accordingly.
		timeout = smallerTimeout(timeout, txTimeout)
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

			plan, err := tsv.qe.GetPlan(ctx, logStats, query, skipQueryPlanCache(options))
			if err != nil {
				return err
			}
			if err = plan.IsValid(reservedID != 0, len(settings) > 0); err != nil {
				return err
			}
			// If both the values are non-zero then by design they are same value. So, it is safe to overwrite.
			connID := reservedID
			if transactionID != 0 {
				connID = transactionID
			}
			logStats.ReservedID = reservedID
			logStats.TransactionID = transactionID

			var connSetting *smartconnpool.Setting
			if len(settings) > 0 {
				connSetting, err = tsv.qe.GetConnSetting(ctx, settings)
				if err != nil {
					return err
				}
			}
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			qre := &QueryExecutor{
				query:            query,
				marginComments:   comments,
				bindVars:         bindVariables,
				connID:           connID,
				options:          options,
				plan:             plan,
				ctx:              ctx,
				logStats:         logStats,
				tsv:              tsv,
				targetTabletType: targetType,
				setting:          connSetting,
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
	return min(t1, t2)
}

// StreamExecute executes the query and streams the result.
// The first QueryResult will have Fields set (and Rows nil).
// The subsequent QueryResult will have Rows set (and Fields nil).
func (tsv *TabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (err error) {
	if transactionID != 0 && reservedID != 0 && transactionID != reservedID {
		return vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] transactionID and reserveID must match if both are non-zero")
	}

	return tsv.streamExecute(ctx, target, sql, bindVariables, transactionID, reservedID, nil, options, callback)
}

func (tsv *TabletServer) streamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, settings []string, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	allowOnShutdown := false
	var timeout time.Duration
	if transactionID != 0 {
		allowOnShutdown = true
		// Use the transaction timeout. StreamExecute calls happen for OLAP only,
		// so we can directly fetch the OLAP TX timeout.
		timeout = tsv.config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLAP)
	}

	return tsv.execRequest(
		ctx, timeout,
		"StreamExecute", sql, bindVariables,
		target, options, allowOnShutdown,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			if bindVariables == nil {
				bindVariables = make(map[string]*querypb.BindVariable)
			}
			query, comments := sqlparser.SplitMarginComments(sql)
			plan, err := tsv.qe.GetStreamPlan(ctx, logStats, query, skipQueryPlanCache(options))
			if err != nil {
				return err
			}
			if err = plan.IsValid(reservedID != 0, len(settings) > 0); err != nil {
				return err
			}
			// If both the values are non-zero then by design they are same value. So, it is safe to overwrite.
			connID := reservedID
			if transactionID != 0 {
				connID = transactionID
			}
			logStats.ReservedID = reservedID
			logStats.TransactionID = transactionID

			var connSetting *smartconnpool.Setting
			if len(settings) > 0 {
				connSetting, err = tsv.qe.GetConnSetting(ctx, settings)
				if err != nil {
					return err
				}
			}
			qre := &QueryExecutor{
				query:            query,
				marginComments:   comments,
				bindVars:         bindVariables,
				connID:           connID,
				options:          options,
				plan:             plan,
				ctx:              ctx,
				logStats:         logStats,
				tsv:              tsv,
				targetTabletType: target.GetTabletType(),
				setting:          connSetting,
			}
			return qre.Stream(callback)
		},
	)
}

// BeginExecute combines Begin and Execute.
func (tsv *TabletServer) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {

	// Disable hot row protection in case of reserve connection.
	if tsv.enableHotRowProtection && reservedID == 0 {
		txDone, err := tsv.beginWaitForSameRangeTransactions(ctx, target, options, sql, bindVariables)
		if err != nil {
			return queryservice.TransactionState{}, nil, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	state, err := tsv.begin(ctx, target, preQueries, reservedID, nil, options)
	if err != nil {
		return state, nil, err
	}

	result, err := tsv.Execute(ctx, target, sql, bindVariables, state.TransactionID, reservedID, options)
	return state, result, err
}

// BeginStreamExecute combines Begin and StreamExecute.
func (tsv *TabletServer) BeginStreamExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	reservedID int64,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) (queryservice.TransactionState, error) {
	state, err := tsv.begin(ctx, target, preQueries, reservedID, nil, options)
	if err != nil {
		return state, err
	}

	err = tsv.StreamExecute(ctx, target, sql, bindVariables, state.TransactionID, reservedID, options, callback)
	return state, err
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
		ctx, tsv.loadQueryTimeout(),
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
	plan, err := tsv.qe.GetPlan(ctx, logStats, sql, false)
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
	querygen, err := tsv.messager.GetGenerator(name)
	if err != nil {
		return 0, err
	}
	count, err = tsv.execDML(ctx, target, func() (string, map[string]*querypb.BindVariable, error) {
		query, bv := querygen.GenerateAckQuery(sids)
		return query, bv, nil
	})
	if err != nil {
		return 0, err
	}
	messager.MessageStats.Add([]string{name, "Acked"}, count)
	return count, nil
}

// PostponeMessages postpones the list of messages for a given message table.
// It returns the number of messages successfully postponed.
func (tsv *TabletServer) PostponeMessages(ctx context.Context, target *querypb.Target, querygen messager.QueryGenerator, ids []string) (count int64, err error) {
	return tsv.execDML(ctx, target, func() (string, map[string]*querypb.BindVariable, error) {
		query, bv := querygen.GeneratePostponeQuery(ids)
		return query, bv, nil
	})
}

// PurgeMessages purges messages older than specified time in Unix Nanoseconds.
// It purges at most 500 messages. It returns the number of messages successfully purged.
func (tsv *TabletServer) PurgeMessages(ctx context.Context, target *querypb.Target, querygen messager.QueryGenerator, timeCutoff int64) (count int64, err error) {
	return tsv.execDML(ctx, target, func() (string, map[string]*querypb.BindVariable, error) {
		query, bv := querygen.GeneratePurgeQuery(timeCutoff)
		return query, bv, nil
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
		return 0, err
	}

	state, err := tsv.Begin(ctx, target, nil)
	if err != nil {
		return 0, err
	}
	// If transaction was not committed by the end, it means
	// that there was an error, roll it back.
	defer func() {
		if state.TransactionID != 0 {
			tsv.Rollback(ctx, target, state.TransactionID)
		}
	}()
	qr, err := tsv.Execute(ctx, target, query, bv, state.TransactionID, 0, nil)
	if err != nil {
		return 0, err
	}
	if _, err = tsv.Commit(ctx, target, state.TransactionID); err != nil {
		state.TransactionID = 0
		return 0, err
	}
	state.TransactionID = 0
	return int64(qr.RowsAffected), nil
}

// VStream streams VReplication events.
func (tsv *TabletServer) VStream(ctx context.Context, request *binlogdatapb.VStreamRequest, send func([]*binlogdatapb.VEvent) error) error {
	if err := tsv.sm.VerifyTarget(ctx, request.Target); err != nil {
		return err
	}
	return tsv.vstreamer.Stream(ctx, request.Position, request.TableLastPKs, request.Filter, throttlerapp.VStreamerName, send)
}

// VStreamRows streams rows from the specified starting point.
func (tsv *TabletServer) VStreamRows(ctx context.Context, request *binlogdatapb.VStreamRowsRequest, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	if err := tsv.sm.VerifyTarget(ctx, request.Target); err != nil {
		return err
	}
	var row []sqltypes.Value
	if request.Lastpk != nil {
		r := sqltypes.Proto3ToResult(request.Lastpk)
		if len(r.Rows) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected lastpk input: %v", request.Lastpk)
		}
		row = r.Rows[0]
	}
	return tsv.vstreamer.StreamRows(ctx, request.Query, row, send)
}

// VStreamTables streams all tables.
func (tsv *TabletServer) VStreamTables(ctx context.Context, request *binlogdatapb.VStreamTablesRequest, send func(*binlogdatapb.VStreamTablesResponse) error) error {
	if err := tsv.sm.VerifyTarget(ctx, request.Target); err != nil {
		return err
	}
	return tsv.vstreamer.StreamTables(ctx, send)
}

// VStreamResults streams rows from the specified starting point.
func (tsv *TabletServer) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	if err := tsv.sm.VerifyTarget(ctx, target); err != nil {
		return err
	}
	return tsv.vstreamer.StreamResults(ctx, query, send)
}

// ReserveBeginExecute implements the QueryService interface
func (tsv *TabletServer) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (state queryservice.ReservedTransactionState, result *sqltypes.Result, err error) {
	if tsv.config.EnableSettingsPool {
		state, result, err = tsv.beginExecuteWithSettings(ctx, target, preQueries, postBeginQueries, sql, bindVariables, options)
		// If there is an error and the error message is about allowing query in reserved connection only,
		// then we do not return an error from here and continue to use the reserved connection path.
		// This is specially for get_lock function call from vtgate that needs a reserved connection.
		if err == nil || !strings.Contains(err.Error(), "not allowed without reserved connection") {
			return state, result, err
		}
		// rollback if transaction was started.
		if state.TransactionID != 0 {
			_, _ = tsv.Rollback(ctx, target, state.TransactionID)
		}
	}
	var connID int64
	var sessionStateChanges string
	state.TabletAlias = tsv.alias

	err = tsv.execRequest(
		ctx, tsv.loadQueryTimeout(),
		"ReserveBegin", "begin", bindVariables,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RESERVE", time.Now())
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			defer tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), time.Now())
			connID, sessionStateChanges, err = tsv.te.ReserveBegin(ctx, options, preQueries, postBeginQueries)
			if err != nil {
				return err
			}
			logStats.TransactionID = connID
			logStats.ReservedID = connID
			return nil
		},
	)

	if err != nil {
		return state, nil, err
	}
	state.ReservedID = connID
	state.TransactionID = connID
	state.SessionStateChanges = sessionStateChanges

	result, err = tsv.execute(ctx, target, sql, bindVariables, state.TransactionID, state.ReservedID, nil, options)
	return state, result, err
}

// ReserveBeginStreamExecute combines Begin and StreamExecute.
func (tsv *TabletServer) ReserveBeginStreamExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	postBeginQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) (state queryservice.ReservedTransactionState, err error) {
	if tsv.config.EnableSettingsPool {
		return tsv.beginStreamExecuteWithSettings(ctx, target, preQueries, postBeginQueries, sql, bindVariables, options, callback)
	}

	var connID int64
	var sessionStateChanges string

	err = tsv.execRequest(
		ctx, tsv.loadQueryTimeout(),
		"ReserveBegin", "begin", bindVariables,
		target, options, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RESERVE", time.Now())
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			defer tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), time.Now())
			connID, sessionStateChanges, err = tsv.te.ReserveBegin(ctx, options, preQueries, postBeginQueries)
			if err != nil {
				return err
			}
			logStats.TransactionID = connID
			logStats.ReservedID = connID
			return nil
		},
	)

	if err != nil {
		return state, err
	}
	state.ReservedID = connID
	state.TransactionID = connID
	state.TabletAlias = tsv.alias
	state.SessionStateChanges = sessionStateChanges

	err = tsv.streamExecute(ctx, target, sql, bindVariables, state.TransactionID, state.ReservedID, nil, options, callback)
	return state, err
}

// ReserveExecute implements the QueryService interface
func (tsv *TabletServer) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (state queryservice.ReservedState, result *sqltypes.Result, err error) {
	if tsv.config.EnableSettingsPool {
		result, err = tsv.executeWithSettings(ctx, target, preQueries, sql, bindVariables, transactionID, options)
		// If there is an error and the error message is about allowing query in reserved connection only,
		// then we do not return an error from here and continue to use the reserved connection path.
		// This is specially for get_lock function call from vtgate that needs a reserved connection.
		if err == nil || !strings.Contains(err.Error(), "not allowed without reserved connection") {
			return state, result, err
		}
	}

	state.TabletAlias = tsv.alias

	allowOnShutdown := false
	timeout := tsv.loadQueryTimeout()
	if transactionID != 0 {
		allowOnShutdown = true
		// ReserveExecute is for OLTP only, so we can directly fetch the OLTP
		// TX timeout.
		txTimeout := tsv.config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLTP)
		// Use the smaller of the two values (0 means infinity).
		timeout = smallerTimeout(timeout, txTimeout)
	}

	err = tsv.execRequest(
		ctx, timeout,
		"Reserve", "", bindVariables,
		target, options, allowOnShutdown,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RESERVE", time.Now())
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			defer tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), time.Now())
			state.ReservedID, err = tsv.te.Reserve(ctx, options, transactionID, preQueries)
			if err != nil {
				return err
			}
			logStats.TransactionID = transactionID
			logStats.ReservedID = state.ReservedID
			return nil
		},
	)

	if err != nil {
		return state, nil, err
	}

	result, err = tsv.execute(ctx, target, sql, bindVariables, transactionID, state.ReservedID, nil, options)
	return state, result, err
}

// ReserveStreamExecute combines Begin and StreamExecute.
func (tsv *TabletServer) ReserveStreamExecute(
	ctx context.Context,
	target *querypb.Target,
	preQueries []string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	transactionID int64,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) (state queryservice.ReservedState, err error) {
	if tsv.config.EnableSettingsPool {
		return state, tsv.streamExecute(ctx, target, sql, bindVariables, transactionID, 0, preQueries, options, callback)
	}

	state.TabletAlias = tsv.alias

	allowOnShutdown := false
	var timeout time.Duration
	if transactionID != 0 {
		allowOnShutdown = true
		// Use the transaction timeout. ReserveStreamExecute is used for OLAP
		// only, so we can directly fetch the OLAP TX timeout.
		timeout = tsv.config.TxTimeoutForWorkload(querypb.ExecuteOptions_OLAP)
	}

	err = tsv.execRequest(
		ctx, timeout,
		"Reserve", "", bindVariables,
		target, options, allowOnShutdown,
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RESERVE", time.Now())
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			defer tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), time.Now())
			state.ReservedID, err = tsv.te.Reserve(ctx, options, transactionID, preQueries)
			if err != nil {
				return err
			}
			logStats.TransactionID = state.ReservedID
			logStats.ReservedID = state.ReservedID
			return nil
		},
	)

	if err != nil {
		return state, err
	}

	err = tsv.streamExecute(ctx, target, sql, bindVariables, transactionID, state.ReservedID, nil, options, callback)
	return state, err
}

// Release implements the QueryService interface
func (tsv *TabletServer) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	if reservedID == 0 && transactionID == 0 {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NoSuchSession, "connection ID and transaction ID do not exist")
	}
	return tsv.execRequest(
		ctx, tsv.loadQueryTimeout(),
		"Release", "", nil,
		target, nil, true, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("RELEASE", time.Now())
			targetType, err := tsv.resolveTargetType(ctx, target)
			if err != nil {
				return err
			}
			defer tsv.stats.QueryTimingsByTabletType.Record(targetType.String(), time.Now())
			logStats.TransactionID = transactionID
			logStats.ReservedID = reservedID
			if reservedID != 0 {
				// Release to close the underlying connection.
				return tsv.te.Release(reservedID)
			}
			// Rollback to cleanup the transaction before returning to the pool.
			_, err = tsv.te.Rollback(ctx, transactionID)
			return err
		},
	)
}

func (tsv *TabletServer) executeWithSettings(ctx context.Context, target *querypb.Target, settings []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (result *sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "TabletServer.ExecuteWithSettings")
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	return tsv.execute(ctx, target, sql, bindVariables, transactionID, 0, settings, options)
}

func (tsv *TabletServer) beginExecuteWithSettings(ctx context.Context, target *querypb.Target, settings []string, savepointQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	txState, err := tsv.begin(ctx, target, savepointQueries, 0, settings, options)
	if err != nil {
		return txToReserveState(txState), nil, err
	}

	result, err := tsv.execute(ctx, target, sql, bindVariables, txState.TransactionID, 0, settings, options)
	return txToReserveState(txState), result, err
}

func (tsv *TabletServer) beginStreamExecuteWithSettings(ctx context.Context, target *querypb.Target, settings []string, savepointQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedTransactionState, error) {
	txState, err := tsv.begin(ctx, target, savepointQueries, 0, settings, options)
	if err != nil {
		return txToReserveState(txState), err
	}

	err = tsv.streamExecute(ctx, target, sql, bindVariables, txState.TransactionID, 0, settings, options, callback)
	return txToReserveState(txState), err
}

func txToReserveState(state queryservice.TransactionState) queryservice.ReservedTransactionState {
	return queryservice.ReservedTransactionState{
		TabletAlias:         state.TabletAlias,
		TransactionID:       state.TransactionID,
		SessionStateChanges: state.SessionStateChanges,
	}
}

// GetSchema returns table definitions for the specified tables.
func (tsv *TabletServer) GetSchema(ctx context.Context, target *querypb.Target, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) (err error) {
	err = tsv.execRequest(
		ctx, tsv.loadQueryTimeout(),
		"GetSchema", "", nil,
		target, nil, false, /* allowOnShutdown */
		func(ctx context.Context, logStats *tabletenv.LogStats) error {
			defer tsv.stats.QueryTimings.Record("GetSchema", time.Now())

			qre := &QueryExecutor{
				ctx:      ctx,
				logStats: logStats,
				tsv:      tsv,
			}
			return qre.GetSchemaDefinitions(tableType, tableNames, callback)
		},
	)
	return
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
		span.Annotate("workload_name", options.WorkloadName)
	}
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	// With a tabletenv.LocalContext() the target will be nil.
	if target != nil {
		span.Annotate("cell", target.Cell)
		span.Annotate("shard", target.Shard)
		span.Annotate("keyspace", target.Keyspace)
	}

	defer span.Finish()

	logStats := tabletenv.NewLogStats(ctx, requestName)
	logStats.Target = target
	logStats.OriginalSQL = sql
	logStats.BindVariables = sqltypes.CopyBindVariables(bindVariables)
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
		// Redaction/sanitization of the client error message is controlled by TerseErrors while
		// the log message is controlled by SanitizeLogMessages.
		// We are handling an unrecoverable panic, so the cost of the dual message handling is
		// not a concern.
		var messagef, logMessage, query, truncatedQuery string
		messagef = fmt.Sprintf("Uncaught panic for %%v:\n%v\n%s", x, tb.Stack(4) /* Skip the last 4 boiler-plate frames. */)
		query = queryAsString(sql, bindVariables, tsv.TerseErrors, false, tsv.env.Parser())
		terr := vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "%s", fmt.Sprintf(messagef, query))
		if tsv.TerseErrors == tsv.Config().SanitizeLogMessages {
			truncatedQuery = queryAsString(sql, bindVariables, tsv.TerseErrors, true, tsv.env.Parser())
			logMessage = fmt.Sprintf(messagef, truncatedQuery)
		} else {
			truncatedQuery = queryAsString(sql, bindVariables, tsv.Config().SanitizeLogMessages, true, tsv.env.Parser())
			logMessage = fmt.Sprintf(messagef, truncatedQuery)
		}
		log.Error(logMessage)
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
	// There is one exception:
	// 1. FAILED_PRECONDITION errors. These are caused when a failover is in progress.
	// If so, we don't want to suppress the error. This will allow VTGate to
	// detect and perform buffering during failovers.
	var message string
	sqlErr, ok := err.(*sqlerror.SQLError)
	if ok {
		sqlState := sqlErr.SQLState()
		errnum := sqlErr.Number()
		if tsv.TerseErrors && errCode != vtrpcpb.Code_FAILED_PRECONDITION {
			err = vterrors.Errorf(errCode, "(errno %d) (sqlstate %s)%s: %s", errnum, sqlState, callerID, queryAsString(sql, bindVariables, tsv.TerseErrors, false, tsv.env.Parser()))
			if logMethod != nil {
				message = fmt.Sprintf("(errno %d) (sqlstate %s)%s: %s", errnum, sqlState, callerID, queryAsString(sql, bindVariables, tsv.Config().SanitizeLogMessages, true, tsv.env.Parser()))
			}
		} else {
			err = vterrors.Errorf(errCode, "%s (errno %d) (sqlstate %s)%s: %s", sqlErr.Message, errnum, sqlState, callerID, queryAsString(sql, bindVariables, false, false, tsv.env.Parser()))
			if logMethod != nil {
				message = fmt.Sprintf("%s (errno %d) (sqlstate %s)%s: %s", sqlErr.Message, errnum, sqlState, callerID, queryAsString(sql, bindVariables, tsv.Config().SanitizeLogMessages, true, tsv.env.Parser()))
			}
		}
	} else {
		err = vterrors.Errorf(errCode, "%v%s", err.Error(), callerID)
		if logMethod != nil {
			message = fmt.Sprintf("%v: %v", err, queryAsString(sql, bindVariables, tsv.Config().SanitizeLogMessages, true, tsv.env.Parser()))
		}
	}

	if logMethod != nil {
		logMethod(message)
	}

	if logStats != nil {
		logStats.Error = err
	}

	return vterrors.TruncateError(err, tsv.TruncateErrorLen)
}

func convertErrorCode(err error) vtrpcpb.Code {
	errCode := vterrors.Code(err)
	sqlErr, ok := err.(*sqlerror.SQLError)
	if !ok {
		return errCode
	}

	switch sqlErr.Number() {
	case sqlerror.ERNotSupportedYet:
		errCode = vtrpcpb.Code_UNIMPLEMENTED
	case sqlerror.ERDiskFull, sqlerror.EROutOfMemory, sqlerror.EROutOfSortMemory, sqlerror.ERConCount, sqlerror.EROutOfResources, sqlerror.ERRecordFileFull, sqlerror.ERHostIsBlocked,
		sqlerror.ERCantCreateThread, sqlerror.ERTooManyDelayedThreads, sqlerror.ERNetPacketTooLarge, sqlerror.ERTooManyUserConnections, sqlerror.ERLockTableFull, sqlerror.ERUserLimitReached:
		errCode = vtrpcpb.Code_RESOURCE_EXHAUSTED
	case sqlerror.ERLockWaitTimeout:
		errCode = vtrpcpb.Code_DEADLINE_EXCEEDED
	case sqlerror.CRServerGone, sqlerror.ERServerShutdown, sqlerror.ERServerIsntAvailable, sqlerror.CRConnectionError, sqlerror.CRConnHostError:
		errCode = vtrpcpb.Code_UNAVAILABLE
	case sqlerror.ERFormNotFound, sqlerror.ERKeyNotFound, sqlerror.ERBadFieldError, sqlerror.ERNoSuchThread, sqlerror.ERUnknownTable, sqlerror.ERCantFindUDF, sqlerror.ERNonExistingGrant,
		sqlerror.ERNoSuchTable, sqlerror.ERNonExistingTableGrant, sqlerror.ERKeyDoesNotExist:
		errCode = vtrpcpb.Code_NOT_FOUND
	case sqlerror.ERDBAccessDenied, sqlerror.ERAccessDeniedError, sqlerror.ERKillDenied, sqlerror.ERNoPermissionToCreateUsers:
		errCode = vtrpcpb.Code_PERMISSION_DENIED
	case sqlerror.ERNoDb, sqlerror.ERNoSuchIndex, sqlerror.ERCantDropFieldOrKey, sqlerror.ERTableNotLockedForWrite, sqlerror.ERTableNotLocked, sqlerror.ERTooBigSelect, sqlerror.ERNotAllowedCommand,
		sqlerror.ERTooLongString, sqlerror.ERDelayedInsertTableLocked, sqlerror.ERDupUnique, sqlerror.ERRequiresPrimaryKey, sqlerror.ERCantDoThisDuringAnTransaction, sqlerror.ERReadOnlyTransaction,
		sqlerror.ERCannotAddForeign, sqlerror.ERNoReferencedRow, sqlerror.ERRowIsReferenced, sqlerror.ERCantUpdateWithReadLock, sqlerror.ERNoDefault, sqlerror.EROperandColumns,
		sqlerror.ERSubqueryNo1Row, sqlerror.ERNonUpdateableTable, sqlerror.ERFeatureDisabled, sqlerror.ERDuplicatedValueInType, sqlerror.ERRowIsReferenced2,
		sqlerror.ErNoReferencedRow2, sqlerror.ERWarnDataOutOfRange:
		errCode = vtrpcpb.Code_FAILED_PRECONDITION
	case sqlerror.EROptionPreventsStatement:
		errCode = vtrpcpb.Code_CLUSTER_EVENT
	case sqlerror.ERTableExists, sqlerror.ERDupEntry, sqlerror.ERFileExists, sqlerror.ERUDFExists:
		errCode = vtrpcpb.Code_ALREADY_EXISTS
	case sqlerror.ERGotSignal, sqlerror.ERForcingClose, sqlerror.ERAbortingConnection, sqlerror.ERLockDeadlock:
		// For ERLockDeadlock, a deadlock rolls back the transaction.
		errCode = vtrpcpb.Code_ABORTED
	case sqlerror.ERUnknownComError, sqlerror.ERBadNullError, sqlerror.ERBadDb, sqlerror.ERBadTable, sqlerror.ERNonUniq, sqlerror.ERWrongFieldWithGroup, sqlerror.ERWrongGroupField,
		sqlerror.ERWrongSumSelect, sqlerror.ERWrongValueCount, sqlerror.ERTooLongIdent, sqlerror.ERDupFieldName, sqlerror.ERDupKeyName, sqlerror.ERWrongFieldSpec, sqlerror.ERParseError,
		sqlerror.EREmptyQuery, sqlerror.ERNonUniqTable, sqlerror.ERInvalidDefault, sqlerror.ERMultiplePriKey, sqlerror.ERTooManyKeys, sqlerror.ERTooManyKeyParts, sqlerror.ERTooLongKey,
		sqlerror.ERKeyColumnDoesNotExist, sqlerror.ERBlobUsedAsKey, sqlerror.ERTooBigFieldLength, sqlerror.ERWrongAutoKey, sqlerror.ERWrongFieldTerminators, sqlerror.ERBlobsAndNoTerminated,
		sqlerror.ERTextFileNotReadable, sqlerror.ERWrongSubKey, sqlerror.ERCantRemoveAllFields, sqlerror.ERUpdateTableUsed, sqlerror.ERNoTablesUsed, sqlerror.ERTooBigSet,
		sqlerror.ERBlobCantHaveDefault, sqlerror.ERWrongDbName, sqlerror.ERWrongTableName, sqlerror.ERUnknownProcedure, sqlerror.ERWrongParamCountToProcedure,
		sqlerror.ERWrongParametersToProcedure, sqlerror.ERFieldSpecifiedTwice, sqlerror.ERInvalidGroupFuncUse, sqlerror.ERTableMustHaveColumns, sqlerror.ERUnknownCharacterSet,
		sqlerror.ERTooManyTables, sqlerror.ERTooManyFields, sqlerror.ERTooBigRowSize, sqlerror.ERWrongOuterJoin, sqlerror.ERNullColumnInIndex, sqlerror.ERFunctionNotDefined,
		sqlerror.ERWrongValueCountOnRow, sqlerror.ERInvalidUseOfNull, sqlerror.ERRegexpError, sqlerror.ERMixOfGroupFuncAndFields, sqlerror.ERIllegalGrantForTable, sqlerror.ERSyntaxError,
		sqlerror.ERWrongColumnName, sqlerror.ERWrongKeyColumn, sqlerror.ERBlobKeyWithoutLength, sqlerror.ERPrimaryCantHaveNull, sqlerror.ERTooManyRows, sqlerror.ERUnknownSystemVariable,
		sqlerror.ERSetConstantsOnly, sqlerror.ERWrongArguments, sqlerror.ERWrongUsage, sqlerror.ERWrongNumberOfColumnsInSelect, sqlerror.ERDupArgument, sqlerror.ERLocalVariable,
		sqlerror.ERGlobalVariable, sqlerror.ERWrongValueForVar, sqlerror.ERWrongTypeForVar, sqlerror.ERVarCantBeRead, sqlerror.ERCantUseOptionHere, sqlerror.ERIncorrectGlobalLocalVar,
		sqlerror.ERWrongFKDef, sqlerror.ERKeyRefDoNotMatchTableRef, sqlerror.ERCyclicReference, sqlerror.ERCollationCharsetMismatch, sqlerror.ERCantAggregate2Collations,
		sqlerror.ERCantAggregate3Collations, sqlerror.ERCantAggregateNCollations, sqlerror.ERVariableIsNotStruct, sqlerror.ERUnknownCollation, sqlerror.ERWrongNameForIndex,
		sqlerror.ERWrongNameForCatalog, sqlerror.ERBadFTColumn, sqlerror.ERTruncatedWrongValue, sqlerror.ERTooMuchAutoTimestampCols, sqlerror.ERInvalidOnUpdate, sqlerror.ERUnknownTimeZone,
		sqlerror.ERInvalidCharacterString, sqlerror.ERIllegalReference, sqlerror.ERDerivedMustHaveAlias, sqlerror.ERTableNameNotAllowedHere, sqlerror.ERDataTooLong, sqlerror.ERDataOutOfRange,
		sqlerror.ERTruncatedWrongValueForField, sqlerror.ERIllegalValueForType:
		errCode = vtrpcpb.Code_INVALID_ARGUMENT
	case sqlerror.ERSpecifiedAccessDenied:
		errCode = vtrpcpb.Code_PERMISSION_DENIED
		// This code is also utilized for Google internal failover error code.
		if strings.Contains(err.Error(), "failover in progress") {
			errCode = vtrpcpb.Code_FAILED_PRECONDITION
		}
	case sqlerror.CRServerLost:
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
	tsv.sm.checkMySQL()
}

// TopoServer returns the topo server.
func (tsv *TabletServer) TopoServer() *topo.Server {
	return tsv.topoServer
}

// CheckThrottler issues a self check
func (tsv *TabletServer) CheckThrottler(ctx context.Context, appName string, flags *throttle.CheckFlags) *throttle.CheckResult {
	r := tsv.lagThrottler.CheckByType(ctx, appName, "", flags, throttle.ThrottleCheckSelf)
	return r
}

// HandlePanic is part of the queryservice.QueryService interface
func (tsv *TabletServer) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v\n. Stack-trace:\n%s", x, tb.Stack(4))
	}
}

// Close shuts down any remaining go routines
func (tsv *TabletServer) Close(ctx context.Context) error {
	tsv.sm.closeAll()
	tsv.stats.Stop()
	return nil
}

var okMessage = []byte("ok\n")

// Health check
// Returns ok if we are in the desired serving state
func (tsv *TabletServer) registerHealthzHealthHandler() {
	tsv.exporter.HandleFunc("/healthz", tsv.healthzHandler)
}

func (tsv *TabletServer) healthzHandler(w http.ResponseWriter, r *http.Request) {
	if (tsv.sm.wantState == StateServing || tsv.sm.wantState == StateNotConnected) && !tsv.sm.IsServing() {
		http.Error(w, "500 internal server error: vttablet is not serving", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(okMessage)))
	w.WriteHeader(http.StatusOK)
	w.Write(okMessage)
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

func (tsv *TabletServer) registerQuerylogzHandler() {
	tsv.exporter.HandleFunc("/querylogz", func(w http.ResponseWriter, r *http.Request) {
		ch := tabletenv.StatsLogger.Subscribe("querylogz")
		defer tabletenv.StatsLogger.Unsubscribe(ch)
		querylogzHandler(ch, w, r, tsv.env.Parser())
	})
}

func (tsv *TabletServer) registerTxlogzHandler() {
	tsv.exporter.HandleFunc("/txlogz", txlogzHandler)
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
		if err := tsv.onlineDDLExecutor.OnSchemaMigrationStatus(ctx, query.Get("uuid"), query.Get("status"), query.Get("dryrun"), query.Get("progress"), query.Get("eta"), query.Get("rowscopied"), query.Get("hint")); err != nil {
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
				appName = throttlerapp.DefaultName.String()
			}
			flags := &throttle.CheckFlags{
				LowPriority:           (r.URL.Query().Get("p") == "low"),
				SkipRequestHeartbeats: (r.URL.Query().Get("s") == "true"),
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
		var ratio = throttle.DefaultThrottleRatio
		if ratioParam := r.URL.Query().Get("ratio"); ratioParam != "" {
			ratio, err = strconv.ParseFloat(ratioParam, 64)
			if err != nil {
				http.Error(w, fmt.Sprintf("not ok: %v", err), http.StatusInternalServerError)
				return
			}
		}
		appThrottle := tsv.lagThrottler.ThrottleApp(appName, time.Now().Add(d), ratio, false)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(appThrottle)
	})
	tsv.exporter.HandleFunc("/throttler/unthrottle-app", func(w http.ResponseWriter, r *http.Request) {
		appName := r.URL.Query().Get("app")
		appThrottle := tsv.lagThrottler.UnthrottleApp(appName)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(appThrottle)
	})
	tsv.exporter.HandleFunc("/throttler/throttled-apps", func(w http.ResponseWriter, r *http.Request) {
		throttledApps := tsv.lagThrottler.ThrottledApps()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(throttledApps)
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
	tsv.qe.conns.SetCapacity(int64(val))
}

// PoolSize returns the pool size.
func (tsv *TabletServer) PoolSize() int {
	return int(tsv.qe.conns.Capacity())
}

// SetStreamPoolSize changes the pool size to the specified value.
func (tsv *TabletServer) SetStreamPoolSize(val int) {
	tsv.qe.streamConns.SetCapacity(int64(val))
}

// SetStreamConsolidationBlocking sets whether the stream consolidator should wait for slow clients
func (tsv *TabletServer) SetStreamConsolidationBlocking(block bool) {
	tsv.qe.streamConsolidator.SetBlocking(block)
}

// StreamPoolSize returns the pool size.
func (tsv *TabletServer) StreamPoolSize() int {
	return int(tsv.qe.streamConns.Capacity())
}

// SetTxPoolSize changes the tx pool size to the specified value.
func (tsv *TabletServer) SetTxPoolSize(val int) {
	tsv.te.txPool.scp.conns.SetCapacity(int64(val))
}

// TxPoolSize returns the tx pool size.
func (tsv *TabletServer) TxPoolSize() int {
	return tsv.te.txPool.scp.Capacity()
}

// QueryPlanCacheCap returns the plan cache capacity
func (tsv *TabletServer) QueryPlanCacheCap() int {
	return tsv.qe.QueryPlanCacheCap()
}

// QueryPlanCacheLen returns the plan cache length
func (tsv *TabletServer) QueryPlanCacheLen() int {
	return tsv.qe.QueryPlanCacheLen()
}

// SetMaxResultSize changes the max result size to the specified value.
func (tsv *TabletServer) SetMaxResultSize(val int) {
	tsv.qe.maxResultSize.Store(int64(val))
}

// MaxResultSize returns the max result size.
func (tsv *TabletServer) MaxResultSize() int {
	return int(tsv.qe.maxResultSize.Load())
}

// SetWarnResultSize changes the warn result size to the specified value.
func (tsv *TabletServer) SetWarnResultSize(val int) {
	tsv.qe.warnResultSize.Store(int64(val))
}

// WarnResultSize returns the warn result size.
func (tsv *TabletServer) WarnResultSize() int {
	return int(tsv.qe.warnResultSize.Load())
}

// SetThrottleMetricThreshold changes the throttler metric threshold
func (tsv *TabletServer) SetThrottleMetricThreshold(val float64) {
	tsv.lagThrottler.StoreMetricsThreshold(val)
}

// ThrottleMetricThreshold returns the throttler metric threshold
func (tsv *TabletServer) ThrottleMetricThreshold() float64 {
	return math.Float64frombits(tsv.lagThrottler.MetricsThreshold.Load())
}

// SetPassthroughDMLs changes the setting to pass through all DMLs
// It should only be used for testing
func (tsv *TabletServer) SetPassthroughDMLs(val bool) {
	planbuilder.PassthroughDMLs = val
}

// SetConsolidatorMode sets the consolidator mode.
func (tsv *TabletServer) SetConsolidatorMode(mode string) {
	switch mode {
	case tabletenv.NotOnPrimary, tabletenv.Enable, tabletenv.Disable:
		tsv.qe.consolidatorMode.Store(mode)
	}
}

// ConsolidatorMode returns the consolidator mode.
func (tsv *TabletServer) ConsolidatorMode() string {
	return tsv.qe.consolidatorMode.Load().(string)
}

// queryAsString returns a readable normalized version of the query.
// If sanitize is false it also includes the bind variables.
// If truncateForLog is true, it truncates the sql query and the
// bind variables.
func queryAsString(sql string, bindVariables map[string]*querypb.BindVariable, sanitize bool, truncateForLog bool, parser *sqlparser.Parser) string {
	// Add the bind vars unless this needs to be sanitized, e.g. for log messages
	bvBuf := &bytes.Buffer{}
	fmt.Fprintf(bvBuf, "BindVars: {")
	if len(bindVariables) > 0 {
		if sanitize {
			fmt.Fprintf(bvBuf, "[REDACTED]")
		} else {
			var keys []string
			for key := range bindVariables {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			var valString string
			for _, key := range keys {
				valString = fmt.Sprintf("%v", bindVariables[key])
				fmt.Fprintf(bvBuf, "%s: %q", key, valString)
			}
		}
	}
	fmt.Fprintf(bvBuf, "}")

	// Truncate the bind vars if necessary
	bv := bvBuf.String()
	maxLen := parser.GetTruncateErrLen()
	if truncateForLog && maxLen > 0 && len(bv) > maxLen {
		if maxLen <= 12 {
			bv = sqlparser.TruncationText
		} else {
			bv = bv[:maxLen-(len(sqlparser.TruncationText)+1)] + " " + sqlparser.TruncationText
		}
	}

	// Truncate the sql query if necessary
	if truncateForLog {
		sql = parser.TruncateForLog(sql)
	}

	// sql is the normalized query without the bind vars
	return fmt.Sprintf("Sql: %q, %s", sql, bv)
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
