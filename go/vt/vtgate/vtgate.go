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

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vtschema "vitess.io/vitess/go/vt/vtgate/schema"
)

var (
	transactionMode  = "MULTI"
	normalizeQueries = true
	streamBufferSize = 32 * 1024

	terseErrors bool

	// plan cache related flag
	queryPlanCacheSize   = cache.DefaultConfig.MaxEntries
	queryPlanCacheMemory = cache.DefaultConfig.MaxMemoryUsage
	queryPlanCacheLFU    bool

	maxMemoryRows   = 300000
	warnMemoryRows  = 30000
	maxPayloadSize  int
	warnPayloadSize int

	noScatter          bool
	enableShardRouting bool

	// TODO(deepthi): change these two vars to unexported and move to healthcheck.go when LegacyHealthcheck is removed

	// healthCheckRetryDelay is the time to wait before retrying healthcheck
	healthCheckRetryDelay = 2 * time.Millisecond
	// healthCheckTimeout is the timeout on the RPC call to tablets
	healthCheckTimeout = time.Minute

	// System settings related flags
	sysVarSetEnabled = true
	setVarEnabled    = true

	// lockHeartbeatTime is used to set the next heartbeat time.
	lockHeartbeatTime = 5 * time.Second
	warnShardedOnly   bool

	// ddl related flags
	foreignKeyMode     = "allow"
	dbDDLPlugin        = "fail"
	defaultDDLStrategy = string(schema.DDLStrategyDirect)
	enableOnlineDDL    = true
	enableDirectDDL    = true

	// vtgate schema tracking flags
	enableSchemaChangeSignal = true
	schemaChangeUser         string
	queryTimeout             int

	vstreamCellAliasFallback = false
)

func registerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&transactionMode, "transaction_mode", transactionMode, "SINGLE: disallow multi-db transactions, MULTI: allow multi-db transactions with best effort commit, TWOPC: allow multi-db transactions with 2pc commit")
	fs.BoolVar(&normalizeQueries, "normalize_queries", normalizeQueries, "Rewrite queries with bind vars. Turn this off if the app itself sends normalized queries with bind vars.")
	fs.BoolVar(&terseErrors, "vtgate-config-terse-errors", terseErrors, "prevent bind vars from escaping in returned errors")
	fs.IntVar(&streamBufferSize, "stream_buffer_size", streamBufferSize, "the number of bytes sent from vtgate for each stream call. It's recommended to keep this value in sync with vttablet's query-server-config-stream-buffer-size.")
	fs.Int64Var(&queryPlanCacheSize, "gate_query_cache_size", queryPlanCacheSize, "gate server query cache size, maximum number of queries to be cached. vtgate analyzes every incoming query and generate a query plan, these plans are being cached in a cache. This config controls the expected amount of unique entries in the cache.")
	fs.Int64Var(&queryPlanCacheMemory, "gate_query_cache_memory", queryPlanCacheMemory, "gate server query cache size in bytes, maximum amount of memory to be cached. vtgate analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")
	fs.BoolVar(&queryPlanCacheLFU, "gate_query_cache_lfu", cache.DefaultConfig.LFU, "gate server cache algorithm. when set to true, a new cache algorithm based on a TinyLFU admission policy will be used to improve cache behavior and prevent pollution from sparse queries")
	fs.IntVar(&maxMemoryRows, "max_memory_rows", maxMemoryRows, "Maximum number of rows that will be held in memory for intermediate results as well as the final result.")
	fs.IntVar(&warnMemoryRows, "warn_memory_rows", warnMemoryRows, "Warning threshold for in-memory results. A row count higher than this amount will cause the VtGateWarnings.ResultsExceeded counter to be incremented.")
	fs.StringVar(&defaultDDLStrategy, "ddl_strategy", defaultDDLStrategy, "Set default strategy for DDL statements. Override with @@ddl_strategy session variable")
	fs.StringVar(&dbDDLPlugin, "dbddl_plugin", dbDDLPlugin, "controls how to handle CREATE/DROP DATABASE. use it if you are using your own database provisioning service")
	fs.BoolVar(&noScatter, "no_scatter", noScatter, "when set to true, the planner will fail instead of producing a plan that includes scatter queries")
	fs.BoolVar(&enableShardRouting, "enable-partial-keyspace-migration", enableShardRouting, "(Experimental) Follow shard routing rules: enable only while migrating a keyspace shard by shard. See documentation on Partial MoveTables for more. (default false)")
	fs.DurationVar(&healthCheckRetryDelay, "healthcheck_retry_delay", healthCheckRetryDelay, "health check retry delay")
	fs.DurationVar(&healthCheckTimeout, "healthcheck_timeout", healthCheckTimeout, "the health check timeout period")
	fs.IntVar(&maxPayloadSize, "max_payload_size", maxPayloadSize, "The threshold for query payloads in bytes. A payload greater than this threshold will result in a failure to handle the query.")
	fs.IntVar(&warnPayloadSize, "warn_payload_size", warnPayloadSize, "The warning threshold for query payloads in bytes. A payload greater than this threshold will cause the VtGateWarnings.WarnPayloadSizeExceeded counter to be incremented.")
	fs.BoolVar(&sysVarSetEnabled, "enable_system_settings", sysVarSetEnabled, "This will enable the system settings to be changed per session at the database connection level")
	fs.BoolVar(&setVarEnabled, "enable_set_var", setVarEnabled, "This will enable the use of MySQL's SET_VAR query hint for certain system variables instead of using reserved connections")
	fs.DurationVar(&lockHeartbeatTime, "lock_heartbeat_time", lockHeartbeatTime, "If there is lock function used. This will keep the lock connection active by using this heartbeat")
	fs.BoolVar(&warnShardedOnly, "warn_sharded_only", warnShardedOnly, "If any features that are only available in unsharded mode are used, query execution warnings will be added to the session")
	fs.StringVar(&foreignKeyMode, "foreign_key_mode", foreignKeyMode, "This is to provide how to handle foreign key constraint in create/alter table. Valid values are: allow, disallow")
	fs.BoolVar(&enableOnlineDDL, "enable_online_ddl", enableOnlineDDL, "Allow users to submit, review and control Online DDL")
	fs.BoolVar(&enableDirectDDL, "enable_direct_ddl", enableDirectDDL, "Allow users to submit direct DDL statements")
	fs.BoolVar(&enableSchemaChangeSignal, "schema_change_signal", enableSchemaChangeSignal, "Enable the schema tracker; requires queryserver-config-schema-change-signal to be enabled on the underlying vttablets for this to work")
	fs.StringVar(&schemaChangeUser, "schema_change_signal_user", schemaChangeUser, "User to be used to send down query to vttablet to retrieve schema changes")
	fs.IntVar(&queryTimeout, "query-timeout", queryTimeout, "Sets the default query timeout (in ms). Can be overridden by session variable (query_timeout) or comment directive (QUERY_TIMEOUT_MS)")
	fs.BoolVar(&vstreamCellAliasFallback, "vstream_cell_alias_fallback", vstreamCellAliasFallback, "Determines whether to default to cell aliases for VStream tablet selection instead of having the gRPC request specify this in the VStreamrRequest object.")
}
func init() {
	servenv.OnParseFor("vtgate", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)
}

func getTxMode() vtgatepb.TransactionMode {
	switch strings.ToLower(transactionMode) {
	case "single":
		log.Infof("Transaction mode: '%s'", transactionMode)
		return vtgatepb.TransactionMode_SINGLE
	case "multi":
		log.Infof("Transaction mode: '%s'", transactionMode)
		return vtgatepb.TransactionMode_MULTI
	case "twopc":
		log.Infof("Transaction mode: '%s'", transactionMode)
		return vtgatepb.TransactionMode_TWOPC
	default:
		fmt.Printf("Invalid option: %v\n", transactionMode)
		fmt.Println("Usage: -transaction_mode {SINGLE | MULTI | TWOPC}")
		os.Exit(1)
		return -1
	}
}

var (
	rpcVTGate *VTGate

	// vschemaCounters needs to be initialized before planner to
	// catch the initial load stats.
	vschemaCounters = stats.NewCountersWithSingleLabel("VtgateVSchemaCounts", "Vtgate vschema counts", "changes")

	// Error counters should be global so they can be set from anywhere
	errorCounts = stats.NewCountersWithMultiLabels("VtgateApiErrorCounts", "Vtgate API error counts per error type", []string{"Operation", "Keyspace", "DbType", "Code"})

	warnings = stats.NewCountersWithSingleLabel("VtGateWarnings", "Vtgate warnings", "type", "IgnoredSet", "ResultsExceeded", "WarnPayloadSizeExceeded")

	vstreamSkewDelayCount = stats.NewCounter("VStreamEventsDelayedBySkewAlignment",
		"Number of events that had to wait because the skew across shards was too high")
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created. It implements vtgateservice.VTGateService
// VTGate exposes multiple generations of interfaces.
type VTGate struct {
	// Dependency: executor->resolver->scatterConn->txConn->gateway.
	executor *Executor
	resolver *Resolver
	vsm      *vstreamManager
	txConn   *TxConn
	gw       *TabletGateway

	// stats objects.
	// TODO(sougou): This needs to be cleaned up. There
	// are global vars that depend on this member var.
	timings      *stats.MultiTimings
	rowsReturned *stats.CountersWithMultiLabels
	rowsAffected *stats.CountersWithMultiLabels

	// the throttled loggers for all errors, one per API entry
	logExecute       *logutil.ThrottledLogger
	logStreamExecute *logutil.ThrottledLogger
}

// RegisterVTGate defines the type of registration mechanism.
type RegisterVTGate func(vtgateservice.VTGateService)

// RegisterVTGates stores register funcs for VTGate server.
var RegisterVTGates []RegisterVTGate

// Init initializes VTGate server.
func Init(
	ctx context.Context,
	hc discovery.HealthCheck,
	serv srvtopo.Server,
	cell string,
	tabletTypesToWait []topodatapb.TabletType,
	pv plancontext.PlannerVersion,
) *VTGate {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}

	// Build objects from low to high level.
	// Start with the gateway. If we can't reach the topology service,
	// we can't go on much further, so we log.Fatal out.
	// TabletGateway can create it's own healthcheck
	gw := NewTabletGateway(ctx, hc, serv, cell)
	gw.RegisterStats()
	if err := gw.WaitForTablets(tabletTypesToWait); err != nil {
		log.Fatalf("tabletGateway.WaitForTablets failed: %v", err)
	}

	// If we want to filter keyspaces replace the srvtopo.Server with a
	// filtering server
	if discovery.FilteringKeyspaces() {
		log.Infof("Keyspace filtering enabled, selecting %v", discovery.KeyspacesToWatch)
		var err error
		serv, err = srvtopo.NewKeyspaceFilteringServer(serv, discovery.KeyspacesToWatch)
		if err != nil {
			log.Fatalf("Unable to construct SrvTopo server: %v", err.Error())
		}
	}

	if _, err := schema.ParseDDLStrategy(defaultDDLStrategy); err != nil {
		log.Fatalf("Invalid value for -ddl_strategy: %v", err.Error())
	}
	tc := NewTxConn(gw, getTxMode())
	// ScatterConn depends on TxConn to perform forced rollbacks.
	sc := NewScatterConn("VttabletCall", tc, gw)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	resolver := NewResolver(srvResolver, serv, cell, sc)
	vsm := newVStreamManager(srvResolver, serv, cell)

	var si SchemaInfo // default nil
	var st *vtschema.Tracker
	if enableSchemaChangeSignal {
		st = vtschema.NewTracker(gw.hc.Subscribe(), schemaChangeUser)
		addKeyspaceToTracker(ctx, srvResolver, st, gw)
		si = st
	}

	cacheCfg := &cache.Config{
		MaxEntries:     queryPlanCacheSize,
		MaxMemoryUsage: queryPlanCacheMemory,
		LFU:            queryPlanCacheLFU,
	}

	executor := NewExecutor(
		ctx,
		serv,
		cell,
		resolver,
		normalizeQueries,
		warnShardedOnly,
		streamBufferSize,
		cacheCfg,
		si,
		noScatter,
		pv,
	)

	// connect the schema tracker with the vschema manager
	if enableSchemaChangeSignal {
		st.RegisterSignalReceiver(executor.vm.Rebuild)
	}

	// TODO: call serv.WatchSrvVSchema here

	rpcVTGate = &VTGate{
		executor: executor,
		resolver: resolver,
		vsm:      vsm,
		txConn:   tc,
		gw:       gw,
		timings: stats.NewMultiTimings(
			"VtgateApi",
			"VtgateApi timings",
			[]string{"Operation", "Keyspace", "DbType"}),
		rowsReturned: stats.NewCountersWithMultiLabels(
			"VtgateApiRowsReturned",
			"Rows returned through the VTgate API",
			[]string{"Operation", "Keyspace", "DbType"}),
		rowsAffected: stats.NewCountersWithMultiLabels(
			"VtgateApiRowsAffected",
			"Rows affected by a write (DML) operation through the VTgate API",
			[]string{"Operation", "Keyspace", "DbType"}),

		logExecute:       logutil.NewThrottledLogger("Execute", 5*time.Second),
		logStreamExecute: logutil.NewThrottledLogger("StreamExecute", 5*time.Second),
	}

	_ = stats.NewRates("QPSByOperation", stats.CounterForDimension(rpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	_ = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(rpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	_ = stats.NewRates("QPSByDbType", stats.CounterForDimension(rpcVTGate.timings, "DbType"), 15*60/5, 5*time.Second)

	_ = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(errorCounts, "Operation"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(errorCounts, "Keyspace"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(errorCounts, "DbType"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByCode", stats.CounterForDimension(errorCounts, "Code"), 15, 1*time.Minute)

	servenv.OnRun(func() {
		for _, f := range RegisterVTGates {
			f(rpcVTGate)
		}
		if st != nil && enableSchemaChangeSignal {
			st.Start()
		}
	})
	servenv.OnTerm(func() {
		if st != nil && enableSchemaChangeSignal {
			st.Stop()
		}
	})
	rpcVTGate.registerDebugHealthHandler()
	rpcVTGate.registerDebugEnvHandler()
	err := initQueryLogger(rpcVTGate)
	if err != nil {
		log.Fatalf("error initializing query logger: %v", err)
	}

	initAPI(gw.hc)
	return rpcVTGate
}

func addKeyspaceToTracker(ctx context.Context, srvResolver *srvtopo.Resolver, st *vtschema.Tracker, gw *TabletGateway) {
	keyspaces, err := srvResolver.GetAllKeyspaces(ctx)
	if err != nil {
		log.Warningf("Unable to get all keyspaces: %v", err)
		return
	}
	if len(keyspaces) == 0 {
		log.Infof("No keyspace to load")
	}
	for _, keyspace := range keyspaces {
		resolveAndLoadKeyspace(ctx, srvResolver, st, gw, keyspace)
	}
}

func resolveAndLoadKeyspace(ctx context.Context, srvResolver *srvtopo.Resolver, st *vtschema.Tracker, gw *TabletGateway, keyspace string) {
	dest, err := srvResolver.ResolveDestination(ctx, keyspace, topodatapb.TabletType_PRIMARY, key.DestinationAllShards{})
	if err != nil {
		log.Warningf("Unable to resolve destination: %v", err)
		return
	}

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			log.Warningf("Unable to get initial schema reload for keyspace: %s", keyspace)
			return
		case <-time.After(500 * time.Millisecond):
			for _, shard := range dest {
				err := st.AddNewKeyspace(gw, shard.Target)
				if err == nil {
					return
				}
			}
		}
	}
}

func (vtg *VTGate) registerDebugEnvHandler() {
	http.HandleFunc("/debug/env", func(w http.ResponseWriter, r *http.Request) {
		debugEnvHandler(vtg, w, r)
	})
}

func (vtg *VTGate) registerDebugHealthHandler() {
	http.HandleFunc("/debug/health", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
			acl.SendError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		if err := vtg.IsHealthy(); err != nil {
			w.Write([]byte("not ok"))
			return
		}
		w.Write([]byte("ok"))
	})
}

// IsHealthy returns nil if server is healthy.
// Otherwise, it returns an error indicating the reason.
func (vtg *VTGate) IsHealthy() error {
	return nil
}

// Gateway returns the current gateway implementation. Mostly used for tests.
func (vtg *VTGate) Gateway() *TabletGateway {
	return vtg.gw
}

// Execute executes a non-streaming query. This is a V3 function.
func (vtg *VTGate) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (newSession *vtgatepb.Session, qr *sqltypes.Result, err error) {
	// In this context, we don't care if we can't fully parse destination
	destKeyspace, destTabletType, _, _ := vtg.executor.ParseDestinationTarget(session.TargetString)
	statsKey := []string{"Execute", destKeyspace, topoproto.TabletTypeLString(destTabletType)}
	defer vtg.timings.Record(statsKey, time.Now())

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
	} else {
		safeSession := NewSafeSession(session)
		qr, err = vtg.executor.Execute(ctx, "Execute", safeSession, sql, bindVariables)
		safeSession.RemoveInternalSavepoint()
	}
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		vtg.rowsAffected.Add(statsKey, int64(qr.RowsAffected))
		return session, qr, nil
	}

	query := map[string]any{
		"Sql":           sql,
		"BindVariables": bindVariables,
		"Session":       session,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecute)
	return session, nil, err
}

// ExecuteBatch executes a batch of queries. This is a V3 function.
func (vtg *VTGate) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	// In this context, we don't care if we can't fully parse destination
	destKeyspace, destTabletType, _, _ := vtg.executor.ParseDestinationTarget(session.TargetString)
	statsKey := []string{"ExecuteBatch", destKeyspace, topoproto.TabletTypeLString(destTabletType)}
	defer vtg.timings.Record(statsKey, time.Now())

	for _, bindVariables := range bindVariablesList {
		if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
			return session, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		}
	}

	qrl := make([]sqltypes.QueryResponse, len(sqlList))
	for i, sql := range sqlList {
		var bv map[string]*querypb.BindVariable
		if len(bindVariablesList) != 0 {
			bv = bindVariablesList[i]
		}
		session, qrl[i].QueryResult, qrl[i].QueryError = vtg.Execute(ctx, session, sql, bv)
		if qr := qrl[i].QueryResult; qr != nil {
			vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
			vtg.rowsAffected.Add(statsKey, int64(qr.RowsAffected))
		}
	}
	return session, qrl, nil
}

// StreamExecute executes a streaming query. This is a V3 function.
// Note we guarantee the callback will not be called concurrently
// by multiple go routines.
func (vtg *VTGate) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	// In this context, we don't care if we can't fully parse destination
	destKeyspace, destTabletType, _, _ := vtg.executor.ParseDestinationTarget(session.TargetString)
	statsKey := []string{"StreamExecute", destKeyspace, topoproto.TabletTypeLString(destTabletType)}

	defer vtg.timings.Record(statsKey, time.Now())

	var err error
	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
	} else {
		safeSession := NewSafeSession(session)
		err = vtg.executor.StreamExecute(
			ctx,
			"StreamExecute",
			safeSession,
			sql,
			bindVariables,
			func(reply *sqltypes.Result) error {
				vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
				vtg.rowsAffected.Add(statsKey, int64(reply.RowsAffected))
				return callback(reply)
			})
		safeSession.RemoveInternalSavepoint()
	}
	if err != nil {
		query := map[string]any{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Session":       session,
		}
		return recordAndAnnotateError(err, statsKey, query, vtg.logStreamExecute)
	}
	return nil
}

// CloseSession closes the session, rolling back any implicit transactions. This has the
// same effect as if a "rollback" statement was executed, but does not affect the query
// statistics.
func (vtg *VTGate) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	return vtg.executor.CloseSession(ctx, NewSafeSession(session))
}

// ResolveTransaction resolves the specified 2PC transaction.
func (vtg *VTGate) ResolveTransaction(ctx context.Context, dtid string) error {
	return formatError(vtg.txConn.Resolve(ctx, dtid))
}

// Prepare supports non-streaming prepare statement query with multi shards
func (vtg *VTGate) Prepare(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (newSession *vtgatepb.Session, fld []*querypb.Field, err error) {
	// In this context, we don't care if we can't fully parse destination
	destKeyspace, destTabletType, _, _ := vtg.executor.ParseDestinationTarget(session.TargetString)
	statsKey := []string{"Execute", destKeyspace, topoproto.TabletTypeLString(destTabletType)}
	defer vtg.timings.Record(statsKey, time.Now())

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	fld, err = vtg.executor.Prepare(ctx, "Prepare", NewSafeSession(session), sql, bindVariables)
	if err == nil {
		return session, fld, nil
	}

handleError:
	query := map[string]any{
		"Sql":           sql,
		"BindVariables": bindVariables,
		"Session":       session,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecute)
	return session, nil, err
}

// VStream streams binlog events.
func (vtg *VTGate) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	return vtg.vsm.VStream(ctx, tabletType, vgtid, filter, flags, send)
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (vtg *VTGate) GetGatewayCacheStatus() TabletCacheStatusList {
	return vtg.resolver.GetGatewayCacheStatus()
}

// VSchemaStats returns the loaded vschema stats.
func (vtg *VTGate) VSchemaStats() *VSchemaStats {
	return vtg.executor.VSchemaStats()
}

func truncateErrorStrings(data map[string]any) map[string]any {
	ret := map[string]any{}
	if terseErrors {
		// request might have PII information. Return an empty map
		return ret
	}
	for key, val := range data {
		mapVal, ok := val.(map[string]any)
		if ok {
			ret[key] = truncateErrorStrings(mapVal)
		} else {
			strVal := fmt.Sprintf("%v", val)
			ret[key] = sqlparser.TruncateForLog(strVal)
		}
	}
	return ret
}

func recordAndAnnotateError(err error, statsKey []string, request map[string]any, logger *logutil.ThrottledLogger) error {
	ec := vterrors.Code(err)
	fullKey := []string{
		statsKey[0],
		statsKey[1],
		statsKey[2],
		ec.String(),
	}

	if terseErrors {
		regexpBv := regexp.MustCompile(`BindVars: \{.*\}`)
		str := regexpBv.ReplaceAllString(err.Error(), "BindVars: {REDACTED}")
		err = errors.New(str)
	}

	// Traverse the request structure and truncate any long values
	request = truncateErrorStrings(request)

	errorCounts.Add(fullKey, 1)

	// Most errors are not logged by vtgate because they're either too spammy or logged elsewhere.
	switch ec {
	case vtrpcpb.Code_UNKNOWN, vtrpcpb.Code_INTERNAL, vtrpcpb.Code_DATA_LOSS:
		logger.Errorf("%v, request: %+v", err, request)
	case vtrpcpb.Code_UNAVAILABLE:
		logger.Infof("%v, request: %+v", err, request)
	case vtrpcpb.Code_UNIMPLEMENTED:
		sql, exists := request["Sql"]
		if !exists {
			return err
		}
		piiSafeSQL, err2 := sqlparser.RedactSQLQuery(sql.(string))
		if err2 != nil {
			return err
		}
		// log only if sql query present and able to successfully redact the PII.
		logger.Infof("unsupported query: %q", piiSafeSQL)
	}
	return err
}

func formatError(err error) error {
	if err == nil {
		return nil
	}
	return err
}

// HandlePanic recovers from panics, and logs / increment counters
func (vtg *VTGate) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v, vtgate: %v", x, servenv.ListeningURL.String())
		errorCounts.Add([]string{"Panic", "Unknown", "Unknown", vtrpcpb.Code_INTERNAL.String()}, 1)
	}
}
