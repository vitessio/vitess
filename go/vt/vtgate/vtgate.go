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
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	transactionMode      = flag.String("transaction_mode", "MULTI", "SINGLE: disallow multi-db transactions, MULTI: allow multi-db transactions with best effort commit, TWOPC: allow multi-db transactions with 2pc commit")
	normalizeQueries     = flag.Bool("normalize_queries", true, "Rewrite queries with bind vars. Turn this off if the app itself sends normalized queries with bind vars.")
	terseErrors          = flag.Bool("vtgate-config-terse-errors", false, "prevent bind vars from escaping in returned errors")
	streamBufferSize     = flag.Int("stream_buffer_size", 32*1024, "the number of bytes sent from vtgate for each stream call. It's recommended to keep this value in sync with vttablet's query-server-config-stream-buffer-size.")
	queryPlanCacheSize   = flag.Int64("gate_query_cache_size", cache.DefaultConfig.MaxEntries, "gate server query cache size, maximum number of queries to be cached. vtgate analyzes every incoming query and generate a query plan, these plans are being cached in a cache. This config controls the expected amount of unique entries in the cache.")
	queryPlanCacheMemory = flag.Int64("gate_query_cache_memory", cache.DefaultConfig.MaxMemoryUsage, "gate server query cache size in bytes, maximum amount of memory to be cached. vtgate analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")
	queryPlanCacheLFU    = flag.Bool("gate_query_cache_lfu", cache.DefaultConfig.LFU, "gate server cache algorithm. when set to true, a new cache algorithm based on a TinyLFU admission policy will be used to improve cache behavior and prevent pollution from sparse queries")
	_                    = flag.Bool("disable_local_gateway", false, "deprecated: if specified, this process will not route any queries to local tablets in the local cell")
	maxMemoryRows        = flag.Int("max_memory_rows", 300000, "Maximum number of rows that will be held in memory for intermediate results as well as the final result.")
	warnMemoryRows       = flag.Int("warn_memory_rows", 30000, "Warning threshold for in-memory results. A row count higher than this amount will cause the VtGateWarnings.ResultsExceeded counter to be incremented.")
	defaultDDLStrategy   = flag.String("ddl_strategy", string(schema.DDLStrategyDirect), "Set default strategy for DDL statements. Override with @@ddl_strategy session variable")
	dbDDLPlugin          = flag.String("dbddl_plugin", "fail", "controls how to handle CREATE/DROP DATABASE. use it if you are using your own database provisioning service")

	// TODO(deepthi): change these two vars to unexported and move to healthcheck.go when LegacyHealthcheck is removed

	// HealthCheckRetryDelay is the time to wait before retrying healthcheck
	HealthCheckRetryDelay = flag.Duration("healthcheck_retry_delay", 2*time.Millisecond, "health check retry delay")
	// HealthCheckTimeout is the timeout on the RPC call to tablets
	HealthCheckTimeout = flag.Duration("healthcheck_timeout", time.Minute, "the health check timeout period")
	maxPayloadSize     = flag.Int("max_payload_size", 0, "The threshold for query payloads in bytes. A payload greater than this threshold will result in a failure to handle the query.")
	warnPayloadSize    = flag.Int("warn_payload_size", 0, "The warning threshold for query payloads in bytes. A payload greater than this threshold will cause the VtGateWarnings.WarnPayloadSizeExceeded counter to be incremented.")

	// Put set-passthrough under a flag.
	sysVarSetEnabled = flag.Bool("enable_system_settings", true, "This will enable the system settings to be changed per session at the database connection level")
	plannerVersion   = flag.String("planner_version", "v3", "Sets the default planner to use when the session has not changed it. Valid values are: V3, Gen4, Gen4Greedy and Gen4Fallback. Gen4Fallback tries the new gen4 planner and falls back to the V3 planner if the gen4 fails. All Gen4 versions should be considered experimental!")

	// lockHeartbeatTime is used to set the next heartbeat time.
	lockHeartbeatTime = flag.Duration("lock_heartbeat_time", 5*time.Second, "If there is lock function used. This will keep the lock connection active by using this heartbeat")
	warnShardedOnly   = flag.Bool("warn_sharded_only", false, "If any features that are only available in unsharded mode are used, query execution warnings will be added to the session")
)

func getTxMode() vtgatepb.TransactionMode {
	switch strings.ToLower(*transactionMode) {
	case "single":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		return vtgatepb.TransactionMode_SINGLE
	case "multi":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		return vtgatepb.TransactionMode_MULTI
	case "twopc":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		return vtgatepb.TransactionMode_TWOPC
	default:
		fmt.Printf("Invalid option: %v\n", *transactionMode)
		fmt.Println("Usage: -transaction_mode {SINGLE | MULTI | TWOPC}")
		os.Exit(1)
		return -1
	}
}

var (
	rpcVTGate *VTGate

	vschemaCounters *stats.CountersWithSingleLabel

	// Error counters should be global so they can be set from anywhere
	errorCounts *stats.CountersWithMultiLabels

	warnings *stats.CountersWithSingleLabel

	vstreamSkewDelayCount *stats.Counter
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
	gw       Gateway

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
func Init(ctx context.Context, serv srvtopo.Server, cell string, tabletTypesToWait []topodatapb.TabletType) *VTGate {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}

	// vschemaCounters needs to be initialized before planner to
	// catch the initial load stats.
	vschemaCounters = stats.NewCountersWithSingleLabel("VtgateVSchemaCounts", "Vtgate vschema counts", "changes")

	vstreamSkewDelayCount = stats.NewCounter("VStreamEventsDelayedBySkewAlignment",
		"Number of events that had to wait because the skew across shards was too high")

	// Build objects from low to high level.
	// Start with the gateway. If we can't reach the topology service,
	// we can't go on much further, so we log.Fatal out.
	// TabletGateway can create it's own healthcheck
	gw := NewTabletGateway(ctx, nil /*discovery.Healthcheck*/, serv, cell)
	gw.RegisterStats()
	if err := WaitForTablets(gw, tabletTypesToWait); err != nil {
		log.Fatalf("gateway.WaitForTablets failed: %v", err)
	}

	// If we want to filter keyspaces replace the srvtopo.Server with a
	// filtering server
	if len(discovery.KeyspacesToWatch) > 0 {
		log.Infof("Keyspace filtering enabled, selecting %v", discovery.KeyspacesToWatch)
		var err error
		serv, err = srvtopo.NewKeyspaceFilteringServer(serv, discovery.KeyspacesToWatch)
		if err != nil {
			log.Fatalf("Unable to construct SrvTopo server: %v", err.Error())
		}
	}

	if _, _, err := schema.ParseDDLStrategy(*defaultDDLStrategy); err != nil {
		log.Fatalf("Invalid value for -ddl_strategy: %v", err.Error())
	}
	tc := NewTxConn(gw, getTxMode())
	// ScatterConn depends on TxConn to perform forced rollbacks.
	sc := NewScatterConn("VttabletCall", tc, gw)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	resolver := NewResolver(srvResolver, serv, cell, sc)
	vsm := newVStreamManager(srvResolver, serv, cell)
	cacheCfg := &cache.Config{
		MaxEntries:     *queryPlanCacheSize,
		MaxMemoryUsage: *queryPlanCacheMemory,
		LFU:            *queryPlanCacheLFU,
	}

	rpcVTGate = &VTGate{
		executor: NewExecutor(ctx, serv, cell, resolver, *normalizeQueries, *warnShardedOnly, *streamBufferSize, cacheCfg),
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

	errorCounts = stats.NewCountersWithMultiLabels("VtgateApiErrorCounts", "Vtgate API error counts per error type", []string{"Operation", "Keyspace", "DbType", "Code"})

	_ = stats.NewRates("QPSByOperation", stats.CounterForDimension(rpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	_ = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(rpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	_ = stats.NewRates("QPSByDbType", stats.CounterForDimension(rpcVTGate.timings, "DbType"), 15*60/5, 5*time.Second)

	_ = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(errorCounts, "Operation"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(errorCounts, "Keyspace"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(errorCounts, "DbType"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByCode", stats.CounterForDimension(errorCounts, "Code"), 15, 1*time.Minute)

	warnings = stats.NewCountersWithSingleLabel("VtGateWarnings", "Vtgate warnings", "type", "IgnoredSet", "ResultsExceeded", "WarnPayloadSizeExceeded")

	servenv.OnRun(func() {
		for _, f := range RegisterVTGates {
			f(rpcVTGate)
		}
	})
	rpcVTGate.registerDebugHealthHandler()
	err := initQueryLogger(rpcVTGate)
	if err != nil {
		log.Fatalf("error initializing query logger: %v", err)
	}

	initAPI(gw.hc)

	return rpcVTGate
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
func (vtg *VTGate) Gateway() Gateway {
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
		goto handleError
	}

	qr, err = vtg.executor.Execute(ctx, "Execute", NewSafeSession(session), sql, bindVariables)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		vtg.rowsAffected.Add(statsKey, int64(qr.RowsAffected))
		return session, qr, nil
	}

handleError:
	query := map[string]interface{}{
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
		err = vtg.executor.StreamExecute(
			ctx,
			"StreamExecute",
			NewSafeSession(session),
			sql,
			bindVariables,
			querypb.Target{
				Keyspace:   destKeyspace,
				TabletType: destTabletType,
			},
			func(reply *sqltypes.Result) error {
				vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
				vtg.rowsAffected.Add(statsKey, int64(reply.RowsAffected))
				return callback(reply)
			})
	}
	if err != nil {
		query := map[string]interface{}{
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
	query := map[string]interface{}{
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

func truncateErrorStrings(data map[string]interface{}) map[string]interface{} {
	ret := map[string]interface{}{}
	if *terseErrors {
		// request might have PII information. Return an empty map
		return ret
	}
	for key, val := range data {
		mapVal, ok := val.(map[string]interface{})
		if ok {
			ret[key] = truncateErrorStrings(mapVal)
		} else {
			strVal := fmt.Sprintf("%v", val)
			ret[key] = sqlparser.TruncateForLog(strVal)
		}
	}
	return ret
}

func recordAndAnnotateError(err error, statsKey []string, request map[string]interface{}, logger *logutil.ThrottledLogger) error {
	ec := vterrors.Code(err)
	fullKey := []string{
		statsKey[0],
		statsKey[1],
		statsKey[2],
		ec.String(),
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

// LegacyInit initializes VTGate server with LegacyHealthCheck
func LegacyInit(ctx context.Context, hc discovery.LegacyHealthCheck, serv srvtopo.Server, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) *VTGate {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}

	// vschemaCounters needs to be initialized before planner to
	// catch the initial load stats.
	vschemaCounters = stats.NewCountersWithSingleLabel("VtgateVSchemaCounts", "Vtgate vschema counts", "changes")

	// Build objects from low to high level.
	// Start with the gateway. If we can't reach the topology service,
	// we can't go on much further, so we log.Fatal out.
	gw := GatewayCreator()(ctx, hc, serv, cell, retryCount)
	gw.RegisterStats()
	if err := WaitForTablets(gw, tabletTypesToWait); err != nil {
		log.Fatalf("gateway.WaitForTablets failed: %v", err)
	}

	// If we want to filter keyspaces replace the srvtopo.Server with a
	// filtering server
	if len(discovery.KeyspacesToWatch) > 0 {
		log.Infof("Keyspace filtering enabled, selecting %v", discovery.KeyspacesToWatch)
		var err error
		serv, err = srvtopo.NewKeyspaceFilteringServer(serv, discovery.KeyspacesToWatch)
		if err != nil {
			log.Fatalf("Unable to construct SrvTopo server: %v", err.Error())
		}
	}

	tc := NewTxConn(gw, getTxMode())
	// ScatterConn depends on TxConn to perform forced rollbacks.
	sc := NewLegacyScatterConn("VttabletCall", tc, gw, hc)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	resolver := NewResolver(srvResolver, serv, cell, sc)
	vsm := newVStreamManager(srvResolver, serv, cell)
	cacheCfg := &cache.Config{
		MaxEntries:     *queryPlanCacheSize,
		MaxMemoryUsage: *queryPlanCacheMemory,
		LFU:            *queryPlanCacheLFU,
	}

	rpcVTGate = &VTGate{
		executor: NewExecutor(ctx, serv, cell, resolver, *normalizeQueries, *warnShardedOnly, *streamBufferSize, cacheCfg),
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

	errorCounts = stats.NewCountersWithMultiLabels("VtgateApiErrorCounts", "Vtgate API error counts per error type", []string{"Operation", "Keyspace", "DbType", "Code"})

	_ = stats.NewRates("QPSByOperation", stats.CounterForDimension(rpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	_ = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(rpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	_ = stats.NewRates("QPSByDbType", stats.CounterForDimension(rpcVTGate.timings, "DbType"), 15*60/5, 5*time.Second)

	_ = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(errorCounts, "Operation"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(errorCounts, "Keyspace"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(errorCounts, "DbType"), 15, 1*time.Minute)
	_ = stats.NewRates("ErrorsByCode", stats.CounterForDimension(errorCounts, "Code"), 15, 1*time.Minute)

	warnings = stats.NewCountersWithSingleLabel("VtGateWarnings", "Vtgate warnings", "type", "IgnoredSet", "ResultsExceeded")

	servenv.OnRun(func() {
		for _, f := range RegisterVTGates {
			f(rpcVTGate)
		}
	})
	rpcVTGate.registerDebugHealthHandler()
	err := initQueryLogger(rpcVTGate)
	if err != nil {
		log.Fatalf("error initializing query logger: %v", err)
	}

	legacyInitAPI(hc)

	return rpcVTGate
}
