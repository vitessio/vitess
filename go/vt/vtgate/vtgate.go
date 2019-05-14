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

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"flag"
	"fmt"
	"math"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlannotation"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vtgate/gateway"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	transactionMode     = flag.String("transaction_mode", "MULTI", "SINGLE: disallow multi-db transactions, MULTI: allow multi-db transactions with best effort commit, TWOPC: allow multi-db transactions with 2pc commit")
	normalizeQueries    = flag.Bool("normalize_queries", true, "Rewrite queries with bind vars. Turn this off if the app itself sends normalized queries with bind vars.")
	terseErrors         = flag.Bool("vtgate-config-terse-errors", false, "prevent bind vars from escaping in returned errors")
	streamBufferSize    = flag.Int("stream_buffer_size", 32*1024, "the number of bytes sent from vtgate for each stream call. It's recommended to keep this value in sync with vttablet's query-server-config-stream-buffer-size.")
	queryPlanCacheSize  = flag.Int64("gate_query_cache_size", 10000, "gate server query cache size, maximum number of queries to be cached. vtgate analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")
	disableLocalGateway = flag.Bool("disable_local_gateway", false, "if specified, this process will not route any queries to local tablets in the local cell")
)

func getTxMode() vtgatepb.TransactionMode {
	switch *transactionMode {
	case "SINGLE":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		return vtgatepb.TransactionMode_SINGLE
	case "MULTI":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		return vtgatepb.TransactionMode_MULTI
	case "TWOPC":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		return vtgatepb.TransactionMode_TWOPC
	default:
		log.Warningf("Unrecognized transactionMode '%s'. Continuing with default 'MULTI'", *transactionMode)
		return vtgatepb.TransactionMode_MULTI
	}
}

var (
	rpcVTGate *VTGate

	qpsByOperation *stats.Rates
	qpsByKeyspace  *stats.Rates
	qpsByDbType    *stats.Rates

	vschemaCounters *stats.CountersWithSingleLabel

	errorsByOperation *stats.Rates
	errorsByKeyspace  *stats.Rates
	errorsByDbType    *stats.Rates
	errorsByCode      *stats.Rates

	// Error counters should be global so they can be set from anywhere
	errorCounts *stats.CountersWithMultiLabels

	warnings *stats.CountersWithSingleLabel
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created. It implements vtgateservice.VTGateService
// VTGate exposes multiple generations of interfaces. The V3
// interface is the latest one, which is capable of processing
// queries with no additional hints. V2 functions require
// the keyspace id or keyrange to be specified. V1 functions
// require shard info. V0 functions are informational that
// return topo information. Often, 'V2' or 'legacy' is used
// to refer to all legacy versions of the API (V2, V1 and V0).
type VTGate struct {
	// Dependency: executor->resolver->scatterConn->txConn->gateway.
	// VTGate still needs resolver and txConn to support legacy functions.
	executor *Executor
	resolver *Resolver
	txConn   *TxConn
	gw       gateway.Gateway

	// stats objects.
	// TODO(sougou): This needs to be cleaned up. There
	// are global vars that depend on this member var.
	timings      *stats.MultiTimings
	rowsReturned *stats.CountersWithMultiLabels

	// the throttled loggers for all errors, one per API entry
	logExecute                  *logutil.ThrottledLogger
	logStreamExecute            *logutil.ThrottledLogger
	logExecuteShards            *logutil.ThrottledLogger
	logExecuteKeyspaceIds       *logutil.ThrottledLogger
	logExecuteKeyRanges         *logutil.ThrottledLogger
	logExecuteEntityIds         *logutil.ThrottledLogger
	logExecuteBatchShards       *logutil.ThrottledLogger
	logExecuteBatchKeyspaceIds  *logutil.ThrottledLogger
	logStreamExecuteKeyspaceIds *logutil.ThrottledLogger
	logStreamExecuteKeyRanges   *logutil.ThrottledLogger
	logStreamExecuteShards      *logutil.ThrottledLogger
	logUpdateStream             *logutil.ThrottledLogger
	logMessageStream            *logutil.ThrottledLogger
}

// RegisterVTGate defines the type of registration mechanism.
type RegisterVTGate func(vtgateservice.VTGateService)

// RegisterVTGates stores register funcs for VTGate server.
var RegisterVTGates []RegisterVTGate

// Init initializes VTGate server.
func Init(ctx context.Context, hc discovery.HealthCheck, serv srvtopo.Server, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) *VTGate {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}

	// vschemaCounters needs to be initialized before planner to
	// catch the initial load stats.
	vschemaCounters = stats.NewCountersWithSingleLabel("VtgateVSchemaCounts", "Vtgate vschema counts", "changes")

	// Build objects from low to high level.
	// Start with the gateway. If we can't reach the topology service,
	// we can't go on much further, so we log.Fatal out.
	var gw gateway.Gateway
	if !*disableLocalGateway {
		gw = gateway.GetCreator()(ctx, hc, serv, cell, retryCount)
		gw.RegisterStats()
		if err := gateway.WaitForTablets(gw, tabletTypesToWait); err != nil {
			log.Fatalf("gateway.WaitForTablets failed: %v", err)
		}
	}

	// Check we have something to do.
	if gw == nil {
		log.Fatalf("'-disable_local_gateway' cannot be specified if 'l2vtgate_addrs' is also empty, otherwise this vtgate has no backend")
	}

	// If we want to filter keyspaces replace the srvtopo.Server with a
	// filtering server
	if len(gateway.KeyspacesToWatch) > 0 {
		log.Infof("Keyspace filtering enabled, selecting %v", gateway.KeyspacesToWatch)
		var err error
		serv, err = srvtopo.NewKeyspaceFilteringServer(serv, gateway.KeyspacesToWatch)
		if err != nil {
			log.Fatalf("Unable to construct SrvTopo server: %v", err.Error())
		}
	}

	tc := NewTxConn(gw, getTxMode())
	// ScatterConn depends on TxConn to perform forced rollbacks.
	sc := NewScatterConn("VttabletCall", tc, gw, hc)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	resolver := NewResolver(srvResolver, serv, cell, sc)

	rpcVTGate = &VTGate{
		executor: NewExecutor(ctx, serv, cell, "VTGateExecutor", resolver, *normalizeQueries, *streamBufferSize, *queryPlanCacheSize),
		resolver: resolver,
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

		logExecute:                  logutil.NewThrottledLogger("Execute", 5*time.Second),
		logStreamExecute:            logutil.NewThrottledLogger("StreamExecute", 5*time.Second),
		logExecuteShards:            logutil.NewThrottledLogger("ExecuteShards", 5*time.Second),
		logExecuteKeyspaceIds:       logutil.NewThrottledLogger("ExecuteKeyspaceIds", 5*time.Second),
		logExecuteKeyRanges:         logutil.NewThrottledLogger("ExecuteKeyRanges", 5*time.Second),
		logExecuteEntityIds:         logutil.NewThrottledLogger("ExecuteEntityIds", 5*time.Second),
		logExecuteBatchShards:       logutil.NewThrottledLogger("ExecuteBatchShards", 5*time.Second),
		logExecuteBatchKeyspaceIds:  logutil.NewThrottledLogger("ExecuteBatchKeyspaceIds", 5*time.Second),
		logStreamExecuteKeyspaceIds: logutil.NewThrottledLogger("StreamExecuteKeyspaceIds", 5*time.Second),
		logStreamExecuteKeyRanges:   logutil.NewThrottledLogger("StreamExecuteKeyRanges", 5*time.Second),
		logStreamExecuteShards:      logutil.NewThrottledLogger("StreamExecuteShards", 5*time.Second),
		logUpdateStream:             logutil.NewThrottledLogger("UpdateStream", 5*time.Second),
		logMessageStream:            logutil.NewThrottledLogger("MessageStream", 5*time.Second),
	}

	errorCounts = stats.NewCountersWithMultiLabels("VtgateApiErrorCounts", "Vtgate API error counts per error type", []string{"Operation", "Keyspace", "DbType", "Code"})

	qpsByOperation = stats.NewRates("QPSByOperation", stats.CounterForDimension(rpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	qpsByKeyspace = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(rpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	qpsByDbType = stats.NewRates("QPSByDbType", stats.CounterForDimension(rpcVTGate.timings, "DbType"), 15*60/5, 5*time.Second)

	errorsByOperation = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(errorCounts, "Operation"), 15, 1*time.Minute)
	errorsByKeyspace = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(errorCounts, "Keyspace"), 15, 1*time.Minute)
	errorsByDbType = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(errorCounts, "DbType"), 15, 1*time.Minute)
	errorsByCode = stats.NewRates("ErrorsByCode", stats.CounterForDimension(errorCounts, "Code"), 15, 1*time.Minute)

	warnings = stats.NewCountersWithSingleLabel("VtGateWarnings", "Vtgate warnings", "type", "IgnoredSet")

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

	initAPI(ctx, hc)

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
func (vtg *VTGate) Gateway() gateway.Gateway {
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
		}
	}
	return session, qrl, nil
}

// StreamExecute executes a streaming query. This is a V3 function.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines.
func (vtg *VTGate) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	// In this context, we don't care if we can't fully parse destination
	destKeyspace, destTabletType, dest, _ := vtg.executor.ParseDestinationTarget(session.TargetString)
	statsKey := []string{"StreamExecute", destKeyspace, topoproto.TabletTypeLString(destTabletType)}

	defer vtg.timings.Record(statsKey, time.Now())

	var err error
	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	// TODO: This could be simplified to have a StreamExecute that takes
	// a destTarget without explicit destination.
	switch dest.(type) {
	case key.DestinationShard:
		err = vtg.resolver.StreamExecute(
			ctx,
			sql,
			bindVariables,
			destKeyspace,
			destTabletType,
			dest,
			session.Options,
			func(reply *sqltypes.Result) error {
				vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
				return callback(reply)
			})
	default:
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
				return callback(reply)
			})
	}
handleError:
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

// ExecuteShards executes a non-streaming query on the specified shards.
// This is a legacy function.
func (vtg *VTGate) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteShards", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var qr *sqltypes.Result
	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	sql = sqlannotation.AnnotateIfDML(sql, nil)

	qr, err = vtg.resolver.Execute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		key.DestinationShards(shards),
		session,
		notInTransaction,
		options,
		nil,
	)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

handleError:
	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"Keyspace":         keyspace,
		"Shards":           shards,
		"TabletType":       ltt,
		"Session":          session,
		"NotInTransaction": notInTransaction,
		"Options":          options,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecuteShards)
	return nil, err
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids. This is a legacy function.
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteKeyspaceIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var qr *sqltypes.Result
	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	sql = sqlannotation.AnnotateIfDML(sql, keyspaceIds)
	if sqlparser.IsDML(sql) && len(keyspaceIds) > 1 {
		err = vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "DML should not span multiple keyspace_ids")
		goto handleError
	}

	qr, err = vtg.resolver.Execute(ctx, sql, bindVariables, keyspace, tabletType, key.DestinationKeyspaceIDs(keyspaceIds), session, notInTransaction, options, nil /* LogStats */)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

handleError:
	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"Keyspace":         keyspace,
		"KeyspaceIds":      keyspaceIds,
		"TabletType":       ltt,
		"Session":          session,
		"NotInTransaction": notInTransaction,
		"Options":          options,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecuteKeyspaceIds)
	return nil, err
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges. This is a legacy function.
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteKeyRanges", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var qr *sqltypes.Result
	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	sql = sqlannotation.AnnotateIfDML(sql, nil)

	qr, err = vtg.resolver.Execute(ctx, sql, bindVariables, keyspace, tabletType, key.DestinationKeyRanges(keyRanges), session, notInTransaction, options, nil /* LogStats */)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

handleError:
	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"Keyspace":         keyspace,
		"KeyRanges":        keyRanges,
		"TabletType":       ltt,
		"Session":          session,
		"NotInTransaction": notInTransaction,
		"Options":          options,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecuteKeyRanges)
	return nil, err
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map. This is a legacy function.
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteEntityIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var qr *sqltypes.Result
	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	sql = sqlannotation.AnnotateIfDML(sql, nil)

	qr, err = vtg.resolver.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, options)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

handleError:
	query := map[string]interface{}{
		"Sql":               sql,
		"BindVariables":     bindVariables,
		"Keyspace":          keyspace,
		"EntityColumnName":  entityColumnName,
		"EntityKeyspaceIDs": entityKeyspaceIDs,
		"TabletType":        ltt,
		"Session":           session,
		"NotInTransaction":  notInTransaction,
		"Options":           options,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecuteEntityIds)
	return nil, err
}

// ExecuteBatchShards executes a group of queries on the specified shards. This is a legacy function.
func (vtg *VTGate) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteBatchShards", unambiguousKeyspaceBSQ(queries), ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var qrs []sqltypes.Result
	var err error

	for _, query := range queries {
		if bvErr := sqltypes.ValidateBindVariables(query.Query.BindVariables); bvErr != nil {
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
			goto handleError
		}
	}

	annotateBoundShardQueriesAsUnfriendly(queries)

	qrs, err = vtg.resolver.ExecuteBatch(
		ctx,
		tabletType,
		asTransaction,
		session,
		options,
		func() (*scatterBatchRequest, error) {
			return boundShardQueriesToScatterBatchRequest(ctx, vtg.resolver.resolver, queries, tabletType)
		})
	if err == nil {
		var rowCount int64
		for _, qr := range qrs {
			rowCount += int64(len(qr.Rows))
		}
		vtg.rowsReturned.Add(statsKey, rowCount)
		return qrs, nil
	}

handleError:
	query := map[string]interface{}{
		"Queries":       queries,
		"TabletType":    ltt,
		"AsTransaction": asTransaction,
		"Session":       session,
		"Options":       options,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecuteBatchShards)
	return nil, err
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids. This is a legacy function.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteBatchKeyspaceIds", unambiguousKeyspaceBKSIQ(queries), ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var qrs []sqltypes.Result
	var err error

	for _, query := range queries {
		if bvErr := sqltypes.ValidateBindVariables(query.Query.BindVariables); bvErr != nil {
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
			goto handleError
		}
	}

	annotateBoundKeyspaceIDQueries(queries)

	qrs, err = vtg.resolver.ExecuteBatch(
		ctx,
		tabletType,
		asTransaction,
		session,
		options,
		func() (*scatterBatchRequest, error) {
			return boundKeyspaceIDQueriesToScatterBatchRequest(ctx, vtg.resolver.resolver, queries, tabletType)
		})
	if err == nil {
		var rowCount int64
		for _, qr := range qrs {
			rowCount += int64(len(qr.Rows))
		}
		vtg.rowsReturned.Add(statsKey, rowCount)
		return qrs, nil
	}

handleError:
	query := map[string]interface{}{
		"Queries":       queries,
		"TabletType":    ltt,
		"AsTransaction": asTransaction,
		"Session":       session,
		"Options":       options,
	}
	err = recordAndAnnotateError(err, statsKey, query, vtg.logExecuteBatchKeyspaceIds)
	return nil, err
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof. This is a legacy function.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines.
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecuteKeyspaceIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	err = vtg.resolver.StreamExecute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		key.DestinationKeyspaceIDs(keyspaceIds),
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return callback(reply)
		})

handleError:
	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"KeyspaceIds":   keyspaceIds,
			"TabletType":    ltt,
			"Options":       options,
		}
		return recordAndAnnotateError(err, statsKey, query, vtg.logStreamExecuteKeyspaceIds)
	}
	return nil
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof. This is a legacy function.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines.
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecuteKeyRanges", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	err = vtg.resolver.StreamExecute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		key.DestinationKeyRanges(keyRanges),
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return callback(reply)
		})

handleError:
	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"KeyRanges":     keyRanges,
			"TabletType":    ltt,
			"Options":       options,
		}
		return recordAndAnnotateError(err, statsKey, query, vtg.logStreamExecuteKeyRanges)
	}
	return nil
}

// StreamExecuteShards executes a streaming query on the specified shards. This is a legacy function.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines.
func (vtg *VTGate) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecuteShards", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	var err error

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
		goto handleError
	}

	err = vtg.resolver.StreamExecute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		key.DestinationShards(shards),
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return callback(reply)
		})

handleError:
	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"Shards":        shards,
			"TabletType":    ltt,
			"Options":       options,
		}
		return recordAndAnnotateError(err, statsKey, query, vtg.logStreamExecuteShards)
	}
	return nil
}

// Begin begins a transaction. This is a legacy function.
func (vtg *VTGate) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	if !singledb && vtg.txConn.mode == vtgatepb.TransactionMode_SINGLE {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "multi-db transaction disallowed")
	}
	return &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      singledb,
	}, nil
}

// Commit commits a transaction. This is a legacy function.
func (vtg *VTGate) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	if session == nil {
		return formatError(vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "cannot commit: empty session"))
	}
	if !session.InTransaction {
		return formatError(vterrors.New(vtrpcpb.Code_ABORTED, "cannot commit: not in transaction"))
	}
	if twopc {
		session.TransactionMode = vtgatepb.TransactionMode_TWOPC
	}
	return formatError(vtg.txConn.Commit(ctx, NewSafeSession(session)))
}

// Rollback rolls back a transaction. This is a legacy function.
func (vtg *VTGate) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	return formatError(vtg.txConn.Rollback(ctx, NewSafeSession(session)))
}

// ResolveTransaction resolves the specified 2PC transaction.
func (vtg *VTGate) ResolveTransaction(ctx context.Context, dtid string) error {
	return formatError(vtg.txConn.Resolve(ctx, dtid))
}

// isKeyspaceRangeBasedSharded returns true if a keyspace is sharded
// by range.  This is true when there is a ShardingColumnType defined
// in the SrvKeyspace (that is using the range-based sharding with the
// client specifying the sharding key), or when the VSchema for the
// keyspace is Sharded.
func (vtg *VTGate) isKeyspaceRangeBasedSharded(keyspace string, srvKeyspace *topodatapb.SrvKeyspace) bool {
	if srvKeyspace.ShardingColumnType != topodatapb.KeyspaceIdType_UNSET {
		// We are using range based sharding with the application
		// providing the sharding key value.
		return true
	}
	if vtg.executor.IsKeyspaceRangeBasedSharded(keyspace) {
		// We are using range based sharding with the VSchema
		// poviding the routing information
		return true
	}

	// Not range based sharded, might be un-sharded or custom sharded.
	return false
}

// SplitQuery implements the SplitQuery RPC. This is the new version that
// supports multiple split-columns and multiple splitting algorithms.
// See the documentation of SplitQueryRequest in "proto/vtgate.proto" for more
// information.
func (vtg *VTGate) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	if bvErr := sqltypes.ValidateBindVariables(bindVariables); bvErr != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", bvErr)
	}

	// TODO(erez): Add validation of SplitQuery parameters.
	rss, srvKeyspace, err := vtg.resolver.resolver.GetAllShards(ctx, keyspace, topodatapb.TabletType_RDONLY)
	if err != nil {
		return nil, err
	}

	// If the caller specified a splitCount (vs. specifying 'numRowsPerQueryPart') scale it by the
	// number of shards (otherwise it stays 0).
	perShardSplitCount := int64(math.Ceil(float64(splitCount) / float64(len(rss))))

	// Determine whether to return SplitQueryResponse_KeyRangeParts or SplitQueryResponse_ShardParts.
	// We return 'KeyRangeParts' for sharded keyspaces that are not custom sharded. If the
	// keyspace is custom sharded or unsharded we return 'ShardParts'.
	var querySplitToQueryPartFunc func(
		querySplit *querypb.QuerySplit, rs *srvtopo.ResolvedShard) (*vtgatepb.SplitQueryResponse_Part, error)
	if vtg.isKeyspaceRangeBasedSharded(keyspace, srvKeyspace) {
		querySplitToQueryPartFunc = func(querySplit *querypb.QuerySplit, rs *srvtopo.ResolvedShard) (*vtgatepb.SplitQueryResponse_Part, error) {
			// Use ValidateShardName to extract the keyrange.
			_, kr, err := topo.ValidateShardName(rs.Target.Shard)
			if err != nil {
				return nil, fmt.Errorf("cannot extract keyrange from shard name %v: %v", rs.Target.Shard, err)
			}
			if kr == nil {
				// Keyrange can be nil for the shard (e.g. for single-sharded keyspaces during resharding).
				// In this case we append an empty keyrange that represents the entire keyspace.
				kr = &topodatapb.KeyRange{
					Start: []byte{},
					End:   []byte{},
				}
			}
			return &vtgatepb.SplitQueryResponse_Part{
				Query: querySplit.Query,
				KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{
					Keyspace:  keyspace,
					KeyRanges: []*topodatapb.KeyRange{kr},
				},
				Size: querySplit.RowCount,
			}, nil
		}
	} else {
		// Keyspace is either unsharded or custom-sharded.
		querySplitToQueryPartFunc = func(querySplit *querypb.QuerySplit, rs *srvtopo.ResolvedShard) (*vtgatepb.SplitQueryResponse_Part, error) {
			return &vtgatepb.SplitQueryResponse_Part{
				Query: querySplit.Query,
				ShardPart: &vtgatepb.SplitQueryResponse_ShardPart{
					Keyspace: keyspace,
					Shards:   []string{rs.Target.Shard},
				},
				Size: querySplit.RowCount,
			}, nil
		}
	}

	return vtg.resolver.scatterConn.SplitQuery(
		ctx,
		sql,
		bindVariables,
		splitColumns,
		perShardSplitCount,
		numRowsPerQueryPart,
		algorithm,
		rss,
		querySplitToQueryPartFunc)
}

// GetSrvKeyspace is part of the vtgate service API.
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return vtg.resolver.toposerv.GetSrvKeyspace(ctx, vtg.resolver.cell, keyspace)
}

// MessageStream is part of the vtgate service API. This is a V2 level API
// that's sent to the Resolver.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines.
func (vtg *VTGate) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(topodatapb.TabletType_MASTER)
	statsKey := []string{"MessageStream", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.executor.MessageStream(
		ctx,
		keyspace,
		shard,
		keyRange,
		name,
		callback,
	)
	if err != nil {
		request := map[string]interface{}{
			"Keyspace":    keyspace,
			"Shard":       shard,
			"KeyRange":    keyRange,
			"TabletType":  ltt,
			"MessageName": name,
		}
		recordAndAnnotateError(err, statsKey, request, vtg.logMessageStream)
	}
	return formatError(err)
}

// MessageAck is part of the vtgate service API. This is a V3 level API that's sent
// to the executor. The table name will be resolved using V3 rules, and the routing
// will make use of vindexes for sharded keyspaces.
// TODO(sougou): Deprecate this in favor of an SQL statement.
func (vtg *VTGate) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(topodatapb.TabletType_MASTER)
	statsKey := []string{"MessageAck", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	for _, id := range ids {
		if _, err := sqltypes.NewValue(id.Type, id.Value); err != nil {
			return 0, formatError(err)
		}
	}

	count, err := vtg.executor.MessageAck(ctx, keyspace, name, ids)
	return count, formatError(err)
}

// MessageAckKeyspaceIds is part of the vtgate service API. It routes
// message acks based on the associated keyspace ids.
func (vtg *VTGate) MessageAckKeyspaceIds(ctx context.Context, keyspace string, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(topodatapb.TabletType_MASTER)
	statsKey := []string{"MessageAckKeyspaceIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	for _, idKeyspaceID := range idKeyspaceIDs {
		if _, err := sqltypes.NewValue(idKeyspaceID.Id.Type, idKeyspaceID.Id.Value); err != nil {
			return 0, formatError(err)
		}
	}

	count, err := vtg.resolver.MessageAckKeyspaceIds(ctx, keyspace, name, idKeyspaceIDs)
	return count, formatError(err)
}

// UpdateStream is part of the vtgate service API.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines, as the current implementation can only target
// one shard.
func (vtg *VTGate) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"UpdateStream", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.UpdateStream(
		ctx,
		keyspace,
		shard,
		keyRange,
		tabletType,
		timestamp,
		event,
		callback,
	)
	if err != nil {
		request := map[string]interface{}{
			"Keyspace":   keyspace,
			"Shard":      shard,
			"KeyRange":   keyRange,
			"TabletType": ltt,
			"Timestamp":  timestamp,
		}
		recordAndAnnotateError(err, statsKey, request, vtg.logUpdateStream)
	}
	return formatError(err)
}

// VStream streams binlog events.
func (vtg *VTGate) VStream(ctx context.Context, position string, tabletType topodatapb.TabletType, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return vtg.resolver.VStream(ctx, position, tabletType, filter, send)
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (vtg *VTGate) GetGatewayCacheStatus() gateway.TabletCacheStatusList {
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
	return vterrors.Wrapf(err, "vtgate: %s", servenv.ListeningURL.String())
}

func formatError(err error) error {
	if err == nil {
		return nil
	}
	return vterrors.Wrapf(err, "vtgate: %s", servenv.ListeningURL.String())
}

// HandlePanic recovers from panics, and logs / increment counters
func (vtg *VTGate) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v, vtgate: %v", x, servenv.ListeningURL.String())
		errorCounts.Add([]string{"Panic", "Unknown", "Unknown", vtrpcpb.Code_INTERNAL.String()}, 1)
	}
}

// Helper function used in ExecuteBatchKeyspaceIds
func annotateBoundKeyspaceIDQueries(queries []*vtgatepb.BoundKeyspaceIdQuery) {
	for i, q := range queries {
		queries[i].Query.Sql = sqlannotation.AnnotateIfDML(q.Query.Sql, q.KeyspaceIds)
	}
}

// Helper function used in ExecuteBatchShards
func annotateBoundShardQueriesAsUnfriendly(queries []*vtgatepb.BoundShardQuery) {
	for i, q := range queries {
		queries[i].Query.Sql = sqlannotation.AnnotateIfDML(q.Query.Sql, nil)
	}
}

// unambiguousKeyspaceBKSIQ is a helper function used in the
// ExecuteBatchKeyspaceIds method to determine the "keyspace" label for the
// stats reporting.
// If all queries target the same keyspace, it returns that keyspace.
// Otherwise it returns an empty string.
func unambiguousKeyspaceBKSIQ(queries []*vtgatepb.BoundKeyspaceIdQuery) string {
	switch len(queries) {
	case 0:
		return ""
	case 1:
		return queries[0].Keyspace
	default:
		keyspace := queries[0].Keyspace
		for _, q := range queries[1:] {
			if q.Keyspace != keyspace {
				// Request targets at least two different keyspaces.
				return ""
			}
		}
		return keyspace
	}
}

// unambiguousKeyspaceBSQ is the same as unambiguousKeyspaceBKSIQ but for the
// ExecuteBatchShards method. We are intentionally duplicating the code here and
// do not try to generalize it because this may be less performant.
func unambiguousKeyspaceBSQ(queries []*vtgatepb.BoundShardQuery) string {
	switch len(queries) {
	case 0:
		return ""
	case 1:
		return queries[0].Keyspace
	default:
		keyspace := queries[0].Keyspace
		for _, q := range queries[1:] {
			if q.Keyspace != keyspace {
				// Request targets at least two different keyspaces.
				return ""
			}
		}
		return keyspace
	}
}
