// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"

	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	transactionMode  = flag.String("transaction_mode", "multi", "single: disallow multi-db transactions, multi: allow multi-db transactions with best effort commit, twopc: allow multi-db transactions with 2pc commit")
	normalizeQueries = flag.Bool("normalize_queries", false, "Turning this flag on will cause vtgate to rewrite queries with bind vars. This is beneficial if the app doesn't itself send normalized queries.")
)

// Transaction modes. The value specifies what's allowed.
const (
	TxSingle = iota
	TxMulti
	TxTwoPC
)

func getTxMode() int {
	txMode := TxMulti
	switch *transactionMode {
	case "single":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		txMode = TxSingle
	case "multi":
		log.Infof("Transaction mode: '%s'", *transactionMode)
	case "twopc":
		log.Infof("Transaction mode: '%s'", *transactionMode)
		txMode = TxTwoPC
	default:
		log.Warningf("Unrecognized transactionMode '%s'. Continuing with default 'multi'", *transactionMode)
	}
	return txMode
}

var (
	rpcVTGate *VTGate

	qpsByOperation *stats.Rates
	qpsByKeyspace  *stats.Rates
	qpsByDbType    *stats.Rates

	vschemaCounters *stats.Counters

	errorsByOperation *stats.Rates
	errorsByKeyspace  *stats.Rates
	errorsByDbType    *stats.Rates

	// Error counters should be global so they can be set from anywhere
	normalErrors   *stats.MultiCounters
	infoErrors     *stats.Counters
	internalErrors *stats.Counters
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created. It implements vtgateservice.VTGateService
type VTGate struct {
	transactionMode int

	// router and resolver are top-level objects
	// that make routing decisions.
	router   *Router
	resolver *Resolver

	// scatterConn and txConn are mid-level objects
	// that execute requests.
	scatterConn *ScatterConn
	txConn      *TxConn

	// gateway is the low-level outgoing connection
	// object to vttablet or l2vtgate.
	gateway gateway.Gateway

	// stats objects.
	// TODO(sougou): This needs to be cleaned up. There
	// are global vars that depend on this member var.
	timings      *stats.MultiTimings
	rowsReturned *stats.MultiCounters

	// the throttled loggers for all errors, one per API entry
	logExecute                  *logutil.ThrottledLogger
	logExecuteShards            *logutil.ThrottledLogger
	logExecuteKeyspaceIds       *logutil.ThrottledLogger
	logExecuteKeyRanges         *logutil.ThrottledLogger
	logExecuteEntityIds         *logutil.ThrottledLogger
	logExecuteBatchShards       *logutil.ThrottledLogger
	logExecuteBatchKeyspaceIds  *logutil.ThrottledLogger
	logStreamExecute            *logutil.ThrottledLogger
	logStreamExecuteKeyspaceIds *logutil.ThrottledLogger
	logStreamExecuteKeyRanges   *logutil.ThrottledLogger
	logStreamExecuteShards      *logutil.ThrottledLogger
	logUpdateStream             *logutil.ThrottledLogger
}

// RegisterVTGate defines the type of registration mechanism.
type RegisterVTGate func(vtgateservice.VTGateService)

// RegisterVTGates stores register funcs for VTGate server.
var RegisterVTGates []RegisterVTGate

var vtgateOnce sync.Once

// Init initializes VTGate server.
func Init(ctx context.Context, hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) *VTGate {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}

	// vschemaCounters needs to be initialized before planner to
	// catch the initial load stats.
	vschemaCounters = stats.NewCounters("VtgateVSchemaCounts")

	// Build objects from low to high level.
	gw := gateway.GetCreator()(hc, topoServer, serv, cell, retryCount)
	gateway.WaitForTablets(gw, tabletTypesToWait)

	tc := NewTxConn(gw)
	// ScatterConn depends on TxConn to perform forced rollbacks.
	sc := NewScatterConn("VttabletCall", tc, gw)

	rpcVTGate = &VTGate{
		transactionMode: getTxMode(),
		router:          NewRouter(ctx, serv, cell, "VTGateRouter", sc, *normalizeQueries),
		resolver:        NewResolver(serv, cell, sc),
		scatterConn:     sc,
		txConn:          tc,
		gateway:         gw,
		timings:         stats.NewMultiTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),
		rowsReturned:    stats.NewMultiCounters("VtgateApiRowsReturned", []string{"Operation", "Keyspace", "DbType"}),

		logExecute:                  logutil.NewThrottledLogger("Execute", 5*time.Second),
		logExecuteShards:            logutil.NewThrottledLogger("ExecuteShards", 5*time.Second),
		logExecuteKeyspaceIds:       logutil.NewThrottledLogger("ExecuteKeyspaceIds", 5*time.Second),
		logExecuteKeyRanges:         logutil.NewThrottledLogger("ExecuteKeyRanges", 5*time.Second),
		logExecuteEntityIds:         logutil.NewThrottledLogger("ExecuteEntityIds", 5*time.Second),
		logExecuteBatchShards:       logutil.NewThrottledLogger("ExecuteBatchShards", 5*time.Second),
		logExecuteBatchKeyspaceIds:  logutil.NewThrottledLogger("ExecuteBatchKeyspaceIds", 5*time.Second),
		logStreamExecute:            logutil.NewThrottledLogger("StreamExecute", 5*time.Second),
		logStreamExecuteKeyspaceIds: logutil.NewThrottledLogger("StreamExecuteKeyspaceIds", 5*time.Second),
		logStreamExecuteKeyRanges:   logutil.NewThrottledLogger("StreamExecuteKeyRanges", 5*time.Second),
		logStreamExecuteShards:      logutil.NewThrottledLogger("StreamExecuteShards", 5*time.Second),
		logUpdateStream:             logutil.NewThrottledLogger("UpdateStream", 5*time.Second),
	}

	normalErrors = stats.NewMultiCounters("VtgateApiErrorCounts", []string{"Operation", "Keyspace", "DbType"})
	infoErrors = stats.NewCounters("VtgateInfoErrorCounts")
	internalErrors = stats.NewCounters("VtgateInternalErrorCounts")

	qpsByOperation = stats.NewRates("QPSByOperation", stats.CounterForDimension(rpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	qpsByKeyspace = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(rpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	qpsByDbType = stats.NewRates("QPSByDbType", stats.CounterForDimension(rpcVTGate.timings, "DbType"), 15, 1*time.Minute)

	errorsByOperation = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(normalErrors, "Operation"), 15, 1*time.Minute)
	errorsByKeyspace = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(normalErrors, "Keyspace"), 15, 1*time.Minute)
	errorsByDbType = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(normalErrors, "DbType"), 15, 1*time.Minute)

	servenv.OnRun(func() {
		for _, f := range RegisterVTGates {
			f(rpcVTGate)
		}
	})
	vtgateOnce.Do(rpcVTGate.registerDebugHealthHandler)
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

// Execute executes a non-streaming query by routing based on the values in the query.
func (vtg *VTGate) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"Execute", "Any", ltt}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.router.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, notInTransaction, options)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"Keyspace":         keyspace,
		"TabletType":       ltt,
		"Session":          session,
		"NotInTransaction": notInTransaction,
		"Options":          options,
	}
	err = handleExecuteError(err, statsKey, query, vtg.logExecute)
	return nil, err
}

// ExecuteShards executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteShards", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	sql = sqlannotation.AnnotateIfDML(sql, nil)

	qr, err := vtg.resolver.Execute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		session,
		func(keyspace string) (string, []string, error) {
			return keyspace, shards, nil
		},
		notInTransaction,
		options,
	)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

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
	err = handleExecuteError(err, statsKey, query, vtg.logExecuteShards)
	return nil, err
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteKeyspaceIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	sql = sqlannotation.AnnotateIfDML(sql, keyspaceIds)

	qr, err := vtg.resolver.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, options)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

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
	err = handleExecuteError(err, statsKey, query, vtg.logExecuteKeyspaceIds)
	return nil, err
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteKeyRanges", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	sql = sqlannotation.AnnotateIfDML(sql, nil)

	qr, err := vtg.resolver.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, options)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

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
	err = handleExecuteError(err, statsKey, query, vtg.logExecuteKeyRanges)
	return nil, err
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteEntityIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	sql = sqlannotation.AnnotateIfDML(sql, nil)

	qr, err := vtg.resolver.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, options)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

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
	err = handleExecuteError(err, statsKey, query, vtg.logExecuteEntityIds)
	return nil, err
}

// ExecuteBatch executes a non-streaming queries by routing based on the values in the query.
func (vtg *VTGate) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteBatch", "Any", ltt}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.router.ExecuteBatch(ctx, sqlList, bindVariablesList, keyspace, tabletType, asTransaction, session, options)
	if err == nil {
		for _, queryResponse := range qr {
			if queryResponse.QueryResult != nil {
				vtg.rowsReturned.Add(statsKey, int64(len(queryResponse.QueryResult.Rows)))
			}
		}
		return qr, nil
	}

	query := map[string]interface{}{
		"Sql":           sqlList,
		"BindVariables": bindVariablesList,
		"Keyspace":      keyspace,
		"TabletType":    ltt,
		"Session":       session,
		"AsTransaction": asTransaction,
		"Options":       options,
	}
	err = handleExecuteError(err, statsKey, query, vtg.logExecute)
	return nil, err
}

// ExecuteBatchShards executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteBatchShards", "", ltt}
	defer vtg.timings.Record(statsKey, startTime)

	annotateBoundShardQueriesAsUnfriendly(queries)

	qrs, err := vtg.resolver.ExecuteBatch(
		ctx,
		tabletType,
		asTransaction,
		session,
		options,
		func() (*scatterBatchRequest, error) {
			return boundShardQueriesToScatterBatchRequest(queries)
		})
	if err == nil {
		var rowCount int64
		for _, qr := range qrs {
			rowCount += int64(len(qr.Rows))
		}
		vtg.rowsReturned.Add(statsKey, rowCount)
		return qrs, nil
	}

	query := map[string]interface{}{
		"Queries":       queries,
		"TabletType":    ltt,
		"AsTransaction": asTransaction,
		"Session":       session,
		"Options":       options,
	}
	err = handleExecuteError(err, statsKey, query, vtg.logExecuteBatchShards)
	return nil, err
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"ExecuteBatchKeyspaceIds", "", ltt}
	defer vtg.timings.Record(statsKey, startTime)

	annotateBoundKeyspaceIDQueries(queries)

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		ctx,
		queries,
		tabletType,
		asTransaction,
		session,
		options)
	if err == nil {
		var rowCount int64
		for _, qr := range qrs {
			rowCount += int64(len(qr.Rows))
		}
		vtg.rowsReturned.Add(statsKey, rowCount)
		return qrs, nil
	}

	query := map[string]interface{}{
		"Queries":       queries,
		"TabletType":    ltt,
		"AsTransaction": asTransaction,
		"Session":       session,
		"Options":       options,
	}
	err = handleExecuteError(err, statsKey, query, vtg.logExecuteBatchKeyspaceIds)
	return nil, err
}

// StreamExecute executes a streaming query by routing based on the values in the query.
func (vtg *VTGate) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecute", "Any", ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.router.StreamExecute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"TabletType":    ltt,
			"Options":       options,
		}
		return handleExecuteError(err, statsKey, query, vtg.logStreamExecute)
	}
	return nil
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof.
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecuteKeyspaceIds", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecuteKeyspaceIds(
		ctx,
		sql,
		bindVariables,
		keyspace,
		keyspaceIds,
		tabletType,
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"KeyspaceIds":   keyspaceIds,
			"TabletType":    ltt,
			"Options":       options,
		}
		return handleExecuteError(err, statsKey, query, vtg.logStreamExecuteKeyspaceIds)
	}
	return nil
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecuteKeyRanges", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecuteKeyRanges(
		ctx,
		sql,
		bindVariables,
		keyspace,
		keyRanges,
		tabletType,
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"KeyRanges":     keyRanges,
			"TabletType":    ltt,
			"Options":       options,
		}
		return handleExecuteError(err, statsKey, query, vtg.logStreamExecuteKeyRanges)
	}
	return nil
}

// StreamExecuteShards executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(tabletType)
	statsKey := []string{"StreamExecuteShards", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.streamExecute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		func(keyspace string) (string, []string, error) {
			return keyspace, shards, nil
		},
		options,
		func(reply *sqltypes.Result) error {
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"Shards":        shards,
			"TabletType":    ltt,
			"Options":       options,
		}
		return handleExecuteError(err, statsKey, query, vtg.logStreamExecuteShards)
	}
	return nil
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	if !singledb && vtg.transactionMode == TxSingle {
		return nil, vterrors.FromError(vtrpcpb.ErrorCode_BAD_INPUT, errors.New("multi-db transaction disallowed"))
	}
	return &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      singledb,
	}, nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	if twopc && vtg.transactionMode != TxTwoPC {
		// Rollback the transaction to prevent future deadlocks.
		vtg.txConn.Rollback(ctx, NewSafeSession(session))
		return vterrors.FromError(vtrpcpb.ErrorCode_BAD_INPUT, errors.New("2pc transaction disallowed"))
	}
	return formatError(vtg.txConn.Commit(ctx, twopc, NewSafeSession(session)))
}

// Rollback rolls back a transaction.
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
	if vtg.router.IsKeyspaceRangeBasedSharded(keyspace) {
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
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	// TODO(erez): Add validation of SplitQuery parameters.
	keyspace, srvKeyspace, shardRefs, err := getKeyspaceShards(
		ctx, vtg.resolver.toposerv, vtg.resolver.cell, keyspace, topodatapb.TabletType_RDONLY)
	if err != nil {
		return nil, err
	}

	// If the caller specified a splitCount (vs. specifying 'numRowsPerQueryPart') scale it by the
	// number of shards (otherwise it stays 0).
	perShardSplitCount := int64(math.Ceil(float64(splitCount) / float64(len(shardRefs))))

	// Determine whether to return SplitQueryResponse_KeyRangeParts or SplitQueryResponse_ShardParts.
	// We return 'KeyRangeParts' for sharded keyspaces that are not custom sharded. If the
	// keyspace is custom sharded or unsharded we return 'ShardParts'.
	var querySplitToQueryPartFunc func(
		querySplit *querytypes.QuerySplit, shard string) (*vtgatepb.SplitQueryResponse_Part, error)
	if vtg.isKeyspaceRangeBasedSharded(keyspace, srvKeyspace) {
		// Index the shard references in 'shardRefs' by shard name.
		shardRefByName := make(map[string]*topodatapb.ShardReference, len(shardRefs))
		for _, shardRef := range shardRefs {
			shardRefByName[shardRef.Name] = shardRef
		}
		querySplitToQueryPartFunc = getQuerySplitToKeyRangePartFunc(keyspace, shardRefByName)
	} else {
		// Keyspace is either unsharded or custom-sharded.
		querySplitToQueryPartFunc = getQuerySplitToShardPartFunc(keyspace)
	}

	// Collect all shard names into a slice.
	shardNames := make([]string, 0, len(shardRefs))
	for _, shardRef := range shardRefs {
		shardNames = append(shardNames, shardRef.Name)
	}
	return vtg.resolver.scatterConn.SplitQuery(
		ctx,
		sql,
		bindVariables,
		splitColumns,
		perShardSplitCount,
		numRowsPerQueryPart,
		algorithm,
		shardNames,
		querySplitToQueryPartFunc,
		keyspace)
}

// getQuerySplitToKeyRangePartFunc returns a function to use with scatterConn.SplitQuery
// that converts the given QuerySplit to a SplitQueryResponse_Part message whose KeyRangePart field
// is set.
func getQuerySplitToKeyRangePartFunc(
	keyspace string,
	shardReferenceByName map[string]*topodatapb.ShardReference) func(
	querySplit *querytypes.QuerySplit, shard string) (*vtgatepb.SplitQueryResponse_Part, error) {

	return func(
		querySplit *querytypes.QuerySplit, shard string) (*vtgatepb.SplitQueryResponse_Part, error) {
		// TODO(erez): Assert that shardReferenceByName contains an entry for 'shard'.
		// Keyrange can be nil for the shard (e.g. for single-sharded keyspaces during resharding).
		// In this case we append an empty keyrange that represents the entire keyspace.
		keyranges := []*topodatapb.KeyRange{{Start: []byte{}, End: []byte{}}}
		if shardReferenceByName[shard].KeyRange != nil {
			keyranges = []*topodatapb.KeyRange{shardReferenceByName[shard].KeyRange}
		}
		bindVars, err := querytypes.BindVariablesToProto3(querySplit.BindVariables)
		if err != nil {
			return nil, err
		}
		return &vtgatepb.SplitQueryResponse_Part{
			Query: &querypb.BoundQuery{
				Sql:           querySplit.Sql,
				BindVariables: bindVars,
			},
			KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{
				Keyspace:  keyspace,
				KeyRanges: keyranges,
			},
			Size: querySplit.RowCount,
		}, nil
	}
}

// getQuerySplitToShardPartFunc returns a function to use with scatterConn.SplitQuery
// that converts the given QuerySplit to a SplitQueryResponse_Part message whose ShardPart field
// is set.
func getQuerySplitToShardPartFunc(keyspace string) func(
	querySplit *querytypes.QuerySplit, shard string) (*vtgatepb.SplitQueryResponse_Part, error) {

	return func(
		querySplit *querytypes.QuerySplit, shard string) (*vtgatepb.SplitQueryResponse_Part, error) {
		bindVars, err := querytypes.BindVariablesToProto3(querySplit.BindVariables)
		if err != nil {
			return nil, err
		}
		return &vtgatepb.SplitQueryResponse_Part{
			Query: &querypb.BoundQuery{
				Sql:           querySplit.Sql,
				BindVariables: bindVars,
			},
			ShardPart: &vtgatepb.SplitQueryResponse_ShardPart{
				Keyspace: keyspace,
				Shards:   []string{shard},
			},
			Size: querySplit.RowCount,
		}, nil
	}
}

// GetSrvKeyspace is part of the vtgate service API.
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return vtg.resolver.toposerv.GetSrvKeyspace(ctx, vtg.resolver.cell, keyspace)
}

// MessageStream is part of the vtgate service API.
func (vtg *VTGate) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, sendReply func(*querypb.MessageStreamResponse) error) error {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(topodatapb.TabletType_MASTER)
	statsKey := []string{"MessageStream", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.MessageStream(
		ctx,
		keyspace,
		shard,
		keyRange,
		name,
		sendReply,
	)
	if err != nil {
		normalErrors.Add(statsKey, 1)
		query := map[string]interface{}{
			"Keyspace":    keyspace,
			"Shard":       shard,
			"KeyRange":    keyRange,
			"TabletType":  ltt,
			"MessageName": name,
		}
		logError(err, query, vtg.logUpdateStream)
	}
	return formatError(err)
}

// MessageAck is part of the vtgate service API.
func (vtg *VTGate) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	startTime := time.Now()
	ltt := topoproto.TabletTypeLString(topodatapb.TabletType_MASTER)
	statsKey := []string{"MessageAck", keyspace, ltt}
	defer vtg.timings.Record(statsKey, startTime)
	count, err := vtg.router.MessageAck(ctx, keyspace, name, ids)
	return count, formatError(err)
}

// UpdateStream is part of the vtgate service API.
func (vtg *VTGate) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, sendReply func(*querypb.StreamEvent, int64) error) error {
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
		sendReply,
	)
	if err != nil {
		normalErrors.Add(statsKey, 1)
		query := map[string]interface{}{
			"Keyspace":   keyspace,
			"Shard":      shard,
			"KeyRange":   keyRange,
			"TabletType": ltt,
			"Timestamp":  timestamp,
		}
		logError(err, query, vtg.logUpdateStream)
	}
	return formatError(err)
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (vtg *VTGate) GetGatewayCacheStatus() gateway.TabletCacheStatusList {
	return vtg.resolver.GetGatewayCacheStatus()
}

// VSchemaStats returns the loaded vschema stats.
func (vtg *VTGate) VSchemaStats() *VSchemaStats {
	return vtg.router.planner.VSchemaStats()
}

// Any errors that are caused by VTGate dependencies (e.g, VtTablet) should be logged
// as errors in those components, but logged to Info in VTGate itself.
func logError(err error, query map[string]interface{}, logger *logutil.ThrottledLogger) {
	if err == context.DeadlineExceeded {
		// Count these but don't log them because they are very common and not
		// likely to indicate a bug in VTGate
		infoErrors.Add("TimeoutErrors", 1)
		return
	}
	logMethod := logger.Errorf
	if !isErrorCausedByVTGate(err) {
		infoErrors.Add("NonVtgateErrors", 1)
		// Log non-vtgate errors (e.g. a query failed on vttablet because a failover
		// is in progress) on INFO only because vttablet already logs them as ERROR.
		logMethod = logger.Infof
	}
	logMethod("%v, query: %+v", err, query)
}

// Returns true if a given error is caused entirely due to VTGate, and not any
// of the components that it depends on.
// If the error is an aggregation of multiple errors e.g. in case of a scatter
// query, the function returns true if *any* error is caused by vtgate.
// Consequently, the function returns false if *all* errors are caused by
// vttablet (actual errors) or the client (e.g. context canceled).
func isErrorCausedByVTGate(err error) bool {
	var errQueue []error
	errQueue = append(errQueue, err)
	for len(errQueue) > 0 {
		// pop the first item from the queue
		e := errQueue[0]
		errQueue = errQueue[1:]

		switch e := e.(type) {
		case *ScatterConnError:
			errQueue = append(errQueue, e.Errs...)
		case *gateway.ShardError:
			errQueue = append(errQueue, e.Err)
		case tabletconn.OperationalError:
			// Communication with vttablet failed i.e. the error is caused by vtgate.
			// (For actual vttablet errors, see the next case "ServerError".)
			return true
		case *tabletconn.ServerError:
			// The query failed on vttablet and it returned this error.
			// Ignore it and check the next error in the queue.
		default:
			if e == context.Canceled {
				// Caused by the client and not vtgate.
				// Ignore it and check the next error in the queue.
				continue
			}

			// Return true if even a single error within
			// the error queue was caused by VTGate. If
			// we're not certain what caused the error, we
			// default to assuming that VTGate was at fault.
			return true
		}
	}
	return false
}

func handleExecuteError(err error, statsKey []string, query map[string]interface{}, logger *logutil.ThrottledLogger) error {
	// First we log in the right category.
	ec := vterrors.RecoverVtErrorCode(err)
	switch ec {
	case vtrpcpb.ErrorCode_INTEGRITY_ERROR:
		// Duplicate key error, no need to log.
		infoErrors.Add("DupKey", 1)
	case vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, vtrpcpb.ErrorCode_BAD_INPUT:
		// Tx pool full error, or bad input, no need to log.
		normalErrors.Add(statsKey, 1)
	case vtrpcpb.ErrorCode_PERMISSION_DENIED:
		// User violated permissions (TableACL), no need to log.
		infoErrors.Add("PermissionDenied", 1)
	case vtrpcpb.ErrorCode_TRANSIENT_ERROR:
		// Temporary error which should be retried by user. Do not log.
		// As of 01/2017, only the vttablet transaction throttler and the vtgate
		// master buffer (if buffer full) return this error.
		infoErrors.Add("TransientError", 1)
	default:
		// Regular error, we will log if caused by vtgate.
		normalErrors.Add(statsKey, 1)
		logError(err, query, logger)
	}

	// Then we suffix the error with our address.
	s := fmt.Sprintf(", vtgate: %v", servenv.ListeningURL.String())
	return vterrors.WithSuffix(err, s)
}

func formatError(err error) error {
	if err == nil {
		return nil
	}
	s := fmt.Sprintf(", vtgate: %v", servenv.ListeningURL.String())
	return vterrors.WithSuffix(err, s)
}

// HandlePanic recovers from panics, and logs / increment counters
func (vtg *VTGate) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v, vtgate: %v", x, servenv.ListeningURL.String())
		internalErrors.Add("Panic", 1)
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
