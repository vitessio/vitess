// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"

	// import vindexes implementations
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const errDupKey = "errno 1062"
const errOutOfRange = "errno 1264"
const errTxPoolFull = "tx_pool_full"

var (
	rpcVTGate *VTGate

	qpsByOperation *stats.Rates
	qpsByKeyspace  *stats.Rates
	qpsByDbType    *stats.Rates

	errorsByOperation *stats.Rates
	errorsByKeyspace  *stats.Rates
	errorsByDbType    *stats.Rates

	errTooManyInFlight = vterrors.FromError(
		vtrpcpb.ErrorCode_TRANSIENT_ERROR,
		errors.New("request_backlog: too many requests in flight"),
	)

	// Error counters should be global so they can be set from anywhere
	normalErrors   *stats.MultiCounters
	infoErrors     *stats.Counters
	internalErrors *stats.Counters
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created. It implements vtgateservice.VTGateService
type VTGate struct {
	resolver     *Resolver
	router       *Router
	timings      *stats.MultiTimings
	rowsReturned *stats.MultiCounters

	maxInFlight int64
	inFlight    sync2.AtomicInt64

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
}

// RegisterVTGate defines the type of registration mechanism.
type RegisterVTGate func(vtgateservice.VTGateService)

// RegisterVTGates stores register funcs for VTGate server.
var RegisterVTGates []RegisterVTGate

// Init initializes VTGate server.
func Init(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, vschema *planbuilder.VSchema, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, tabletTypesToWait []topodatapb.TabletType, maxInFlight int, testGateway string) *VTGate {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	rpcVTGate = &VTGate{
		resolver:     NewResolver(hc, topoServer, serv, "VttabletCall", cell, retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, tabletTypesToWait, testGateway),
		timings:      stats.NewMultiTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),
		rowsReturned: stats.NewMultiCounters("VtgateApiRowsReturned", []string{"Operation", "Keyspace", "DbType"}),

		maxInFlight: int64(maxInFlight),
		inFlight:    sync2.NewAtomicInt64(0),

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
	}
	// Resuse resolver's scatterConn.
	rpcVTGate.router = NewRouter(serv, cell, vschema, "VTGateRouter", rpcVTGate.resolver.scatterConn)
	normalErrors = stats.NewMultiCounters("VtgateApiErrorCounts", []string{"Operation", "Keyspace", "DbType"})
	infoErrors = stats.NewCounters("VtgateInfoErrorCounts")
	internalErrors = stats.NewCounters("VtgateInternalErrorCounts")

	qpsByOperation = stats.NewRates("QPSByOperation", stats.CounterForDimension(rpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	qpsByKeyspace = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(rpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	qpsByDbType = stats.NewRates("QPSByDbType", stats.CounterForDimension(rpcVTGate.timings, "DbType"), 15, 1*time.Minute)

	errorsByOperation = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(normalErrors, "Operation"), 15, 1*time.Minute)
	errorsByKeyspace = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(normalErrors, "Keyspace"), 15, 1*time.Minute)
	errorsByDbType = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(normalErrors, "DbType"), 15, 1*time.Minute)

	for _, f := range RegisterVTGates {
		f(rpcVTGate)
	}
	return rpcVTGate
}

// InitializeConnections pre-initializes VTGate by connecting to vttablets of all keyspace/shard/type.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving.
func (vtg *VTGate) InitializeConnections(ctx context.Context) (err error) {
	defer vtg.HandlePanic(&err)

	log.Infof("Initialize VTTablet connections")
	err = vtg.resolver.InitializeConnections(ctx)
	if err != nil {
		log.Errorf("failed to initialize connections: %v", err)
		return err
	}
	log.Infof("Initialize VTTablet connections completed")
	return nil
}

// Execute executes a non-streaming query by routing based on the values in the query.
func (vtg *VTGate) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"Execute", "Any", strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	qr, err := vtg.router.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"TabletType":       strings.ToLower(tabletType.String()),
		"Session":          session,
		"NotInTransaction": notInTransaction,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecute)
	return nil, err
}

// ExecuteShards executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteShards", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	sql = sqlannotation.AddFilteredReplicationUnfriendlyIfDML(sql)

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
		"TabletType":       strings.ToLower(tabletType.String()),
		"Session":          session,
		"NotInTransaction": notInTransaction,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecuteShards)
	return nil, err
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteKeyspaceIds", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	sql = sqlannotation.AddIfDML(sql, keyspaceIds)

	qr, err := vtg.resolver.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"Keyspace":         keyspace,
		"KeyspaceIds":      keyspaceIds,
		"TabletType":       strings.ToLower(tabletType.String()),
		"Session":          session,
		"NotInTransaction": notInTransaction,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecuteKeyspaceIds)
	return nil, err
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteKeyRanges", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	sql = sqlannotation.AddFilteredReplicationUnfriendlyIfDML(sql)

	qr, err := vtg.resolver.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction)
	if err == nil {
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
		return qr, nil
	}

	query := map[string]interface{}{
		"Sql":              sql,
		"BindVariables":    bindVariables,
		"Keyspace":         keyspace,
		"KeyRanges":        keyRanges,
		"TabletType":       strings.ToLower(tabletType.String()),
		"Session":          session,
		"NotInTransaction": notInTransaction,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecuteKeyRanges)
	return nil, err
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteEntityIds", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	sql = sqlannotation.AddFilteredReplicationUnfriendlyIfDML(sql)

	qr, err := vtg.resolver.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction)
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
		"TabletType":        strings.ToLower(tabletType.String()),
		"Session":           session,
		"NotInTransaction":  notInTransaction,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecuteEntityIds)
	return nil, err
}

// ExecuteBatchShards executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchShards", "", ""}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	annotateBoundShardQueriesAsUnfriendly(queries)

	qrs, err := vtg.resolver.ExecuteBatch(
		ctx,
		tabletType,
		asTransaction,
		session,
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
		"TabletType":    strings.ToLower(tabletType.String()),
		"AsTransaction": asTransaction,
		"Session":       session,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecuteBatchShards)
	return nil, err
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchKeyspaceIds", "", ""}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return nil, errTooManyInFlight
	}

	annotateBoundKeyspaceIDQueries(queries)

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		ctx,
		queries,
		tabletType,
		asTransaction,
		session)
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
		"TabletType":    strings.ToLower(tabletType.String()),
		"AsTransaction": asTransaction,
		"Session":       session,
	}
	handleExecuteError(err, statsKey, query, vtg.logExecuteBatchKeyspaceIds)
	return nil, err
}

// StreamExecute executes a streaming query by routing based on the values in the query.
func (vtg *VTGate) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecute", "Any", strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.router.StreamExecute(
		ctx,
		sql,
		bindVariables,
		tabletType,
		func(reply *sqltypes.Result) error {
			rowCount += int64(len(reply.Rows))
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		normalErrors.Add(statsKey, 1)
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"TabletType":    strings.ToLower(tabletType.String()),
		}
		logError(err, query, vtg.logStreamExecute)
	}
	return formatError(err)
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof.
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyspaceIds", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.resolver.StreamExecuteKeyspaceIds(
		ctx,
		sql,
		bindVariables,
		keyspace,
		keyspaceIds,
		tabletType,
		func(reply *sqltypes.Result) error {
			rowCount += int64(len(reply.Rows))
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		normalErrors.Add(statsKey, 1)
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"KeyspaceIds":   keyspaceIds,
			"TabletType":    strings.ToLower(tabletType.String()),
		}
		logError(err, query, vtg.logStreamExecuteKeyspaceIds)
	}
	return formatError(err)
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyRanges", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.resolver.StreamExecuteKeyRanges(
		ctx,
		sql,
		bindVariables,
		keyspace,
		keyRanges,
		tabletType,
		func(reply *sqltypes.Result) error {
			rowCount += int64(len(reply.Rows))
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		normalErrors.Add(statsKey, 1)
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"KeyRanges":     keyRanges,
			"TabletType":    strings.ToLower(tabletType.String()),
		}
		logError(err, query, vtg.logStreamExecuteKeyRanges)
	}
	return formatError(err)
}

// StreamExecuteShards executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteShards", keyspace, strings.ToLower(tabletType.String())}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.resolver.StreamExecute(
		ctx,
		sql,
		bindVariables,
		keyspace,
		tabletType,
		func(keyspace string) (string, []string, error) {
			return keyspace, shards, nil
		},
		func(reply *sqltypes.Result) error {
			rowCount += int64(len(reply.Rows))
			vtg.rowsReturned.Add(statsKey, int64(len(reply.Rows)))
			return sendReply(reply)
		})

	if err != nil {
		normalErrors.Add(statsKey, 1)
		query := map[string]interface{}{
			"Sql":           sql,
			"BindVariables": bindVariables,
			"Keyspace":      keyspace,
			"Shards":        shards,
			"TabletType":    strings.ToLower(tabletType.String()),
		}
		logError(err, query, vtg.logStreamExecuteShards)
	}
	return formatError(err)
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(ctx context.Context) (*vtgatepb.Session, error) {
	return &vtgatepb.Session{
		InTransaction: true,
	}, nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(ctx context.Context, session *vtgatepb.Session) error {
	return formatError(vtg.resolver.Commit(ctx, session))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	return formatError(vtg.resolver.Rollback(ctx, session))
}

// SplitQuery splits a query into sub queries by appending keyranges and
// primary key range clauses. Rows corresponding to the sub queries
// are guaranteed to be non-overlapping and will add up to the rows of
// original query. Number of sub queries will be a multiple of N that is
// greater than or equal to SplitQueryRequest.SplitCount, where N is the
// number of shards.
func (vtg *VTGate) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	keyspace, srvKeyspace, shards, err := getKeyspaceShards(ctx, vtg.resolver.toposerv, vtg.resolver.cell, keyspace, topodatapb.TabletType_RDONLY)
	if err != nil {
		return nil, err
	}
	perShardSplitCount := int64(math.Ceil(float64(splitCount) / float64(len(shards))))
	// sharding_column_type != KeyspaceIdType_UNSET can happen in one of the following two cases:
	// 1. We are querying a sharded keyspace;
	// 2. We are querying an unsharded keyspace which is being sharded.
	if srvKeyspace.ShardingColumnType != topodatapb.KeyspaceIdType_UNSET {
		// we are using range-based sharding, so the result
		// will be a list of Splits with KeyRange clauses
		keyRangeByShard := make(map[string]*topodatapb.KeyRange)
		for _, shard := range shards {
			keyRangeByShard[shard.Name] = shard.KeyRange
		}
		return vtg.resolver.scatterConn.SplitQueryKeyRange(ctx, sql, bindVariables, splitColumn, perShardSplitCount, keyRangeByShard, keyspace)
	}

	// sharding_column_type == KeyspaceIdType_UNSET can happen in one of the following two cases:
	// 1. We are querying a sharded keyspace which uses custom sharding;
	// 2. We are querying a unsharded keyspace which is NOT being sharded.
	shardNames := make([]string, len(shards))
	for i, shard := range shards {
		shardNames[i] = shard.Name
	}
	return vtg.resolver.scatterConn.SplitQueryCustomSharding(ctx, sql, bindVariables, splitColumn, perShardSplitCount, shardNames, keyspace)
}

// GetSrvKeyspace is part of the vtgate service API.
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return vtg.resolver.toposerv.GetSrvKeyspace(ctx, vtg.resolver.cell, keyspace)
}

// GetSrvShard is part of the vtgate service API.
func (vtg *VTGate) GetSrvShard(ctx context.Context, keyspace, shard string) (*topodatapb.SrvShard, error) {
	return vtg.resolver.toposerv.GetSrvShard(ctx, vtg.resolver.cell, keyspace, shard)
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (vtg *VTGate) GetGatewayCacheStatus() GatewayEndPointCacheStatusList {
	return vtg.resolver.GetGatewayCacheStatus()
}

// Any errors that are caused by VTGate dependencies (e.g, VtTablet) should be logged
// as errors in those components, but logged to Info in VTGate itself.
func logError(err error, query map[string]interface{}, logger *logutil.ThrottledLogger) {
	logMethod := logger.Errorf
	if isErrorCausedByVTGate(err) {
		logMethod = logger.Errorf
	} else {
		infoErrors.Add("NonVtgateErrors", 1)
		logMethod = logger.Infof
	}
	logMethod("%v, query: %+v", err, query)
}

// Returns true if a given error is caused entirely due to VTGate, and not any of
// the components that it depends on.
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
		case *ShardConnError:
			errQueue = append(errQueue, e.Err)
		case tabletconn.OperationalError:
			// tabletconn.Cancelled errors are due to client behavior, not VTGate errors.
			if e != tabletconn.Cancelled {
				return true
			}
		case *tabletconn.ServerError:
			break
		default:
			// Return true if even a single error within the error queue was
			// caused by VTGate. If we're not certain what caused the error, we
			// default to assuming that VTGate was at fault
			return true
		}
	}
	return false
}

func handleExecuteError(err error, statsKey []string, query map[string]interface{}, logger *logutil.ThrottledLogger) {
	s := fmt.Sprintf(", vtgate: %v", servenv.ListeningURL.String())
	newErr := vterrors.WithSuffix(err, s)
	errStr := newErr.Error()
	if strings.Contains(errStr, errDupKey) {
		infoErrors.Add("DupKey", 1)
	} else if strings.Contains(errStr, errOutOfRange) {
		infoErrors.Add("OutOfRange", 1)
	} else if strings.Contains(errStr, errTxPoolFull) {
		normalErrors.Add(statsKey, 1)
	} else {
		normalErrors.Add(statsKey, 1)
		logError(err, query, logger)
	}
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
	for i := range queries {
		if len(queries[i].KeyspaceIds) == 1 {
			queries[i].Query.Sql = sqlannotation.AddKeyspaceIDIfDML(queries[i].Query.Sql, []byte(queries[i].KeyspaceIds[0]))
		} else {
			queries[i].Query.Sql = sqlannotation.AddFilteredReplicationUnfriendlyIfDML(queries[i].Query.Sql)
		}
	}
}

// Helper function used in ExecuteBatchShards
func annotateBoundShardQueriesAsUnfriendly(queries []*vtgatepb.BoundShardQuery) {
	for i := range queries {
		queries[i].Query.Sql =
			sqlannotation.AddFilteredReplicationUnfriendlyIfDML(queries[i].Query.Sql)
	}
}
