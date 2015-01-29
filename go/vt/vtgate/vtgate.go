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
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	kproto "github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"golang.org/x/net/context"
)

const errDupKey = "errno 1062"
const errTxPoolFull = "tx_pool_full"

var (
	rpcVTGate *VTGate

	qpsByOperation *stats.Rates
	qpsByKeyspace  *stats.Rates
	qpsByDbType    *stats.Rates

	errorsByOperation *stats.Rates
	errorsByKeyspace  *stats.Rates
	errorsByDbType    *stats.Rates

	errTooManyInFlight = errors.New("request_backlog: too many requests in flight")

	// Error counters should be global so they can be set from anywhere
	normalErrors   *stats.MultiCounters
	infoErrors     *stats.Counters
	internalErrors *stats.Counters
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	resolver     *Resolver
	router       *Router
	timings      *stats.MultiTimings
	rowsReturned *stats.MultiCounters

	maxInFlight int64
	inFlight    sync2.AtomicInt64

	// the throttled loggers for all errors, one per API entry
	logExecute                  *logutil.ThrottledLogger
	logExecuteShard             *logutil.ThrottledLogger
	logExecuteKeyspaceIds       *logutil.ThrottledLogger
	logExecuteKeyRanges         *logutil.ThrottledLogger
	logExecuteEntityIds         *logutil.ThrottledLogger
	logExecuteBatchShard        *logutil.ThrottledLogger
	logExecuteBatchKeyspaceIds  *logutil.ThrottledLogger
	logStreamExecute            *logutil.ThrottledLogger
	logStreamExecuteKeyspaceIds *logutil.ThrottledLogger
	logStreamExecuteKeyRanges   *logutil.ThrottledLogger
	logStreamExecuteShard       *logutil.ThrottledLogger
}

// RegisterVTGate defines the type of registration mechanism.
type RegisterVTGate func(*VTGate)

// RegisterVTGates stores register funcs for VTGate server.
var RegisterVTGates []RegisterVTGate

// Init initializes VTGate server.
func Init(serv SrvTopoServer, schema *planbuilder.Schema, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration, maxInFlight int) {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	rpcVTGate = &VTGate{
		resolver:     NewResolver(serv, "VttabletCall", cell, retryDelay, retryCount, timeout),
		timings:      stats.NewMultiTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),
		rowsReturned: stats.NewMultiCounters("VtgateApiRowsReturned", []string{"Operation", "Keyspace", "DbType"}),

		maxInFlight: int64(maxInFlight),
		inFlight:    0,

		logExecute:                  logutil.NewThrottledLogger("Execute", 5*time.Second),
		logExecuteShard:             logutil.NewThrottledLogger("ExecuteShard", 5*time.Second),
		logExecuteKeyspaceIds:       logutil.NewThrottledLogger("ExecuteKeyspaceIds", 5*time.Second),
		logExecuteKeyRanges:         logutil.NewThrottledLogger("ExecuteKeyRanges", 5*time.Second),
		logExecuteEntityIds:         logutil.NewThrottledLogger("ExecuteEntityIds", 5*time.Second),
		logExecuteBatchShard:        logutil.NewThrottledLogger("ExecuteBatchShard", 5*time.Second),
		logExecuteBatchKeyspaceIds:  logutil.NewThrottledLogger("ExecuteBatchKeyspaceIds", 5*time.Second),
		logStreamExecute:            logutil.NewThrottledLogger("StreamExecute", 5*time.Second),
		logStreamExecuteKeyspaceIds: logutil.NewThrottledLogger("StreamExecuteKeyspaceIds", 5*time.Second),
		logStreamExecuteKeyRanges:   logutil.NewThrottledLogger("StreamExecuteKeyRanges", 5*time.Second),
		logStreamExecuteShard:       logutil.NewThrottledLogger("StreamExecuteShard", 5*time.Second),
	}
	// Resuse resolver's scatterConn.
	rpcVTGate.router = NewRouter(serv, cell, schema, "VTGateRouter", rpcVTGate.resolver.scatterConn)
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
}

// InitializeConnections pre-initializes VTGate by connecting to vttablets of all keyspace/shard/type.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving.
func (vtg *VTGate) InitializeConnections(ctx context.Context) (err error) {
	defer handlePanic(&err)

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
func (vtg *VTGate) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"Execute", "Any", string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qr, err := vtg.router.Execute(ctx, query)
	if err == nil {
		reply.Result = qr
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
	} else {
		reply.Error = handleExecuteError(err, statsKey, query, vtg.logExecute)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qr, err := vtg.resolver.Execute(
		ctx,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.TabletType,
		query.Session,
		func(keyspace string) (string, []string, error) {
			return query.Keyspace, query.Shards, nil
		},
	)
	if err == nil {
		reply.Result = qr
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
	} else {
		reply.Error = handleExecuteError(err, statsKey, query, vtg.logExecuteShard)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qr, err := vtg.resolver.ExecuteKeyspaceIds(ctx, query)
	if err == nil {
		reply.Result = qr
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
	} else {
		reply.Error = handleExecuteError(err, statsKey, query, vtg.logExecuteKeyspaceIds)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qr, err := vtg.resolver.ExecuteKeyRanges(ctx, query)
	if err == nil {
		reply.Result = qr
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
	} else {
		reply.Error = handleExecuteError(err, statsKey, query, vtg.logExecuteKeyRanges)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteEntityIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qr, err := vtg.resolver.ExecuteEntityIds(ctx, query)
	if err == nil {
		reply.Result = qr
		vtg.rowsReturned.Add(statsKey, int64(len(qr.Rows)))
	} else {
		reply.Error = handleExecuteError(err, statsKey, query, vtg.logExecuteEntityIds)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteBatchShard", batchQuery.Keyspace, string(batchQuery.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qrs, err := vtg.resolver.ExecuteBatch(
		ctx,
		batchQuery.Queries,
		batchQuery.Keyspace,
		batchQuery.TabletType,
		batchQuery.Session,
		func(keyspace string) (string, []string, error) {
			return batchQuery.Keyspace, batchQuery.Shards, nil
		},
	)
	if err == nil {
		reply.List = qrs.List
		var rowCount int64
		for _, qr := range qrs.List {
			rowCount += int64(len(qr.Rows))
		}
		vtg.rowsReturned.Add(statsKey, rowCount)
	} else {
		reply.Error = handleExecuteError(err, statsKey, batchQuery, vtg.logExecuteBatchShard)
	}
	reply.Session = batchQuery.Session
	return nil
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteBatchKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		ctx,
		query)
	if err == nil {
		reply.List = qrs.List
		var rowCount int64
		for _, qr := range qrs.List {
			rowCount += int64(len(qr.Rows))
		}
		vtg.rowsReturned.Add(statsKey, rowCount)
	} else {
		reply.Error = handleExecuteError(err, statsKey, query, vtg.logExecuteBatchKeyspaceIds)
	}
	reply.Session = query.Session
	return nil
}

// StreamExecute executes a streaming query by routing based on the values in the query.
func (vtg *VTGate) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecute", "Any", string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err = vtg.router.StreamExecute(
		ctx,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			reply.Result = mreply
			rowCount += int64(len(mreply.Rows))
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	vtg.rowsReturned.Add(statsKey, rowCount)

	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecute.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return formatError(err)
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof.
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err = vtg.resolver.StreamExecuteKeyspaceIds(
		ctx,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			reply.Result = mreply
			rowCount += int64(len(mreply.Rows))
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	vtg.rowsReturned.Add(statsKey, rowCount)

	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyspaceIds.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return formatError(err)
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err = vtg.resolver.StreamExecuteKeyRanges(
		ctx,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			reply.Result = mreply
			rowCount += int64(len(mreply.Rows))
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	vtg.rowsReturned.Add(statsKey, rowCount)

	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return formatError(err)
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err = vtg.resolver.StreamExecute(
		ctx,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.TabletType,
		query.Session,
		func(keyspace string) (string, []string, error) {
			return query.Keyspace, query.Shards, nil
		},
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			reply.Result = mreply
			rowCount += int64(len(mreply.Rows))
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	vtg.rowsReturned.Add(statsKey, rowCount)

	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecuteShard.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return formatError(err)
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(ctx context.Context, outSession *proto.Session) (err error) {
	defer handlePanic(&err)
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(ctx context.Context, inSession *proto.Session) (err error) {
	defer handlePanic(&err)
	return formatError(vtg.resolver.Commit(ctx, inSession))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(ctx context.Context, inSession *proto.Session) (err error) {
	defer handlePanic(&err)
	return formatError(vtg.resolver.Rollback(ctx, inSession))
}

// SplitQuery splits a query into sub queries by appending keyranges and
// primary key range clauses. Rows corresponding to the sub queries
// are guaranteed to be non-overlapping and will add up to the rows of
// original query. Number of sub queries will be a multiple of N that is
// greater than or equal to SplitQueryRequest.SplitCount, where N is the
// number of shards.
func (vtg *VTGate) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer handlePanic(&err)
	sc := vtg.resolver.scatterConn
	keyspace, shards, err := getKeyspaceShards(ctx, sc.toposerv, sc.cell, req.Keyspace, topo.TYPE_RDONLY)
	if err != nil {
		return err
	}
	keyRangeByShard := map[string]kproto.KeyRange{}
	for _, shard := range shards {
		keyRangeByShard[shard.ShardName()] = shard.KeyRange
	}
	perShardSplitCount := int(math.Ceil(float64(req.SplitCount) / float64(len(shards))))
	splits, err := vtg.resolver.scatterConn.SplitQuery(ctx, req.Query, perShardSplitCount, keyRangeByShard, keyspace)
	if err != nil {
		return err
	}
	reply.Splits = splits
	return nil
}

func handleExecuteError(err error, statsKey []string, query interface{}, logger *logutil.ThrottledLogger) string {
	errStr := err.Error() + ", vtgate: " + servenv.ListeningURL.String()
	if strings.Contains(errStr, errDupKey) {
		infoErrors.Add("DupKey", 1)
	} else if strings.Contains(errStr, errTxPoolFull) {
		normalErrors.Add(statsKey, 1)
	} else {
		normalErrors.Add(statsKey, 1)
		logger.Errorf("%v, query: %+v", err, query)
	}
	return errStr
}

func formatError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%v, vtgate: %v", err, servenv.ListeningURL.String())
}

func handlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v, vtgate: %v", x, servenv.ListeningURL.String())
		internalErrors.Add("Panic", 1)
	}
}
