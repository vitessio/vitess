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
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	kproto "github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	// import vindexes implementations
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
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
type RegisterVTGate func(vtgateservice.VTGateService)

// RegisterVTGates stores register funcs for VTGate server.
var RegisterVTGates []RegisterVTGate

var (
	// RPCErrorOnlyInReply informs vtgateservice(s) about how to return errors.
	RPCErrorOnlyInReply = flag.Bool("rpc-error-only-in-reply", false, "if true, supported RPC calls from vtgateservice(s) will only return errors as part of the RPC server response")
)

// Init initializes VTGate server.
func Init(serv SrvTopoServer, schema *planbuilder.Schema, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, maxInFlight int) {
	if rpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	rpcVTGate = &VTGate{
		resolver:     NewResolver(serv, "VttabletCall", cell, retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife),
		timings:      stats.NewMultiTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),
		rowsReturned: stats.NewMultiCounters("VtgateApiRowsReturned", []string{"Operation", "Keyspace", "DbType"}),

		maxInFlight: int64(maxInFlight),
		inFlight:    sync2.NewAtomicInt64(0),

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
func (vtg *VTGate) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) error {
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
func (vtg *VTGate) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
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
		query.NotInTransaction,
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
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
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
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
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
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
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
func (vtg *VTGate) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchShard", "", ""}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	qrs, err := vtg.resolver.ExecuteBatch(
		ctx,
		batchQuery.TabletType,
		batchQuery.AsTransaction,
		batchQuery.Session,
		func() (*scatterBatchRequest, error) {
			return boundShardQueriesToScatterBatchRequest(batchQuery.Queries), nil
		})
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
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchKeyspaceIds", "", ""}
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
func (vtg *VTGate) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecute", "Any", string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.router.StreamExecute(
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
		logError(err, query, vtg.logStreamExecute)
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
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.resolver.StreamExecuteKeyspaceIds(
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
		logError(err, query, vtg.logStreamExecuteKeyspaceIds)
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
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.resolver.StreamExecuteKeyRanges(
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
		logError(err, query, vtg.logStreamExecuteKeyRanges)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return formatError(err)
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return errTooManyInFlight
	}

	var rowCount int64
	err := vtg.resolver.StreamExecute(
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
		},
		query.NotInTransaction)
	vtg.rowsReturned.Add(statsKey, rowCount)

	if err != nil {
		normalErrors.Add(statsKey, 1)
		logError(err, query, vtg.logStreamExecuteShard)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return formatError(err)
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(ctx context.Context, outSession *proto.Session) error {
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(ctx context.Context, inSession *proto.Session) error {
	return formatError(vtg.resolver.Commit(ctx, inSession))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(ctx context.Context, inSession *proto.Session) error {
	return formatError(vtg.resolver.Rollback(ctx, inSession))
}

// SplitQuery splits a query into sub queries by appending keyranges and
// primary key range clauses. Rows corresponding to the sub queries
// are guaranteed to be non-overlapping and will add up to the rows of
// original query. Number of sub queries will be a multiple of N that is
// greater than or equal to SplitQueryRequest.SplitCount, where N is the
// number of shards.
func (vtg *VTGate) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	sc := vtg.resolver.scatterConn
	keyspace, srvKeyspace, shards, err := getKeyspaceShards(ctx, sc.toposerv, sc.cell, req.Keyspace, topo.TYPE_RDONLY)
	if err != nil {
		return err
	}
	perShardSplitCount := int(math.Ceil(float64(req.SplitCount) / float64(len(shards))))
	if srvKeyspace.ShardingColumnName != "" {
		// we are using range-based sharding, so the result
		// will be a list of Splits with KeyRange clauses
		keyRangeByShard := map[string]kproto.KeyRange{}
		for _, shard := range shards {
			keyRangeByShard[shard.Name] = shard.KeyRange
		}
		splits, err := vtg.resolver.scatterConn.SplitQueryKeyRange(ctx, req.Query, req.SplitColumn, perShardSplitCount, keyRangeByShard, keyspace)
		if err != nil {
			return err
		}
		reply.Splits = splits
		return nil
	}

	// we are using custom sharding, so the result
	// will be a list of Splits with Shard clauses
	shardNames := make([]string, len(shards))
	for i, shard := range shards {
		shardNames[i] = shard.Name
	}
	splits, err := vtg.resolver.scatterConn.SplitQueryCustomSharding(ctx, req.Query, req.SplitColumn, perShardSplitCount, shardNames, keyspace)
	if err != nil {
		return err
	}
	reply.Splits = splits
	return nil
}

// GetSrvKeyspace is part of the vtgate service API.
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	return vtg.resolver.scatterConn.toposerv.GetSrvKeyspace(ctx, vtg.resolver.scatterConn.cell, keyspace)
}

// Any errors that are caused by VTGate dependencies (e.g, VtTablet) should be logged
// as errors in those components, but logged to Info in VTGate itself.
func logError(err error, query interface{}, logger *logutil.ThrottledLogger) {
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

func handleExecuteError(err error, statsKey []string, query interface{}, logger *logutil.ThrottledLogger) string {
	errStr := err.Error() + ", vtgate: " + servenv.ListeningURL.String()
	if strings.Contains(errStr, errDupKey) {
		infoErrors.Add("DupKey", 1)
	} else if strings.Contains(errStr, errTxPoolFull) {
		normalErrors.Add(statsKey, 1)
	} else {
		normalErrors.Add(statsKey, 1)
		logError(err, query, logger)
	}
	return errStr
}

func formatError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%v, vtgate: %v", err, servenv.ListeningURL.String())
}

// HandlePanic recovers from panics, and logs / increment counters
func (vtg *VTGate) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v, vtgate: %v", x, servenv.ListeningURL.String())
		internalErrors.Add("Panic", 1)
	}
}
