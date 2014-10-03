// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/context"
	kproto "github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

const errDupKey = "errno 1062"

var (
	RpcVTGate *VTGate

	QPSByOperation *stats.Rates
	QPSByKeyspace  *stats.Rates
	QPSByDbType    *stats.Rates

	ErrorsByOperation *stats.Rates
	ErrorsByKeyspace  *stats.Rates
	ErrorsByDbType    *stats.Rates

	ErrTooManyInFlight = errors.New("request_backlog: too many requests in flight")

	// Error counters should be global so they can be set from anywhere
	normalErrors   *stats.MultiCounters
	infoErrors     *stats.Counters
	internalErrors *stats.Counters
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	resolver *Resolver
	timings  *stats.MultiTimings

	maxInFlight int64
	inFlight    sync2.AtomicInt64

	// the throttled loggers for all errors, one per API entry
	logExecuteShard             *logutil.ThrottledLogger
	logExecuteKeyspaceIds       *logutil.ThrottledLogger
	logExecuteKeyRanges         *logutil.ThrottledLogger
	logExecuteEntityIds         *logutil.ThrottledLogger
	logExecuteBatchShard        *logutil.ThrottledLogger
	logExecuteBatchKeyspaceIds  *logutil.ThrottledLogger
	logStreamExecuteKeyspaceIds *logutil.ThrottledLogger
	logStreamExecuteKeyRanges   *logutil.ThrottledLogger
	logStreamExecuteShard       *logutil.ThrottledLogger
}

// registration mechanism
type RegisterVTGate func(*VTGate)

var RegisterVTGates []RegisterVTGate

func Init(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration, maxInFlight int) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		resolver: NewResolver(serv, "VttabletCall", cell, retryDelay, retryCount, timeout),
		timings:  stats.NewMultiTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),

		maxInFlight: int64(maxInFlight),
		inFlight:    0,

		logExecuteShard:             logutil.NewThrottledLogger("ExecuteShard", 5*time.Second),
		logExecuteKeyspaceIds:       logutil.NewThrottledLogger("ExecuteKeyspaceIds", 5*time.Second),
		logExecuteKeyRanges:         logutil.NewThrottledLogger("ExecuteKeyRanges", 5*time.Second),
		logExecuteEntityIds:         logutil.NewThrottledLogger("ExecuteEntityIds", 5*time.Second),
		logExecuteBatchShard:        logutil.NewThrottledLogger("ExecuteBatchShard", 5*time.Second),
		logExecuteBatchKeyspaceIds:  logutil.NewThrottledLogger("ExecuteBatchKeyspaceIds", 5*time.Second),
		logStreamExecuteKeyspaceIds: logutil.NewThrottledLogger("StreamExecuteKeyspaceIds", 5*time.Second),
		logStreamExecuteKeyRanges:   logutil.NewThrottledLogger("StreamExecuteKeyRanges", 5*time.Second),
		logStreamExecuteShard:       logutil.NewThrottledLogger("StreamExecuteShard", 5*time.Second),
	}
	normalErrors = stats.NewMultiCounters("VtgateApiErrorCounts", []string{"Operation", "Keyspace", "DbType"})
	infoErrors = stats.NewCounters("VtgateInfoErrorCounts")
	internalErrors = stats.NewCounters("VtgateInternalErrorCounts")

	QPSByOperation = stats.NewRates("QPSByOperation", stats.CounterForDimension(RpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	QPSByKeyspace = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(RpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	QPSByDbType = stats.NewRates("QPSByDbType", stats.CounterForDimension(RpcVTGate.timings, "DbType"), 15, 1*time.Minute)

	ErrorsByOperation = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(normalErrors, "Operation"), 15, 1*time.Minute)
	ErrorsByKeyspace = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(normalErrors, "Keyspace"), 15, 1*time.Minute)
	ErrorsByDbType = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(normalErrors, "DbType"), 15, 1*time.Minute)

	for _, f := range RegisterVTGates {
		f(RpcVTGate)
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

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context context.Context, query *proto.QueryShard, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	qr, err := vtg.resolver.Execute(
		context,
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
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			infoErrors.Add("DupKey", 1)
		} else {
			normalErrors.Add(statsKey, 1)
			vtg.logExecuteShard.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(context context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	qr, err := vtg.resolver.ExecuteKeyspaceIds(context, query)
	if err == nil {
		reply.Result = qr
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			infoErrors.Add("DupKey", 1)
		} else {
			normalErrors.Add(statsKey, 1)
			vtg.logExecuteKeyspaceIds.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(context context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	qr, err := vtg.resolver.ExecuteKeyRanges(context, query)
	if err == nil {
		reply.Result = qr
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			infoErrors.Add("DupKey", 1)
		} else {
			normalErrors.Add(statsKey, 1)
			vtg.logExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(context context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteEntityIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	qr, err := vtg.resolver.ExecuteEntityIds(context, query)
	if err == nil {
		reply.Result = qr
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			infoErrors.Add("DupKey", 1)
		} else {
			normalErrors.Add(statsKey, 1)
			vtg.logExecuteEntityIds.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteBatchShard", batchQuery.Keyspace, string(batchQuery.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	qrs, err := vtg.resolver.ExecuteBatch(
		context,
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
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			infoErrors.Add("DupKey", 1)
		} else {
			normalErrors.Add(statsKey, 1)
			vtg.logExecuteBatchShard.Errorf("%v, queries: %+v", err, batchQuery)
		}
	}
	reply.Session = batchQuery.Session
	return nil
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(context context.Context, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"ExecuteBatchKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		context,
		query)
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			infoErrors.Add("DupKey", 1)
		} else {
			normalErrors.Add(statsKey, 1)
			vtg.logExecuteBatchKeyspaceIds.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof.
func (vtg *VTGate) StreamExecuteKeyspaceIds(context context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	err = vtg.resolver.StreamExecuteKeyspaceIds(
		context,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			reply.Result = mreply
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyspaceIds.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRanges(context context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	err = vtg.resolver.StreamExecuteKeyRanges(
		context,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			reply.Result = mreply
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})

	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) (err error) {
	defer handlePanic(&err)

	startTime := time.Now()
	statsKey := []string{"StreamExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	x := vtg.inFlight.Add(1)
	defer vtg.inFlight.Add(-1)
	if 0 < vtg.maxInFlight && vtg.maxInFlight < x {
		return ErrTooManyInFlight
	}

	err = vtg.resolver.StreamExecute(
		context,
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
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})

	if err != nil {
		normalErrors.Add(statsKey, 1)
		vtg.logStreamExecuteShard.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(context context.Context, outSession *proto.Session) (err error) {
	defer handlePanic(&err)
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(context context.Context, inSession *proto.Session) (err error) {
	defer handlePanic(&err)
	return vtg.resolver.Commit(context, inSession)
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context context.Context, inSession *proto.Session) (err error) {
	defer handlePanic(&err)
	return vtg.resolver.Rollback(context, inSession)
}

// GetMRSplits is the endpoint used by MapReduce controllers to fetch InputSplits
// for its jobs. An InputSplit represents a set of rows in the specified table.
// The mapper responsible for an InputSplit can execute the KeyRangeQuery of
// that split to fetch the corresponding rows. The sum of InputSplits returned
// by this method will add up to the entire table. By default one split is created
// per shard, but this can be controlled by changing req.SplitsPerShard
func (vtg *VTGate) GetMRSplits(context context.Context, req *proto.GetMRSplitsRequest, reply *proto.GetMRSplitsResult) (err error) {
	defer handlePanic(&err)
	sc := vtg.resolver.scatterConn
	keyspace, shards, err := getKeyspaceShards(sc.toposerv, sc.cell, req.Keyspace, topo.TYPE_RDONLY)
	if err != nil {
		return err
	}
	keyRangeByShard := map[string]kproto.KeyRange{}
	for _, shard := range shards {
		keyRangeByShard[shard.ShardName()] = shard.KeyRange
	}
	splits, err := vtg.resolver.scatterConn.SplitQuery(context, req.Query, req.SplitsPerShard, keyRangeByShard, keyspace)
	if err != nil {
		return err
	}
	reply.Splits = splits
	return nil
}

func handlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
		internalErrors.Add("Panic", 1)
	}
}
