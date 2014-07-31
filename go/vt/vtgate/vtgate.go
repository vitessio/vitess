// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"strings"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/logutil"
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
)

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	resolver   *Resolver
	timings    *stats.MultiTimings
	errors     *stats.MultiCounters
	infoErrors *stats.Counters

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

func Init(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		resolver:   NewResolver(serv, "VttabletCall", cell, retryDelay, retryCount, timeout),
		timings:    stats.NewMultiTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),
		errors:     stats.NewMultiCounters("VtgateApiErrorCounts", []string{"Operation", "Keyspace", "DbType"}),
		infoErrors: stats.NewCounters("VtgateInfoErrorCounts"),

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
	QPSByOperation = stats.NewRates("QPSByOperation", stats.CounterForDimension(RpcVTGate.timings, "Operation"), 15, 1*time.Minute)
	QPSByKeyspace = stats.NewRates("QPSByKeyspace", stats.CounterForDimension(RpcVTGate.timings, "Keyspace"), 15, 1*time.Minute)
	QPSByDbType = stats.NewRates("QPSByDbType", stats.CounterForDimension(RpcVTGate.timings, "DbType"), 15, 1*time.Minute)

	ErrorsByOperation = stats.NewRates("ErrorsByOperation", stats.CounterForDimension(RpcVTGate.errors, "Operation"), 15, 1*time.Minute)
	ErrorsByKeyspace = stats.NewRates("ErrorsByKeyspace", stats.CounterForDimension(RpcVTGate.errors, "Keyspace"), 15, 1*time.Minute)
	ErrorsByDbType = stats.NewRates("ErrorsByDbType", stats.CounterForDimension(RpcVTGate.errors, "DbType"), 15, 1*time.Minute)

	for _, f := range RegisterVTGates {
		f(RpcVTGate)
	}
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context context.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

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
			vtg.infoErrors.Add("DupKey", 1)
		} else {
			vtg.errors.Add(statsKey, 1)
			vtg.logExecuteShard.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(context context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.resolver.ExecuteKeyspaceIds(context, query)
	if err == nil {
		reply.Result = qr
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			vtg.infoErrors.Add("DupKey", 1)
		} else {
			vtg.errors.Add(statsKey, 1)
			vtg.logExecuteKeyspaceIds.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(context context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.resolver.ExecuteKeyRanges(context, query)
	if err == nil {
		reply.Result = qr
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			vtg.infoErrors.Add("DupKey", 1)
		} else {
			vtg.errors.Add(statsKey, 1)
			vtg.logExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(context context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteEntityIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.resolver.ExecuteEntityIds(context, query)
	if err == nil {
		reply.Result = qr
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			vtg.infoErrors.Add("DupKey", 1)
		} else {
			vtg.errors.Add(statsKey, 1)
			vtg.logExecuteEntityIds.Errorf("%v, query: %+v", err, query)
		}
	}
	reply.Session = query.Session
	return nil
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchShard", batchQuery.Keyspace, string(batchQuery.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

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
			vtg.infoErrors.Add("DupKey", 1)
		} else {
			vtg.errors.Add(statsKey, 1)
			vtg.logExecuteBatchShard.Errorf("%v, queries: %+v", err, batchQuery)
		}
	}
	reply.Session = batchQuery.Session
	return nil
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(context context.Context, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		context,
		query)
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		if strings.Contains(reply.Error, errDupKey) {
			vtg.infoErrors.Add("DupKey", 1)
		} else {
			vtg.errors.Add(statsKey, 1)
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
func (vtg *VTGate) StreamExecuteKeyspaceIds(context context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecuteKeyspaceIds(
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
		vtg.errors.Add(statsKey, 1)
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
func (vtg *VTGate) StreamExecuteKeyRanges(context context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecuteKeyRanges(
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
		vtg.errors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteShard", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecute(
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
		vtg.errors.Add(statsKey, 1)
		vtg.logStreamExecuteShard.Errorf("%v, query: %+v", err, query)
	}
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(context context.Context, outSession *proto.Session) error {
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(context context.Context, inSession *proto.Session) error {
	return vtg.resolver.Commit(context, inSession)
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context context.Context, inSession *proto.Session) error {
	return vtg.resolver.Rollback(context, inSession)
}
