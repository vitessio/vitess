// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var RpcVTGate *VTGate

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	resolver *Resolver
	timings  *stats.MapTimings
	errors   *stats.MapCounters

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
		resolver: NewResolver(serv, "VttabletCall", cell, retryDelay, retryCount, timeout),
		timings:  stats.NewMapTimings("VtgateApi", []string{"Operation", "Keyspace", "DbType"}),
		errors:   stats.NewMapCounters("VtgateApiErrorCounts", []string{"Operation", "Keyspace", "DbType"}),

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
	for _, f := range RegisterVTGates {
		f(RpcVTGate)
	}
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context interface{}, query *proto.QueryShard) (interface{}, *proto.Session, error) {
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
		return qr, query.Session, nil
	} else {
		vtg.errors.Add(statsKey, 1)
		vtg.logExecuteShard.Errorf("%v, query: %+v", err, query)
		return nil, query.Session, err
	}
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery) (interface{}, *proto.Session, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.resolver.ExecuteKeyspaceIds(context, query)
	if err == nil {
		return qr, query.Session, nil
	} else {
		vtg.errors.Add(statsKey, 1)
		vtg.logExecuteKeyspaceIds.Errorf("%v, query: %+v", err, query)
		return nil, query.Session, err
	}
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(context interface{}, query *proto.KeyRangeQuery) (interface{}, *proto.Session, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.resolver.ExecuteKeyRanges(context, query)
	if err == nil {
		return qr, query.Session, nil
	} else {
		vtg.errors.Add(statsKey, 1)
		vtg.logExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
		return nil, query.Session, err
	}
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(context interface{}, query *proto.EntityIdsQuery) (interface{}, *proto.Session, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteEntityIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qr, err := vtg.resolver.ExecuteEntityIds(context, query)
	if err == nil {
		return qr, query.Session, nil
	} else {
		vtg.errors.Add(statsKey, 1)
		vtg.logExecuteEntityIds.Errorf("%v, query: %+v", err, query)
		return nil, query.Session, err
	}
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context interface{}, batchQuery *proto.BatchQueryShard) (interface{}, *proto.Session, error) {
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
		return qrs, batchQuery.Session, nil
	} else {
		vtg.errors.Add(statsKey, 1)
		vtg.logExecuteBatchShard.Errorf("%v, queries: %+v", err, batchQuery)
		return nil, batchQuery.Session, err
	}
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(context interface{}, query *proto.KeyspaceIdBatchQuery) (interface{}, *proto.Session, error) {
	startTime := time.Now()
	statsKey := []string{"ExecuteBatchKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		context,
		query)
	if err == nil {
		return qrs, query.Session, nil
	} else {
		vtg.errors.Add(statsKey, 1)
		vtg.logExecuteBatchKeyspaceIds.Errorf("%v, query: %+v", err, query)
		return nil, query.Session, err
	}
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof.
func (vtg *VTGate) StreamExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery, sendReply func(interface{}, *proto.Session) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecuteKeyspaceIds(
		context,
		query,
		func(reply interface{}) error {
			return sendReply(reply, nil)
		})
	if err != nil {
		vtg.errors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyspaceIds.Errorf("%v, query: %+v", err, query)
	}
	// Note we don't populate session above,
	// as it may change incrementaly as responses are sent.
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(nil, query.Session)
	}
	return err
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRanges(context interface{}, query *proto.KeyRangeQuery, sendReply func(interface{}, *proto.Session) error) error {
	startTime := time.Now()
	statsKey := []string{"StreamExecuteKeyRanges", query.Keyspace, string(query.TabletType)}
	defer vtg.timings.Record(statsKey, startTime)

	err := vtg.resolver.StreamExecuteKeyRanges(
		context,
		query,
		func(reply interface{}) error {
			return sendReply(reply, nil)
		})

	if err != nil {
		vtg.errors.Add(statsKey, 1)
		vtg.logStreamExecuteKeyRanges.Errorf("%v, query: %+v", err, query)
	}
	// Note we don't populate session above,
	// as it may change incrementaly as responses are sent.
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(nil, query.Session)
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context interface{}, query *proto.QueryShard, sendReply func(interface{}, *proto.Session) error) error {
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
		func(reply interface{}) error {
			return sendReply(reply, nil)
		})

	if err != nil {
		vtg.errors.Add(statsKey, 1)
		vtg.logStreamExecuteShard.Errorf("%v, query: %+v", err, query)
	}
	// Note we don't populate session above,
	// as it may change incrementaly as responses are sent.
	// Now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(nil, query.Session)
	}
	return err
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(context interface{}, outSession *proto.Session) error {
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(context interface{}, inSession *proto.Session) error {
	return vtg.resolver.Commit(context, inSession)
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context interface{}, inSession *proto.Session) error {
	return vtg.resolver.Rollback(context, inSession)
}
