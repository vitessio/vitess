// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var RpcVTGate *VTGate

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	resolver *Resolver
	timings  *stats.MapTimings
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
	}
	for _, f := range RegisterVTGates {
		f(RpcVTGate)
	}
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context interface{}, query *proto.QueryShard, reply *proto.QueryResult) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"ExecuteShard", query.Keyspace, string(query.TabletType)}, startTime)

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
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteShard: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"ExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}, startTime)

	qr, err := vtg.resolver.ExecuteKeyspaceIds(context, query)
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteKeyspaceIds: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(context interface{}, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"ExecuteKeyRanges", query.Keyspace, string(query.TabletType)}, startTime)

	qr, err := vtg.resolver.ExecuteKeyRanges(context, query)
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteKeyRange: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(context interface{}, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"ExecuteEntityIds", query.Keyspace, string(query.TabletType)}, startTime)

	qr, err := vtg.resolver.ExecuteEntityIds(context, query)
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteEntityIds: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context interface{}, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"ExecuteBatchShard", batchQuery.Keyspace, string(batchQuery.TabletType)}, startTime)

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
		log.Errorf("ExecuteBatchShard: %v, queries: %+v", err, batchQuery)
	}
	reply.Session = batchQuery.Session
	return nil
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(context interface{}, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"ExecuteBatchKeyspaceIds", query.Keyspace, string(query.TabletType)}, startTime)

	qrs, err := vtg.resolver.ExecuteBatchKeyspaceIds(
		context,
		query)
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteBatchKeyspaceIds: %v, query: %+v", err, query)
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
func (vtg *VTGate) StreamExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"StreamExecuteKeyspaceIds", query.Keyspace, string(query.TabletType)}, startTime)

	err := vtg.resolver.StreamExecuteKeyspaceIds(
		context,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	if err != nil {
		log.Errorf("StreamExecuteKeyspaceIds: %v, query: %+v", err, query)
	}
	// now we can send the final Sessoin info.
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
func (vtg *VTGate) StreamExecuteKeyRanges(context interface{}, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"StreamExecuteKeyRanges", query.Keyspace, string(query.TabletType)}, startTime)

	err := vtg.resolver.StreamExecuteKeyRanges(
		context,
		query,
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})

	if err != nil {
		log.Errorf("StreamExecuteKeyRange: %v, query: %+v", err, query)
	}
	// now we can send the final Session info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context interface{}, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	startTime := time.Now()
	defer vtg.timings.Record([]string{"StreamExecuteShard", query.Keyspace, string(query.TabletType)}, startTime)

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
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses
			// are sent.
			return sendReply(reply)
		})

	if err != nil {
		log.Errorf("StreamExecuteShard: %v, query: %+v", err, query)
	}
	// now we can send the final Session info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
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
