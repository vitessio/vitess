// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var RpcVTGate *VTGate

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	scatterConn *ScatterConn
}

// registration mechanism
type RegisterVTGate func(*VTGate)

var RegisterVTGates []RegisterVTGate

func Init(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		scatterConn: NewScatterConn(serv, cell, retryDelay, retryCount),
	}
	for _, f := range RegisterVTGates {
		f(RpcVTGate)
	}
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context interface{}, query *proto.QueryShard, reply *proto.QueryResult) error {
	qr, err := vtg.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.Shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteShard: %v, query: %#v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context interface{}, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	qrs, err := vtg.scatterConn.ExecuteBatch(
		context,
		batchQuery.Queries,
		batchQuery.Keyspace,
		batchQuery.Shards,
		batchQuery.TabletType,
		NewSafeSession(batchQuery.Session))
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteBatchShard: %v, queries: %#v", err, batchQuery)
	}
	reply.Session = batchQuery.Session
	return nil
}

// This function implements the restriction of handling one keyrange
// and one shard since streaming doesn't support merge sorting the results.
// The input/output api is generic though.
func (vtg *VTGate) mapKrToShardsForStreaming(streamQuery *proto.StreamQueryKeyRange) ([]string, error) {
	var keyRange key.KeyRange
	var err error
	if streamQuery.KeyRange == "" {
		keyRange = key.KeyRange{Start: "", End: ""}
	} else {
		krArray, err := key.ParseShardingSpec(streamQuery.KeyRange)
		if err != nil {
			return nil, err
		}
		keyRange = krArray[0]
	}
	shards, err := resolveKeyRangeToShards(vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		streamQuery.Keyspace,
		streamQuery.TabletType,
		keyRange)
	if err != nil {
		return nil, err
	}

	if len(shards) != 1 {
		return nil, fmt.Errorf("KeyRange cannot map to more than one shard")
	}

	return shards, nil
}

// StreamExecuteKeyRange executes a streaming query on the specified KeyRange.
// The KeyRange is resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on one keyrange
// and one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing. The api supports supplying multiple keyranges
// to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRange(context interface{}, streamQuery *proto.StreamQueryKeyRange, sendReply func(*proto.QueryResult) error) error {
	shards, err := vtg.mapKrToShardsForStreaming(streamQuery)
	if err != nil {
		return err
	}

	err = vtg.scatterConn.StreamExecute(
		context,
		streamQuery.Sql,
		streamQuery.BindVariables,
		streamQuery.Keyspace,
		shards,
		streamQuery.TabletType,
		NewSafeSession(streamQuery.Session),
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses
			// are sent.
			return sendReply(reply)
		})

	if err != nil {
		log.Errorf("StreamExecuteKeyRange: %v, query: %#v", err, streamQuery)
	}
	// now we can send the final Session info.
	if streamQuery.Session != nil {
		sendReply(&proto.QueryResult{Session: streamQuery.Session})
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context interface{}, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	err := vtg.scatterConn.StreamExecute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.Shards,
		query.TabletType,
		NewSafeSession(query.Session),
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses
			// are sent.
			return sendReply(reply)
		})

	if err != nil {
		log.Errorf("StreamExecuteShard: %v, query: %#v", err, query)
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
	return vtg.scatterConn.Commit(context, NewSafeSession(inSession))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context interface{}, inSession *proto.Session) error {
	return vtg.scatterConn.Rollback(context, NewSafeSession(inSession))
}
