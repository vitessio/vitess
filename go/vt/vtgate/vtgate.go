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
	"github.com/youtube/vitess/go/vt/topo"
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

func Init(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		scatterConn: NewScatterConn(serv, cell, retryDelay, retryCount, timeout),
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
		log.Errorf("ExecuteShard: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

func (vtg *VTGate) ExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	shards, err := vtg.mapKeyspaceIdsToShards(query.Keyspace, query.TabletType, query.KeyspaceIds)
	if err != nil {
		return err
	}
	qr, err := vtg.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteKeyRange: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

func (vtg *VTGate) ExecuteKeyRange(context interface{}, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	shards, err := vtg.mapKrToShards(query.Keyspace, query.TabletType, query.KeyRange)
	if err != nil {
		return err
	}
	qr, err := vtg.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteKeyRange: %v, query: %+v", err, query)
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
		log.Errorf("ExecuteBatchShard: %v, queries: %+v", err, batchQuery)
	}
	reply.Session = batchQuery.Session
	return nil
}

func (vtg *VTGate) ExecuteBatchKeyspaceIds(context interface{}, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	shards, err := vtg.mapKeyspaceIdsToShards(query.Keyspace, query.TabletType, query.KeyspaceIds)
	if err != nil {
		return err
	}
	qrs, err := vtg.scatterConn.ExecuteBatch(
		context,
		query.Queries,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteBatchKeyspaceIds: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

func (vtg *VTGate) mapKeyspaceIdsToShards(keyspace string, tabletType topo.TabletType, keyspaceIds []string) ([]string, error) {
	var shards = make(map[string]int)
	for _, ksId := range keyspaceIds {
		keyspaceId, err := key.HexKeyspaceId(ksId).Unhex()
		if err != nil {
			return nil, err
		}
		shard, err := getShardForKeyspaceId(
			vtg.scatterConn.toposerv,
			vtg.scatterConn.cell,
			keyspace,
			keyspaceId,
			tabletType)
		if err != nil {
			return nil, err
		}
		shards[shard] = 0
	}
	var res = make([]string, 0, 1)
	for s, _ := range shards {
		res = append(res, s)
	}
	return res, nil
}

// This function implements the restriction of handling one keyrange
// and one shard since streaming doesn't support merge sorting the results.
// The input/output api is generic though.
func (vtg *VTGate) mapKrToShards(keyspace string, tabletType topo.TabletType, keyRange string) ([]string, error) {
	var krArray []key.KeyRange
	var err error
	if keyRange == "" {
		krArray = []key.KeyRange{key.KeyRange{Start: "", End: ""}}
	} else {
		krArray, err = key.ParseShardingSpec(keyRange)
		if err != nil {
			return nil, err
		}
	}
	uniqueShards := make(map[string]int)
	for _, kr := range krArray {
		shards, err := resolveKeyRangeToShards(vtg.scatterConn.toposerv,
			vtg.scatterConn.cell,
			keyspace,
			tabletType,
			kr)
		if err != nil {
			return nil, err
		}
		for _, shard := range shards {
			uniqueShards[shard] = 0
		}
	}
	var res = make([]string, 0, 1)
	for s, _ := range uniqueShards {
		res = append(res, s)
	}
	return res, nil
}

// StreamExecuteKeyRange executes a streaming query on the specified KeyRange.
// The KeyRange is resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on one keyrange
// and one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing. The api supports supplying multiple keyranges
// to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRange(context interface{}, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	shards, err := vtg.mapKrToShards(query.Keyspace, query.TabletType, query.KeyRange)
	if err != nil {
		return err
	}
	if len(shards) != 1 {
		return fmt.Errorf("KeyRange cannot map to more than one shard")
	}

	err = vtg.scatterConn.StreamExecute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
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
	return vtg.scatterConn.Commit(context, NewSafeSession(inSession))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context interface{}, inSession *proto.Session) error {
	return vtg.scatterConn.Rollback(context, NewSafeSession(inSession))
}
