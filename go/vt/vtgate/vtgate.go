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
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var RpcVTGate *VTGate

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	scatterConn *ScatterConn
}

func Init(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		scatterConn: NewScatterConn(serv, cell, retryDelay, retryCount),
	}
	proto.RegisterAuthenticated(RpcVTGate)
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context *rpcproto.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	qr, err := vtg.scatterConn.Execute(
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.Shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		log.Errorf("ExecuteShard: %v, query: %#v", err, query)
	}
	reply.Session = query.Session
	return err
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context *rpcproto.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	qrs, err := vtg.scatterConn.ExecuteBatch(
		batchQuery.Queries,
		batchQuery.Keyspace,
		batchQuery.Shards,
		batchQuery.TabletType,
		NewSafeSession(batchQuery.Session))
	if err == nil {
		reply.List = qrs.List
	} else {
		log.Errorf("ExecuteBatchShard: %v, queries: %#v", err, batchQuery)
	}
	reply.Session = batchQuery.Session
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context *rpcproto.Context, query *proto.QueryShard, sendReply func(interface{}) error) error {
	err := vtg.scatterConn.StreamExecute(
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.Shards,
		query.TabletType,
		NewSafeSession(query.Session),
		func(mreply interface{}) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply.(*mproto.QueryResult), reply)
			return sendReply(reply)
		})
	if err != nil {
		log.Errorf("StreamExecuteShard: %v, query: %#v", err, query)
	}
	if err != nil {
		return err
	}
	if query.Session != nil {
		return sendReply(&proto.QueryResult{Session: query.Session})
	}
	return nil
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(context *rpcproto.Context, noInput *rpc.UnusedRequest, outSession *proto.Session) error {
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(context *rpcproto.Context, inSession *proto.Session, noOutput *rpc.UnusedResponse) error {
	return vtg.scatterConn.Commit(NewSafeSession(inSession))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context *rpcproto.Context, inSession *proto.Session, noOutput *rpc.UnusedResponse) error {
	return vtg.scatterConn.Rollback(NewSafeSession(inSession))
}
