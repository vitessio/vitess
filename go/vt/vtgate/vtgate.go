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
	"github.com/youtube/vitess/go/pools"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var RpcVTGate *VTGate

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	balancerMap *BalancerMap
	connections *pools.Numbered
	retryDelay  time.Duration
	retryCount  int
}

func Init(blm *BalancerMap, retryDelay time.Duration, retryCount int) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		balancerMap: blm,
		connections: pools.NewNumbered(),
		retryDelay:  retryDelay,
		retryCount:  retryCount,
	}
	proto.RegisterAuthenticated(RpcVTGate)
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context *rpcproto.Context, query *proto.QueryShard, reply *mproto.QueryResult) error {
	scatterConn, err := vtg.connections.Get(query.SessionId, "for query")
	if err != nil {
		return fmt.Errorf("query: %s, session %d: %v", query.Sql, query.SessionId, err)
	}
	defer vtg.connections.Put(query.SessionId)
	qr, err := scatterConn.(*ScatterConn).Execute(query.Sql, query.BindVariables, query.Keyspace, query.Shards)
	if err == nil {
		*reply = *qr
	} else {
		log.Errorf("ExecuteShard: %v, query: %#v", err, query)
	}
	return err
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context *rpcproto.Context, batchQuery *proto.BatchQueryShard, reply *tproto.QueryResultList) error {
	scatterConn, err := vtg.connections.Get(batchQuery.SessionId, "for batch query")
	if err != nil {
		return fmt.Errorf("query: %v, session %d: %v", batchQuery.Queries, batchQuery.SessionId, err)
	}
	defer vtg.connections.Put(batchQuery.SessionId)
	qrs, err := scatterConn.(*ScatterConn).ExecuteBatch(batchQuery.Queries, batchQuery.Keyspace, batchQuery.Shards)
	if err == nil {
		*reply = *qrs
	} else {
		log.Errorf("ExecuteBatchShard: %v, queries: %#v", err, batchQuery)
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context *rpcproto.Context, query *proto.QueryShard, sendReply func(interface{}) error) error {
	scatterConn, err := vtg.connections.Get(query.SessionId, "for stream query")
	if err != nil {
		return fmt.Errorf("query: %s, session %d: %v", query.Sql, query.SessionId, err)
	}
	defer vtg.connections.Put(query.SessionId)
	err = scatterConn.(*ScatterConn).StreamExecute(query.Sql, query.BindVariables, query.Keyspace, query.Shards, sendReply)
	if err != nil {
		log.Errorf("StreamExecuteShard: %v, query: %#v", err, query)
	}
	return err
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(context *rpcproto.Context, session *proto.Session, noOutput *rpc.UnusedResponse) error {
	scatterConn, err := vtg.connections.Get(session.SessionId, "for begin")
	if err != nil {
		return fmt.Errorf("session %d: %v", session.SessionId, err)
	}
	defer vtg.connections.Put(session.SessionId)
	err = scatterConn.(*ScatterConn).Begin()
	if err != nil {
		log.Errorf("Begin: %v, Session: %#v", err, session)
	}
	return err
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(context *rpcproto.Context, session *proto.Session, noOutput *rpc.UnusedResponse) error {
	scatterConn, err := vtg.connections.Get(session.SessionId, "for commit")
	if err != nil {
		return fmt.Errorf("session %d: %v", session.SessionId, err)
	}
	defer vtg.connections.Put(session.SessionId)
	err = scatterConn.(*ScatterConn).Commit()
	if err != nil {
		log.Errorf("Commit: %v, Session: %#v", err, session)
	}
	return err
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context *rpcproto.Context, session *proto.Session, noOutput *rpc.UnusedResponse) error {
	scatterConn, err := vtg.connections.Get(session.SessionId, "for rollback")
	if err != nil {
		return fmt.Errorf("session %d: %v", session.SessionId, err)
	}
	defer vtg.connections.Put(session.SessionId)
	err = scatterConn.(*ScatterConn).Rollback()
	if err != nil {
		log.Errorf("Rollback: %v, Session: %#v", err, session)
	}
	return err
}
