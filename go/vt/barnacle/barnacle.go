// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package barnacle provides query routing rpc services
// for vttablets.
package barnacle

import (
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/barnacle/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

var RpcBarnacle *Barnacle

// Barnacle is the rpc interface to barnacle. Only one instance
// can be created.
type Barnacle struct {
	balancerMap *BalancerMap
	connections *pools.Numbered
	retryDelay  time.Duration
	retryCount  int
}

func Init(cell, portName string, retryDelay time.Duration, retryCount int) {
	if RpcBarnacle != nil {
		log.Fatalf("Barnacle already initialized")
	}
	RpcBarnacle = &Barnacle{
		balancerMap: NewBalancerMap(topo.GetServer(), cell, portName),
		connections: pools.NewNumbered(),
		retryDelay:  retryDelay,
		retryCount:  retryCount,
	}
	proto.RegisterAuthenticated(RpcBarnacle)
}

func (bnc *Barnacle) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	vtconn := NewVTConn(bnc.balancerMap, sessionParams.TabletType, bnc.retryDelay, bnc.retryCount)
	sessionInfo.SessionId = vtconn.Id
	bnc.connections.Register(vtconn.Id, vtconn)
	return nil
}

func (bnc *Barnacle) Execute(context *rpcproto.Context, query *proto.Query, reply *mproto.QueryResult) error {
	vtconn, err := bnc.connections.Get(query.SessionId)
	if err != nil {
		return err
	}
	defer bnc.connections.Put(query.SessionId)
	qr, err := vtconn.(*VTConn).Execute(query.Sql, query.BindVariables, query.Keyspace, query.Shards)
	if err == nil {
		*reply = *qr
	}
	return err
}

func (bnc *Barnacle) StreamExecute(context *rpcproto.Context, query *proto.Query, sendReply func(interface{}) error) error {
	vtconn, err := bnc.connections.Get(query.SessionId)
	if err != nil {
		return err
	}
	defer bnc.connections.Put(query.SessionId)
	return vtconn.(*VTConn).StreamExecute(query.Sql, query.BindVariables, query.Keyspace, query.Shards, sendReply)
}

func (bnc *Barnacle) Begin(context *rpcproto.Context, session *proto.Session, txInfo *proto.TransactionInfo) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return err
	}
	defer bnc.connections.Put(session.SessionId)
	txInfo.TransactionId, err = vtconn.(*VTConn).Begin()
	return err
}

func (bnc *Barnacle) Commit(context *rpcproto.Context, session *proto.Session, noOutput *string) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return err
	}
	defer bnc.connections.Put(session.SessionId)
	return vtconn.(*VTConn).Commit()
}

func (bnc *Barnacle) Rollback(context *rpcproto.Context, session *proto.Session, noOutput *string) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return err
	}
	defer bnc.connections.Put(session.SessionId)
	return vtconn.(*VTConn).Rollback()
}
