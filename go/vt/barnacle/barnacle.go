// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package barnacle provides query routing rpc services
// for vttablets.
package barnacle

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/pools"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/barnacle/proto"
)

var RpcBarnacle *Barnacle

// Barnacle is the rpc interface to barnacle. Only one instance
// can be created.
type Barnacle struct {
	balancerMap    *BalancerMap
	tabletProtocol string
	connections    *pools.Numbered
	retryDelay     time.Duration
	retryCount     int
}

func Init(blm *BalancerMap, tabletProtocol string, retryDelay time.Duration, retryCount int) {
	if RpcBarnacle != nil {
		log.Fatalf("Barnacle already initialized")
	}
	RpcBarnacle = &Barnacle{
		balancerMap:    blm,
		tabletProtocol: tabletProtocol,
		connections:    pools.NewNumbered(),
		retryDelay:     retryDelay,
		retryCount:     retryCount,
	}
	proto.RegisterAuthenticated(RpcBarnacle)
}

// GetSessionId is the first request sent by the client to begin a session. The returned
// id should be used for all subsequent communications.
func (bnc *Barnacle) GetSessionId(sessionParams *proto.SessionParams, session *proto.Session) error {
	vtconn := NewVTConn(bnc.balancerMap, bnc.tabletProtocol, sessionParams.TabletType, bnc.retryDelay, bnc.retryCount)
	session.SessionId = vtconn.Id
	bnc.connections.Register(vtconn.Id, vtconn)
	return nil
}

// Execute executes a non-streaming query.
func (bnc *Barnacle) Execute(context *rpcproto.Context, query *proto.Query, reply *mproto.QueryResult) error {
	vtconn, err := bnc.connections.Get(query.SessionId)
	if err != nil {
		return fmt.Errorf("query: %s, session %d: %v", query.Sql, query.SessionId, err)
	}
	defer bnc.connections.Put(query.SessionId)
	qr, err := vtconn.(*VTConn).Execute(query.Sql, query.BindVariables, query.Keyspace, query.Shards)
	if err == nil {
		*reply = *qr
	}
	return err
}

// StreamExecute executes a streaming query.
func (bnc *Barnacle) StreamExecute(context *rpcproto.Context, query *proto.Query, sendReply func(interface{}) error) error {
	vtconn, err := bnc.connections.Get(query.SessionId)
	if err != nil {
		return fmt.Errorf("query: %s, session %d: %v", query.Sql, query.SessionId, err)
	}
	defer bnc.connections.Put(query.SessionId)
	return vtconn.(*VTConn).StreamExecute(query.Sql, query.BindVariables, query.Keyspace, query.Shards, sendReply)
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (bnc *Barnacle) Begin(context *rpcproto.Context, session *proto.Session, noOutput *string) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return fmt.Errorf("session %d: %v", session.SessionId, err)
	}
	defer bnc.connections.Put(session.SessionId)
	return vtconn.(*VTConn).Begin()
}

// Commit commits a transaction.
func (bnc *Barnacle) Commit(context *rpcproto.Context, session *proto.Session, noOutput *string) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return fmt.Errorf("session %d: %v", session.SessionId, err)
	}
	defer bnc.connections.Put(session.SessionId)
	return vtconn.(*VTConn).Commit()
}

// Rollback rolls back a transaction.
func (bnc *Barnacle) Rollback(context *rpcproto.Context, session *proto.Session, noOutput *string) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return fmt.Errorf("session %d: %v", session.SessionId, err)
	}
	defer bnc.connections.Put(session.SessionId)
	return vtconn.(*VTConn).Rollback()
}

// CloseSession closes the current session and releases all associated resources for the session.
func (bnc *Barnacle) CloseSession(context *rpcproto.Context, session *proto.Session, noOutput *string) error {
	vtconn, err := bnc.connections.Get(session.SessionId)
	if err != nil {
		return nil
	}
	defer bnc.connections.Unregister(session.SessionId)
	vtconn.(*VTConn).Close()
	return nil
}
