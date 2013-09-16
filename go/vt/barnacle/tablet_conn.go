// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

type TabletConn struct {
	rpcClient *rpcplus.Client
	tproto.Session
}

type StreamResult struct {
	Call   *rpcplus.Call
	Stream <-chan *mproto.QueryResult
}

func DialTablet(addr, keyspace, shard, username, password string, encrypted bool) (conn *TabletConn, err error) {
	// FIXME(sougou/shrutip): Add encrypted support
	conn = new(TabletConn)
	if username != "" {
		conn.rpcClient, err = bsonrpc.DialAuthHTTP("tcp", addr, username, password, 0)
	} else {
		conn.rpcClient, err = bsonrpc.DialHTTP("tcp", addr, 0)
	}
	if err != nil {
		return nil, err
	}

	var sessionInfo tproto.SessionInfo
	if err = conn.rpcClient.Call("SqlQuery.GetSessionId", tproto.SessionParams{Keyspace: keyspace, Shard: shard}, &sessionInfo); err != nil {
		return nil, err
	}
	conn.SessionId = sessionInfo.SessionId
	return conn, nil
}

func (conn *TabletConn) Close() error {
	conn.Session = tproto.Session{0, 0, 0}
	rpcClient := conn.rpcClient
	conn.rpcClient = nil
	return rpcClient.Close()
}

func (conn *TabletConn) ExecDirect(query string, bindVars map[string]interface{}) (*mproto.QueryResult, error) {
	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.TransactionId,
		ConnectionId:  conn.ConnectionId,
		SessionId:     conn.SessionId,
	}
	qr := new(mproto.QueryResult)
	if err := conn.rpcClient.Call("SqlQuery.Execute", req, qr); err != nil {
		return nil, err
	}
	return qr, nil
}

func (conn *TabletConn) ExecStream(query string, bindVars map[string]interface{}) (*StreamResult, error) {
	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.TransactionId,
		ConnectionId:  conn.ConnectionId,
		SessionId:     conn.SessionId,
	}
	sr := make(chan *mproto.QueryResult, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)
	return &StreamResult{c, sr}, nil
}

func (conn *TabletConn) Begin() error {
	return conn.rpcClient.Call("SqlQuery.Begin", &conn.Session, &conn.TransactionId)
}

func (conn *TabletConn) Commit() error {
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Commit", &conn.Session, &noOutput)
}

func (conn *TabletConn) Rollback() error {
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Rollback", &conn.Session, &noOutput)
}
