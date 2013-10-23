// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func init() {
	RegisterDialer("bson", DialTablet)
}

func DialTablet(addr, keyspace, shard, username, password string, encrypted bool) (TabletConn, error) {
	// FIXME(sougou/shrutip): Add encrypted support
	conn := new(TabletBson)
	var err error
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
	conn.session.SessionId = sessionInfo.SessionId
	return conn, nil
}

// TabletBson implements a bson rpcplus implementation for TabletConn
type TabletBson struct {
	rpcClient *rpcplus.Client
	session   tproto.Session
}

func (conn *TabletBson) Execute(query string, bindVars map[string]interface{}) (*mproto.QueryResult, error) {
	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.session.TransactionId,
		SessionId:     conn.session.SessionId,
	}
	qr := new(mproto.QueryResult)
	if err := conn.rpcClient.Call("SqlQuery.Execute", req, qr); err != nil {
		return nil, err
	}
	return qr, nil
}

func (conn *TabletBson) ExecuteBatch(queries []tproto.BoundQuery) (*tproto.QueryResultList, error) {
	req := tproto.QueryList{
		Queries:       make([]tproto.BoundQuery, 0, len(queries)),
		TransactionId: conn.session.TransactionId,
		SessionId:     conn.session.SessionId,
	}

	for _, q := range req.Queries {
		req.Queries = append(req.Queries, tproto.BoundQuery{
			Sql:           q.Sql,
			BindVariables: q.BindVariables,
		})
	}
	qrs := new(tproto.QueryResultList)
	if err := conn.rpcClient.Call("SqlQuery.ExecuteBatch", req, qrs); err != nil {
		return nil, err
	}
	return qrs, nil
}

func (conn *TabletBson) StreamExecute(query string, bindVars map[string]interface{}) (<-chan *mproto.QueryResult, ErrFunc) {
	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.session.TransactionId,
		SessionId:     conn.session.SessionId,
	}
	sr := make(chan *mproto.QueryResult, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)
	return sr, func() error { return c.Error }
}

func (conn *TabletBson) Begin() error {
	return conn.rpcClient.Call("SqlQuery.Begin", &conn.session, &conn.session.TransactionId)
}

func (conn *TabletBson) Commit() error {
	defer func() { conn.session.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Commit", &conn.session, &noOutput)
}

func (conn *TabletBson) Rollback() error {
	defer func() { conn.session.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Rollback", &conn.session, &noOutput)
}

func (conn *TabletBson) TransactionId() int64 {
	return conn.session.TransactionId
}

func (conn *TabletBson) Close() error {
	conn.session = tproto.Session{TransactionId: 0, SessionId: 0}
	rpcClient := conn.rpcClient
	conn.rpcClient = nil
	return rpcClient.Close()
}
