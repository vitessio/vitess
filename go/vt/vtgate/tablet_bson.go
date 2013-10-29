// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"strings"

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
		return nil, tabletError(err)
	}

	var sessionInfo tproto.SessionInfo
	if err = conn.rpcClient.Call("SqlQuery.GetSessionId", tproto.SessionParams{Keyspace: keyspace, Shard: shard}, &sessionInfo); err != nil {
		return nil, tabletError(err)
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
		return nil, tabletError(err)
	}
	return qr, nil
}

func (conn *TabletBson) ExecuteBatch(queries []tproto.BoundQuery) (*tproto.QueryResultList, error) {
	req := tproto.QueryList{
		Queries:       queries,
		TransactionId: conn.session.TransactionId,
		SessionId:     conn.session.SessionId,
	}
	qrs := new(tproto.QueryResultList)
	if err := conn.rpcClient.Call("SqlQuery.ExecuteBatch", req, qrs); err != nil {
		return nil, tabletError(err)
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
	return sr, func() error { return tabletError(c.Error) }
}

func (conn *TabletBson) Begin() error {
	return tabletError(conn.rpcClient.Call("SqlQuery.Begin", &conn.session, &conn.session.TransactionId))
}

func (conn *TabletBson) Commit() error {
	defer func() { conn.session.TransactionId = 0 }()
	var noOutput string
	return tabletError(conn.rpcClient.Call("SqlQuery.Commit", &conn.session, &noOutput))
}

func (conn *TabletBson) Rollback() error {
	defer func() { conn.session.TransactionId = 0 }()
	var noOutput string
	return tabletError(conn.rpcClient.Call("SqlQuery.Rollback", &conn.session, &noOutput))
}

func (conn *TabletBson) TransactionId() int64 {
	return conn.session.TransactionId
}

func (conn *TabletBson) Close() error {
	conn.session = tproto.Session{TransactionId: 0, SessionId: 0}
	rpcClient := conn.rpcClient
	conn.rpcClient = nil
	return tabletError(rpcClient.Close())
}

func tabletError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(rpcplus.ServerError); ok {
		var code int
		errStr := err.Error()
		switch {
		case strings.HasPrefix(errStr, "fatal"):
			code = ERR_FATAL
		case strings.HasPrefix(errStr, "retry"):
			code = ERR_RETRY
		case strings.HasPrefix(errStr, "tx_pool_full"):
			code = ERR_TX_POOL_FULL
		case strings.HasPrefix(errStr, "not_in_tx"):
			code = ERR_NOT_IN_TX
		default:
			code = ERR_NORMAL
		}
		return &ServerError{Code: code, Err: fmt.Sprintf("vttablet: %v", err)}
	}
	return OperationalError(fmt.Sprintf("vttablet: %v", err))
}
