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

// TabletConn is a thin rpc client for a vttablet. It should
// not be concurrently used across goroutines.
type TabletConn struct {
	rpcClient *rpcplus.Client
	tproto.Session
}

type ErrFunc func() error

// StreamResult is the object used to stream query results from
// StreamExecute.
type StreamResult struct {
	Call   *rpcplus.Call
	Stream <-chan *mproto.QueryResult
}

// DialTablet connects to a vttablet.
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

// Close closes the connection to the vttablet.
func (conn *TabletConn) Close() error {
	conn.Session = tproto.Session{0, 0, 0}
	rpcClient := conn.rpcClient
	conn.rpcClient = nil
	return rpcClient.Close()
}

// Execute executes a non-streaming query on vttablet.
func (conn *TabletConn) Execute(query string, bindVars map[string]interface{}) (*mproto.QueryResult, error) {
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

// StreamExecute exectutes a streaming query on vttablet. It returns a channel that will stream results.
// It also returns an ErrFunc that can be called to check if there were any errors. ErrFunc can be called
// immediately after StreamExecute returns to check if there were errors sending the call. It should also
// be called after finishing the iteration over the channel to see if there were other errors.
func (conn *TabletConn) StreamExecute(query string, bindVars map[string]interface{}) (<-chan *mproto.QueryResult, ErrFunc) {
	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: conn.TransactionId,
		ConnectionId:  conn.ConnectionId,
		SessionId:     conn.SessionId,
	}
	sr := make(chan *mproto.QueryResult, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)
	return sr, func() error { return c.Error }
}

// Begin issues a vttablet Begin. TransactionId is set to the
// vttablet issued transaction id if it succeeds.
func (conn *TabletConn) Begin() error {
	return conn.rpcClient.Call("SqlQuery.Begin", &conn.Session, &conn.TransactionId)
}

// Commit issues a Commit. TransactionId is reset.
func (conn *TabletConn) Commit() error {
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Commit", &conn.Session, &noOutput)
}

// Rollback issues a Rollback. TransactionId is reset.
func (conn *TabletConn) Rollback() error {
	defer func() { conn.TransactionId = 0 }()
	var noOutput string
	return conn.rpcClient.Call("SqlQuery.Rollback", &conn.Session, &noOutput)
}
