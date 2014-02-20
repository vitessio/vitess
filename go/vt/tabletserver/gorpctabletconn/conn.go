// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctabletconn

import (
	"crypto/tls"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	tabletBsonUsername  = flag.String("tablet-bson-username", "", "user to use for bson rpc connections")
	tabletBsonPassword  = flag.String("tablet-bson-password", "", "password to use for bson rpc connections (ignored if username is empty)")
	tabletBsonEncrypted = flag.Bool("tablet-bson-encrypted", false, "use encryption to talk to vttablet")
)

func init() {
	tabletconn.RegisterDialer("gorpc", DialTablet)
}

// TabletBson implements a bson rpcplus implementation for TabletConn
type TabletBson struct {
	mu        sync.RWMutex
	endPoint  topo.EndPoint
	rpcClient *rpcplus.Client
	sessionId int64
}

func DialTablet(context interface{}, endPoint topo.EndPoint, keyspace, shard string, timeout time.Duration) (tabletconn.TabletConn, error) {
	var addr string
	var config *tls.Config
	if *tabletBsonEncrypted {
		addr = fmt.Sprintf("%v:%v", endPoint.Host, endPoint.NamedPortMap["_vts"])
		config = &tls.Config{}
		config.InsecureSkipVerify = true
	} else {
		addr = fmt.Sprintf("%v:%v", endPoint.Host, endPoint.NamedPortMap["_vtocc"])
	}

	conn := &TabletBson{endPoint: endPoint}
	var err error
	if *tabletBsonUsername != "" {
		conn.rpcClient, err = bsonrpc.DialAuthHTTP("tcp", addr, *tabletBsonUsername, *tabletBsonPassword, timeout, config)
	} else {
		conn.rpcClient, err = bsonrpc.DialHTTP("tcp", addr, timeout, config)
	}
	if err != nil {
		return nil, tabletError(err)
	}

	var sessionInfo tproto.SessionInfo
	if err = conn.rpcClient.Call("SqlQuery.GetSessionId", tproto.SessionParams{Keyspace: keyspace, Shard: shard}, &sessionInfo); err != nil {
		conn.rpcClient.Close()
		return nil, tabletError(err)
	}
	conn.sessionId = sessionInfo.SessionId
	return conn, nil
}

func (conn *TabletBson) Execute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.CONN_CLOSED
	}

	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionId,
		SessionId:     conn.sessionId,
	}
	qr := new(mproto.QueryResult)
	if err := conn.rpcClient.Call("SqlQuery.Execute", req, qr); err != nil {
		return nil, tabletError(err)
	}
	return qr, nil
}

func (conn *TabletBson) ExecuteBatch(context interface{}, queries []tproto.BoundQuery, transactionId int64) (*tproto.QueryResultList, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.CONN_CLOSED
	}

	req := tproto.QueryList{
		Queries:       queries,
		TransactionId: transactionId,
		SessionId:     conn.sessionId,
	}
	qrs := new(tproto.QueryResultList)
	if err := conn.rpcClient.Call("SqlQuery.ExecuteBatch", req, qrs); err != nil {
		return nil, tabletError(err)
	}
	return qrs, nil
}

func (conn *TabletBson) StreamExecute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		sr := make(chan *mproto.QueryResult, 1)
		close(sr)
		return sr, func() error { return tabletconn.CONN_CLOSED }
	}

	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionId,
		SessionId:     conn.sessionId,
	}
	sr := make(chan *mproto.QueryResult, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)
	return sr, func() error { return tabletError(c.Error) }
}

func (conn *TabletBson) Begin(context interface{}) (transactionId int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return 0, tabletconn.CONN_CLOSED
	}

	req := &tproto.Session{
		SessionId: conn.sessionId,
	}
	var txInfo tproto.TransactionInfo
	err = conn.rpcClient.Call("SqlQuery.Begin", req, &txInfo)
	return txInfo.TransactionId, tabletError(err)
}

func (conn *TabletBson) Commit(context interface{}, transactionId int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.CONN_CLOSED
	}

	req := &tproto.Session{
		SessionId:     conn.sessionId,
		TransactionId: transactionId,
	}
	var noOutput rpc.UnusedResponse
	return tabletError(conn.rpcClient.Call("SqlQuery.Commit", req, &noOutput))
}

func (conn *TabletBson) Rollback(context interface{}, transactionId int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.CONN_CLOSED
	}

	req := &tproto.Session{
		SessionId:     conn.sessionId,
		TransactionId: transactionId,
	}
	var noOutput rpc.UnusedResponse
	return tabletError(conn.rpcClient.Call("SqlQuery.Rollback", req, &noOutput))
}

func (conn *TabletBson) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.rpcClient == nil {
		return
	}

	conn.sessionId = 0
	rpcClient := conn.rpcClient
	conn.rpcClient = nil
	rpcClient.Close()
}

func (conn *TabletBson) EndPoint() topo.EndPoint {
	return conn.endPoint
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
			code = tabletconn.ERR_FATAL
		case strings.HasPrefix(errStr, "retry"):
			code = tabletconn.ERR_RETRY
		case strings.HasPrefix(errStr, "tx_pool_full"):
			code = tabletconn.ERR_TX_POOL_FULL
		case strings.HasPrefix(errStr, "not_in_tx"):
			code = tabletconn.ERR_NOT_IN_TX
		default:
			code = tabletconn.ERR_NORMAL
		}
		return &tabletconn.ServerError{Code: code, Err: fmt.Sprintf("vttablet: %v", err)}
	}
	return tabletconn.OperationalError(fmt.Sprintf("vttablet: %v", err))
}
