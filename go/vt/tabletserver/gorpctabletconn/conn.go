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
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
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
	sessionID int64
}

// DialTablet creates and initializes TabletBson.
func DialTablet(ctx context.Context, endPoint topo.EndPoint, keyspace, shard string, timeout time.Duration) (tabletconn.TabletConn, error) {
	var addr string
	var config *tls.Config
	if *tabletBsonEncrypted {
		addr = netutil.JoinHostPort(endPoint.Host, endPoint.NamedPortMap["vts"])
		config = &tls.Config{}
		config.InsecureSkipVerify = true
	} else {
		addr = netutil.JoinHostPort(endPoint.Host, endPoint.NamedPortMap["vt"])
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
	if err = conn.rpcClient.Call(ctx, "SqlQuery.GetSessionId", tproto.SessionParams{Keyspace: keyspace, Shard: shard}, &sessionInfo); err != nil {
		conn.rpcClient.Close()
		return nil, tabletError(err)
	}
	conn.sessionID = sessionInfo.SessionId
	return conn, nil
}

func (conn *TabletBson) withTimeout(ctx context.Context, action func() error) error {
	var err error
	var errAction error
	done := make(chan int)
	go func() {
		errAction = action()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
		err = errAction
	}
	return err
}

// Execute sends the query to VTTablet.
func (conn *TabletBson) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.CONN_CLOSED
	}

	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	qr := new(mproto.QueryResult)
	action := func() error {
		return conn.rpcClient.Call(ctx, "SqlQuery.Execute", req, qr)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return qr, nil
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *TabletBson) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, transactionID int64) (*tproto.QueryResultList, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.CONN_CLOSED
	}

	req := tproto.QueryList{
		Queries:       queries,
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	qrs := new(tproto.QueryResultList)
	action := func() error {
		return conn.rpcClient.Call(ctx, "SqlQuery.ExecuteBatch", req, qrs)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return qrs, nil
}

// StreamExecute starts a streaming query to VTTablet.
func (conn *TabletBson) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, nil, tabletconn.CONN_CLOSED
	}

	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	sr := make(chan *mproto.QueryResult, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)
	firstResult, ok := <-sr
	if !ok {
		return nil, nil, tabletError(c.Error)
	}
	srout := make(chan *mproto.QueryResult, 1)
	go func() {
		defer close(srout)
		srout <- firstResult
		for r := range sr {
			srout <- r
		}
	}()
	return srout, func() error { return tabletError(c.Error) }, nil
}

// Begin starts a transaction.
func (conn *TabletBson) Begin(ctx context.Context) (transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return 0, tabletconn.CONN_CLOSED
	}

	req := &tproto.Session{
		SessionId: conn.sessionID,
	}
	var txInfo tproto.TransactionInfo
	action := func() error {
		return conn.rpcClient.Call(ctx, "SqlQuery.Begin", req, &txInfo)
	}
	err = conn.withTimeout(ctx, action)
	return txInfo.TransactionId, tabletError(err)
}

// Commit commits the ongoing transaction.
func (conn *TabletBson) Commit(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.CONN_CLOSED
	}

	req := &tproto.Session{
		SessionId:     conn.sessionID,
		TransactionId: transactionID,
	}
	action := func() error {
		return conn.rpcClient.Call(ctx, "SqlQuery.Commit", req, &rpc.Unused{})
	}
	err := conn.withTimeout(ctx, action)
	return tabletError(err)
}

// Rollback rolls back the ongoing transaction.
func (conn *TabletBson) Rollback(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.CONN_CLOSED
	}

	req := &tproto.Session{
		SessionId:     conn.sessionID,
		TransactionId: transactionID,
	}
	action := func() error {
		return conn.rpcClient.Call(ctx, "SqlQuery.Rollback", req, &rpc.Unused{})
	}
	err := conn.withTimeout(ctx, action)
	return tabletError(err)
}

// SplitQuery is the stub for SqlQuery.SplitQuery RPC
func (conn *TabletBson) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitCount int) (queries []tproto.QuerySplit, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		err = tabletconn.CONN_CLOSED
		return
	}
	req := &tproto.SplitQueryRequest{
		Query:      query,
		SplitCount: splitCount,
	}
	reply := new(tproto.SplitQueryResult)
	action := func() error {
		return conn.rpcClient.Call(ctx, "SqlQuery.SplitQuery", req, reply)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return reply.Queries, nil
}

// Close closes underlying bsonrpc.
func (conn *TabletBson) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.rpcClient == nil {
		return
	}

	conn.sessionID = 0
	rpcClient := conn.rpcClient
	conn.rpcClient = nil
	rpcClient.Close()
}

// EndPoint returns the rpc end point.
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
