// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctabletconn

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/vterrors"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

const protocolName = "gorpc"

var (
	tabletBsonUsername = flag.String("tablet-bson-username", "", "user to use for bson rpc connections")
	tabletBsonPassword = flag.String("tablet-bson-password", "", "password to use for bson rpc connections (ignored if username is empty)")
)

func init() {
	tabletconn.RegisterDialer(protocolName, DialTablet)
}

// TabletBson implements a bson rpcplus implementation for TabletConn
type TabletBson struct {
	// endPoint is set at construction time, and never changed
	endPoint *pbt.EndPoint

	// mu protects the next fields
	mu        sync.RWMutex
	rpcClient *rpcplus.Client
	sessionID int64
	target    *tproto.Target
}

// DialTablet creates and initializes TabletBson.
func DialTablet(ctx context.Context, endPoint *pbt.EndPoint, keyspace, shard string, tabletType pbt.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.PortMap["vt"])
	conn := &TabletBson{endPoint: endPoint}
	var err error
	if *tabletBsonUsername != "" {
		conn.rpcClient, err = bsonrpc.DialAuthHTTP("tcp", addr, *tabletBsonUsername, *tabletBsonPassword, timeout)
	} else {
		conn.rpcClient, err = bsonrpc.DialHTTP("tcp", addr, timeout)
	}
	if err != nil {
		return nil, tabletError(err)
	}

	if tabletType == pbt.TabletType_UNKNOWN {
		// we use session
		var sessionInfo tproto.SessionInfo
		if err = conn.rpcClient.Call(ctx, "SqlQuery.GetSessionId", tproto.SessionParams{Keyspace: keyspace, Shard: shard}, &sessionInfo); err != nil {
			conn.rpcClient.Close()
			return nil, tabletError(err)
		}
		// SqlQuery.GetSessionId might return an application error inside the SessionInfo
		if err = vterrors.FromRPCError(sessionInfo.Err); err != nil {
			conn.rpcClient.Close()
			return nil, tabletError(err)
		}
		conn.sessionID = sessionInfo.SessionId
	} else {
		// we use target
		conn.target = &tproto.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tproto.TabletType(tabletType),
		}
	}
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
		return nil, tabletconn.ConnClosed
	}

	req := &tproto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	qr := new(mproto.QueryResult)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.Execute", req, qr)
		if err != nil {
			return err
		}
		// SqlQuery.Execute might return an application error inside the QueryResult
		return vterrors.FromRPCError(qr.Err)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return qr, nil
}

func getEffectiveCallerID(ctx context.Context) *tproto.CallerID {
	if ef := callerid.EffectiveCallerIDFromContext(ctx); ef != nil {
		return &tproto.CallerID{
			Principal:    ef.Principal,
			Component:    ef.Component,
			Subcomponent: ef.Subcomponent,
		}
	}
	return nil
}

func getImmediateCallerID(ctx context.Context) *tproto.VTGateCallerID {
	if im := callerid.ImmediateCallerIDFromContext(ctx); im != nil {
		return &tproto.VTGateCallerID{
			Username: im.Username,
		}
	}
	return nil
}

// Execute2 should not be used now other than in tests.
// It is the CallerID enabled version of Execute
// Execute2 sends to query to VTTablet
func (conn *TabletBson) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &tproto.ExecuteRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		QueryRequest: tproto.Query{
			Sql:           query,
			BindVariables: bindVars,
			TransactionId: transactionID,
			SessionId:     conn.sessionID,
		},
		// TODO::Fill in EffectiveCallerID and ImmediateCallerID
	}
	qr := new(mproto.QueryResult)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.Execute2", req, qr)
		if err != nil {
			return err
		}
		// SqlQuery.Execute2 might return an application error inside the QueryRequest
		return vterrors.FromRPCError(qr.Err)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return qr, nil
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *TabletBson) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.ConnClosed
	}

	req := tproto.QueryList{
		Queries:       queries,
		AsTransaction: asTransaction,
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	qrs := new(tproto.QueryResultList)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.ExecuteBatch", req, qrs)
		if err != nil {
			return err
		}
		// SqlQuery.ExecuteBatch might return an application error inside the QueryResultList
		return vterrors.FromRPCError(qrs.Err)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return qrs, nil
}

// ExecuteBatch2 should not be used now other than in tests.
// It is the CallerID enabled version of ExecuteBatch
// ExecuteBatch2 sends a batch query to VTTablet
func (conn *TabletBson) ExecuteBatch2(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, tabletconn.ConnClosed
	}

	req := tproto.ExecuteBatchRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		QueryBatch: tproto.QueryList{
			Queries:       queries,
			AsTransaction: asTransaction,
			TransactionId: transactionID,
			SessionId:     conn.sessionID,
		},
		//TODO::Add CallerID information after it is passed down by context
	}
	qrs := new(tproto.QueryResultList)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.ExecuteBatch2", req, qrs)
		if err != nil {
			return err
		}
		// SqlQuery.ExecuteBatch might return an application error inside the QueryResultList
		return vterrors.FromRPCError(qrs.Err)
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
		return nil, nil, tabletconn.ConnClosed
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
	// SqlQuery.StreamExecute might return an application error inside the QueryResult
	vtErr := vterrors.FromRPCError(firstResult.Err)
	if vtErr != nil {
		return nil, nil, tabletError(vtErr)
	}
	srout := make(chan *mproto.QueryResult, 1)
	go func() {
		defer close(srout)
		srout <- firstResult
		for r := range sr {
			vtErr = vterrors.FromRPCError(r.Err)
			// If we get a QueryResult with an RPCError, that was an extra QueryResult sent by
			// the server specifically to indicate an error, and we shouldn't surface it to clients.
			if vtErr == nil {
				srout <- r
			}
		}
	}()
	// errFunc will return either an RPC-layer error or an application error, if one exists.
	// It will only return the most recent application error (i.e, from the QueryResult that
	// most recently contained an error). It will prioritize an RPC-layer error over an apperror,
	// if both exist.
	errFunc := func() error {
		rpcErr := tabletError(c.Error)
		if rpcErr != nil {
			return rpcErr
		}
		return tabletError(vtErr)
	}
	return srout, errFunc, nil
}

// StreamExecute2 starts a streaming query to VTTablet. This differs from StreamExecute in that
// it expects errors to be returned as part of the StreamExecute results.
func (conn *TabletBson) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, nil, tabletconn.ConnClosed
	}

	req := &tproto.StreamExecuteRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		Query: &tproto.Query{
			Sql:           query,
			BindVariables: bindVars,
			TransactionId: transactionID,
			SessionId:     conn.sessionID,
		},
	}
	// Use QueryResult instead of StreamExecuteResult for now, due to backwards compatability reasons.
	// It'll be easuer to migrate all end-points to using StreamExecuteResult instead of
	// maintaining a mixture of QueryResult and StreamExecuteResult channel returns.
	sr := make(chan *mproto.QueryResult, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamExecute2", req, sr)
	firstResult, ok := <-sr
	if !ok {
		return nil, nil, tabletError(c.Error)
	}
	// SqlQuery.StreamExecute might return an application error inside the QueryResult
	vtErr := vterrors.FromRPCError(firstResult.Err)
	if vtErr != nil {
		return nil, nil, tabletError(vtErr)
	}
	srout := make(chan *mproto.QueryResult, 1)
	go func() {
		defer close(srout)
		srout <- firstResult
		for r := range sr {
			vtErr = vterrors.FromRPCError(r.Err)
			// If we get a QueryResult with an RPCError, that was an extra QueryResult sent by
			// the server specifically to indicate an error, and we shouldn't surface it to clients.
			if vtErr == nil {
				srout <- r
			}
		}
	}()
	// errFunc will return either an RPC-layer error or an application error, if one exists.
	// It will only return the most recent application error (i.e, from the QueryResult that
	// most recently contained an error). It will prioritize an RPC-layer error over an apperror,
	// if both exist.
	errFunc := func() error {
		rpcErr := tabletError(c.Error)
		if rpcErr != nil {
			return rpcErr
		}
		return tabletError(vtErr)
	}
	return srout, errFunc, nil
}

// Begin starts a transaction.
func (conn *TabletBson) Begin(ctx context.Context) (transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &tproto.Session{
		SessionId: conn.sessionID,
	}
	var txInfo tproto.TransactionInfo
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.Begin", req, &txInfo)
		if err != nil {
			return err
		}
		// SqlQuery.Begin might return an application error inside the TransactionInfo
		return vterrors.FromRPCError(txInfo.Err)
	}
	err = conn.withTimeout(ctx, action)
	return txInfo.TransactionId, tabletError(err)
}

// Begin2 should not be used for anything except tests for now;
// it will eventually replace the existing Begin.
// Begin2 starts a transaction.
func (conn *TabletBson) Begin2(ctx context.Context) (transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return 0, tabletconn.ConnClosed
	}

	beginRequest := &tproto.BeginRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		SessionId:         conn.sessionID,
	}
	beginResponse := new(tproto.BeginResponse)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.Begin2", beginRequest, beginResponse)
		if err != nil {
			return err
		}
		// SqlQuery.Begin might return an application error inside the TransactionInfo
		return vterrors.FromRPCError(beginResponse.Err)
	}
	err = conn.withTimeout(ctx, action)
	return beginResponse.TransactionId, tabletError(err)
}

// Commit commits the ongoing transaction.
func (conn *TabletBson) Commit(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.ConnClosed
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

// Commit2 should not be used for anything except tests for now;
// it will eventually replace the existing Commit.
// Commit2 commits the ongoing transaction.
func (conn *TabletBson) Commit2(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.ConnClosed
	}

	commitRequest := &tproto.CommitRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		SessionId:         conn.sessionID,
		TransactionId:     transactionID,
	}
	commitResponse := new(tproto.CommitResponse)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.Commit2", commitRequest, commitResponse)
		if err != nil {
			return err
		}
		// SqlQuery.Commit might return an application error inside the ErrorOnly
		return vterrors.FromRPCError(commitResponse.Err)
	}
	err := conn.withTimeout(ctx, action)
	return tabletError(err)
}

// Rollback rolls back the ongoing transaction.
func (conn *TabletBson) Rollback(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.ConnClosed
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

// Rollback2 should not be used for anything except tests for now;
// it will eventually replace the existing Rollback.
// Rollback2 rolls back the ongoing transaction.
func (conn *TabletBson) Rollback2(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return tabletconn.ConnClosed
	}

	rollbackRequest := &tproto.RollbackRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		SessionId:         conn.sessionID,
		TransactionId:     transactionID,
	}
	rollbackResponse := new(tproto.RollbackResponse)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.Rollback2", rollbackRequest, rollbackResponse)
		if err != nil {
			return err
		}
		// SqlQuery.Rollback might return an application error inside the ErrorOnly
		return vterrors.FromRPCError(rollbackResponse.Err)
	}
	err := conn.withTimeout(ctx, action)
	return tabletError(err)
}

// SplitQuery is the stub for SqlQuery.SplitQuery RPC
func (conn *TabletBson) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitColumn string, splitCount int) (queries []tproto.QuerySplit, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		err = tabletconn.ConnClosed
		return
	}
	req := &tproto.SplitQueryRequest{
		Target:            conn.target,
		EffectiveCallerID: getEffectiveCallerID(ctx),
		ImmediateCallerID: getImmediateCallerID(ctx),
		Query:             query,
		SplitColumn:       splitColumn,
		SplitCount:        splitCount,
		SessionID:         conn.sessionID,
	}
	reply := new(tproto.SplitQueryResult)
	action := func() error {
		err := conn.rpcClient.Call(ctx, "SqlQuery.SplitQuery", req, reply)
		if err != nil {
			return err
		}
		// SqlQuery.SplitQuery might return an application error inside the SplitQueryRequest
		return vterrors.FromRPCError(reply.Err)
	}
	if err := conn.withTimeout(ctx, action); err != nil {
		return nil, tabletError(err)
	}
	return reply.Queries, nil
}

// StreamHealth is the stub for SqlQuery.StreamHealth RPC
func (conn *TabletBson) StreamHealth(ctx context.Context) (<-chan *pb.StreamHealthResponse, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.rpcClient == nil {
		return nil, nil, tabletconn.ConnClosed
	}

	result := make(chan *pb.StreamHealthResponse, 10)
	c := conn.rpcClient.StreamGo("SqlQuery.StreamHealth", &rpc.Unused{}, result)
	return result, func() error {
		return c.Error
	}, nil
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

// SetTarget can be called to change the target used for subsequent calls.
func (conn *TabletBson) SetTarget(keyspace, shard string, tabletType pbt.TabletType) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.target == nil {
		return fmt.Errorf("cannot set target on sessionId based conn")
	}
	if tabletType == pbt.TabletType_UNKNOWN {
		return fmt.Errorf("cannot set tablet type to UNKNOWN")
	}
	conn.target = &tproto.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tproto.TabletType(tabletType),
	}
	return nil
}

// EndPoint returns the rpc end point.
func (conn *TabletBson) EndPoint() *pbt.EndPoint {
	return conn.endPoint
}

func tabletError(err error) error {
	if err == nil {
		return nil
	}
	// TODO(aaijazi): tabletconn is in an intermediate state right now, where application errors
	// can be returned as rpcplus.ServerError or vterrors.VitessError. Soon, it will be standardized
	// to only VitessError.
	if ve, ok := err.(*vterrors.VitessError); ok {
		return tabletErrorFromVitessError(ve)
	}
	if _, ok := err.(rpcplus.ServerError); ok {
		var code int
		errStr := err.Error()
		switch {
		case strings.Contains(errStr, "fatal: "):
			code = tabletconn.ERR_FATAL
		case strings.Contains(errStr, "retry: "):
			code = tabletconn.ERR_RETRY
		case strings.Contains(errStr, "tx_pool_full: "):
			code = tabletconn.ERR_TX_POOL_FULL
		case strings.Contains(errStr, "not_in_tx: "):
			code = tabletconn.ERR_NOT_IN_TX
		default:
			code = tabletconn.ERR_NORMAL
		}
		return &tabletconn.ServerError{Code: code, Err: fmt.Sprintf("vttablet: %v", err)}
	}
	if err == context.Canceled {
		return tabletconn.Cancelled
	}
	return tabletconn.OperationalError(fmt.Sprintf("vttablet: %v", err))
}

func tabletErrorFromVitessError(ve *vterrors.VitessError) error {
	// see if the range is in the tablet error range
	if ve.Code >= vterrors.TabletError && ve.Code <= vterrors.UnknownTabletError {
		return &tabletconn.ServerError{
			Code: int(ve.Code - vterrors.TabletError),
			Err:  fmt.Sprintf("vttablet: %v", ve.Error()),
		}
	}

	return tabletconn.OperationalError(fmt.Sprintf("vttablet: %v", ve.Message))
}
