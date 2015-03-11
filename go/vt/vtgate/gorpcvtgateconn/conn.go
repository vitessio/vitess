// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateconn provides go rpc connectivity for VTGate.
package gorpcvtgateconn

import (
	"errors"
	"strings"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"
)

func init() {
	vtgateconn.RegisterDialer("gorpc", dial)
}

type vtgateConn struct {
	rpcConn *rpcplus.Client
}

func dial(ctx context.Context, address string, timeout time.Duration) (vtgateconn.VTGateConn, error) {
	network := "tcp"
	if strings.Contains(address, "/") {
		network = "unix"
	}
	rpcConn, err := bsonrpc.DialHTTP(network, address, timeout, nil)
	if err != nil {
		return nil, err
	}
	return &vtgateConn{rpcConn: rpcConn}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	r, _, err := conn.execute(ctx, query, bindVars, tabletType, nil)
	return r, err
}

func (conn *vtgateConn) execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType, session *proto.Session) (*mproto.QueryResult, *proto.Session, error) {
	request := proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       session,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.Execute", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	r, _, err := conn.executeShard(ctx, query, keyspace, shards, bindVars, tabletType, nil)
	return r, err
}

func (conn *vtgateConn) executeShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType, session *proto.Session) (*mproto.QueryResult, *proto.Session, error) {
	request := proto.QueryShard{
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		Shards:        shards,
		TabletType:    tabletType,
		Session:       session,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteShard", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	req := &proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *proto.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecute", req, sr)
	srout := make(chan *mproto.QueryResult, 1)
	go func() {
		defer close(srout)
		for r := range sr {
			srout <- r.Result
		}
	}()
	return srout, func() error { return c.Error }
}

func (conn *vtgateConn) Begin(ctx context.Context) (vtgateconn.VTGateTx, error) {
	tx := &vtgateTx{conn: conn, session: &proto.Session{}}
	if err := conn.rpcConn.Call(ctx, "VTGate.Begin", &rpc.Unused{}, tx.session); err != nil {
		return nil, err
	}
	return tx, nil
}

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitCount int) ([]proto.SplitQueryPart, error) {
	request := &proto.SplitQueryRequest{
		Keyspace:   keyspace,
		Query:      query,
		SplitCount: splitCount,
	}
	result := &proto.SplitQueryResult{}
	if err := conn.rpcConn.Call(ctx, "VTGate.SplitQuery", request, result); err != nil {
		return nil, err
	}
	return result.Splits, nil
}

func (conn *vtgateConn) Close() {
	conn.rpcConn.Close()
}

type vtgateTx struct {
	conn    *vtgateConn
	session *proto.Session
}

func (tx *vtgateTx) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, errors.New("execute: not in transaction")
	}
	r, session, err := tx.conn.execute(ctx, query, bindVars, tabletType, tx.session)
	tx.session = session
	return r, err
}

func (tx *vtgateTx) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, errors.New("executeShard: not in transaction")
	}
	r, session, err := tx.conn.executeShard(ctx, query, keyspace, shards, bindVars, tabletType, tx.session)
	tx.session = session
	return r, err
}

func (tx *vtgateTx) Commit(ctx context.Context) error {
	if tx.session == nil {
		return errors.New("commit: not in transaction")
	}
	defer func() { tx.session = nil }()
	return tx.conn.rpcConn.Call(ctx, "VTGate.Commit", tx.session, &rpc.Unused{})
}

func (tx *vtgateTx) Rollback(ctx context.Context) error {
	if tx.session == nil {
		return nil
	}
	defer func() { tx.session = nil }()
	return tx.conn.rpcConn.Call(ctx, "VTGate.Rollback", tx.session, &rpc.Unused{})
}
