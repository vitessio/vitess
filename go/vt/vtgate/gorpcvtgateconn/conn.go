// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateconn provides go rpc connectivity for VTGate.
package gorpcvtgateconn

import (
	"fmt"
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
	session *proto.Session
	address string
	timeout time.Duration
}

func dial(ctx context.Context, address string, timeout time.Duration) (vtgateconn.VTGateConn, error) {
	var network string
	if strings.Contains(address, "/") {
		network = "unix"
	} else {
		network = "tcp"
	}
	conn := &vtgateConn{
		address: address,
		timeout: timeout,
	}
	var err error
	conn.rpcConn, err = bsonrpc.DialHTTP(network, address, timeout, nil)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	request := proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       conn.session,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.Execute", request, &result); err != nil {
		return nil, fmt.Errorf("execute: %v", err)
	}
	conn.session = result.Session
	if result.Error != "" {
		return nil, fmt.Errorf("execute: %s", result.Error)
	}
	return result.Result, nil
}

func (conn *vtgateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	request := proto.QueryShard{
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		Shards:        shards,
		TabletType:    tabletType,
		Session:       conn.session,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteShard", request, &result); err != nil {
		return nil, fmt.Errorf("execute: %v", err)
	}
	conn.session = result.Session
	if result.Error != "" {
		return nil, fmt.Errorf("execute shards: %s", result.Error)
	}
	return result.Result, nil
}

func (conn *vtgateConn) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, tabletType topo.TabletType) (*tproto.QueryResultList, error) {
	return nil, fmt.Errorf("not implemented yet")
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	req := &proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       conn.session,
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

func (conn *vtgateConn) Begin(ctx context.Context) error {
	if conn.session != nil {
		return fmt.Errorf("begin: already in a transaction")
	}
	session := &proto.Session{}
	if err := conn.rpcConn.Call(ctx, "VTGate.Begin", &rpc.Unused{}, session); err != nil {
		return fmt.Errorf("begin: %v", err)
	}
	conn.session = session
	return nil
}

func (conn *vtgateConn) Commit(ctx context.Context) error {
	if conn.session == nil {
		return fmt.Errorf("commit: not in transaction")
	}
	defer func() { conn.session = nil }()
	if err := conn.rpcConn.Call(ctx, "VTGate.Commit", conn.session, &rpc.Unused{}); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	return nil
}

func (conn *vtgateConn) Rollback(ctx context.Context) error {
	if conn.session == nil {
		return nil
	}
	defer func() { conn.session = nil }()
	if err := conn.rpcConn.Call(ctx, "VTGate.Rollback", conn.session, &rpc.Unused{}); err != nil {
		return fmt.Errorf("rollback: %v", err)
	}
	return nil
}

func (conn *vtgateConn) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitCount int) ([]tproto.QuerySplit, error) {
	return nil, fmt.Errorf("not implemented yet")
}

func (conn *vtgateConn) Close() {
	if conn.session != nil {
		conn.Rollback(context.Background())
	}
	conn.rpcConn.Close()
}
