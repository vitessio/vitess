// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateservice provides to go rpc glue for vtgate
package gorpcvtgateservice

import (
	"flag"
	"time"

	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"
)

var (
	rpcTimeout = flag.Duration("bsonrpc_timeout", 20*time.Second, "rpc timeout")
)

// VTGate is the public structure that is exported via BSON RPC
type VTGate struct {
	server vtgateservice.VTGateService
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Execute(callinfo.RPCWrapCallInfo(ctx), query, reply)
}

// ExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.ExecuteShard(callinfo.RPCWrapCallInfo(ctx), query, reply)
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.ExecuteKeyspaceIds(callinfo.RPCWrapCallInfo(ctx), query, reply)
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.ExecuteKeyRanges(callinfo.RPCWrapCallInfo(ctx), query, reply)
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.ExecuteEntityIds(callinfo.RPCWrapCallInfo(ctx), query, reply)
}

// ExecuteBatchShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.ExecuteBatchShard(callinfo.RPCWrapCallInfo(ctx), batchQuery, reply)
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.ExecuteBatchKeyspaceIds(callinfo.RPCWrapCallInfo(ctx), batchQuery, reply)
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecute(callinfo.RPCWrapCallInfo(ctx), query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// StreamExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecuteShard(callinfo.RPCWrapCallInfo(ctx), query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecuteKeyRanges(callinfo.RPCWrapCallInfo(ctx), query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecuteKeyspaceIds(callinfo.RPCWrapCallInfo(ctx), query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, noInput *rpc.Unused, outSession *proto.Session) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Begin(callinfo.RPCWrapCallInfo(ctx), outSession)
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit(ctx context.Context, inSession *proto.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Commit(callinfo.RPCWrapCallInfo(ctx), inSession)
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback(ctx context.Context, inSession *proto.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Rollback(callinfo.RPCWrapCallInfo(ctx), inSession)
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.SplitQuery(callinfo.RPCWrapCallInfo(ctx), req, reply)
}

// New returns a new VTGate service
func New(vtGate vtgateservice.VTGateService) *VTGate {
	return &VTGate{vtGate}
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate vtgateservice.VTGateService) {
		servenv.Register("vtgateservice", New(vtGate))
	})
}
