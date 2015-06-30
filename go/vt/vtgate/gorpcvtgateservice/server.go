// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateservice provides to go rpc glue for vtgate
package gorpcvtgateservice

import (
	"flag"
	"time"

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
	vtgErr := vtg.server.Execute(ctx, query, reply)
	vtgate.AddVtGateErrorToQueryResult(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.ExecuteShard(ctx, query, reply)
	vtgate.AddVtGateErrorToQueryResult(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.ExecuteKeyspaceIds(ctx, query, reply)
	vtgate.AddVtGateErrorToQueryResult(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.ExecuteKeyRanges(ctx, query, reply)
	vtgate.AddVtGateErrorToQueryResult(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.ExecuteEntityIds(ctx, query, reply)
	vtgate.AddVtGateErrorToQueryResult(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteBatchShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.ExecuteBatchShard(ctx, batchQuery, reply)
	vtgate.AddVtGateErrorToQueryResultList(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.ExecuteBatchKeyspaceIds(ctx, batchQuery, reply)
	vtgate.AddVtGateErrorToQueryResultList(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecute(ctx, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// StreamExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecuteShard(ctx, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecuteKeyRanges(ctx, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	return vtg.server.StreamExecuteKeyspaceIds(ctx, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, noInput *rpc.Unused, outSession *proto.Session) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Begin(ctx, outSession)
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit(ctx context.Context, inSession *proto.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Commit(ctx, inSession)
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback(ctx context.Context, inSession *proto.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Rollback(ctx, inSession)
}

// Begin2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin2(ctx context.Context, request *proto.BeginRequest, reply *proto.BeginResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	reply.Session = &proto.Session{}
	vtgErr := vtg.server.Begin(ctx, reply.Session)
	vtgate.AddVtGateErrorToBeginResponse(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// Commit2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit2(ctx context.Context, request *proto.CommitRequest, reply *proto.CommitResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.Commit(ctx, request.Session)
	vtgate.AddVtGateErrorToCommitResponse(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// Rollback2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback2(ctx context.Context, request *proto.RollbackRequest, reply *proto.RollbackResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.Rollback(ctx, request.Session)
	vtgate.AddVtGateErrorToRollbackResponse(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	vtgErr := vtg.server.SplitQuery(ctx, req, reply)
	vtgate.AddVtGateErrorToSplitQueryResult(vtgErr, reply)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
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
