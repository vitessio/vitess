// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bsonp3vtgateservice provides to go rpc glue for vtgate, with
// BSON-encoded proto3 structs.
package bsonp3vtgateservice

import (
	"flag"
	"time"

	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	pb "github.com/youtube/vitess/go/vt/proto/vtgate"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

var (
	rpcTimeout = flag.Duration("bsonp3_timeout", 20*time.Second, "rpc timeout")
)

// VTGateP3 is the public structure that is exported via BSON RPC
type VTGateP3 struct {
	server vtgateservice.VTGateService
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Execute(ctx context.Context, request *pb.ExecuteRequest, response *pb.ExecuteResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.Execute(ctx, string(request.Query.Sql), tproto.Proto3ToBindVariables(request.Query.BindVariables), request.TabletType, proto.ProtoToSession(request.Session), request.NotInTransaction, reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// ExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteShards(ctx context.Context, request *pb.ExecuteShardsRequest, response *pb.ExecuteShardsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteShards(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.Shards,
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteKeyspaceIds(ctx context.Context, request *pb.ExecuteKeyspaceIdsRequest, response *pb.ExecuteKeyspaceIdsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteKeyRanges(ctx context.Context, request *pb.ExecuteKeyRangesRequest, response *pb.ExecuteKeyRangesResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteEntityIds(ctx context.Context, request *pb.ExecuteEntityIdsRequest, response *pb.ExecuteEntityIdsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteEntityIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.EntityColumnName,
		request.EntityKeyspaceIds,
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// ExecuteBatchShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteBatchShards(ctx context.Context, request *pb.ExecuteBatchShardsRequest, response *pb.ExecuteBatchShardsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResultList)
	executeErr := vtg.server.ExecuteBatchShards(ctx,
		proto.ProtoToBoundShardQueries(request.Queries),
		request.TabletType,
		request.AsTransaction,
		proto.ProtoToSession(request.Session),
		reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Results = tproto.QueryResultListToProto3(reply.List)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteBatchKeyspaceIds(ctx context.Context, request *pb.ExecuteBatchKeyspaceIdsRequest, response *pb.ExecuteBatchKeyspaceIdsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	reply := new(proto.QueryResultList)
	executeErr := vtg.server.ExecuteBatchKeyspaceIds(ctx,
		proto.ProtoToBoundKeyspaceIdQueries(request.Queries),
		request.TabletType,
		request.AsTransaction,
		proto.ProtoToSession(request.Session),
		reply)
	if executeErr == nil {
		response.Error = vtgate.RPCErrorToVtRPCError(reply.Err)
		response.Results = tproto.QueryResultListToProto3(reply.List)
		response.Session = proto.SessionToProto(reply.Session)
	}
	return vterrors.ToJSONError(executeErr)
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecute(ctx context.Context, request *pb.StreamExecuteRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	vtgErr := vtg.server.StreamExecute(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.TabletType,
		func(reply *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteResponse{
				Result: mproto.QueryResultToProto3(reply.Result),
			})
		})
	return vterrors.ToJSONError(vtgErr)
}

// StreamExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecuteShards(ctx context.Context, request *pb.StreamExecuteShardsRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	vtgErr := vtg.server.StreamExecuteShards(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.Shards,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteShardsResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
	return vterrors.ToJSONError(vtgErr)
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecuteKeyspaceIds(ctx context.Context, request *pb.StreamExecuteKeyspaceIdsRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	vtgErr := vtg.server.StreamExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteKeyspaceIdsResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
	return vterrors.ToJSONError(vtgErr)
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecuteKeyRanges(ctx context.Context, request *pb.StreamExecuteKeyRangesRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	vtgErr := vtg.server.StreamExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteKeyRangesResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
	return vterrors.ToJSONError(vtgErr)
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Begin(ctx context.Context, request *pb.BeginRequest, response *pb.BeginResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	outSession := new(proto.Session)
	vtgErr := vtg.server.Begin(ctx, outSession)
	if vtgErr == nil {
		response.Session = proto.SessionToProto(outSession)
		return nil
	}
	return vterrors.ToJSONError(vtgErr)
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Commit(ctx context.Context, request *pb.CommitRequest, response *pb.CommitResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	vtgErr := vtg.server.Commit(ctx, proto.ProtoToSession(request.Session))
	return vterrors.ToJSONError(vtgErr)
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Rollback(ctx context.Context, request *pb.RollbackRequest, response *pb.RollbackResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	vtgErr := vtg.server.Rollback(ctx, proto.ProtoToSession(request.Session))
	return vterrors.ToJSONError(vtgErr)
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest, response *pb.SplitQueryResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("bsonp3 client"))
	splits, vtgErr := vtg.server.SplitQuery(ctx,
		request.Keyspace,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.SplitColumn,
		int(request.SplitCount))
	if vtgErr != nil {
		return vterrors.ToJSONError(vtgErr)
	}
	response.Splits = splits
	return nil
}

// GetSrvKeyspace is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) GetSrvKeyspace(ctx context.Context, request *pb.GetSrvKeyspaceRequest, response *pb.GetSrvKeyspaceResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	sk, vtgErr := vtg.server.GetSrvKeyspace(ctx, request.Keyspace)
	if vtgErr != nil {
		return vterrors.ToJSONError(vtgErr)
	}
	response.SrvKeyspace = sk
	return nil
}

// New returns a new VTGateP3 service
func New(vtGate vtgateservice.VTGateService) *VTGateP3 {
	return &VTGateP3{vtGate}
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate vtgateservice.VTGateService) {
		// Using same name as regular gorpc, so they're enabled/disabled together.
		servenv.Register("vtgateservice", New(vtGate))
	})
}
