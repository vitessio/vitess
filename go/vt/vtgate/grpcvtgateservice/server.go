// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtgateservice provides the gRPC glue for vtgate
package grpcvtgateservice

import (
	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/servenv"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/vtgate"
	pbs "github.com/youtube/vitess/go/vt/proto/vtgateservice"
)

// VTGate is the public structure that is exported via gRPC
type VTGate struct {
	server vtgateservice.VTGateService
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Execute(ctx context.Context, request *pb.ExecuteRequest) (response *pb.ExecuteResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.Execute(ctx, string(request.Query.Sql), tproto.Proto3ToBindVariables(request.Query.BindVariables), request.TabletType, proto.ProtoToSession(request.Session), request.NotInTransaction, reply)
	response = &pb.ExecuteResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// ExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShards(ctx context.Context, request *pb.ExecuteShardsRequest) (response *pb.ExecuteShardsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
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
	response = &pb.ExecuteShardsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, request *pb.ExecuteKeyspaceIdsRequest) (response *pb.ExecuteKeyspaceIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyspaceIds(request.KeyspaceIds),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response = &pb.ExecuteKeyspaceIdsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, request *pb.ExecuteKeyRangesRequest) (response *pb.ExecuteKeyRangesResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyRanges(request.KeyRanges),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response = &pb.ExecuteKeyRangesResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, request *pb.ExecuteEntityIdsRequest) (response *pb.ExecuteEntityIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteEntityIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.EntityColumnName,
		proto.ProtoToEntityIds(request.EntityKeyspaceIds),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response = &pb.ExecuteEntityIdsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Result = mproto.QueryResultToProto3(reply.Result)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// ExecuteBatchShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShards(ctx context.Context, request *pb.ExecuteBatchShardsRequest) (response *pb.ExecuteBatchShardsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.QueryResultList)
	executeErr := vtg.server.ExecuteBatchShards(ctx,
		proto.ProtoToBoundShardQueries(request.Queries),
		request.TabletType,
		request.AsTransaction,
		proto.ProtoToSession(request.Session),
		reply)
	response = &pb.ExecuteBatchShardsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Results = tproto.QueryResultListToProto3(reply.List)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, request *pb.ExecuteBatchKeyspaceIdsRequest) (response *pb.ExecuteBatchKeyspaceIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.QueryResultList)
	executeErr := vtg.server.ExecuteBatchKeyspaceIds(ctx,
		proto.ProtoToBoundKeyspaceIdQueries(request.Queries),
		request.TabletType,
		request.AsTransaction,
		proto.ProtoToSession(request.Session),
		reply)
	response = &pb.ExecuteBatchKeyspaceIdsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr, reply.Error),
	}
	if executeErr == nil {
		response.Results = tproto.QueryResultListToProto3(reply.List)
		response.Session = proto.SessionToProto(reply.Session)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, executeErr
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(request *pb.StreamExecuteRequest, stream pbs.Vitess_StreamExecuteServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	return vtg.server.StreamExecute(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.TabletType,
		func(value *proto.QueryResult) error {
			return stream.Send(&pb.StreamExecuteResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
}

// StreamExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShards(request *pb.StreamExecuteShardsRequest, stream pbs.Vitess_StreamExecuteShardsServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	return vtg.server.StreamExecuteShards(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.Shards,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return stream.Send(&pb.StreamExecuteShardsResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(request *pb.StreamExecuteKeyspaceIdsRequest, stream pbs.Vitess_StreamExecuteKeyspaceIdsServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	return vtg.server.StreamExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyspaceIds(request.KeyspaceIds),
		request.TabletType,
		func(value *proto.QueryResult) error {
			return stream.Send(&pb.StreamExecuteKeyspaceIdsResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(request *pb.StreamExecuteKeyRangesRequest, stream pbs.Vitess_StreamExecuteKeyRangesServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	return vtg.server.StreamExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyRanges(request.KeyRanges),
		request.TabletType,
		func(value *proto.QueryResult) error {
			return stream.Send(&pb.StreamExecuteKeyRangesResponse{
				Result: mproto.QueryResultToProto3(value.Result),
			})
		})
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, request *pb.BeginRequest) (response *pb.BeginResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	outSession := new(proto.Session)
	beginErr := vtg.server.Begin(ctx, outSession)
	response = &pb.BeginResponse{
		Error: vtgate.VtGateErrorToVtRPCError(beginErr, ""),
	}
	if beginErr == nil {
		response.Session = proto.SessionToProto(outSession)
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, beginErr
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit(ctx context.Context, request *pb.CommitRequest) (response *pb.CommitResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	commitErr := vtg.server.Commit(ctx, proto.ProtoToSession(request.Session))
	response = &pb.CommitResponse{
		Error: vtgate.VtGateErrorToVtRPCError(commitErr, ""),
	}
	if commitErr == nil {
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, commitErr
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback(ctx context.Context, request *pb.RollbackRequest) (response *pb.RollbackResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	rollbackErr := vtg.server.Rollback(ctx, proto.ProtoToSession(request.Session))
	response = &pb.RollbackResponse{
		Error: vtgate.VtGateErrorToVtRPCError(rollbackErr, ""),
	}
	if rollbackErr == nil {
		return response, nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		return response, nil
	}
	return nil, rollbackErr
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest) (response *pb.SplitQueryResponse, err error) {

	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.CallerId,
		callerid.NewImmediateCallerID("grpc client"))
	reply := new(proto.SplitQueryResult)
	if err := vtg.server.SplitQuery(ctx,
		request.Keyspace,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.SplitColumn,
		int(request.SplitCount),
		reply); err != nil {
		return nil, err
	}
	return proto.SplitQueryPartsToProto(reply.Splits), nil
}

// GetSrvKeyspace is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, request *pb.GetSrvKeyspaceRequest) (response *pb.GetSrvKeyspaceResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	sk, err := vtg.server.GetSrvKeyspace(ctx, request.Keyspace)
	if err != nil {
		return nil, err
	}
	return &pb.GetSrvKeyspaceResponse{
		SrvKeyspace: topo.SrvKeyspaceToProto(sk),
	}, nil
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate vtgateservice.VTGateService) {
		if servenv.GRPCCheckServiceMap("vtgateservice") {
			pbs.RegisterVitessServer(servenv.GRPCServer, &VTGate{vtGate})
		}
	})
}

// RegisterForTest registers the gRPC implementation on the gRPC
// server.  Useful for unit tests only, for real use, the init()
// function does the registration.
func RegisterForTest(s *grpc.Server, service vtgateservice.VTGateService) {
	pbs.RegisterVitessServer(s, &VTGate{service})
}
