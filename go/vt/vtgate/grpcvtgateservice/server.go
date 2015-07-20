// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtgateservice provides the gRPC glue for vtgate
package grpcvtgateservice

import (
	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
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
	query := &proto.Query{
		Sql:              string(request.Query.Sql),
		BindVariables:    tproto.Proto3ToBindVariables(request.Query.BindVariables),
		TabletType:       topo.ProtoToTabletType(request.TabletType),
		Session:          proto.ProtoToSession(request.Session),
		NotInTransaction: request.NotInTransaction,
	}
	reply := new(proto.QueryResult)
	executeErr := vtg.server.Execute(ctx, query, reply)
	response = &pb.ExecuteResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr),
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

// ExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShards(ctx context.Context, request *pb.ExecuteShardsRequest) (response *pb.ExecuteShardsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	query := &proto.QueryShard{
		Sql:              string(request.Query.Sql),
		BindVariables:    tproto.Proto3ToBindVariables(request.Query.BindVariables),
		Keyspace:         request.Keyspace,
		Shards:           request.Shards,
		TabletType:       topo.ProtoToTabletType(request.TabletType),
		Session:          proto.ProtoToSession(request.Session),
		NotInTransaction: request.NotInTransaction,
	}
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteShard(ctx, query, reply)
	response = &pb.ExecuteShardsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr),
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
	query := &proto.KeyspaceIdQuery{
		Sql:              string(request.Query.Sql),
		BindVariables:    tproto.Proto3ToBindVariables(request.Query.BindVariables),
		Keyspace:         request.Keyspace,
		KeyspaceIds:      key.ProtoToKeyspaceIds(request.KeyspaceIds),
		TabletType:       topo.ProtoToTabletType(request.TabletType),
		Session:          proto.ProtoToSession(request.Session),
		NotInTransaction: request.NotInTransaction,
	}
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteKeyspaceIds(ctx, query, reply)
	response = &pb.ExecuteKeyspaceIdsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr),
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
	query := &proto.KeyRangeQuery{
		Sql:              string(request.Query.Sql),
		BindVariables:    tproto.Proto3ToBindVariables(request.Query.BindVariables),
		Keyspace:         request.Keyspace,
		KeyRanges:        key.ProtoToKeyRanges(request.KeyRanges),
		TabletType:       topo.ProtoToTabletType(request.TabletType),
		Session:          proto.ProtoToSession(request.Session),
		NotInTransaction: request.NotInTransaction,
	}
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteKeyRanges(ctx, query, reply)
	response = &pb.ExecuteKeyRangesResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr),
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
	query := &proto.EntityIdsQuery{
		Sql:           string(request.Query.Sql),
		BindVariables: tproto.Proto3ToBindVariables(request.Query.BindVariables),
		Keyspace:      request.Keyspace,

		// FIXME
		TabletType:       topo.ProtoToTabletType(request.TabletType),
		Session:          proto.ProtoToSession(request.Session),
		NotInTransaction: request.NotInTransaction,
	}
	reply := new(proto.QueryResult)
	executeErr := vtg.server.ExecuteEntityIds(ctx, query, reply)
	response = &pb.ExecuteEntityIdsResponse{
		Error: vtgate.VtGateErrorToVtRPCError(executeErr),
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

// ExecuteBatchShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShards(ctx context.Context, request *pb.ExecuteBatchShardsRequest) (response *pb.ExecuteBatchShardsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	// FIXME
	return nil, nil
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, request *pb.ExecuteBatchKeyspaceIdsRequest) (response *pb.ExecuteBatchKeyspaceIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	// FIXME
	return nil, nil
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(request *pb.StreamExecuteRequest, stream pbs.Vitess_StreamExecuteServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	return nil
}

// StreamExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShards(request *pb.StreamExecuteShardsRequest, stream pbs.Vitess_StreamExecuteShardsServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	// FIXME
	return nil
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(request *pb.StreamExecuteKeyRangesRequest, stream pbs.Vitess_StreamExecuteKeyRangesServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	// FIXME
	return nil
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(request *pb.StreamExecuteKeyspaceIdsRequest, stream pbs.Vitess_StreamExecuteKeyspaceIdsServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	// FIXME
	return nil
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, request *pb.BeginRequest) (response *pb.BeginResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	outSession := new(proto.Session)
	beginErr := vtg.server.Begin(ctx, outSession)
	response = &pb.BeginResponse{
		Error: vtgate.VtGateErrorToVtRPCError(beginErr),
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
	commitErr := vtg.server.Commit(ctx, proto.ProtoToSession(request.Session))
	response = &pb.CommitResponse{
		Error: vtgate.VtGateErrorToVtRPCError(commitErr),
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
	rollbackErr := vtg.server.Rollback(ctx, proto.ProtoToSession(request.Session))
	response = &pb.RollbackResponse{
		Error: vtgate.VtGateErrorToVtRPCError(rollbackErr),
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
	// FIXME
	return nil, nil
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate vtgateservice.VTGateService) {
		if servenv.GRPCCheckServiceMap("vtgateservice") {
			pbs.RegisterVitessServer(servenv.GRPCServer, &VTGate{vtGate})
		}
	})
}

func RegisterForTest(s *grpc.Server, service vtgateservice.VTGateService) {
	pbs.RegisterVitessServer(s, &VTGate{service})
}
