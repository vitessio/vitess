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
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
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
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResult{}
	vtgErr := vtg.server.Execute(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Result = mproto.QueryResultToProto3(reply.Result)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteShards(ctx context.Context, request *pb.ExecuteShardsRequest, response *pb.ExecuteShardsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResult{}
	vtgErr := vtg.server.ExecuteShards(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.Shards,
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Result = mproto.QueryResultToProto3(reply.Result)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteKeyspaceIds(ctx context.Context, request *pb.ExecuteKeyspaceIdsRequest, response *pb.ExecuteKeyspaceIdsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResult{}
	vtgErr := vtg.server.ExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyspaceIds(request.KeyspaceIds),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Result = mproto.QueryResultToProto3(reply.Result)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteKeyRanges(ctx context.Context, request *pb.ExecuteKeyRangesRequest, response *pb.ExecuteKeyRangesResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResult{}
	vtgErr := vtg.server.ExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyRanges(request.KeyRanges),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Result = mproto.QueryResultToProto3(reply.Result)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteEntityIds(ctx context.Context, request *pb.ExecuteEntityIdsRequest, response *pb.ExecuteEntityIdsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResult{}
	vtgErr := vtg.server.ExecuteEntityIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.EntityColumnName,
		proto.ProtoToEntityIds(request.EntityKeyspaceIds),
		request.TabletType,
		proto.ProtoToSession(request.Session),
		request.NotInTransaction,
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Result = mproto.QueryResultToProto3(reply.Result)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteBatchShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteBatchShards(ctx context.Context, request *pb.ExecuteBatchShardsRequest, response *pb.ExecuteBatchShardsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResultList{}
	vtgErr := vtg.server.ExecuteBatchShards(ctx,
		proto.ProtoToBoundShardQueries(request.Queries),
		request.TabletType,
		request.AsTransaction,
		proto.ProtoToSession(request.Session),
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Results = tproto.QueryResultListToProto3(reply.List)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGateP3) ExecuteBatchKeyspaceIds(ctx context.Context, request *pb.ExecuteBatchKeyspaceIdsRequest, response *pb.ExecuteBatchKeyspaceIdsResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.QueryResultList{}
	vtgErr := vtg.server.ExecuteBatchKeyspaceIds(ctx,
		proto.ProtoToBoundKeyspaceIdQueries(request.Queries),
		request.TabletType,
		request.AsTransaction,
		proto.ProtoToSession(request.Session),
		reply)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, reply.Error)
	response.Results = tproto.QueryResultListToProto3(reply.List)
	response.Session = proto.SessionToProto(reply.Session)
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// StreamExecute2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecute2(ctx context.Context, request *pb.StreamExecuteRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecute(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.TabletType,
		func(reply *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteResponse{
				Error:  vtgate.VtGateErrorToVtRPCError(nil, reply.Error),
				Result: mproto.QueryResultToProto3(reply.Result),
			})
		})
	if vtgErr == nil {
		return nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		// If there was an app error, send a QueryResult back with it.
		// Sending back errors this way is not backwards compatible. If a (new) server sends an additional
		// QueryResult with an error, and the (old) client doesn't know how to read it, it will cause
		// problems where the client will get out of sync with the number of QueryResults sent.
		// That's why this the error is only sent this way when the --rpc_errors_only_in_reply flag is set
		// (signalling that all clients are able to handle new-style errors).
		return sendReply(&pb.StreamExecuteResponse{
			Error: vtgate.VtGateErrorToVtRPCError(vtgErr, ""),
		})
	}
	return vtgErr
}

// StreamExecuteShards2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecuteShards2(ctx context.Context, request *pb.StreamExecuteShardsRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecuteShards(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		request.Shards,
		request.TabletType,
		func(reply *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteShardsResponse{
				Error:  vtgate.VtGateErrorToVtRPCError(nil, reply.Error),
				Result: mproto.QueryResultToProto3(reply.Result),
			})
		})
	if vtgErr == nil {
		return nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		// If there was an app error, send a QueryResult back with it.
		// Sending back errors this way is not backwards compatible. If a (new) server sends an additional
		// QueryResult with an error, and the (old) client doesn't know how to read it, it will cause
		// problems where the client will get out of sync with the number of QueryResults sent.
		// That's why this the error is only sent this way when the --rpc_errors_only_in_reply flag is set
		// (signalling that all clients are able to handle new-style errors).
		return sendReply(&pb.StreamExecuteShardsResponse{
			Error: vtgate.VtGateErrorToVtRPCError(vtgErr, ""),
		})
	}
	return vtgErr
}

// StreamExecuteKeyspaceIds2 is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecuteKeyspaceIds2(ctx context.Context, request *pb.StreamExecuteKeyspaceIdsRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyspaceIds(request.KeyspaceIds),
		request.TabletType,
		func(reply *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteKeyspaceIdsResponse{
				Error:  vtgate.VtGateErrorToVtRPCError(nil, reply.Error),
				Result: mproto.QueryResultToProto3(reply.Result),
			})
		})
	if vtgErr == nil {
		return nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		// If there was an app error, send a QueryResult back with it.
		// Sending back errors this way is not backwards compatible. If a (new) server sends an additional
		// QueryResult with an error, and the (old) client doesn't know how to read it, it will cause
		// problems where the client will get out of sync with the number of QueryResults sent.
		// That's why this the error is only sent this way when the --rpc_errors_only_in_reply flag is set
		// (signalling that all clients are able to handle new-style errors).
		return sendReply(&pb.StreamExecuteKeyspaceIdsResponse{
			Error: vtgate.VtGateErrorToVtRPCError(vtgErr, ""),
		})
	}
	return vtgErr
}

// StreamExecuteKeyRanges2 is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGateP3) StreamExecuteKeyRanges2(ctx context.Context, request *pb.StreamExecuteKeyRangesRequest, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.Keyspace,
		key.ProtoToKeyRanges(request.KeyRanges),
		request.TabletType,
		func(reply *proto.QueryResult) error {
			return sendReply(&pb.StreamExecuteKeyRangesResponse{
				Error:  vtgate.VtGateErrorToVtRPCError(nil, reply.Error),
				Result: mproto.QueryResultToProto3(reply.Result),
			})
		})
	if vtgErr == nil {
		return nil
	}
	if *vtgate.RPCErrorOnlyInReply {
		// If there was an app error, send a QueryResult back with it.
		// Sending back errors this way is not backwards compatible. If a (new) server sends an additional
		// QueryResult with an error, and the (old) client doesn't know how to read it, it will cause
		// problems where the client will get out of sync with the number of QueryResults sent.
		// That's why this the error is only sent this way when the --rpc_errors_only_in_reply flag is set
		// (signalling that all clients are able to handle new-style errors).
		return sendReply(&pb.StreamExecuteKeyRangesResponse{
			Error: vtgate.VtGateErrorToVtRPCError(vtgErr, ""),
		})
	}
	return vtgErr
}

// Begin2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Begin2(ctx context.Context, request *pb.BeginRequest, response *pb.BeginResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	// Don't pass in a nil pointer
	session := &proto.Session{}
	vtgErr := vtg.server.Begin(ctx, session)
	response.Session = proto.SessionToProto(session)
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, "")
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// Commit2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Commit2(ctx context.Context, request *pb.CommitRequest, response *pb.CommitResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.Commit(ctx, proto.ProtoToSession(request.Session))
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, "")
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// Rollback2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) Rollback2(ctx context.Context, request *pb.RollbackRequest, response *pb.RollbackResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.Rollback(ctx, proto.ProtoToSession(request.Session))
	response.Error = vtgate.VtGateErrorToVtRPCError(vtgErr, "")
	if *vtgate.RPCErrorOnlyInReply {
		return nil
	}
	return vtgErr
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest, response *pb.SplitQueryResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		request.CallerId,
		callerid.NewImmediateCallerID("gorpc client"))
	reply := &proto.SplitQueryResult{}
	vtgErr := vtg.server.SplitQuery(ctx,
		request.Keyspace,
		string(request.Query.Sql),
		tproto.Proto3ToBindVariables(request.Query.BindVariables),
		request.SplitColumn,
		int(request.SplitCount),
		reply)
	if vtgErr != nil {
		return vtgErr
	}
	*response = *proto.SplitQueryPartsToProto(reply.Splits)
	return nil
}

// GetSrvKeyspace is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGateP3) GetSrvKeyspace(ctx context.Context, request *pb.GetSrvKeyspaceRequest, response *pb.GetSrvKeyspaceResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ks, err := vtg.server.GetSrvKeyspace(ctx, request.Keyspace)
	if err != nil {
		return err
	}
	response.SrvKeyspace = topo.SrvKeyspaceToProto(ks)
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
