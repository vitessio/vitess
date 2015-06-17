// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcqueryservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbs "github.com/youtube/vitess/go/vt/proto/queryservice"
)

// Query is the gRPC query service implementation.
// It implements the queryservice.QueryServer interface.
type Query struct {
	server queryservice.QueryService
}

// New returns a new server. It is public for unit tests to use.
func New(server queryservice.QueryService) *Query {
	return &Query{server}
}

// GetSessionId is part of the queryservice.QueryServer interface
func (q *Query) GetSessionId(ctx context.Context, request *pb.GetSessionIdRequest) (response *pb.GetSessionIdResponse, err error) {
	defer q.server.HandlePanic(&err)

	sessionInfo := new(proto.SessionInfo)
	gsiErr := q.server.GetSessionId(&proto.SessionParams{
		Keyspace: request.Keyspace,
		Shard:    request.Shard,
	}, sessionInfo)

	response = &pb.GetSessionIdResponse{
		SessionId: sessionInfo.SessionId,
	}
	tabletserver.AddTabletErrorToResult(gsiErr, &response.Error)
	return response, nil
}

// Execute is part of the queryservice.QueryServer interface
func (q *Query) Execute(ctx context.Context, request *pb.ExecuteRequest) (response *pb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)

	reply := new(mproto.QueryResult)
	execErr := q.server.Execute(ctx, &proto.Query{
		Sql:           string(request.Query.Sql),
		BindVariables: proto.Proto3ToBindVariables(request.Query.BindVariables),
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	}, reply)
	if execErr != nil {
		response := new(pb.ExecuteResponse)
		tabletserver.AddTabletErrorToResult(execErr, &response.Error)
		return response, nil
	}
	return &pb.ExecuteResponse{
		Result: proto.QueryResultToProto3(reply),
	}, nil
}

// ExecuteBatch is part of the queryservice.QueryServer interface
func (q *Query) ExecuteBatch(ctx context.Context, request *pb.ExecuteBatchRequest) (response *pb.ExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)

	reply := new(proto.QueryResultList)
	execErr := q.server.ExecuteBatch(ctx, &proto.QueryList{
		Queries:       proto.Proto3ToBoundQueryList(request.Queries),
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	}, reply)
	if execErr != nil {
		response := new(pb.ExecuteBatchResponse)
		tabletserver.AddTabletErrorToResult(execErr, &response.Error)
		return response, nil
	}
	return &pb.ExecuteBatchResponse{
		Results: proto.QueryResultListToProto3(reply.List),
	}, nil
}

// StreamExecute is part of the queryservice.QueryServer interface
func (q *Query) StreamExecute(request *pb.StreamExecuteRequest, stream pbs.Query_StreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callinfo.GRPCCallInfo(stream.Context())

	seErr := q.server.StreamExecute(ctx, &proto.Query{
		Sql:           string(request.Query.Sql),
		BindVariables: proto.Proto3ToBindVariables(request.Query.BindVariables),
		SessionId:     request.SessionId,
	}, func(reply *mproto.QueryResult) error {
		return stream.Send(&pb.StreamExecuteResponse{
			Result: proto.QueryResultToProto3(reply),
		})
	})
	if seErr != nil {
		response := new(pb.StreamExecuteResponse)
		tabletserver.AddTabletErrorToResult(seErr, &response.Error)
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

// Begin is part of the queryservice.QueryServer interface
func (q *Query) Begin(ctx context.Context, request *pb.BeginRequest) (response *pb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)

	txInfo := new(proto.TransactionInfo)
	if beginErr := q.server.Begin(ctx, &proto.Session{
		SessionId: request.SessionId,
	}, txInfo); beginErr != nil {
		response := new(pb.BeginResponse)
		tabletserver.AddTabletErrorToResult(beginErr, &response.Error)
		return response, nil
	}

	return &pb.BeginResponse{
		TransactionId: txInfo.TransactionId,
	}, nil
}

// Commit is part of the queryservice.QueryServer interface
func (q *Query) Commit(ctx context.Context, request *pb.CommitRequest) (response *pb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)

	commitErr := q.server.Commit(ctx, &proto.Session{
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	})
	response = new(pb.CommitResponse)
	tabletserver.AddTabletErrorToResult(commitErr, &response.Error)
	return response, nil
}

// Rollback is part of the queryservice.QueryServer interface
func (q *Query) Rollback(ctx context.Context, request *pb.RollbackRequest) (response *pb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)

	rollbackErr := q.server.Rollback(ctx, &proto.Session{
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	})

	response = new(pb.RollbackResponse)
	tabletserver.AddTabletErrorToResult(rollbackErr, &response.Error)
	return response, nil
}

// SplitQuery is part of the queryservice.QueryServer interface
func (q *Query) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest) (response *pb.SplitQueryResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)

	reply := &proto.SplitQueryResult{}
	if sqErr := q.server.SplitQuery(ctx, &proto.SplitQueryRequest{
		Query:       *proto.Proto3ToBoundQuery(request.Query),
		SplitColumn: request.SplitColumn,
		SplitCount:  int(request.SplitCount),
		SessionID:   request.SessionId,
	}, reply); sqErr != nil {
		response = new(pb.SplitQueryResponse)
		tabletserver.AddTabletErrorToResult(sqErr, &response.Error)
		return response, nil
	}
	return &pb.SplitQueryResponse{
		Queries: proto.QuerySplitsToProto3(reply.Queries),
	}, nil
}

func init() {
	tabletserver.QueryServiceControlRegisterFunctions = append(tabletserver.QueryServiceControlRegisterFunctions, func(qsc tabletserver.QueryServiceControl) {
		if servenv.GRPCCheckServiceMap("queryservice") {
			pbs.RegisterQueryServer(servenv.GRPCServer, New(qsc.QueryService()))
		}
	})
}
