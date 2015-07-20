// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcqueryservice

import (
	"sync"

	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbs "github.com/youtube/vitess/go/vt/proto/queryservice"
)

// query is the gRPC query service implementation.
// It implements the queryservice.QueryServer interface.
type query struct {
	server queryservice.QueryService
}

// GetSessionId is part of the queryservice.QueryServer interface
func (q *query) GetSessionId(ctx context.Context, request *pb.GetSessionIdRequest) (response *pb.GetSessionIdResponse, err error) {
	defer q.server.HandlePanic(&err)

	sessionInfo := new(proto.SessionInfo)
	gsiErr := q.server.GetSessionId(&proto.SessionParams{
		Keyspace: request.Keyspace,
		Shard:    request.Shard,
	}, sessionInfo)

	return &pb.GetSessionIdResponse{
		SessionId: sessionInfo.SessionId,
		Error:     tabletserver.TabletErrorToRPCError(gsiErr),
	}, nil
}

// Execute is part of the queryservice.QueryServer interface
func (q *query) Execute(ctx context.Context, request *pb.ExecuteRequest) (response *pb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	reply := new(mproto.QueryResult)
	execErr := q.server.Execute(ctx, &proto.Query{
		Sql:           string(request.Query.Sql),
		BindVariables: proto.Proto3ToBindVariables(request.Query.BindVariables),
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	}, reply)
	if execErr != nil {
		return &pb.ExecuteResponse{
			Error: tabletserver.TabletErrorToRPCError(execErr),
		}, nil
	}
	return &pb.ExecuteResponse{
		Result: mproto.QueryResultToProto3(reply),
	}, nil
}

// ExecuteBatch is part of the queryservice.QueryServer interface
func (q *query) ExecuteBatch(ctx context.Context, request *pb.ExecuteBatchRequest) (response *pb.ExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	reply := new(proto.QueryResultList)
	execErr := q.server.ExecuteBatch(ctx, &proto.QueryList{
		Queries:       proto.Proto3ToBoundQueryList(request.Queries),
		SessionId:     request.SessionId,
		AsTransaction: request.AsTransaction,
		TransactionId: request.TransactionId,
	}, reply)
	if execErr != nil {
		return &pb.ExecuteBatchResponse{
			Error: tabletserver.TabletErrorToRPCError(execErr),
		}, nil
	}
	return &pb.ExecuteBatchResponse{
		Results: proto.QueryResultListToProto3(reply.List),
	}, nil
}

// StreamExecute is part of the queryservice.QueryServer interface
func (q *query) StreamExecute(request *pb.StreamExecuteRequest, stream pbs.Query_StreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	seErr := q.server.StreamExecute(ctx, &proto.Query{
		Sql:           string(request.Query.Sql),
		BindVariables: proto.Proto3ToBindVariables(request.Query.BindVariables),
		SessionId:     request.SessionId,
	}, func(reply *mproto.QueryResult) error {
		return stream.Send(&pb.StreamExecuteResponse{
			Result: mproto.QueryResultToProto3(reply),
		})
	})
	if seErr != nil {
		response := &pb.StreamExecuteResponse{
			Error: tabletserver.TabletErrorToRPCError(seErr),
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

// Begin is part of the queryservice.QueryServer interface
func (q *query) Begin(ctx context.Context, request *pb.BeginRequest) (response *pb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	txInfo := new(proto.TransactionInfo)
	if beginErr := q.server.Begin(ctx, &proto.Session{
		SessionId: request.SessionId,
	}, txInfo); beginErr != nil {
		return &pb.BeginResponse{
			Error: tabletserver.TabletErrorToRPCError(beginErr),
		}, nil
	}

	return &pb.BeginResponse{
		TransactionId: txInfo.TransactionId,
	}, nil
}

// Commit is part of the queryservice.QueryServer interface
func (q *query) Commit(ctx context.Context, request *pb.CommitRequest) (response *pb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	commitErr := q.server.Commit(ctx, &proto.Session{
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	})
	return &pb.CommitResponse{
		Error: tabletserver.TabletErrorToRPCError(commitErr),
	}, nil
}

// Rollback is part of the queryservice.QueryServer interface
func (q *query) Rollback(ctx context.Context, request *pb.RollbackRequest) (response *pb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	rollbackErr := q.server.Rollback(ctx, &proto.Session{
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	})

	return &pb.RollbackResponse{
		Error: tabletserver.TabletErrorToRPCError(rollbackErr),
	}, nil
}

// SplitQuery is part of the queryservice.QueryServer interface
func (q *query) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest) (response *pb.SplitQueryResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		callerid.GRPCEffectiveCallerID(request.GetEffectiveCallerId()),
		callerid.GRPCImmediateCallerID(request.GetImmediateCallerId()),
	)
	reply := &proto.SplitQueryResult{}
	if sqErr := q.server.SplitQuery(ctx, &proto.SplitQueryRequest{
		Query:       *proto.Proto3ToBoundQuery(request.Query),
		SplitColumn: request.SplitColumn,
		SplitCount:  int(request.SplitCount),
		SessionID:   request.SessionId,
	}, reply); sqErr != nil {
		return &pb.SplitQueryResponse{
			Error: tabletserver.TabletErrorToRPCError(sqErr),
		}, nil
	}
	return &pb.SplitQueryResponse{
		Queries: proto.QuerySplitsToProto3(reply.Queries),
	}, nil
}

// StreamHealth is part of the queryservice.QueryServer interface
func (q *query) StreamHealth(request *pb.StreamHealthRequest, stream pbs.Query_StreamHealthServer) (err error) {
	defer q.server.HandlePanic(&err)

	c := make(chan *pb.StreamHealthResponse, 10)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for shr := range c {
			// we send until the client disconnects
			if err := stream.Send(shr); err != nil {
				return
			}
		}
	}()

	id, err := q.server.StreamHealthRegister(c)
	if err != nil {
		close(c)
		wg.Wait()
		return err
	}
	wg.Wait()
	return q.server.StreamHealthUnregister(id)
}

func init() {
	tabletserver.QueryServiceControlRegisterFunctions = append(tabletserver.QueryServiceControlRegisterFunctions, func(qsc tabletserver.QueryServiceControl) {
		if servenv.GRPCCheckServiceMap("queryservice") {
			pbs.RegisterQueryServer(servenv.GRPCServer, &query{qsc.QueryService()})
		}
	})
}

// RegisterForTest should only be used by unit tests
func RegisterForTest(s *grpc.Server, server queryservice.QueryService) {
	pbs.RegisterQueryServer(s, &query{server})
}
