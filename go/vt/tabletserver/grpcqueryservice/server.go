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
	if err := q.server.GetSessionId(&proto.SessionParams{
		Keyspace: request.Keyspace,
		Shard:    request.Shard,
	}, sessionInfo); err != nil {
		return nil, err
	}

	return &pb.GetSessionIdResponse{
		SessionId: sessionInfo.SessionId,
	}, nil
}

// Execute is part of the queryservice.QueryServer interface
func (q *query) Execute(ctx context.Context, request *pb.ExecuteRequest) (response *pb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	reply := new(mproto.QueryResult)
	if err := q.server.Execute(ctx, request.Target, &proto.Query{
		Sql:           string(request.Query.Sql),
		BindVariables: proto.Proto3ToBindVariables(request.Query.BindVariables),
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	}, reply); err != nil {
		return nil, err
	}
	return &pb.ExecuteResponse{
		Result: mproto.QueryResultToProto3(reply),
	}, nil
}

// ExecuteBatch is part of the queryservice.QueryServer interface
func (q *query) ExecuteBatch(ctx context.Context, request *pb.ExecuteBatchRequest) (response *pb.ExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	reply := new(proto.QueryResultList)
	if err := q.server.ExecuteBatch(ctx, request.Target, &proto.QueryList{
		Queries:       proto.Proto3ToBoundQueryList(request.Queries),
		SessionId:     request.SessionId,
		AsTransaction: request.AsTransaction,
		TransactionId: request.TransactionId,
	}, reply); err != nil {
		return nil, err
	}
	return &pb.ExecuteBatchResponse{
		Results: proto.QueryResultListToProto3(reply.List),
	}, nil
}

// StreamExecute is part of the queryservice.QueryServer interface
func (q *query) StreamExecute(request *pb.StreamExecuteRequest, stream pbs.Query_StreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	ctx := callerid.NewContext(callinfo.GRPCCallInfo(stream.Context()),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	return q.server.StreamExecute(ctx, request.Target, &proto.Query{
		Sql:           string(request.Query.Sql),
		BindVariables: proto.Proto3ToBindVariables(request.Query.BindVariables),
		SessionId:     request.SessionId,
	}, func(reply *mproto.QueryResult) error {
		return stream.Send(&pb.StreamExecuteResponse{
			Result: mproto.QueryResultToProto3(reply),
		})
	})
}

// Begin is part of the queryservice.QueryServer interface
func (q *query) Begin(ctx context.Context, request *pb.BeginRequest) (response *pb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	txInfo := new(proto.TransactionInfo)
	if err := q.server.Begin(ctx, request.Target, &proto.Session{
		SessionId: request.SessionId,
	}, txInfo); err != nil {
		return nil, err
	}

	return &pb.BeginResponse{
		TransactionId: txInfo.TransactionId,
	}, nil
}

// Commit is part of the queryservice.QueryServer interface
func (q *query) Commit(ctx context.Context, request *pb.CommitRequest) (response *pb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.Commit(ctx, request.Target, &proto.Session{
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	}); err != nil {
		return nil, err
	}
	return &pb.CommitResponse{}, nil
}

// Rollback is part of the queryservice.QueryServer interface
func (q *query) Rollback(ctx context.Context, request *pb.RollbackRequest) (response *pb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	if err := q.server.Rollback(ctx, request.Target, &proto.Session{
		SessionId:     request.SessionId,
		TransactionId: request.TransactionId,
	}); err != nil {
		return nil, err
	}

	return &pb.RollbackResponse{}, nil
}

// SplitQuery is part of the queryservice.QueryServer interface
func (q *query) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest) (response *pb.SplitQueryResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		request.EffectiveCallerId,
		request.ImmediateCallerId,
	)
	reply := &proto.SplitQueryResult{}
	if err := q.server.SplitQuery(ctx, request.Target, &proto.SplitQueryRequest{
		Query:       *proto.Proto3ToBoundQuery(request.Query),
		SplitColumn: request.SplitColumn,
		SplitCount:  int(request.SplitCount),
		SessionID:   request.SessionId,
	}, reply); err != nil {
		return nil, err
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
