// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcqueryservice

import (
	"fmt"

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
	ctx = callinfo.GRPCCallInfo(ctx)
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
func (q *Query) Execute(ctx context.Context, request *pb.ExecuteRequest) (response *pb.ExecuteResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)
	return nil, fmt.Errorf("NYI")
}

// ExecuteBatch is part of the queryservice.QueryServer interface
func (q *Query) ExecuteBatch(ctx context.Context, request *pb.ExecuteBatchRequest) (response *pb.ExecuteBatchResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)
	return nil, fmt.Errorf("NYI")
}

// StreamExecute is part of the queryservice.QueryServer interface
func (q *Query) StreamExecute(request *pb.StreamExecuteRequest, stream pbs.Query_StreamExecuteServer) (err error) {
	defer q.server.HandlePanic(&err)
	/*ctx :*/ _ = callinfo.GRPCCallInfo(stream.Context())
	return fmt.Errorf("NYI")
}

// Begin is part of the queryservice.QueryServer interface
func (q *Query) Begin(ctx context.Context, request *pb.BeginRequest) (response *pb.BeginResponse, err error) {
	defer q.server.HandlePanic(&err)
	return nil, fmt.Errorf("NYI")
}

// Commit is part of the queryservice.QueryServer interface
func (q *Query) Commit(ctx context.Context, request *pb.CommitRequest) (response *pb.CommitResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)
	return nil, fmt.Errorf("NYI")
}

// Rollback is part of the queryservice.QueryServer interface
func (q *Query) Rollback(ctx context.Context, request *pb.RollbackRequest) (response *pb.RollbackResponse, err error) {
	defer q.server.HandlePanic(&err)
	return nil, fmt.Errorf("NYI")
}

// SplitQuery is part of the queryservice.QueryServer interface
func (q *Query) SplitQuery(ctx context.Context, request *pb.SplitQueryRequest) (response *pb.SplitQueryResponse, err error) {
	defer q.server.HandlePanic(&err)
	ctx = callinfo.GRPCCallInfo(ctx)
	return nil, fmt.Errorf("NYI")
}

func init() {
	tabletserver.QueryServiceControlRegisterFunctions = append(tabletserver.QueryServiceControlRegisterFunctions, func(qsc tabletserver.QueryServiceControl) {
		if servenv.GRPCCheckServiceMap("queryservice") {
			pbs.RegisterQueryServer(servenv.GRPCServer, New(qsc.QueryService()))
		}
	})
}
