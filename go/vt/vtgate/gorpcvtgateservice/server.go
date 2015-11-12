// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateservice provides to go rpc glue for vtgate
package gorpcvtgateservice

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	rpcTimeout = flag.Duration("bsonrpc_timeout", 20*time.Second, "rpc timeout")
)

// VTGate is the public structure that is exported via BSON RPC
type VTGate struct {
	server vtgateservice.VTGateService
}

func sessionToRPC(session *pbg.Session) *pbg.Session {
	if session == nil {
		return nil
	}
	if session.ShardSessions == nil {
		return &pbg.Session{
			InTransaction: session.InTransaction,
			ShardSessions: []*pbg.Session_ShardSession{},
		}
	}
	return session
}

func sessionFromRPC(session *pbg.Session) {
	if session == nil {
		return
	}
	if len(session.ShardSessions) == 0 {
		session.ShardSessions = nil
	}
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Execute(ctx context.Context, request *proto.Query, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.Execute(ctx,
		request.Sql,
		request.BindVariables,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// ExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShard(ctx context.Context, request *proto.QueryShard, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.ExecuteShards(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.Shards,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, request *proto.KeyspaceIdQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.ExecuteKeyspaceIds(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, request *proto.KeyRangeQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.ExecuteKeyRanges(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, request *proto.EntityIdsQuery, reply *proto.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.ExecuteEntityIds(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.EntityColumnName,
		proto.EntityIdsToProto(request.EntityKeyspaceIDs),
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// ExecuteBatchShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShard(ctx context.Context, request *proto.BatchQueryShard, reply *proto.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.ExecuteBatchShards(ctx,
		request.Queries,
		request.TabletType,
		request.AsTransaction,
		request.Session,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, request *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.ExecuteBatchKeyspaceIds(ctx,
		request.Queries,
		request.TabletType,
		request.AsTransaction,
		request.Session,
		reply)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(ctx context.Context, request *proto.Query, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	return vtg.server.StreamExecute(ctx,
		request.Sql,
		request.BindVariables,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
}

// StreamExecute2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute2(ctx context.Context, request *proto.Query, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecute(ctx,
		request.Sql,
		request.BindVariables,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
	if vtgErr == nil {
		return nil
	}
	// If there was an app error, send a QueryResult back with it.
	qr := new(proto.QueryResult)
	vtgate.AddVtGateError(vtgErr, &qr.Err)
	return sendReply(qr)
}

// StreamExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShard(ctx context.Context, request *proto.QueryShard, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	return vtg.server.StreamExecuteShards(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.Shards,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
}

// StreamExecuteShard2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShard2(ctx context.Context, request *proto.QueryShard, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecuteShards(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.Shards,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
	if vtgErr == nil {
		return nil
	}
	// If there was an app error, send a QueryResult back with it.
	qr := new(proto.QueryResult)
	vtgate.AddVtGateError(vtgErr, &qr.Err)
	return sendReply(qr)
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, request *proto.KeyspaceIdQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	return vtg.server.StreamExecuteKeyspaceIds(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
}

// StreamExecuteKeyspaceIds2 is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds2(ctx context.Context, request *proto.KeyspaceIdQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecuteKeyspaceIds(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
	if vtgErr == nil {
		return nil
	}
	// If there was an app error, send a QueryResult back with it.
	qr := new(proto.QueryResult)
	vtgate.AddVtGateError(vtgErr, &qr.Err)
	return sendReply(qr)
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, request *proto.KeyRangeQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	return vtg.server.StreamExecuteKeyRanges(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
}

// StreamExecuteKeyRanges2 is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges2(ctx context.Context, request *proto.KeyRangeQuery, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecuteKeyRanges(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		func(value *proto.QueryResult) error {
			return sendReply(value)
		})
	if vtgErr == nil {
		return nil
	}
	// If there was an app error, send a QueryResult back with it.
	qr := new(proto.QueryResult)
	vtgate.AddVtGateError(vtgErr, &qr.Err)
	return sendReply(qr)
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, noInput *rpc.Unused, outSession *pbg.Session) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	err = vtg.server.Begin(ctx, outSession)
	outSession = sessionToRPC(outSession)
	return err
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit(ctx context.Context, inSession *pbg.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Commit(ctx, inSession)
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback(ctx context.Context, inSession *pbg.Session, noOutput *rpc.Unused) (err error) {
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
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	// Don't pass in a nil pointer
	reply.Session = &pbg.Session{}
	vtgErr := vtg.server.Begin(ctx, reply.Session)
	reply.Session = sessionToRPC(reply.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// Commit2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit2(ctx context.Context, request *proto.CommitRequest, reply *proto.CommitResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.Commit(ctx, request.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// Rollback2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback2(ctx context.Context, request *proto.RollbackRequest, reply *proto.RollbackResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.Rollback(ctx, request.Session)
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) SplitQuery(ctx context.Context, request *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	splits, vtgErr := vtg.server.SplitQuery(ctx,
		request.Keyspace,
		request.Query.Sql,
		request.Query.BindVariables,
		request.SplitColumn,
		request.SplitCount)
	reply.Splits = splits
	vtgate.AddVtGateError(vtgErr, &reply.Err)
	return nil
}

// GetSrvKeyspace is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, request *proto.GetSrvKeyspaceRequest, reply *topodatapb.SrvKeyspace) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ks, err := vtg.server.GetSrvKeyspace(ctx, request.Keyspace)
	if err != nil {
		return err
	}
	*reply = *ks
	return nil
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
