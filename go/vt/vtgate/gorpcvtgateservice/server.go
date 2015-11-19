// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateservice provides to go rpc glue for vtgate
package gorpcvtgateservice

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/gorpcvtgatecommon"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	rpcTimeout = flag.Duration("bsonrpc_timeout", 20*time.Second, "rpc timeout")
)

// VTGate is the public structure that is exported via BSON RPC
type VTGate struct {
	server vtgateservice.VTGateService
}

func sessionToRPC(session *vtgatepb.Session) *vtgatepb.Session {
	if session == nil {
		return nil
	}
	if session.ShardSessions == nil {
		return &vtgatepb.Session{
			InTransaction: session.InTransaction,
			ShardSessions: []*vtgatepb.Session_ShardSession{},
		}
	}
	return session
}

func sessionFromRPC(session *vtgatepb.Session) {
	if session == nil {
		return
	}
	if len(session.ShardSessions) == 0 {
		session.ShardSessions = nil
	}
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Execute(ctx context.Context, request *gorpcvtgatecommon.Query, reply *gorpcvtgatecommon.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	var vtgErr error
	reply.Result, vtgErr = vtg.server.Execute(ctx,
		request.Sql,
		request.BindVariables,
		request.TabletType,
		request.Session,
		request.NotInTransaction)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// ExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShard(ctx context.Context, request *gorpcvtgatecommon.QueryShard, reply *gorpcvtgatecommon.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	var vtgErr error
	reply.Result, vtgErr = vtg.server.ExecuteShards(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.Shards,
		request.TabletType,
		request.Session,
		request.NotInTransaction)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, request *gorpcvtgatecommon.KeyspaceIdQuery, reply *gorpcvtgatecommon.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	var vtgErr error
	reply.Result, vtgErr = vtg.server.ExecuteKeyspaceIds(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		request.Session,
		request.NotInTransaction)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, request *gorpcvtgatecommon.KeyRangeQuery, reply *gorpcvtgatecommon.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	var vtgErr error
	reply.Result, vtgErr = vtg.server.ExecuteKeyRanges(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		request.Session,
		request.NotInTransaction)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, request *gorpcvtgatecommon.EntityIdsQuery, reply *gorpcvtgatecommon.QueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	var vtgErr error
	reply.Result, vtgErr = vtg.server.ExecuteEntityIds(ctx,
		request.Sql,
		request.BindVariables,
		request.Keyspace,
		request.EntityColumnName,
		gorpcvtgatecommon.EntityIdsToProto(request.EntityKeyspaceIDs),
		request.TabletType,
		request.Session,
		request.NotInTransaction)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// ExecuteBatchShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShard(ctx context.Context, request *gorpcvtgatecommon.BatchQueryShard, reply *gorpcvtgatecommon.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	qs, err := gorpcvtgatecommon.BoundShardQueriesToProto(request.Queries)
	if err != nil {
		return err
	}
	var vtgErr error
	reply.List, vtgErr = vtg.server.ExecuteBatchShards(ctx,
		qs,
		request.TabletType,
		request.AsTransaction,
		request.Session)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, request *gorpcvtgatecommon.KeyspaceIdBatchQuery, reply *gorpcvtgatecommon.QueryResultList) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	qs, err := gorpcvtgatecommon.BoundKeyspaceIdQueriesToProto(request.Queries)
	if err != nil {
		return err
	}
	var vtgErr error
	reply.List, vtgErr = vtg.server.ExecuteBatchKeyspaceIds(ctx,
		qs,
		request.TabletType,
		request.AsTransaction,
		request.Session)
	reply.Session = sessionToRPC(request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(ctx context.Context, request *gorpcvtgatecommon.Query, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	return vtg.server.StreamExecute(ctx,
		request.Sql,
		request.BindVariables,
		request.TabletType,
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
}

// StreamExecute2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute2(ctx context.Context, request *gorpcvtgatecommon.Query, sendReply func(interface{}) error) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	vtgErr := vtg.server.StreamExecute(ctx,
		request.Sql,
		request.BindVariables,
		request.TabletType,
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
	if vtgErr == nil {
		return nil
	}

	// If there was an app error, send a QueryResult back with it.
	return sendReply(&gorpcvtgatecommon.QueryResult{
		Err: vterrors.RPCErrFromVtError(vtgErr),
	})
}

// StreamExecuteShard is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShard(ctx context.Context, request *gorpcvtgatecommon.QueryShard, sendReply func(interface{}) error) (err error) {
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
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
}

// StreamExecuteShard2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShard2(ctx context.Context, request *gorpcvtgatecommon.QueryShard, sendReply func(interface{}) error) (err error) {
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
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
	if vtgErr == nil {
		return nil
	}

	// If there was an app error, send a QueryResult back with it.
	return sendReply(&gorpcvtgatecommon.QueryResult{
		Err: vterrors.RPCErrFromVtError(vtgErr),
	})
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(ctx context.Context, request *gorpcvtgatecommon.KeyspaceIdQuery, sendReply func(interface{}) error) (err error) {
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
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
}

// StreamExecuteKeyspaceIds2 is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds2(ctx context.Context, request *gorpcvtgatecommon.KeyspaceIdQuery, sendReply func(interface{}) error) (err error) {
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
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
	if vtgErr == nil {
		return nil
	}

	// If there was an app error, send a QueryResult back with it.
	return sendReply(&gorpcvtgatecommon.QueryResult{
		Err: vterrors.RPCErrFromVtError(vtgErr),
	})
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(ctx context.Context, request *gorpcvtgatecommon.KeyRangeQuery, sendReply func(interface{}) error) (err error) {
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
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
}

// StreamExecuteKeyRanges2 is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges2(ctx context.Context, request *gorpcvtgatecommon.KeyRangeQuery, sendReply func(interface{}) error) (err error) {
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
		func(value *sqltypes.Result) error {
			return sendReply(&gorpcvtgatecommon.QueryResult{
				Result: value,
			})
		})
	if vtgErr == nil {
		return nil
	}

	// If there was an app error, send a QueryResult back with it.
	return sendReply(&gorpcvtgatecommon.QueryResult{
		Err: vterrors.RPCErrFromVtError(vtgErr),
	})
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, noInput *rpc.Unused, outSession *vtgatepb.Session) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	session, err := vtg.server.Begin(ctx)
	if err != nil {
		return err
	}
	*outSession = *sessionToRPC(session)
	return nil
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit(ctx context.Context, inSession *vtgatepb.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Commit(ctx, inSession)
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback(ctx context.Context, inSession *vtgatepb.Session, noOutput *rpc.Unused) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	return vtg.server.Rollback(ctx, inSession)
}

// Begin2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin2(ctx context.Context, request *gorpcvtgatecommon.BeginRequest, reply *gorpcvtgatecommon.BeginResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	// Don't pass in a nil pointer
	session, vtgErr := vtg.server.Begin(ctx)
	reply.Session = sessionToRPC(session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// Commit2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit2(ctx context.Context, request *gorpcvtgatecommon.CommitRequest, reply *gorpcvtgatecommon.CommitResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.Commit(ctx, request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// Rollback2 is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback2(ctx context.Context, request *gorpcvtgatecommon.RollbackRequest, reply *gorpcvtgatecommon.RollbackResponse) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	sessionFromRPC(request.Session)
	vtgErr := vtg.server.Rollback(ctx, request.Session)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) SplitQuery(ctx context.Context, request *gorpcvtgatecommon.SplitQueryRequest, reply *gorpcvtgatecommon.SplitQueryResult) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(*rpcTimeout))
	defer cancel()
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(request.CallerID),
		callerid.NewImmediateCallerID("gorpc client"))
	var vtgErr error
	reply.Splits, vtgErr = vtg.server.SplitQuery(ctx,
		request.Keyspace,
		request.Query.Sql,
		request.Query.BindVariables,
		request.SplitColumn,
		request.SplitCount)
	reply.Err = vterrors.RPCErrFromVtError(vtgErr)
	return nil
}

// GetSrvKeyspace is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, request *gorpcvtgatecommon.GetSrvKeyspaceRequest, reply *topodatapb.SrvKeyspace) (err error) {
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
