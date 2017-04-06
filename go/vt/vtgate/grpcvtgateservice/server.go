// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtgateservice provides the gRPC glue for vtgate
package grpcvtgateservice

import (
	"flag"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtgateservicepb "github.com/youtube/vitess/go/vt/proto/vtgateservice"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	unsecureClient = "unsecure_grpc_client"
)

var (
	useEffective = flag.Bool("grpc_use_effective_callerid", false, "If set, and SSL is not used, will set the immediate caller id from the effective caller id's principal.")
)

// VTGate is the public structure that is exported via gRPC
type VTGate struct {
	server vtgateservice.VTGateService
}

// immediateCallerID tries to extract the common name of the certificate
// that was used to connect to vtgate. If it fails for any reason,
// it will return "". That immediate caller id is then inserted
// into a Context, and will be used when talking to vttablet.
// vttablet in turn can use table ACLs to validate access is authorized.
func immediateCallerID(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	if p.AuthInfo == nil {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	if len(tlsInfo.State.VerifiedChains) < 1 {
		return ""
	}
	if len(tlsInfo.State.VerifiedChains[0]) < 1 {
		return ""
	}
	cert := tlsInfo.State.VerifiedChains[0][0]
	return cert.Subject.CommonName
}

// withCallerIDContext creates a context that extracts what we need
// from the incoming call and can be forwarded for use when talking to vttablet.
func withCallerIDContext(ctx context.Context, effectiveCallerID *vtrpcpb.CallerID) context.Context {
	immediate := immediateCallerID(ctx)
	if immediate == "" && *useEffective && effectiveCallerID != nil {
		immediate = effectiveCallerID.Principal
	}
	if immediate == "" {
		immediate = unsecureClient
	}
	return callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		effectiveCallerID,
		callerid.NewImmediateCallerID(immediate))
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Execute(ctx context.Context, request *vtgatepb.ExecuteRequest) (response *vtgatepb.ExecuteResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	session, result, err := vtg.server.Execute(ctx, string(request.Query.Sql), bv, request.KeyspaceShard, request.TabletType, request.Session, request.NotInTransaction, request.Options)
	return &vtgatepb.ExecuteResponse{
		Result:  sqltypes.ResultToProto3(result),
		Session: session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteShards(ctx context.Context, request *vtgatepb.ExecuteShardsRequest) (response *vtgatepb.ExecuteShardsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	result, err := vtg.server.ExecuteShards(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.Shards,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		request.Options)
	return &vtgatepb.ExecuteShardsResponse{
		Result:  sqltypes.ResultToProto3(result),
		Session: request.Session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteKeyspaceIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyspaceIds(ctx context.Context, request *vtgatepb.ExecuteKeyspaceIdsRequest) (response *vtgatepb.ExecuteKeyspaceIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	result, err := vtg.server.ExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		request.Options)
	return &vtgatepb.ExecuteKeyspaceIdsResponse{
		Result:  sqltypes.ResultToProto3(result),
		Session: request.Session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteKeyRanges is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteKeyRanges(ctx context.Context, request *vtgatepb.ExecuteKeyRangesRequest) (response *vtgatepb.ExecuteKeyRangesResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	result, err := vtg.server.ExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		request.Options)
	return &vtgatepb.ExecuteKeyRangesResponse{
		Result:  sqltypes.ResultToProto3(result),
		Session: request.Session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteEntityIds is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteEntityIds(ctx context.Context, request *vtgatepb.ExecuteEntityIdsRequest) (response *vtgatepb.ExecuteEntityIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	result, err := vtg.server.ExecuteEntityIds(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.EntityColumnName,
		request.EntityKeyspaceIds,
		request.TabletType,
		request.Session,
		request.NotInTransaction,
		request.Options)
	return &vtgatepb.ExecuteEntityIdsResponse{
		Result:  sqltypes.ResultToProto3(result),
		Session: request.Session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteBatch is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatch(ctx context.Context, request *vtgatepb.ExecuteBatchRequest) (response *vtgatepb.ExecuteBatchResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	results := make([]sqltypes.QueryResponse, len(request.Queries))
	sqlQueries := make([]string, len(request.Queries))
	bindVars := make([]map[string]interface{}, len(request.Queries))
	for queryNum, query := range request.Queries {
		bv, err := querytypes.Proto3ToBindVariables(query.BindVariables)
		if err != nil {
			return nil, vterrors.ToGRPC(err)
		}
		sqlQueries[queryNum] = query.Sql
		bindVars[queryNum] = bv
	}
	session, results, err := vtg.server.ExecuteBatch(ctx, sqlQueries, bindVars, request.KeyspaceShard, request.TabletType, request.Session, request.Options)
	return &vtgatepb.ExecuteBatchResponse{
		Results: sqltypes.QueryResponsesToProto3(results),
		Session: session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteBatchShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchShards(ctx context.Context, request *vtgatepb.ExecuteBatchShardsRequest) (response *vtgatepb.ExecuteBatchShardsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	result, err := vtg.server.ExecuteBatchShards(ctx,
		request.Queries,
		request.TabletType,
		request.AsTransaction,
		request.Session,
		request.Options)
	return &vtgatepb.ExecuteBatchShardsResponse{
		Results: sqltypes.ResultsToProto3(result),
		Session: request.Session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteBatchKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatchKeyspaceIds(ctx context.Context, request *vtgatepb.ExecuteBatchKeyspaceIdsRequest) (response *vtgatepb.ExecuteBatchKeyspaceIdsResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	result, err := vtg.server.ExecuteBatchKeyspaceIds(ctx,
		request.Queries,
		request.TabletType,
		request.AsTransaction,
		request.Session,
		request.Options)
	return &vtgatepb.ExecuteBatchKeyspaceIdsResponse{
		Results: sqltypes.ResultsToProto3(result),
		Session: request.Session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(request *vtgatepb.StreamExecuteRequest, stream vtgateservicepb.Vitess_StreamExecuteServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return vterrors.ToGRPC(err)
	}
	vtgErr := vtg.server.StreamExecute(ctx,
		string(request.Query.Sql),
		bv,
		request.KeyspaceShard,
		request.TabletType,
		request.Options,
		func(value *sqltypes.Result) error {
			return stream.Send(&vtgatepb.StreamExecuteResponse{
				Result: sqltypes.ResultToProto3(value),
			})
		})
	return vterrors.ToGRPC(vtgErr)
}

// StreamExecuteShards is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteShards(request *vtgatepb.StreamExecuteShardsRequest, stream vtgateservicepb.Vitess_StreamExecuteShardsServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return vterrors.ToGRPC(err)
	}
	vtgErr := vtg.server.StreamExecuteShards(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.Shards,
		request.TabletType,
		request.Options,
		func(value *sqltypes.Result) error {
			return stream.Send(&vtgatepb.StreamExecuteShardsResponse{
				Result: sqltypes.ResultToProto3(value),
			})
		})
	return vterrors.ToGRPC(vtgErr)
}

// StreamExecuteKeyspaceIds is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyspaceIds(request *vtgatepb.StreamExecuteKeyspaceIdsRequest, stream vtgateservicepb.Vitess_StreamExecuteKeyspaceIdsServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return vterrors.ToGRPC(err)
	}
	vtgErr := vtg.server.StreamExecuteKeyspaceIds(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.KeyspaceIds,
		request.TabletType,
		request.Options,
		func(value *sqltypes.Result) error {
			return stream.Send(&vtgatepb.StreamExecuteKeyspaceIdsResponse{
				Result: sqltypes.ResultToProto3(value),
			})
		})
	return vterrors.ToGRPC(vtgErr)
}

// StreamExecuteKeyRanges is the RPC version of
// vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecuteKeyRanges(request *vtgatepb.StreamExecuteKeyRangesRequest, stream vtgateservicepb.Vitess_StreamExecuteKeyRangesServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return vterrors.ToGRPC(err)
	}
	vtgErr := vtg.server.StreamExecuteKeyRanges(ctx,
		string(request.Query.Sql),
		bv,
		request.Keyspace,
		request.KeyRanges,
		request.TabletType,
		request.Options,
		func(value *sqltypes.Result) error {
			return stream.Send(&vtgatepb.StreamExecuteKeyRangesResponse{
				Result: sqltypes.ResultToProto3(value),
			})
		})
	return vterrors.ToGRPC(vtgErr)
}

// Begin is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Begin(ctx context.Context, request *vtgatepb.BeginRequest) (response *vtgatepb.BeginResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	session, vtgErr := vtg.server.Begin(ctx, request.SingleDb)
	if vtgErr == nil {
		return &vtgatepb.BeginResponse{
			Session: session,
		}, nil
	}
	return nil, vterrors.ToGRPC(vtgErr)
}

// Commit is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Commit(ctx context.Context, request *vtgatepb.CommitRequest) (response *vtgatepb.CommitResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	vtgErr := vtg.server.Commit(ctx, request.Atomic, request.Session)
	response = &vtgatepb.CommitResponse{}
	if vtgErr == nil {
		return response, nil
	}
	return nil, vterrors.ToGRPC(vtgErr)
}

// Rollback is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Rollback(ctx context.Context, request *vtgatepb.RollbackRequest) (response *vtgatepb.RollbackResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	vtgErr := vtg.server.Rollback(ctx, request.Session)
	response = &vtgatepb.RollbackResponse{}
	if vtgErr == nil {
		return response, nil
	}
	return nil, vterrors.ToGRPC(vtgErr)
}

// ResolveTransaction is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ResolveTransaction(ctx context.Context, request *vtgatepb.ResolveTransactionRequest) (response *vtgatepb.ResolveTransactionResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	vtgErr := vtg.server.ResolveTransaction(ctx, request.Dtid)
	response = &vtgatepb.ResolveTransactionResponse{}
	if vtgErr == nil {
		return response, nil
	}
	return nil, vterrors.ToGRPC(vtgErr)
}

// MessageStream is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) MessageStream(request *vtgatepb.MessageStreamRequest, stream vtgateservicepb.Vitess_MessageStreamServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)
	vtgErr := vtg.server.MessageStream(ctx, request.Keyspace, request.Shard, request.KeyRange, request.Name, func(qr *sqltypes.Result) error {
		return stream.Send(&querypb.MessageStreamResponse{
			Result: sqltypes.ResultToProto3(qr),
		})
	})
	return vterrors.ToGRPC(vtgErr)
}

// MessageAck is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) MessageAck(ctx context.Context, request *vtgatepb.MessageAckRequest) (response *querypb.MessageAckResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	count, vtgErr := vtg.server.MessageAck(ctx, request.Keyspace, request.Name, request.Ids)
	if vtgErr != nil {
		return nil, vterrors.ToGRPC(vtgErr)
	}
	return &querypb.MessageAckResponse{
		Result: &querypb.QueryResult{
			RowsAffected: uint64(count),
		},
	}, nil
}

// SplitQuery is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) SplitQuery(ctx context.Context, request *vtgatepb.SplitQueryRequest) (response *vtgatepb.SplitQueryResponse, err error) {

	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	bv, err := querytypes.Proto3ToBindVariables(request.Query.BindVariables)
	if err != nil {
		return nil, vterrors.ToGRPC(err)
	}
	splits, vtgErr := vtg.server.SplitQuery(
		ctx,
		request.Keyspace,
		string(request.Query.Sql),
		bv,
		request.SplitColumn,
		request.SplitCount,
		request.NumRowsPerQueryPart,
		request.Algorithm)
	if vtgErr != nil {
		return nil, vterrors.ToGRPC(vtgErr)
	}
	return &vtgatepb.SplitQueryResponse{
		Splits: splits,
	}, nil
}

// GetSrvKeyspace is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) GetSrvKeyspace(ctx context.Context, request *vtgatepb.GetSrvKeyspaceRequest) (response *vtgatepb.GetSrvKeyspaceResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	sk, vtgErr := vtg.server.GetSrvKeyspace(ctx, request.Keyspace)
	if vtgErr != nil {
		return nil, vterrors.ToGRPC(vtgErr)
	}
	return &vtgatepb.GetSrvKeyspaceResponse{
		SrvKeyspace: sk,
	}, nil
}

// UpdateStream is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) UpdateStream(request *vtgatepb.UpdateStreamRequest, stream vtgateservicepb.Vitess_UpdateStreamServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)
	vtgErr := vtg.server.UpdateStream(ctx,
		request.Keyspace,
		request.Shard,
		request.KeyRange,
		request.TabletType,
		request.Timestamp,
		request.Event,
		func(event *querypb.StreamEvent, resumeTimestamp int64) error {
			return stream.Send(&vtgatepb.UpdateStreamResponse{
				Event:           event,
				ResumeTimestamp: resumeTimestamp,
			})
		})
	return vterrors.ToGRPC(vtgErr)
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate vtgateservice.VTGateService) {
		if servenv.GRPCCheckServiceMap("vtgateservice") {
			vtgateservicepb.RegisterVitessServer(servenv.GRPCServer, &VTGate{vtGate})
		}
	})
}

// RegisterForTest registers the gRPC implementation on the gRPC
// server.  Useful for unit tests only, for real use, the init()
// function does the registration.
func RegisterForTest(s *grpc.Server, service vtgateservice.VTGateService) {
	vtgateservicepb.RegisterVitessServer(s, &VTGate{service})
}
