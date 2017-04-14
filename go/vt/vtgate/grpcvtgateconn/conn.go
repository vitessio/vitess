// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtgateconn provides gRPC connectivity for VTGate.
package grpcvtgateconn

import (
	"flag"
	"io"
	"time"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtgateservicepb "github.com/youtube/vitess/go/vt/proto/vtgateservice"
)

var (
	cert = flag.String("vtgate_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("vtgate_grpc_key", "", "the key to use to connect")
	ca   = flag.String("vtgate_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("vtgate_grpc_server_name", "", "the server name to use to validate server certificate")
)

func init() {
	vtgateconn.RegisterDialer("grpc", dial)
}

type vtgateConn struct {
	cc *grpc.ClientConn
	c  vtgateservicepb.VitessClient
}

func dial(ctx context.Context, addr string, timeout time.Duration) (vtgateconn.Impl, error) {
	opt, err := grpcutils.ClientSecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}
	cc, err := grpc.Dial(addr, opt, grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := vtgateservicepb.NewVitessClient(cc)
	return &vtgateConn{
		cc: cc,
		c:  c,
	}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, keyspaceShard string, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Query:         q,
		KeyspaceShard: keyspaceShard,
		TabletType:    tabletType,
		Options:       options,
	}
	response, err := conn.c.Execute(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteShardsRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Session:    s,
		Query:      q,
		Keyspace:   keyspace,
		Shards:     shards,
		TabletType: tabletType,
		Options:    options,
	}
	response, err := conn.c.ExecuteShards(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteKeyspaceIdsRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Session:     s,
		Query:       q,
		Keyspace:    keyspace,
		KeyspaceIds: keyspaceIds,
		TabletType:  tabletType,
		Options:     options,
	}
	response, err := conn.c.ExecuteKeyspaceIds(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteKeyRangesRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Session:    s,
		Query:      q,
		Keyspace:   keyspace,
		KeyRanges:  keyRanges,
		TabletType: tabletType,
		Options:    options,
	}
	response, err := conn.c.ExecuteKeyRanges(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteEntityIdsRequest{
		CallerId:          callerid.EffectiveCallerIDFromContext(ctx),
		Session:           s,
		Query:             q,
		Keyspace:          keyspace,
		EntityColumnName:  entityColumnName,
		EntityKeyspaceIds: entityKeyspaceIDs,
		TabletType:        tabletType,
		Options:           options,
	}
	response, err := conn.c.ExecuteEntityIds(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatch(ctx context.Context, queryList []string, bindVarsList []map[string]interface{}, keyspaceShard string, tabletType topodatapb.TabletType, asTransaction bool, session interface{}, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := querytypes.BoundQueriesToProto3(queryList, bindVarsList)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteBatchRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       q,
		KeyspaceShard: keyspaceShard,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Options:       options,
	}
	response, err := conn.c.ExecuteBatch(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToQueryReponses(response.Results), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}, options *querypb.ExecuteOptions) ([]sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	request := &vtgatepb.ExecuteBatchShardsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Options:       options,
	}
	response, err := conn.c.ExecuteBatchShards(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}, options *querypb.ExecuteOptions) ([]sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	request := &vtgatepb.ExecuteBatchKeyspaceIdsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Options:       options,
	}
	response, err := conn.c.ExecuteBatchKeyspaceIds(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVTRPC(response.Error)
	}
	return sqltypes.Proto3ToResults(response.Results), response.Session, nil
}

type streamExecuteAdapter struct {
	recv   func() (*querypb.QueryResult, error)
	fields []*querypb.Field
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	qr, err := a.recv()
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	if a.fields == nil {
		a.fields = qr.Fields
	}
	return sqltypes.CustomProto3ToResult(a.fields, qr), nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, keyspaceShard string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}
	req := &vtgatepb.StreamExecuteRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Query:         q,
		KeyspaceShard: keyspaceShard,
		TabletType:    tabletType,
		Options:       options,
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &streamExecuteAdapter{
		recv: func() (*querypb.QueryResult, error) {
			ser, err := stream.Recv()
			if err != nil {
				return nil, err
			}
			return ser.Result, nil
		},
	}, nil
}

func (conn *vtgateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}
	req := &vtgatepb.StreamExecuteShardsRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      q,
		Keyspace:   keyspace,
		Shards:     shards,
		TabletType: tabletType,
		Options:    options,
	}
	stream, err := conn.c.StreamExecuteShards(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &streamExecuteAdapter{
		recv: func() (*querypb.QueryResult, error) {
			ser, err := stream.Recv()
			if err != nil {
				return nil, err
			}
			return ser.Result, nil
		},
	}, nil
}

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}
	req := &vtgatepb.StreamExecuteKeyRangesRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      q,
		Keyspace:   keyspace,
		KeyRanges:  keyRanges,
		TabletType: tabletType,
		Options:    options,
	}
	stream, err := conn.c.StreamExecuteKeyRanges(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &streamExecuteAdapter{
		recv: func() (*querypb.QueryResult, error) {
			ser, err := stream.Recv()
			if err != nil {
				return nil, err
			}
			return ser.Result, nil
		},
	}, nil
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}
	req := &vtgatepb.StreamExecuteKeyspaceIdsRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Query:       q,
		Keyspace:    keyspace,
		KeyspaceIds: keyspaceIds,
		TabletType:  tabletType,
		Options:     options,
	}
	stream, err := conn.c.StreamExecuteKeyspaceIds(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &streamExecuteAdapter{
		recv: func() (*querypb.QueryResult, error) {
			ser, err := stream.Recv()
			if err != nil {
				return nil, err
			}
			return ser.Result, nil
		},
	}, nil
}

func (conn *vtgateConn) Begin(ctx context.Context, singledb bool) (interface{}, error) {
	request := &vtgatepb.BeginRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		SingleDb: singledb,
	}
	response, err := conn.c.Begin(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Session, nil
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}, twopc bool) error {
	request := &vtgatepb.CommitRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*vtgatepb.Session),
		Atomic:   twopc,
	}
	_, err := conn.c.Commit(ctx, request)
	return vterrors.FromGRPC(err)
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	request := &vtgatepb.RollbackRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*vtgatepb.Session),
	}
	_, err := conn.c.Rollback(ctx, request)
	return vterrors.FromGRPC(err)
}

func (conn *vtgateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	request := &vtgatepb.ResolveTransactionRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Dtid:     dtid,
	}
	_, err := conn.c.ResolveTransaction(ctx, request)
	return vterrors.FromGRPC(err)
}

func (conn *vtgateConn) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	request := &vtgatepb.MessageStreamRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace: keyspace,
		Shard:    shard,
		KeyRange: keyRange,
		Name:     name,
	}
	stream, err := conn.c.MessageStream(ctx, request)
	if err != nil {
		return vterrors.FromGRPC(err)
	}
	var fields []*querypb.Field
	for {
		r, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return vterrors.FromGRPC(err)
			}
			return nil
		}
		if fields == nil {
			fields = r.Result.Fields
		}
		err = callback(sqltypes.CustomProto3ToResult(fields, r.Result))
		if err != nil {
			return err
		}
	}
}

func (conn *vtgateConn) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	request := &vtgatepb.MessageAckRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace: keyspace,
		Name:     name,
		Ids:      ids,
	}
	r, err := conn.c.MessageAck(ctx, request)
	if err != nil {
		return 0, vterrors.FromGRPC(err)
	}
	return int64(r.Result.RowsAffected), nil
}

func (conn *vtgateConn) SplitQuery(
	ctx context.Context,
	keyspace string,
	query string,
	bindVars map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}

	request := &vtgatepb.SplitQueryRequest{
		CallerId:            callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace:            keyspace,
		Query:               q,
		SplitColumn:         splitColumns,
		SplitCount:          splitCount,
		NumRowsPerQueryPart: numRowsPerQueryPart,
		Algorithm:           algorithm,
	}
	response, err := conn.c.SplitQuery(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.Splits, nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	request := &vtgatepb.GetSrvKeyspaceRequest{
		Keyspace: keyspace,
	}
	response, err := conn.c.GetSrvKeyspace(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return response.SrvKeyspace, nil
}

type updateStreamAdapter struct {
	stream vtgateservicepb.Vitess_UpdateStreamClient
}

func (a *updateStreamAdapter) Recv() (*querypb.StreamEvent, int64, error) {
	r, err := a.stream.Recv()
	if err != nil {
		return nil, 0, vterrors.FromGRPC(err)
	}
	return r.Event, r.ResumeTimestamp, nil
}

func (conn *vtgateConn) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken) (vtgateconn.UpdateStreamReader, error) {
	req := &vtgatepb.UpdateStreamRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace:   keyspace,
		Shard:      shard,
		KeyRange:   keyRange,
		TabletType: tabletType,
		Timestamp:  timestamp,
		Event:      event,
	}
	stream, err := conn.c.UpdateStream(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &updateStreamAdapter{
		stream: stream,
	}, nil
}

func (conn *vtgateConn) Close() {
	conn.cc.Close()
}
