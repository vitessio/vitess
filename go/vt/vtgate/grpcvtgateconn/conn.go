/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package grpcvtgateconn provides gRPC connectivity for VTGate.
package grpcvtgateconn

import (
	"flag"

	"google.golang.org/grpc"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
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

func dial(ctx context.Context, addr string) (vtgateconn.Impl, error) {
	return DialWithOpts(ctx)(ctx, addr)
}

// DialWithOpts allows for custom dial options to be set on a vtgateConn.
func DialWithOpts(ctx context.Context, opts ...grpc.DialOption) vtgateconn.DialerFunc {
	return func(ctx context.Context, address string) (vtgateconn.Impl, error) {
		opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
		if err != nil {
			return nil, err
		}

		opts = append(opts, opt)

		cc, err := grpcclient.Dial(address, grpcclient.FailFast(false), opts...)
		if err != nil {
			return nil, err
		}

		c := vtgateservicepb.NewVitessClient(cc)
		return &vtgateConn{
			cc: cc,
			c:  c,
		}, nil
	}
}

func (conn *vtgateConn) Execute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	request := &vtgatepb.ExecuteRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
	}
	response, err := conn.c.Execute(ctx, request)
	if err != nil {
		return session, nil, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return response.Session, nil, vterrors.FromVTRPC(response.Error)
	}
	return response.Session, sqltypes.Proto3ToResult(response.Result), nil
}

func (conn *vtgateConn) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, queryList []string, bindVarsList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	queries := make([]*querypb.BoundQuery, len(queryList))
	for i, query := range queryList {
		bq := &querypb.BoundQuery{Sql: query}
		if len(bindVarsList) != 0 {
			bq.BindVariables = bindVarsList[i]
		}
		queries[i] = bq
	}
	request := &vtgatepb.ExecuteBatchRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
		Queries:  queries,
	}
	response, err := conn.c.ExecuteBatch(ctx, request)
	if err != nil {
		return session, nil, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return response.Session, nil, vterrors.FromVTRPC(response.Error)
	}
	return response.Session, sqltypes.Proto3ToQueryReponses(response.Results), nil
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

func (conn *vtgateConn) StreamExecute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error) {
	req := &vtgatepb.StreamExecuteRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		Session: session,
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

func (conn *vtgateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	request := &vtgatepb.ResolveTransactionRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Dtid:     dtid,
	}
	_, err := conn.c.ResolveTransaction(ctx, request)
	return vterrors.FromGRPC(err)
}

type vstreamAdapter struct {
	stream vtgateservicepb.Vitess_VStreamClient
}

func (a *vstreamAdapter) Recv() ([]*binlogdatapb.VEvent, error) {
	r, err := a.stream.Recv()
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return r.Events, nil
}

func (conn *vtgateConn) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid,
	filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags) (vtgateconn.VStreamReader, error) {

	req := &vtgatepb.VStreamRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		TabletType: tabletType,
		Vgtid:      vgtid,
		Filter:     filter,
		Flags:      flags,
	}
	stream, err := conn.c.VStream(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &vstreamAdapter{
		stream: stream,
	}, nil
}

func (conn *vtgateConn) Close() {
	conn.cc.Close()
}
