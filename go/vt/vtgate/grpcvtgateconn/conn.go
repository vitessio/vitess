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
	"context"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

var (
	cert string
	key  string
	ca   string
	crl  string
	name string
)

func init() {
	vtgateconn.RegisterDialer("grpc", dial)

	for _, cmd := range []string{
		"vtbench",
		"vtclient",
		"vtcombo",
		"vtctl",
		"vttestserver",
	} {
		servenv.OnParseFor(cmd, registerFlags)
	}
}

func registerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&cert, "vtgate_grpc_cert", "", "the cert to use to connect")
	fs.StringVar(&key, "vtgate_grpc_key", "", "the key to use to connect")
	fs.StringVar(&ca, "vtgate_grpc_ca", "", "the server ca to use to validate servers when connecting")
	fs.StringVar(&crl, "vtgate_grpc_crl", "", "the server crl to use to validate server certificates when connecting")
	fs.StringVar(&name, "vtgate_grpc_server_name", "", "the server name to use to validate server certificate")
}

type vtgateConn struct {
	cc *grpc.ClientConn
	c  vtgateservicepb.VitessClient
}

func dial(ctx context.Context, addr string) (vtgateconn.Impl, error) {
	return Dial()(ctx, addr)
}

// Dial produces a vtgateconn.DialerFunc with custom options.
func Dial(opts ...grpc.DialOption) vtgateconn.DialerFunc {
	return func(ctx context.Context, address string) (vtgateconn.Impl, error) {
		opt, err := grpcclient.SecureDialOption(cert, key, ca, crl, name)
		if err != nil {
			return nil, err
		}

		opts = append(opts, opt)

		cc, err := grpcclient.DialContext(ctx, address, grpcclient.FailFast(false), opts...)
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

// DialWithOpts allows for custom dial options to be set on a vtgateConn.
//
// Deprecated: the context parameter cannot be used by the returned
// vtgateconn.DialerFunc and thus has no effect. Use Dial instead.
func DialWithOpts(_ context.Context, opts ...grpc.DialOption) vtgateconn.DialerFunc {
	return Dial(opts...)
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

func (conn *vtgateConn) Prepare(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (*vtgatepb.Session, []*querypb.Field, error) {
	request := &vtgatepb.PrepareRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
	}
	response, err := conn.c.Prepare(ctx, request)
	if err != nil {
		return session, nil, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return response.Session, nil, vterrors.FromVTRPC(response.Error)
	}
	return response.Session, response.Fields, nil
}

func (conn *vtgateConn) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	request := &vtgatepb.CloseSessionRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
	}
	response, err := conn.c.CloseSession(ctx, request)
	if err != nil {
		return vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return vterrors.FromVTRPC(response.Error)
	}
	return nil
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

// Make sure vtgateConn implements vtgateconn.Impl
var _ vtgateconn.Impl = (*vtgateConn)(nil)
