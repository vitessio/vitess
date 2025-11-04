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
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

var (
	cert     string
	key      string
	ca       string
	crl      string
	name     string
	failFast bool
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
		servenv.OnParseFor(cmd, RegisterFlags)
	}
}

func RegisterFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &cert, "vtgate-grpc-cert", "", "the cert to use to connect")
	utils.SetFlagStringVar(fs, &key, "vtgate-grpc-key", "", "the key to use to connect")
	utils.SetFlagStringVar(fs, &ca, "vtgate-grpc-ca", "", "the server ca to use to validate servers when connecting")
	utils.SetFlagStringVar(fs, &crl, "vtgate-grpc-crl", "", "the server crl to use to validate server certificates when connecting")
	utils.SetFlagStringVar(fs, &name, "vtgate-grpc-server-name", "", "the server name to use to validate server certificate")
	utils.SetFlagBoolVar(fs, &failFast, "vtgate-grpc-fail-fast", false, "whether to enable grpc fail fast when connecting")
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

		cc, err := grpcclient.DialContext(ctx, address, grpcclient.FailFast(failFast), opts...)
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

func (conn *vtgateConn) Execute(
	ctx context.Context,
	session *vtgatepb.Session,
	query string,
	bindVars map[string]*querypb.BindVariable,
	prepared bool,
) (*vtgatepb.Session, *sqltypes.Result, error) {
	request := &vtgatepb.ExecuteRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		Prepared: prepared,
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
	var qr *querypb.QueryResult
	var err error
	for {
		qr, err = a.recv()
		if qr != nil || err != nil {
			break
		}
		// we reach here, only when it is the last packet.
		// as in the last packet we receive the session and there is no result
	}
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	if a.fields == nil {
		a.fields = qr.Fields
	}
	return sqltypes.CustomProto3ToResult(a.fields, qr), nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable, processResponse func(response *vtgatepb.StreamExecuteResponse)) (sqltypes.ResultStream, error) {
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
			processResponse(ser)
			return ser.Result, nil
		},
	}, nil
}

// ExecuteMulti executes multiple non-streaming queries.
func (conn *vtgateConn) ExecuteMulti(ctx context.Context, session *vtgatepb.Session, sqlString string) (newSession *vtgatepb.Session, qrs []*sqltypes.Result, err error) {
	request := &vtgatepb.ExecuteMultiRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
		Sql:      sqlString,
	}
	response, err := conn.c.ExecuteMulti(ctx, request)
	if err != nil {
		return session, nil, vterrors.FromGRPC(err)
	}
	return response.Session, sqltypes.Proto3ToResults(response.Results), vterrors.FromVTRPC(response.Error)
}

type streamExecuteMultiAdapter struct {
	recv   func() (*querypb.QueryResult, bool, error)
	fields []*querypb.Field
}

func (a *streamExecuteMultiAdapter) Recv() (*sqltypes.Result, bool, error) {
	var qr *querypb.QueryResult
	var err error
	var newResult bool
	for {
		qr, newResult, err = a.recv()
		if qr != nil || err != nil {
			break
		}
		// we reach here, only when it is the last packet.
		// as in the last packet we receive the session and there is no result
	}
	if err != nil {
		return nil, newResult, err
	}
	if qr != nil && qr.Fields != nil {
		a.fields = qr.Fields
	}
	return sqltypes.CustomProto3ToResult(a.fields, qr), newResult, nil
}

// StreamExecuteMulti executes multiple streaming queries.
func (conn *vtgateConn) StreamExecuteMulti(ctx context.Context, session *vtgatepb.Session, sqlString string, processResponse func(response *vtgatepb.StreamExecuteMultiResponse)) (sqltypes.MultiResultStream, error) {
	req := &vtgatepb.StreamExecuteMultiRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Sql:      sqlString,
		Session:  session,
	}
	stream, err := conn.c.StreamExecuteMulti(ctx, req)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &streamExecuteMultiAdapter{
		recv: func() (*querypb.QueryResult, bool, error) {
			ser, err := stream.Recv()
			if err != nil {
				return nil, false, vterrors.FromGRPC(err)
			}
			processResponse(ser)
			return ser.Result.GetResult(), ser.NewResult, vterrors.FromVTRPC(ser.Result.GetError())
		},
	}, nil
}

func (conn *vtgateConn) Prepare(ctx context.Context, session *vtgatepb.Session, query string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	request := &vtgatepb.PrepareRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session,
		Query: &querypb.BoundQuery{
			Sql: query,
		},
	}
	response, err := conn.c.Prepare(ctx, request)
	if err != nil {
		return session, nil, 0, vterrors.FromGRPC(err)
	}
	if response.Error != nil {
		return response.Session, nil, 0, vterrors.FromVTRPC(response.Error)
	}
	return response.Session, response.Fields, uint16(response.ParamsCount), nil
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
