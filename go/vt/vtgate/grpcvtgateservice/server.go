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

// Package grpcvtgateservice provides the gRPC glue for vtgate
package grpcvtgateservice

import (
	"context"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

const (
	unsecureClient = "unsecure_grpc_client"
)

var (
	useEffective                    bool
	useEffectiveGroups              bool
	useStaticAuthenticationIdentity bool

	sendSessionInStreaming bool
)

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&useEffective, "grpc_use_effective_callerid", false, "If set, and SSL is not used, will set the immediate caller id from the effective caller id's principal.")
	fs.BoolVar(&useEffectiveGroups, "grpc-use-effective-groups", false, "If set, and SSL is not used, will set the immediate caller's security groups from the effective caller id's groups.")
	fs.BoolVar(&useStaticAuthenticationIdentity, "grpc-use-static-authentication-callerid", false, "If set, will set the immediate caller id to the username authenticated by the static auth plugin.")
	fs.BoolVar(&sendSessionInStreaming, "grpc-send-session-in-streaming", false, "If set, will send the session as last packet in streaming api to support transactions in streaming")
}

func init() {
	servenv.OnParseFor("vtgate", registerFlags)
	servenv.OnParseFor("vtcombo", registerFlags)
}

// VTGate is the public structure that is exported via gRPC
type VTGate struct {
	vtgateservicepb.UnimplementedVitessServer
	server vtgateservice.VTGateService
}

// immediateCallerIDFromCert tries to extract the common name as well as the (domain) subject
// alternative names of the certificate that was used to connect to vtgate.
// If it fails for any reason, it will return "".
// That immediate caller id is then inserted into a Context,
// and will be used when talking to vttablet.
// vttablet in turn can use table ACLs to validate access is authorized.
func immediateCallerIDFromCert(ctx context.Context) (string, []string) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", nil
	}
	if p.AuthInfo == nil {
		return "", nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", nil
	}
	if len(tlsInfo.State.VerifiedChains) < 1 {
		return "", nil
	}
	if len(tlsInfo.State.VerifiedChains[0]) < 1 {
		return "", nil
	}
	cert := tlsInfo.State.VerifiedChains[0][0]
	return cert.Subject.CommonName, cert.DNSNames
}

// immediateCallerIdFromStaticAuthentication extracts the username of the current
// static authentication context and returns that to the caller.
func immediateCallerIdFromStaticAuthentication(ctx context.Context) (string, []string) {
	if immediate := servenv.StaticAuthUsernameFromContext(ctx); immediate != "" {
		return immediate, nil
	}

	return "", nil
}

// withCallerIDContext creates a context that extracts what we need
// from the incoming call and can be forwarded for use when talking to vttablet.
func withCallerIDContext(ctx context.Context, effectiveCallerID *vtrpcpb.CallerID) context.Context {
	// The client cert common name (if using mTLS)
	immediate, securityGroups := immediateCallerIDFromCert(ctx)

	// The effective caller id (if --grpc_use_effective_callerid=true)
	if immediate == "" && useEffective && effectiveCallerID != nil {
		immediate = effectiveCallerID.Principal
		if useEffectiveGroups && len(effectiveCallerID.Groups) > 0 {
			securityGroups = effectiveCallerID.Groups
		}
	}

	// The static auth username (if --grpc-use-static-authentication-callerid=true)
	if immediate == "" && useStaticAuthenticationIdentity {
		immediate, securityGroups = immediateCallerIdFromStaticAuthentication(ctx)
	}

	if immediate == "" {
		immediate = unsecureClient
	}
	return callerid.NewContext(callinfo.GRPCCallInfo(ctx),
		effectiveCallerID,
		&querypb.VTGateCallerID{Username: immediate, Groups: securityGroups})
}

// Execute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Execute(ctx context.Context, request *vtgatepb.ExecuteRequest) (response *vtgatepb.ExecuteResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)

	// Handle backward compatibility.
	session := request.Session
	if session == nil {
		session = &vtgatepb.Session{Autocommit: true}
	}
	session, result, err := vtg.server.Execute(ctx, nil, session, request.Query.Sql, request.Query.BindVariables)
	return &vtgatepb.ExecuteResponse{
		Result:  sqltypes.ResultToProto3(result),
		Session: session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// ExecuteBatch is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) ExecuteBatch(ctx context.Context, request *vtgatepb.ExecuteBatchRequest) (response *vtgatepb.ExecuteBatchResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)
	sqlQueries := make([]string, len(request.Queries))
	bindVars := make([]map[string]*querypb.BindVariable, len(request.Queries))
	for queryNum, query := range request.Queries {
		sqlQueries[queryNum] = query.Sql
		bindVars[queryNum] = query.BindVariables
	}
	// Handle backward compatibility.
	session := request.Session
	if session == nil {
		session = &vtgatepb.Session{Autocommit: true}
	}
	session, results, err := vtg.server.ExecuteBatch(ctx, session, sqlQueries, bindVars)
	return &vtgatepb.ExecuteBatchResponse{
		Results: sqltypes.QueryResponsesToProto3(results),
		Session: session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// StreamExecute is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) StreamExecute(request *vtgatepb.StreamExecuteRequest, stream vtgateservicepb.Vitess_StreamExecuteServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)

	// Handle backward compatibility.
	session := request.Session
	if session == nil {
		session = &vtgatepb.Session{Autocommit: true}
	}

	session, vtgErr := vtg.server.StreamExecute(ctx, nil, session, request.Query.Sql, request.Query.BindVariables, func(value *sqltypes.Result) error {
		// Send is not safe to call concurrently, but vtgate
		// guarantees that it's not.
		return stream.Send(&vtgatepb.StreamExecuteResponse{
			Result: sqltypes.ResultToProto3(value),
		})
	})

	var errs []error
	if vtgErr != nil {
		errs = append(errs, vtgErr)
	}

	if sendSessionInStreaming {
		// even if there is an error, session could have been modified.
		// So, this needs to be sent back to the client. Session is sent in the last stream response.
		lastErr := stream.Send(&vtgatepb.StreamExecuteResponse{
			Session: session,
		})
		if lastErr != nil {
			errs = append(errs, lastErr)
		}
	}

	return vterrors.ToGRPC(vterrors.Aggregate(errs))
}

// Prepare is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) Prepare(ctx context.Context, request *vtgatepb.PrepareRequest) (response *vtgatepb.PrepareResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)

	session := request.Session
	if session == nil {
		session = &vtgatepb.Session{Autocommit: true}
	}

	session, fields, err := vtg.server.Prepare(ctx, session, request.Query.Sql, request.Query.BindVariables)
	return &vtgatepb.PrepareResponse{
		Fields:  fields,
		Session: session,
		Error:   vterrors.ToVTRPC(err),
	}, nil
}

// CloseSession is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) CloseSession(ctx context.Context, request *vtgatepb.CloseSessionRequest) (response *vtgatepb.CloseSessionResponse, err error) {
	defer vtg.server.HandlePanic(&err)
	ctx = withCallerIDContext(ctx, request.CallerId)

	session := request.Session
	if session == nil {
		session = &vtgatepb.Session{Autocommit: true}
	}
	err = vtg.server.CloseSession(ctx, session)
	return &vtgatepb.CloseSessionResponse{
		Error: vterrors.ToVTRPC(err),
	}, nil
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

// VStream is the RPC version of vtgateservice.VTGateService method
func (vtg *VTGate) VStream(request *vtgatepb.VStreamRequest, stream vtgateservicepb.Vitess_VStreamServer) (err error) {
	defer vtg.server.HandlePanic(&err)
	ctx := withCallerIDContext(stream.Context(), request.CallerId)

	// For backward compatibility.
	// The mysql query equivalent has logic to use topodatapb.TabletType_PRIMARY if tablet_type is not set.
	tabletType := request.TabletType
	if tabletType == topodatapb.TabletType_UNKNOWN {
		tabletType = topodatapb.TabletType_PRIMARY
	}
	vtgErr := vtg.server.VStream(ctx,
		tabletType,
		request.Vgtid,
		request.Filter,
		request.Flags,
		func(events []*binlogdatapb.VEvent) error {
			return stream.Send(&vtgatepb.VStreamResponse{
				Events: events,
			})
		})
	return vterrors.ToGRPC(vtgErr)
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate vtgateservice.VTGateService) {
		if servenv.GRPCCheckServiceMap("vtgateservice") {
			vtgateservicepb.RegisterVitessServer(servenv.GRPCServer, &VTGate{server: vtGate})
		}
	})
}

// RegisterForTest registers the gRPC implementation on the gRPC
// server.  Useful for unit tests only, for real use, the init()
// function does the registration.
func RegisterForTest(s *grpc.Server, service vtgateservice.VTGateService) {
	vtgateservicepb.RegisterVitessServer(s, &VTGate{server: service})
}
