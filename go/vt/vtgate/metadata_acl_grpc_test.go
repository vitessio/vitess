/*
Copyright 2026 The Vitess Authors.

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

package vtgate_test

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

const (
	grpcMarkerOne = "marker-one"
	grpcMarkerTwo = "marker-two"
	grpcKeyOne    = "aclprobe"
	grpcKeyTwo    = "otherprobe"

	// unauthenticatedGRPCUser is the immediate caller installed by
	// grpcvtgateservice.withVTGateContext when the request has no verified
	// client certificate and the effective-caller and static-auth flags are off.
	unauthenticatedGRPCUser = "unsecure_grpc_client"
)

// TestShowVitessMetadataGRPCRequiresSameACLAsSet is the gRPC half of the
// metadata read check. The server is created with no auth interceptor, which
// is how vtgate starts when --grpc-auth-mode is empty and --grpc-port is set.
// The request's effective caller principal is the authorized user. The
// immediate caller is still the unauthenticated fallback, and that is the
// identity SET checks.
func TestShowVitessMetadataGRPCRequiresSameACLAsSet(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers("operator"))
	t.Cleanup(func() {
		vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers(""))
	})

	service := vtgate.NewMetadataGateForTest(t)
	operatorCtx := callerid.NewContext(t.Context(), nil, callerid.NewImmediateCallerID("operator"))
	session := &vtgatepb.Session{Autocommit: true}
	_, _, err := service.Execute(operatorCtx, nil, session, "set @@vitess_metadata."+grpcKeyOne+" = '"+grpcMarkerOne+"'", nil, false)
	require.NoError(t, err)
	_, _, err = service.Execute(operatorCtx, nil, session, "set @@vitess_metadata."+grpcKeyTwo+" = '"+grpcMarkerTwo+"'", nil, false)
	require.NoError(t, err)
	_, qr, err := service.Execute(operatorCtx, nil, session, "show vitess_metadata variables", nil, false)
	require.NoError(t, err)
	require.True(t, grpcRowsContain(qr, grpcKeyOne, grpcMarkerOne))
	require.True(t, grpcRowsContain(qr, grpcKeyTwo, grpcMarkerTwo))

	client := dialMetadataGRPC(t, service)
	showAll := "show vitess_metadata variables"
	showOne := "show vitess_metadata variables like '" + grpcKeyOne + "'"
	showAny := "show vitess_metadata variables like '%'"
	spoofedOperator := &vtrpcpb.CallerID{Principal: "operator"}

	t.Run("execute", func(t *testing.T) {
		for _, query := range []string{showAll, showOne, showAny} {
			resp, err := client.Execute(t.Context(), &vtgatepb.ExecuteRequest{
				CallerId: spoofedOperator,
				Session:  session,
				Query:    &querypb.BoundQuery{Sql: query},
			})
			require.NoError(t, err)
			requireGRPCMetadataDenied(t, resp.GetError(), sqltypes.Proto3ToResult(resp.GetResult()))
		}
	})

	t.Run("execute prepared", func(t *testing.T) {
		resp, err := client.Execute(t.Context(), &vtgatepb.ExecuteRequest{
			CallerId: spoofedOperator,
			Session:  session,
			Query:    &querypb.BoundQuery{Sql: showAll},
			Prepared: true,
		})
		require.NoError(t, err)
		requireGRPCMetadataDenied(t, resp.GetError(), sqltypes.Proto3ToResult(resp.GetResult()))
	})

	t.Run("stream execute", func(t *testing.T) {
		stream, err := client.StreamExecute(t.Context(), &vtgatepb.StreamExecuteRequest{
			CallerId: spoofedOperator,
			Session:  session,
			Query:    &querypb.BoundQuery{Sql: showAll},
		})
		require.NoError(t, err)
		var streamErr error
		for {
			msg, recvErr := stream.Recv()
			if recvErr != nil {
				streamErr = recvErr
				break
			}
			require.Falsef(t, grpcProtoLeaks(msg.GetResult()), "stream returned metadata")
		}
		require.Error(t, streamErr)
		require.NotErrorIs(t, streamErr, io.EOF)
		st, ok := status.FromError(streamErr)
		require.True(t, ok)
		require.Equal(t, codes.PermissionDenied, st.Code())
		require.Contains(t, st.Message(), grpcDeniedMessage())
	})

	t.Run("execute multi", func(t *testing.T) {
		resp, err := client.ExecuteMulti(t.Context(), &vtgatepb.ExecuteMultiRequest{
			CallerId: spoofedOperator,
			Session:  session,
			Sql:      showAll + "; " + showOne,
		})
		require.NoError(t, err)
		var rows []*sqltypes.Result
		for _, result := range resp.GetResults() {
			rows = append(rows, sqltypes.Proto3ToResult(result))
		}
		requireGRPCMetadataDenied(t, resp.GetError(), rows...)
	})

	t.Run("execute batch", func(t *testing.T) {
		resp, err := client.ExecuteBatch(t.Context(), &vtgatepb.ExecuteBatchRequest{
			CallerId: spoofedOperator,
			Session:  session,
			Queries: []*querypb.BoundQuery{
				{Sql: showAll},
				{Sql: showAny},
			},
		})
		require.NoError(t, err)
		require.Nil(t, resp.GetError())
		require.NotEmpty(t, resp.GetResults())
		for _, result := range resp.GetResults() {
			requireGRPCMetadataDenied(t, result.GetError(), sqltypes.Proto3ToResult(result.GetResult()))
		}
	})

	t.Run("stream execute multi", func(t *testing.T) {
		stream, err := client.StreamExecuteMulti(t.Context(), &vtgatepb.StreamExecuteMultiRequest{
			CallerId: spoofedOperator,
			Session:  session,
			Sql:      showAll + "; " + showOne,
		})
		require.NoError(t, err)
		sawDenial := false
		for {
			msg, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				st, ok := status.FromError(recvErr)
				require.True(t, ok)
				require.Equal(t, codes.PermissionDenied, st.Code())
				require.Contains(t, st.Message(), grpcDeniedMessage())
				sawDenial = true
				break
			}
			payload := msg.GetResult()
			if payload == nil {
				continue
			}
			require.False(t, grpcProtoLeaks(payload.GetResult()))
			if payload.GetError() != nil {
				require.Contains(t, payload.GetError().GetMessage(), grpcDeniedMessage())
				require.NotContains(t, payload.GetError().GetMessage(), grpcMarkerOne)
				require.NotContains(t, payload.GetError().GetMessage(), grpcMarkerTwo)
				sawDenial = true
			}
		}
		require.True(t, sawDenial, "multi stream returned metadata without a denial")
	})

	t.Run("prepare", func(t *testing.T) {
		resp, err := client.Prepare(t.Context(), &vtgatepb.PrepareRequest{
			CallerId: spoofedOperator,
			Session:  session,
			Query:    &querypb.BoundQuery{Sql: showAll},
		})
		require.NoError(t, err)
		requireGRPCMetadataDenied(t, resp.GetError())
	})
}

func dialMetadataGRPC(t *testing.T, service vtgateservice.VTGateService) vtgateservicepb.VitessClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// No auth interceptor: --grpc-auth-mode defaults to empty, so an open
	// grpc port accepts the call and withVTGateContext supplies the caller.
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		server.GracefulStop()
		lis.Close()
	})
	return vtgateservicepb.NewVitessClient(conn)
}

func grpcDeniedMessage() string {
	return fmt.Sprintf("User '%s' not authorized to perform vitess metadata operations", unauthenticatedGRPCUser)
}

func requireGRPCMetadataDenied(t *testing.T, rpcErr *vtrpcpb.RPCError, qrs ...*sqltypes.Result) {
	t.Helper()
	for _, qr := range qrs {
		requireNoGRPCMetadata(t, qr)
	}
	require.NotNil(t, rpcErr)
	require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, rpcErr.Code)
	require.Contains(t, rpcErr.Message, grpcDeniedMessage())
	require.NotContains(t, rpcErr.Message, grpcMarkerOne)
	require.NotContains(t, rpcErr.Message, grpcMarkerTwo)
}

func grpcRowsContain(qr *sqltypes.Result, key, value string) bool {
	if qr == nil {
		return false
	}
	for _, row := range qr.Rows {
		var keySeen, valueSeen bool
		for _, col := range row {
			text := col.ToString()
			if text == key {
				keySeen = true
			}
			if text == value {
				valueSeen = true
			}
		}
		if keySeen && valueSeen {
			return true
		}
	}
	return false
}

func grpcProtoLeaks(qr *querypb.QueryResult) bool {
	if qr == nil {
		return false
	}
	var blob strings.Builder
	for _, field := range qr.Fields {
		blob.WriteString(field.GetName())
		blob.WriteByte('\n')
	}
	for _, row := range qr.Rows {
		blob.Write(row.GetValues())
		blob.WriteByte('\n')
	}
	text := blob.String()
	for _, needle := range []string{grpcMarkerOne, grpcMarkerTwo, grpcKeyOne, grpcKeyTwo} {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func requireNoGRPCMetadata(t *testing.T, qr *sqltypes.Result) {
	t.Helper()
	if qr == nil {
		return
	}
	for _, row := range qr.Rows {
		for _, col := range row {
			text := col.ToString()
			require.NotEqual(t, grpcMarkerOne, text)
			require.NotEqual(t, grpcMarkerTwo, text)
			require.NotEqual(t, grpcKeyOne, text)
			require.NotEqual(t, grpcKeyTwo, text)
		}
	}
}
