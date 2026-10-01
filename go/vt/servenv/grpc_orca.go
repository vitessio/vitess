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

package servenv

import (
	"context"
	"strings"
	"sync/atomic"

	orcaservicepb "github.com/cncf/xds/go/xds/service/orca/v3"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"vitess.io/vitess/go/sets"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
)

// orcaEgressMessages counts gRPC messages sent since the last ORCA report, so
// unary calls (one message each, i.e. QPS) and long-lived streams like VStream
// are measured the same way. Every message counts equally regardless of cost.
// Calls whose handler returns an error also count once here and once in
// orcaErrors, so EPS/QPS is the gRPC error rate. Errors returned inside a
// successful response, such as VTGate query errors, are not counted as errors.
var orcaEgressMessages atomic.Int64

var orcaErrors atomic.Int64

// orcaUncountedServices and orcaUncountedMethods are not counted because their
// traffic scales with the number of client connections rather than with client
// work.
var orcaUncountedServices = sets.New(
	healthpb.Health_ServiceDesc.ServiceName,
	orcaservicepb.OpenRcaService_ServiceDesc.ServiceName,
)

var orcaUncountedMethods = sets.New(
	queryservicepb.Query_StreamHealth_FullMethodName,
)

// isOrcaCounted reports whether fullMethod, in gRPC's "/service/method" form,
// is counted.
func isOrcaCounted(fullMethod string) bool {
	if orcaUncountedMethods.Has(fullMethod) {
		return false
	}
	serviceEnd := strings.LastIndexByte(fullMethod, '/')
	if serviceEnd <= 0 {
		return true
	}
	return !orcaUncountedServices.Has(fullMethod[1:serviceEnd])
}

func orcaCountingUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !isOrcaCounted(info.FullMethod) {
		return handler(ctx, req)
	}
	resp, err := handler(ctx, req)
	orcaEgressMessages.Add(1)
	if err != nil {
		orcaErrors.Add(1)
	}
	return resp, err
}

func orcaCountingStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !isOrcaCounted(info.FullMethod) {
		return handler(srv, stream)
	}
	err := handler(srv, orcaCountingServerStream{stream})
	if err != nil {
		orcaEgressMessages.Add(1)
		orcaErrors.Add(1)
	}
	return err
}

type orcaCountingServerStream struct {
	grpc.ServerStream
}

func (s orcaCountingServerStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		orcaEgressMessages.Add(1)
	}
	return err
}
