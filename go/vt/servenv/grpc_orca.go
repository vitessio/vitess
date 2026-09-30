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
	"sync/atomic"

	"google.golang.org/grpc"
)

// orcaEgressMessages counts gRPC messages sent since the last ORCA report, so
// unary calls (one message each, i.e. QPS) and long-lived streams like VStream
// are measured the same way. Every message counts equally regardless of cost,
// health checks and ORCA reports included. Every call also counts once when it
// ends, and once in orcaErrors if it ends with an error, so EPS/QPS is its
// error rate.
var orcaEgressMessages atomic.Int64

var orcaErrors atomic.Int64

func orcaCountingUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	orcaEgressMessages.Add(1)
	if err != nil {
		orcaErrors.Add(1)
	}
	return resp, err
}

func orcaCountingStreamInterceptor(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
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
