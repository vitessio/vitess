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

	"google.golang.org/grpc/stats"
)

type grpcIngressBytesKey struct{}

type grpcIngressBytes struct {
	wire uint64
}

var gRPCIngressStatsEnabled bool

type grpcIngressStatsHandler struct{}

func (grpcIngressStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, grpcIngressBytesKey{}, &grpcIngressBytes{})
}

func (grpcIngressStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	inPayload, ok := rpcStats.(*stats.InPayload)
	if !ok {
		return
	}
	ingressBytes, ok := ctx.Value(grpcIngressBytesKey{}).(*grpcIngressBytes)
	if !ok {
		return
	}
	ingressBytes.wire += uint64(inPayload.WireLength)
}

func (grpcIngressStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (grpcIngressStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

// GRPCIngressStatsHandler returns a stats handler that records inbound gRPC
// payload bytes on the RPC context.
func GRPCIngressStatsHandler() stats.Handler {
	return grpcIngressStatsHandler{}
}

// EnableGRPCIngressStats records inbound gRPC payload bytes on RPC contexts for
// gRPC servers created by servenv.
func EnableGRPCIngressStats() {
	gRPCIngressStatsEnabled = true
}

// GRPCIngressBytes returns inbound gRPC payload bytes recorded on ctx.
func GRPCIngressBytes(ctx context.Context) (uint64, bool) {
	ingressBytes, ok := ctx.Value(grpcIngressBytesKey{}).(*grpcIngressBytes)
	if !ok {
		return 0, false
	}
	return ingressBytes.wire, true
}
