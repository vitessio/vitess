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

	"google.golang.org/grpc/stats"
)

// orcaEgressMessages counts gRPC messages sent since the last ORCA report.
// We count sent messages, not requests, so unary calls and long-lived streams
// like VStream are measured the same way. A unary call sends one message, so
// for unary traffic this equals QPS. A stream gets one request and then sends
// many messages over time, so this represents the stream's ongoing work.
var orcaEgressMessages atomic.Int64

type orcaEgressStatsHandler struct{}

func (orcaEgressStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (orcaEgressStatsHandler) HandleRPC(_ context.Context, rpcStats stats.RPCStats) {
	if _, ok := rpcStats.(*stats.OutPayload); ok {
		orcaEgressMessages.Add(1)
	}
}

func (orcaEgressStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (orcaEgressStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
