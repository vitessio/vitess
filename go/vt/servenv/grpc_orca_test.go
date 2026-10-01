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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestOrcaCountingInterceptorsCountQueriesAndErrors(t *testing.T) {
	const (
		vtgateExecute         = "/vtgateservice.Vitess/Execute"
		vtgateVStream         = "/vtgateservice.Vitess/VStream"
		healthCheck           = "/grpc.health.v1.Health/Check"
		healthWatch           = "/grpc.health.v1.Health/Watch"
		orcaStreamCoreMetrics = "/xds.service.orca.v3.OpenRcaService/StreamCoreMetrics"
		tabletExecute         = "/queryservice.Query/Execute"
		tabletStreamExecute   = "/queryservice.Query/StreamExecute"
		tabletStreamHealth    = "/queryservice.Query/StreamHealth"
	)
	errFailed := errors.New("failed")

	tests := []struct {
		name        string
		fullMethod  string
		stream      bool
		sends       int
		err         error
		wantQueries int64
		wantErrors  int64
	}{
		{name: "successful unary call counts one query", fullMethod: vtgateExecute, wantQueries: 1},
		{name: "failed unary call counts a query and an error", fullMethod: vtgateExecute, err: errFailed, wantQueries: 1, wantErrors: 1},
		{name: "successful stream counts every sent message", fullMethod: vtgateVStream, stream: true, sends: 3, wantQueries: 3},
		{name: "failed stream counts sent messages plus a query and an error", fullMethod: vtgateVStream, stream: true, sends: 2, err: errFailed, wantQueries: 3, wantErrors: 1},
		{name: "health check is not counted", fullMethod: healthCheck},
		{name: "failed health check is not counted", fullMethod: healthCheck, err: errFailed},
		{name: "health watch stream is not counted", fullMethod: healthWatch, stream: true, sends: 3, err: errFailed},
		{name: "ORCA report stream is not counted", fullMethod: orcaStreamCoreMetrics, stream: true, sends: 3, err: errFailed},
		{name: "tablet health stream is not counted", fullMethod: tabletStreamHealth, stream: true, sends: 3, err: errFailed},
		{name: "other tablet unary call is counted", fullMethod: tabletExecute, wantQueries: 1},
		{name: "other tablet stream is counted", fullMethod: tabletStreamExecute, stream: true, sends: 3, wantQueries: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orcaEgressMessages.Store(0)
			orcaErrors.Store(0)

			var err error
			if tt.stream {
				err = orcaCountingStreamInterceptor(nil, fakeSendServerStream{}, &grpc.StreamServerInfo{FullMethod: tt.fullMethod}, func(_ any, stream grpc.ServerStream) error {
					for range tt.sends {
						if err := stream.SendMsg("event"); err != nil {
							return err
						}
					}
					return tt.err
				})
			} else {
				_, err = orcaCountingUnaryInterceptor(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: tt.fullMethod}, func(context.Context, any) (any, error) {
					return nil, tt.err
				})
			}

			require.ErrorIs(t, err, tt.err)
			assert.Equal(t, tt.wantQueries, orcaEgressMessages.Load())
			assert.Equal(t, tt.wantErrors, orcaErrors.Load())
		})
	}
}

type fakeSendServerStream struct {
	grpc.ServerStream
}

func (fakeSendServerStream) SendMsg(any) error {
	return nil
}
