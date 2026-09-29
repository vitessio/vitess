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

func TestOrcaCountingUnaryInterceptorCountsFailedCallsAsQueriesAndErrors(t *testing.T) {
	orcaEgressMessages.Store(0)
	orcaErrors.Store(0)

	succeed := func(context.Context, any) (any, error) { return "ok", nil }
	fail := func(context.Context, any) (any, error) { return nil, errors.New("failed") }
	for _, handler := range []grpc.UnaryHandler{succeed, succeed, fail} {
		_, _ = orcaCountingUnaryInterceptor(t.Context(), nil, &grpc.UnaryServerInfo{}, handler)
	}

	assert.EqualValues(t, 3, orcaEgressMessages.Load())
	assert.EqualValues(t, 1, orcaErrors.Load())
}

func TestOrcaCountingStreamInterceptorCountsEverySentMessageAndStreamEnd(t *testing.T) {
	orcaEgressMessages.Store(0)
	orcaErrors.Store(0)

	err := orcaCountingStreamInterceptor(nil, fakeSendServerStream{}, &grpc.StreamServerInfo{}, func(_ any, stream grpc.ServerStream) error {
		for range 3 {
			if err := stream.SendMsg("event"); err != nil {
				return err
			}
		}
		return nil
	})

	require.NoError(t, err)
	assert.EqualValues(t, 4, orcaEgressMessages.Load())
	assert.EqualValues(t, 0, orcaErrors.Load())
}

func TestOrcaCountingStreamInterceptorCountsFailedStreamAsQueryAndError(t *testing.T) {
	orcaEgressMessages.Store(0)
	orcaErrors.Store(0)

	err := orcaCountingStreamInterceptor(nil, fakeSendServerStream{}, &grpc.StreamServerInfo{}, func(_ any, stream grpc.ServerStream) error {
		for range 2 {
			if err := stream.SendMsg("event"); err != nil {
				return err
			}
		}
		return errors.New("failed")
	})

	require.Error(t, err)
	assert.EqualValues(t, 3, orcaEgressMessages.Load())
	assert.EqualValues(t, 1, orcaErrors.Load())
}

type fakeSendServerStream struct {
	grpc.ServerStream
}

func (fakeSendServerStream) SendMsg(any) error {
	return nil
}
