/*
Copyright 2024 The Vitess Authors.

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

package trace

import (
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
)

func TestGetOpenTracingTracer(t *testing.T) {
	tracer := datadogTracer{
		actual: opentracing.GlobalTracer(),
	}
	require.Equal(t, opentracing.GlobalTracer(), tracer.GetOpenTracingTracer())
}

func TestNewDataDogTracerHostAndPortNotSet(t *testing.T) {
	tracingSvc, closer, err := newDatadogTracer("svc")
	expectedErr := "need host and port to datadog agent to use datadog tracing"
	require.ErrorContains(t, err, expectedErr)
	require.Nil(t, tracingSvc)
	require.Nil(t, closer)
}
