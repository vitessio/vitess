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
	"github.com/spf13/pflag"
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

func TestDataDogTraceDebugModeFlag(t *testing.T) {
	// Test that the debug mode flag is properly registered
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	// Get the plugin flag function from init()
	require.Greater(t, len(pluginFlags), 0)

	// Apply all plugin flags to ensure we get the datadog ones
	for _, pluginFlag := range pluginFlags {
		pluginFlag(fs)
	}

	// Verify the debug mode flag exists
	debugFlag := fs.Lookup("datadog-trace-debug-mode")
	require.NotNil(t, debugFlag)
	require.Equal(t, "false", debugFlag.DefValue)

	// Also verify other datadog flags exist
	hostFlag := fs.Lookup("datadog-agent-host")
	require.NotNil(t, hostFlag)

	portFlag := fs.Lookup("datadog-agent-port")
	require.NotNil(t, portFlag)
}

func TestDataDogTraceDebugModeConfiguration(t *testing.T) {
	// Save original value to restore later
	originalValue := dataDogTraceDebugMode.Get()
	defer dataDogTraceDebugMode.Set(originalValue)

	// Test setting to true
	dataDogTraceDebugMode.Set(true)
	require.True(t, dataDogTraceDebugMode.Get())

	// Test setting to false
	dataDogTraceDebugMode.Set(false)
	require.False(t, dataDogTraceDebugMode.Get())
}
