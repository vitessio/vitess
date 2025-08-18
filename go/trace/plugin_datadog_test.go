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

	"vitess.io/vitess/go/test/utils"
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
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vttablet")
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vtgate")
	// Test that the debug mode flag is properly registered
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	// Get the plugin flag function from init()
	require.Greater(t, len(pluginFlags), 0, "pluginFlags should contain at least one function")

	// Apply all plugin flags to ensure we get the datadog ones
	for _, pluginFlag := range pluginFlags {
		pluginFlag(fs)
	}

	// Verify the debug mode flag exists
	debugFlag := fs.Lookup("datadog-trace-debug-mode")
	require.NotNil(t, debugFlag, "datadog-trace-debug-mode flag should be registered")
	require.Equal(t, "false", debugFlag.DefValue, "default value should be false")

	// Also verify other datadog flags exist
	hostFlag := fs.Lookup("datadog-agent-host")
	require.NotNil(t, hostFlag, "datadog-agent-host flag should be registered")

	portFlag := fs.Lookup("datadog-agent-port")
	require.NotNil(t, portFlag, "datadog-agent-port flag should be registered")
}

func TestDataDogTraceDebugModeConfiguration(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vttablet")
	utils.SkipIfBinaryIsBelowVersion(t, 23, "vtgate")
	// Save original value to restore later
	originalValue := dataDogTraceDebugMode.Get()
	defer dataDogTraceDebugMode.Set(originalValue)

	// Test setting to true
	dataDogTraceDebugMode.Set(true)
	require.True(t, dataDogTraceDebugMode.Get(), "debug mode should be true when set to true")

	// Test setting to false
	dataDogTraceDebugMode.Set(false)
	require.False(t, dataDogTraceDebugMode.Get(), "debug mode should be false when set to false")
}
