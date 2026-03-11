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

package trace

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOTelFactoryRegistered(t *testing.T) {
	_, ok := tracingBackendFactories["opentelemetry"]
	assert.True(t, ok, "opentelemetry factory should be registered")
}

func TestOTelPluginFlags(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	for _, fn := range pluginFlags {
		fn(fs)
	}

	endpointFlag := fs.Lookup("otel-endpoint")
	require.NotNil(t, endpointFlag)
	assert.Equal(t, "localhost:4317", endpointFlag.DefValue)

	insecureFlag := fs.Lookup("otel-insecure")
	require.NotNil(t, insecureFlag)
	assert.Equal(t, "false", insecureFlag.DefValue)
}
