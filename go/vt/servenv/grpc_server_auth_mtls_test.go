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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestMtlsAuthPluginInitializerLogsSubstrings(t *testing.T) {
	old := clientCertSubstrings
	clientCertSubstrings = "CN=client1:CN=client2"
	t.Cleanup(func() { clientCertSubstrings = old })
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := mtlsAuthPluginInitializer()
	require.NoError(t, err)

	record := logs.Record(t, "mtls auth plugin have initialized successfully with allowed client cert name substrings")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, []any{"CN=client1", "CN=client2"}, record["client_cert_substrings"])
}
