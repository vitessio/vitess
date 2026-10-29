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

package command

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"

	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
)

// TestLegacyCommandLogsActionFailed checks the record logged when a legacy
// command fails, here because nothing listens on the server address: the
// action, when there is one, and the error are attributes.
func TestLegacyCommandLogsActionFailed(t *testing.T) {
	oldServer, oldActionTimeout := server, actionTimeout
	t.Cleanup(func() { server, actionTimeout = oldServer, oldActionTimeout })
	server, actionTimeout = "localhost:1", time.Second

	logs := logtest.Capture(t, slog.LevelInfo)
	err := runLegacyCommand(t.Context(), []string{"GetKeyspace", "ks"})
	require.Error(t, err)
	record := logs.Record(t, "action failed")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "GetKeyspace", record["action"])
	assert.Equal(t, err.Error(), record["error"])

	logs.Reset()
	err = runLegacyCommand(t.Context(), nil)
	require.Error(t, err)
	record = logs.Record(t, "action failed")
	assert.Equal(t, err.Error(), record["error"])
	assert.NotContains(t, record, "action")
}
