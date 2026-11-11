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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"

	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
)

// TestLegacyCommandWithoutArgs checks that a legacy command with no arguments,
// which has no action to log, fails with an error rather than a panic. Nothing
// listens on the server address, so the command fails.
func TestLegacyCommandWithoutArgs(t *testing.T) {
	oldServer, oldActionTimeout := server, actionTimeout
	t.Cleanup(func() { server, actionTimeout = oldServer, oldActionTimeout })
	server, actionTimeout = "localhost:1", time.Second

	logs := logtest.Capture(t, slog.LevelInfo)
	require.Error(t, runLegacyCommand(t.Context(), nil))
	logs.Record(t, "action failed")
}
