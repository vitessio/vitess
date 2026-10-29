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

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const exitTestEnv = "TEST_VTCTL_EXIT"

// TestActionFailedLogsActionAndError checks the record logged before vtctl
// exits when a command fails, through a legacy command and through a vtctldclient
// command. vtctl exits, so it runs in a child process.
func TestActionFailedLogsActionAndError(t *testing.T) {
	topoFlags := []string{"vtctl", "--topo-implementation", "test", "--topo-global-server-address", "-", "--topo-global-root", "/global"}
	tests := []struct {
		name      string
		args      []string
		action    string
		error     string
		errorCode string
	}{{
		name:      "legacy",
		args:      []string{"CreateKeyspace", "--keyspace_type", "SNAPSHOT", "snapshot_ks"},
		action:    "CreateKeyspace",
		error:     "base_keyspace must be specified",
		errorCode: "INVALID_ARGUMENT",
	}, {
		name:      "vtctldclient",
		args:      []string{"VtctldCommand", "ApplyVSchema", "--vschema", `{"tables": {"t": {"column_vindexes": [{"column": "id", "name": "no_such_vindex"}]}}}`, "ks"},
		action:    "VtctldCommand",
		error:     "vindex no_such_vindex not found for table t",
		errorCode: "NOT_FOUND",
	}}
	if name := os.Getenv(exitTestEnv); name != "" {
		for _, tt := range tests {
			if tt.name != name {
				continue
			}
			ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			require.NoError(t, ts.CreateKeyspace(t.Context(), "ks", &topodatapb.Keyspace{}))
			topo.RegisterFactory("test", factory)
			// Flag parsing replaces the logger, and so does the vtctldclient
			// command tree, so swap it once each is done.
			swap := func() { log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil))) }
			servenv.OnRun(swap)
			preRun := command.Root.PersistentPreRunE
			command.Root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
				err := preRun(cmd, args)
				swap()
				return err
			}
			os.Args = append(topoFlags, tt.args...)
			main()
		}
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestActionFailedLogsActionAndError$")
			cmd.Env = append(os.Environ(), exitTestEnv+"="+tt.name)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			err := cmd.Run()

			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr)
			assert.Equal(t, 255, exitErr.ExitCode())

			var record map[string]any
			for line := range strings.SplitSeq(stderr.String(), "\n") {
				if strings.Contains(line, `"msg":"action failed"`) {
					require.NoError(t, json.Unmarshal([]byte(line), &record))
				}
			}
			require.NotNil(t, record, stderr.String())
			assert.Equal(t, "ERROR", record["level"])
			assert.Equal(t, tt.action, record["action"])
			assert.Contains(t, record["error"], tt.error)
			assert.Equal(t, tt.errorCode, record["error_code"])
		})
	}
}
