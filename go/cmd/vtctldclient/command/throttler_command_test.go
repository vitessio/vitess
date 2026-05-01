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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// fakeQueryThrottlerServer captures UpdateQueryThrottlerConfig calls so we can
// assert what the command sent over the wire.
type fakeQueryThrottlerServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	gotRequest *vtctldatapb.UpdateQueryThrottlerConfigRequest
	returnErr  error
}

func (s *fakeQueryThrottlerServer) UpdateQueryThrottlerConfig(_ context.Context, req *vtctldatapb.UpdateQueryThrottlerConfigRequest) (*vtctldatapb.UpdateQueryThrottlerConfigResponse, error) {
	s.gotRequest = req
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &vtctldatapb.UpdateQueryThrottlerConfigResponse{}, nil
}

const validQueryThrottlerConfigJSON = `{
	"enabled": true,
	"strategy": "TABLET_THROTTLER",
	"tablet_strategy_config": {
		"tablet_rules": {
			"PRIMARY": {
				"statement_rules": {
					"SELECT": {
						"metric_rules": {
							"lag": {
								"thresholds": [
									{"above": 5.0, "throttle": 100}
								]
							}
						}
					}
				}
			}
		}
	}
}`

// setupUpdateQueryThrottlerConfigTest wires the local vtctld client to the
// given fake server and registers cleanup that restores all global state we
// touch (os.Args, the client protocol, the command's option struct, and the
// cobra Changed bits on its flags).
func setupUpdateQueryThrottlerConfigTest(t *testing.T, server vtctlservicepb.VtctldServer) {
	t.Helper()

	savedArgs := append([]string{}, os.Args...)
	savedProtocol := VtctldClientProtocol
	savedOpts := updateQueryThrottlerConfigOptions

	t.Cleanup(func() {
		os.Args = savedArgs
		VtctldClientProtocol = savedProtocol
		updateQueryThrottlerConfigOptions = savedOpts
		UpdateQueryThrottlerConfig.Flags().VisitAll(func(f *pflag.Flag) {
			f.Changed = false
		})
	})

	localvtctldclient.SetServer(server)
	VtctldClientProtocol = "local"
}

func TestUpdateQueryThrottlerConfigCommand(t *testing.T) {
	tmpDir := t.TempDir()
	validConfigFile := filepath.Join(tmpDir, "valid.json")
	require.NoError(t, os.WriteFile(validConfigFile, []byte(validQueryThrottlerConfigJSON), 0o644))

	missingConfigFile := filepath.Join(tmpDir, "no-such-file.json")

	tests := []struct {
		name         string
		args         []string
		wantErr      string
		wantNoCall   bool
		wantKeyspace string
	}{
		{
			name:       "missing keyspace arg",
			args:       []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config", validQueryThrottlerConfigJSON},
			wantErr:    "accepts 1 arg",
			wantNoCall: true,
		},
		{
			name:       "neither --config nor --config-file",
			args:       []string{"vtctldclient", "UpdateQueryThrottlerConfig", "my_keyspace"},
			wantErr:    "must pass exactly one of --config or --config-file",
			wantNoCall: true,
		},
		{
			name:       "both --config and --config-file",
			args:       []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config", validQueryThrottlerConfigJSON, "--config-file", validConfigFile, "my_keyspace"},
			wantErr:    "must pass exactly one of --config or --config-file",
			wantNoCall: true,
		},
		{
			name:       "--config-file points at non-existent file",
			args:       []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config-file", missingConfigFile, "my_keyspace"},
			wantErr:    "no such file",
			wantNoCall: true,
		},
		{
			name:       "--config has invalid JSON",
			args:       []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config", `{not json`, "my_keyspace"},
			wantErr:    "syntax error",
			wantNoCall: true,
		},
		{
			name:       "--config fails content validation",
			args:       []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config", `{"enabled":true}`, "my_keyspace"},
			wantErr:    "strategy must be TABLET_THROTTLER",
			wantNoCall: true,
		},
		{
			name:         "--config happy path",
			args:         []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config", validQueryThrottlerConfigJSON, "my_keyspace"},
			wantKeyspace: "my_keyspace",
		},
		{
			name:         "-c happy path (short flag)",
			args:         []string{"vtctldclient", "UpdateQueryThrottlerConfig", "-c", validQueryThrottlerConfigJSON, "my_keyspace"},
			wantKeyspace: "my_keyspace",
		},
		{
			name:         "--config-file happy path",
			args:         []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config-file", validConfigFile, "my_keyspace"},
			wantKeyspace: "my_keyspace",
		},
		{
			name:         "-f happy path (short flag)",
			args:         []string{"vtctldclient", "UpdateQueryThrottlerConfig", "-f", validConfigFile, "my_keyspace"},
			wantKeyspace: "my_keyspace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeServer := &fakeQueryThrottlerServer{}
			setupUpdateQueryThrottlerConfigTest(t, fakeServer)
			os.Args = tt.args

			err := Root.Execute()

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tt.wantNoCall {
				assert.Nil(t, fakeServer.gotRequest, "server should not have been called")
				return
			}

			require.NotNil(t, fakeServer.gotRequest, "server was not called")
			assert.Equal(t, tt.wantKeyspace, fakeServer.gotRequest.Keyspace)

			wantConfig := &querythrottlerpb.Config{}
			require.NoError(t, json2.UnmarshalPB([]byte(validQueryThrottlerConfigJSON), wantConfig))
			assert.True(t, proto.Equal(wantConfig, fakeServer.gotRequest.QueryThrottlerConfig),
				"config sent to server did not match parsed input\n got: %v\nwant: %v",
				fakeServer.gotRequest.QueryThrottlerConfig, wantConfig)
		})
	}
}

func TestUpdateQueryThrottlerConfigCommand_ServerError(t *testing.T) {
	fakeServer := &fakeQueryThrottlerServer{
		returnErr: errors.New("server boom"),
	}
	setupUpdateQueryThrottlerConfigTest(t, fakeServer)
	os.Args = []string{"vtctldclient", "UpdateQueryThrottlerConfig", "--config", validQueryThrottlerConfigJSON, "my_keyspace"}

	err := Root.Execute()
	require.ErrorContains(t, err, "server boom")
	require.NotNil(t, fakeServer.gotRequest, "server should have been called before returning the error")
}
