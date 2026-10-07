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
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// fakeBackupServer streams one terminal Backup response, then returns err.
type fakeBackupServer struct {
	vtctlservicepb.UnimplementedVtctldServer
	status tabletmanagerdatapb.BackupResponse_Status
	err    error
}

func (s *fakeBackupServer) Backup(_ *vtctldatapb.BackupRequest, stream vtctlservicepb.Vtctld_BackupServer) error {
	if err := stream.Send(&vtctldatapb.BackupResponse{Status: s.status}); err != nil {
		return err
	}
	return s.err
}

func TestRunVtctldCommandBackupExitCode(t *testing.T) {
	origArgs, origProtocol := os.Args, command.VtctldClientProtocol
	t.Cleanup(func() {
		os.Args, command.VtctldClientProtocol = origArgs, origProtocol
		_ = command.Backup.Flags().Set("json", "false")
	})

	tcs := []struct {
		name      string
		args      []string
		status    tabletmanagerdatapb.BackupResponse_Status
		streamErr error
		wantCode  int
		wantErr   string
	}{
		{
			name:     "empty with --json",
			args:     []string{"Backup", "--json", "zone1-100"},
			status:   tabletmanagerdatapb.BackupResponse_EMPTY,
			wantCode: command.EmptyBackupExitCode,
		},
		{
			name:     "usable with --json",
			args:     []string{"Backup", "--json", "zone1-100"},
			status:   tabletmanagerdatapb.BackupResponse_USABLE,
			wantCode: 0,
		},
		{
			name:     "empty without --json",
			args:     []string{"Backup", "zone1-100"},
			status:   tabletmanagerdatapb.BackupResponse_EMPTY,
			wantCode: 0,
		},
		{
			name:      "stream error",
			args:      []string{"Backup", "--json", "zone1-100"},
			streamErr: errors.New("tablet went away"),
			wantCode:  255,
			wantErr:   "tablet went away",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// The command tree is shared across executions: flag values persist,
			// and cobra only sets a subcommand's context if it has none, so an
			// earlier, now-cancelled context would otherwise be reused.
			require.NoError(t, command.Backup.Flags().Set("json", "false"))
			command.Backup.SetContext(t.Context())

			code, err := runVtctldCommand(t.Context(), &fakeBackupServer{status: tc.status, err: tc.streamErr}, tc.args)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantCode, code)
		})
	}
}
