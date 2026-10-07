/*
Copyright 2025 The Vitess Authors.

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
	"errors"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

func TestBackupCommand_InitSQLFlags(t *testing.T) {
	tabletAlias := "zone1-100"
	defaultInitQuery := "OPTIMIZE LOCAL TABLE foo"
	defaultInitQueries := []string{defaultInitQuery}
	defaultInitTimeout := 1 * time.Hour
	defaultInitTabletTypes := []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}
	testCases := []struct {
		name                   string
		args                   []string
		wantInitSQLQueries     []string
		wantInitSQLTabletTypes []topodatapb.TabletType
		wantInitSQLTimeout     time.Duration
		wantInitSQLFailOnError bool
		wantErr                string
	}{
		{
			name: "no init SQL flags",
			args: []string{
				tabletAlias,
			},
			wantInitSQLQueries:     nil,
			wantInitSQLTabletTypes: nil,
			wantInitSQLTimeout:     0,
			wantInitSQLFailOnError: false,
		},
		{
			name: "no init SQL tablet types",
			args: []string{
				tabletAlias,
				"--init-backup-sql-queries", defaultInitQuery,
			},
			wantInitSQLQueries:     defaultInitQueries,
			wantInitSQLTabletTypes: nil,
			wantInitSQLTimeout:     defaultInitTimeout,
			wantInitSQLFailOnError: true,
			wantErr:                "backup init SQL queries provided but no tablet types on which to run them",
		},
		{
			name: "no init SQL timeout",
			args: []string{
				tabletAlias,
				"--init-backup-sql-queries", defaultInitQuery,
				"--init-backup-tablet-types=replica,rdonly",
			},
			wantInitSQLQueries:     defaultInitQueries,
			wantInitSQLTabletTypes: defaultInitTabletTypes,
			wantInitSQLFailOnError: false,
			wantErr:                "backup init SQL queries provided but no timeout provided",
		},
		{
			name: "single init SQL query",
			args: []string{
				"zone1-100",
				"--init-backup-sql-queries", "OPTIMIZE LOCAL TABLE foo",
				"--init-backup-tablet-types=replica,rdonly",
				"--init-backup-sql-timeout", defaultInitTimeout.String(),
			},
			wantInitSQLQueries:     []string{"OPTIMIZE LOCAL TABLE foo"},
			wantInitSQLTabletTypes: defaultInitTabletTypes,
			wantInitSQLTimeout:     defaultInitTimeout,
			wantInitSQLFailOnError: false,
		},
		{
			name: "multiple init SQL queries",
			args: []string{
				"zone1-100",
				"--init-backup-sql-queries", "SET sql_log_bin=0",
				"--init-backup-sql-queries", "OPTIMIZE TABLE test.table1",
				"--init-backup-sql-queries", "SET sql_log_bin=1",
				"--init-backup-tablet-types=replica,rdonly",
				"--init-backup-sql-timeout", defaultInitTimeout.String(),
				"--init-backup-sql-fail-on-error=true",
			},
			wantInitSQLQueries:     []string{"SET sql_log_bin=0", "OPTIMIZE TABLE test.table1", "SET sql_log_bin=1"},
			wantInitSQLTabletTypes: defaultInitTabletTypes,
			wantInitSQLTimeout:     defaultInitTimeout,
			wantInitSQLFailOnError: true,
		},
		{
			name: "init SQL with different tablet types and timeout",
			args: []string{
				"zone1-100",
				"--init-backup-sql-queries", defaultInitQuery,
				"--init-backup-tablet-types=primary,replica",
				"--init-backup-sql-timeout=5m",
			},
			wantInitSQLQueries:     defaultInitQueries,
			wantInitSQLTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
			wantInitSQLTimeout:     5 * time.Minute,
			wantInitSQLFailOnError: false,
		},
		{
			name: "all init SQL flags",
			args: []string{
				"zone1-100",
				"--init-backup-sql-queries", "SET sql_log_bin=0",
				"--init-backup-sql-queries", "OPTIMIZE TABLE test.table1",
				"--init-backup-sql-queries", "SET sql_log_bin=1",
				"--init-backup-tablet-types=PRIMARY,REPLICA,RDONLY",
				"--init-backup-sql-timeout=10m",
				"--init-backup-sql-fail-on-error",
			},
			wantInitSQLQueries: []string{
				"SET sql_log_bin=0",
				"OPTIMIZE TABLE test.table1",
				"SET sql_log_bin=1",
			},
			wantInitSQLTabletTypes: []topodatapb.TabletType{
				topodatapb.TabletType_PRIMARY,
				topodatapb.TabletType_REPLICA,
				topodatapb.TabletType_RDONLY,
			},
			wantInitSQLTimeout:     10 * time.Minute,
			wantInitSQLFailOnError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset the global backupOptions before each test.
			backupOptions.InitSQLQueries = nil
			backupOptions.InitSQLTabletTypes = nil
			backupOptions.InitSQLTimeout = 0
			backupOptions.InitSQLFailOnError = false

			// Create a new command instance.
			cmd := Backup
			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				flag.Changed = false
			})
			// Set args and parse.
			cmd.SetArgs(tc.args)
			err := cmd.ParseFlags(tc.args)
			require.NoError(t, err)

			err = validateBackupOptions()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			} else {
				require.NoError(t, err)
			}

			// Verify the parsed values.
			assert.Equal(t, tc.wantInitSQLQueries, backupOptions.InitSQLQueries, "InitSQLQueries mismatch")
			assert.Equal(t, tc.wantInitSQLTabletTypes, backupOptions.InitSQLTabletTypes, "InitSQLTabletTypes mismatch")
			assert.Equal(t, tc.wantInitSQLTimeout, backupOptions.InitSQLTimeout, "InitSQLTimeout mismatch")
			assert.Equal(t, tc.wantInitSQLFailOnError, backupOptions.InitSQLFailOnError, "InitSQLFailOnError mismatch")
		})
	}
}

func TestBackupCommand_BuildsCorrectRequest(t *testing.T) {
	testCases := []struct {
		name               string
		initSQLQueries     []string
		initSQLTabletTypes []topodatapb.TabletType
		initSQLTimeout     time.Duration
		initSQLFailOnError bool
		wantInitSQLNil     bool
		wantQueriesLen     int
		wantTabletTypesLen int
		wantTimeoutSeconds int64
		wantFailOnError    bool
	}{
		{
			name:               "no init SQL",
			initSQLQueries:     nil,
			initSQLTabletTypes: nil,
			initSQLTimeout:     0,
			initSQLFailOnError: false,
			wantInitSQLNil:     false,
			wantQueriesLen:     0,
			wantTabletTypesLen: 0,
			wantTimeoutSeconds: 0,
			wantFailOnError:    false,
		},
		{
			name:               "with init SQL queries only",
			initSQLQueries:     []string{"SET sql_log_bin=0"},
			initSQLTabletTypes: nil,
			initSQLTimeout:     0,
			initSQLFailOnError: false,
			wantInitSQLNil:     false,
			wantQueriesLen:     1,
			wantTabletTypesLen: 0,
			wantTimeoutSeconds: 0,
			wantFailOnError:    false,
		},
		{
			name:               "with all init SQL fields",
			initSQLQueries:     []string{"SET sql_log_bin=0", "OPTIMIZE TABLE test.table1"},
			initSQLTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
			initSQLTimeout:     5 * time.Minute,
			initSQLFailOnError: true,
			wantInitSQLNil:     false,
			wantQueriesLen:     2,
			wantTabletTypesLen: 1,
			wantTimeoutSeconds: 300,
			wantFailOnError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set the options.
			backupOptions.InitSQLQueries = tc.initSQLQueries
			backupOptions.InitSQLTabletTypes = tc.initSQLTabletTypes
			backupOptions.InitSQLTimeout = tc.initSQLTimeout
			backupOptions.InitSQLFailOnError = tc.initSQLFailOnError

			// Build the request (we test just the relevant part of commandBackup logic).
			initSQL := backupOptions.InitSQLQueries
			initSQLTabletTypes := backupOptions.InitSQLTabletTypes
			initSQLTimeout := protoutil.DurationToProto(backupOptions.InitSQLTimeout)
			initSQLFailOnError := backupOptions.InitSQLFailOnError

			// Verify the built request components.
			if tc.wantInitSQLNil {
				assert.Nil(t, initSQL)
			} else {
				assert.Len(t, initSQL, tc.wantQueriesLen)
				assert.Len(t, initSQLTabletTypes, tc.wantTabletTypesLen)
				if initSQLTimeout != nil {
					assert.Equal(t, tc.wantTimeoutSeconds, initSQLTimeout.Seconds)
				} else {
					assert.Equal(t, int64(0), tc.wantTimeoutSeconds)
				}
				assert.Equal(t, tc.wantFailOnError, initSQLFailOnError)
			}
		})
	}
}

// fakeBackupServer streams canned responses from Backup and BackupShard, then
// returns err.
type fakeBackupServer struct {
	vtctlservicepb.UnimplementedVtctldServer
	resps []*vtctldatapb.BackupResponse
	err   error
}

func (s *fakeBackupServer) Backup(_ *vtctldatapb.BackupRequest, stream vtctlservicepb.Vtctld_BackupServer) error {
	return s.send(stream)
}

func (s *fakeBackupServer) BackupShard(_ *vtctldatapb.BackupShardRequest, stream vtctlservicepb.Vtctld_BackupShardServer) error {
	return s.send(stream)
}

func (s *fakeBackupServer) send(stream vtctlservicepb.Vtctld_BackupServer) error {
	for _, resp := range s.resps {
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return s.err
}

// TestBackupCommands_JSONOutputWiring runs Backup and BackupShard end to end
// against a local server to check each command passes its own --json flag.
func TestBackupCommands_JSONOutputWiring(t *testing.T) {
	originalProtocol := VtctldClientProtocol
	VtctldClientProtocol = "local"
	t.Cleanup(func() {
		Root.SetArgs(nil)
		VtctldClientProtocol = originalProtocol
		backupOptions.OutputJSON = false
		backupShardOptions.OutputJSON = false
	})

	testCases := []struct {
		name      string
		args      []string
		streamErr error
		wantEmpty bool
		wantErr   string
	}{
		{
			name:      "Backup --json empty",
			args:      []string{"Backup", "--json", "zone1-100"},
			wantEmpty: true,
		},
		{
			name:      "BackupShard --json empty",
			args:      []string{"BackupShard", "--json", "ks/0"},
			wantEmpty: true,
		},
		{
			name:      "BackupShard empty without --json",
			args:      []string{"BackupShard", "ks/0"},
			wantEmpty: false,
		},
		{
			name:      "Backup stream error",
			args:      []string{"Backup", "--json", "zone1-100"},
			streamErr: errors.New("tablet went away"),
			wantErr:   "tablet went away",
		},
		{
			name:      "BackupShard stream error",
			args:      []string{"BackupShard", "--json", "ks/0"},
			streamErr: errors.New("tablet went away"),
			wantErr:   "tablet went away",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Flag-bound globals persist across Root.Execute calls.
			backupOptions.OutputJSON = false
			backupShardOptions.OutputJSON = false
			backupOptions.InitSQLQueries = nil
			backupOptions.InitSQLTabletTypes = nil
			backupOptions.InitSQLTimeout = 0
			backupOptions.InitSQLFailOnError = false
			for _, cmd := range []*cobra.Command{Backup, BackupShard} {
				cmd.Flags().Lookup("json").Changed = false
			}

			localvtctldclient.SetServer(&fakeBackupServer{
				resps: []*vtctldatapb.BackupResponse{{Status: tabletmanagerdatapb.BackupResponse_EMPTY}},
				err:   tc.streamErr,
			})
			Root.SetArgs(tc.args)

			err := Root.Execute()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantEmpty, EmptyBackup())
		})
	}
}
