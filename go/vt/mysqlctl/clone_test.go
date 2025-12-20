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

package mysqlctl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestBuildCloneCommand(t *testing.T) {
	tests := []struct {
		name     string
		executor *CloneExecutor
		expected string
	}{
		{
			name: "with SSL",
			executor: &CloneExecutor{
				DonorHost:     "192.168.1.100",
				DonorPort:     3306,
				DonorUser:     "vt_clone",
				DonorPassword: "secret123",
				UseSSL:        true,
			},
			expected: "CLONE INSTANCE FROM 'vt_clone'@'192.168.1.100':3306 IDENTIFIED BY 'secret123' REQUIRE SSL",
		},
		{
			name: "without SSL",
			executor: &CloneExecutor{
				DonorHost:     "10.0.0.50",
				DonorPort:     3307,
				DonorUser:     "clone_user",
				DonorPassword: "password",
				UseSSL:        false,
			},
			expected: "CLONE INSTANCE FROM 'clone_user'@'10.0.0.50':3307 IDENTIFIED BY 'password' REQUIRE NO SSL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.executor.buildCloneCommand()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateRecipient(t *testing.T) {
	tests := []struct {
		name         string
		versionQuery *sqltypes.Result
		pluginQuery  *sqltypes.Result
		expectError  bool
		errorContain string
	}{
		{
			name: "valid MySQL 8.0.32 with clone plugin",
			versionQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("@@version", "varchar"),
				"8.0.32",
			),
			pluginQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar"),
				"ACTIVE",
			),
			expectError: false,
		},
		{
			name: "MySQL version too old",
			versionQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("@@version", "varchar"),
				"8.0.16",
			),
			expectError:  true,
			errorContain: "requires version 8.0.17",
		},
		{
			name: "clone plugin not installed",
			versionQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("@@version", "varchar"),
				"8.0.32",
			),
			pluginQuery:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar")),
			expectError:  true,
			errorContain: "clone plugin is not installed",
		},
		{
			name: "clone plugin not active",
			versionQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("@@version", "varchar"),
				"8.0.32",
			),
			pluginQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar"),
				"DISABLED",
			),
			expectError:  true,
			errorContain: "clone plugin is not active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmd := NewFakeMysqlDaemon(nil)
			defer fmd.Close()

			fmd.FetchSuperQueryMap = map[string]*sqltypes.Result{
				"SELECT @@version": tt.versionQuery,
			}
			if tt.pluginQuery != nil {
				fmd.FetchSuperQueryMap["SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'"] = tt.pluginQuery
			}

			executor := &CloneExecutor{
				DonorHost:     "192.168.1.100",
				DonorPort:     3306,
				DonorUser:     "vt_clone",
				DonorPassword: "secret",
				UseSSL:        false,
			}

			err := executor.ValidateRecipient(context.Background(), fmd)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type cloneFromDonorTestEnv struct {
	ctx      context.Context
	logger   *logutil.MemoryLogger
	ts       *topo.Server
	mysqld   *FakeMysqlDaemon
	keyspace string
	shard    string
}

func createCloneFromDonorTestEnv(t *testing.T) *cloneFromDonorTestEnv {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	// Create in-memory topo server with a test cell
	ts := memorytopo.NewServer(ctx, "cell1")

	keyspace := "test"
	shard := "-"

	// Create keyspace in topology
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))

	// Create fake MySQL daemon
	sqldb := fakesqldb.New(t)
	sqldb.SetNeverFail(true)
	mysqld := NewFakeMysqlDaemon(sqldb)

	t.Cleanup(func() {
		mysqld.Close()
		sqldb.Close()
		utils.EnsureNoLeaks(t)
	})

	return &cloneFromDonorTestEnv{
		ctx:      ctx,
		logger:   logger,
		ts:       ts,
		mysqld:   mysqld,
		keyspace: keyspace,
		shard:    shard,
	}
}

func TestCloneFromDonor(t *testing.T) {
	testCases := []struct {
		name             string
		cloneFromPrimary bool
		cloneFromTablet  string
		setup            func(*testing.T, *cloneFromDonorTestEnv)
		wantErr          bool
		wantErrContains  string
	}{
		{
			name:             "clone from primary, GetShard fails",
			cloneFromPrimary: true,
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// Don't create the shard, so GetShard will fail
			},
			wantErr:         true,
			wantErrContains: "failed to get shard",
		},
		{
			name:             "clone from primary, shard has no primary",
			cloneFromPrimary: true,
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// Create shard without a primary
				require.NoError(t, env.ts.CreateShard(env.ctx, env.keyspace, env.shard))
			},
			wantErr:         true,
			wantErrContains: "has no primary",
		},
		{
			name:            "clone from tablet, invalid tablet alias",
			cloneFromTablet: "invalid-alias-format",
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// No setup needed, invalid alias will fail parsing
			},
			wantErr:         true,
			wantErrContains: "invalid tablet alias",
		},
		{
			name:             "neither cloneFromPrimary nor cloneFromTablet specified",
			cloneFromPrimary: false,
			cloneFromTablet:  "",
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// No setup needed, will fail when no donor is specified
			},
			wantErr:         true,
			wantErrContains: "no donor specified",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := createCloneFromDonorTestEnv(t)

			// Save and restore global flags
			oldCloneFromPrimary := cloneFromPrimary
			oldCloneFromTablet := cloneFromTablet
			defer func() {
				cloneFromPrimary = oldCloneFromPrimary
				cloneFromTablet = oldCloneFromTablet
			}()

			// Set test flag values
			cloneFromPrimary = tc.cloneFromPrimary
			cloneFromTablet = tc.cloneFromTablet

			// Run setup if provided
			if tc.setup != nil {
				tc.setup(t, env)
			}

			// Execute CloneFromDonor
			pos, err := CloneFromDonor(env.ctx, env.ts, env.mysqld, env.keyspace, env.shard)

			// Verify results
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.ErrorContains(t, err, tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, pos)
			}
		})
	}
}
