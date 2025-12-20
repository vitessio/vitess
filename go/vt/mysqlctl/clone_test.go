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
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
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

func Test_waitForCloneComplete(t *testing.T) {
	tests := []struct {
		name         string
		queryResults []*sqltypes.Result
		queryErrors  []error
		expectError  bool
		errorContain string
	}{
		{
			name: "clone completed successfully",
			queryResults: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
					"Completed|0|",
				),
			},
			expectError: false,
		},
		{
			name: "clone completed with error",
			queryResults: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
					"Completed|3862|Clone Donor Error: 3862 : Clone requires redo log archiving to be started by BACKUP.",
				),
			},
			expectError:  true,
			errorContain: "clone completed with error",
		},
		{
			name: "clone failed",
			queryResults: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
					"Failed|3862|Clone Donor Error",
				),
			},
			expectError:  true,
			errorContain: "clone failed",
		},
		{
			name: "clone in progress then completed",
			queryResults: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
					"In Progress|0|",
				),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
					"Completed|0|",
				),
			},
			expectError: false,
		},
		{
			name: "connection error then completed",
			queryResults: []*sqltypes.Result{
				nil,
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
					"Completed|0|",
				),
			},
			queryErrors: []error{
				assert.AnError,
				nil,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmd := NewFakeMysqlDaemon(nil)
			defer fmd.Close()

			// Set up the sequence of results
			callCount := 0
			fmd.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
				if callCount < len(tt.queryResults) {
					idx := callCount
					callCount++
					var err error
					if tt.queryErrors != nil && idx < len(tt.queryErrors) {
						err = tt.queryErrors[idx]
					}
					return tt.queryResults[idx], err
				}
				return nil, assert.AnError
			}

			executor := &CloneExecutor{}

			err := executor.waitForCloneComplete(context.Background(), fmd, 5*time.Second)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_waitForCloneComplete_Timeout(t *testing.T) {
	fmd := NewFakeMysqlDaemon(nil)
	defer fmd.Close()

	// Always return "In Progress"
	fmd.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		return sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
			"In Progress|0|",
		), nil
	}

	executor := &CloneExecutor{}

	err := executor.waitForCloneComplete(context.Background(), fmd, 100*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func Test_waitForCloneComplete_ContextCanceled(t *testing.T) {
	fmd := NewFakeMysqlDaemon(nil)
	defer fmd.Close()

	// Always return "In Progress"
	fmd.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		return sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
			"In Progress|0|",
		), nil
	}

	executor := &CloneExecutor{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := executor.waitForCloneComplete(ctx, fmd, 5*time.Second)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
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

type mockDonorHandler struct {
	mysql.UnimplementedHandler
	t *testing.T
}

func (h *mockDonorHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	// Respond to donor validation queries
	switch {
	case strings.Contains(query, "SELECT @@version"):
		result := sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("@@version", "varchar"),
			"8.0.32",
		)
		return callback(result)
	case strings.Contains(query, "SELECT PLUGIN_STATUS"):
		result := sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar"),
			"ACTIVE",
		)
		return callback(result)
	case strings.Contains(query, "SELECT TABLE_SCHEMA"):
		// Return empty result (no non-InnoDB tables)
		result := sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("TABLE_SCHEMA|TABLE_NAME|ENGINE", "varchar|varchar|varchar"),
		)
		return callback(result)
	default:
		return fmt.Errorf("unexpected query: %s", query)
	}
}

func (h *mockDonorHandler) ComQueryMulti(c *mysql.Conn, sql string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) error {
	return fmt.Errorf("ComQueryMulti not implemented")
}

func (h *mockDonorHandler) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, uint16, error) {
	return nil, 0, fmt.Errorf("ComPrepare not implemented")
}

func (h *mockDonorHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("ComStmtExecute not implemented")
}

func (h *mockDonorHandler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return fmt.Errorf("ComRegisterReplica not implemented")
}

func (h *mockDonorHandler) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	return fmt.Errorf("ComBinlogDump not implemented")
}

func (h *mockDonorHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	return fmt.Errorf("ComBinlogDumpGTID not implemented")
}

func (h *mockDonorHandler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (h *mockDonorHandler) Env() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

type cloneFromDonorTestEnv struct {
	ctx        context.Context
	logger     *logutil.MemoryLogger
	ts         *topo.Server
	mysqld     *FakeMysqlDaemon
	keyspace   string
	shard      string
	donorHost  string
	donorPort  int
	donorAlias *topodatapb.TabletAlias
}

func createCloneFromDonorTestEnv(t *testing.T, donorHost string, donorPort int) *cloneFromDonorTestEnv {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	// Create in-memory topo server with a test cell
	ts := memorytopo.NewServer(ctx, "cell1")

	keyspace := "test"
	shard := "-"

	// Create keyspace in topology
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))

	// Create donor tablet in topology with the mock server's address
	donorAlias := &topodatapb.TabletAlias{Cell: "cell1", Uid: 100}
	tablet := &topodatapb.Tablet{
		Alias:         donorAlias,
		MysqlHostname: donorHost,
		MysqlPort:     int32(donorPort),
		Keyspace:      keyspace,
		Shard:         shard,
	}
	require.NoError(t, ts.CreateTablet(ctx, tablet))

	// Create fake MySQL daemon
	sqldb := fakesqldb.New(t)
	sqldb.SetNeverFail(true)
	mysqld := NewFakeMysqlDaemon(sqldb)

	// Set up default clone credentials (success path)
	dbconfigs.GlobalDBConfigs.CloneUser = dbconfigs.UserConfig{
		User:     "clone_user",
		Password: "password",
	}

	// Configure recipient mysqld for successful validation and clone by default
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT @@version": sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("@@version", "varchar"),
			"8.0.32",
		),
		"SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'": sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar"),
			"ACTIVE",
		),
		"SELECT STATE, ERROR_NO, ERROR_MESSAGE FROM performance_schema.clone_status": sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("STATE|ERROR_NO|ERROR_MESSAGE", "varchar|varchar|varchar"),
			"Completed|0|",
		),
	}

	// Set a valid GTID position by default
	mysqld.CurrentPrimaryPosition = replication.Position{
		GTIDSet: replication.Mysql56GTIDSet{},
	}

	// List all expected queries that ExecuteClone will run
	mysqld.ExpectedExecuteSuperQueryList = []string{
		fmt.Sprintf("SET GLOBAL clone_valid_donor_list = '%s:%d'", donorHost, donorPort),
		fmt.Sprintf("CLONE INSTANCE FROM 'clone_user'@'%s':%d IDENTIFIED BY 'password' REQUIRE NO SSL", donorHost, donorPort),
	}

	t.Cleanup(func() {
		mysqld.Close()
		sqldb.Close()
	})

	return &cloneFromDonorTestEnv{
		ctx:        ctx,
		logger:     logger,
		ts:         ts,
		mysqld:     mysqld,
		keyspace:   keyspace,
		shard:      shard,
		donorHost:  donorHost,
		donorPort:  donorPort,
		donorAlias: donorAlias,
	}
}

func TestCloneFromDonor(t *testing.T) {
	// Create mock donor MySQL server once for all test cases
	jsonConfig := `{"clone_user": [{"Password": "password"}]}`
	authServer := mysql.NewAuthServerStatic("", jsonConfig, 0)
	handler := &mockDonorHandler{t: t}

	listener, err := mysql.NewListener("tcp", "127.0.0.1:", authServer, handler, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)

	// Start accepting connections
	go listener.Accept()

	// Clean up when all tests complete
	t.Cleanup(func() {
		listener.Close()
		utils.EnsureNoLeaks(t)
	})

	// Get the assigned host/port
	donorHost := listener.Addr().(*net.TCPAddr).IP.String()
	donorPort := listener.Addr().(*net.TCPAddr).Port

	testCases := []struct {
		name             string
		cloneFromPrimary bool
		cloneFromTablet  string
		setup            func(*testing.T, *cloneFromDonorTestEnv)
		wantErr          bool
		wantErrContains  string
	}{
		{
			name:             "clone from primary, get shard fails",
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
		{
			name:            "GetTablet fails",
			cloneFromTablet: "cell1-100",
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// Delete the tablet that was created in env setup
				require.NoError(t, env.ts.DeleteTablet(env.ctx, env.donorAlias))
			},
			wantErr:         true,
			wantErrContains: "failed to get tablet",
		},
		{
			name:            "clone user not configured",
			cloneFromTablet: "cell1-100",
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// Clear clone user config
				dbconfigs.GlobalDBConfigs.CloneUser = dbconfigs.UserConfig{}
			},
			wantErr:         true,
			wantErrContains: "clone user not configured",
		},
		{
			name:            "recipient validation fails",
			cloneFromTablet: "cell1-100",
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// Configure mysqld to return an old MySQL version
				env.mysqld.FetchSuperQueryMap["SELECT @@version"] = sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("@@version", "varchar"),
					"8.0.16",
				)
			},
			wantErr:         true,
			wantErrContains: "recipient validation failed",
		},
		{
			name:            "get position after clone fails",
			cloneFromTablet: "cell1-100",
			setup: func(t *testing.T, env *cloneFromDonorTestEnv) {
				// Make PrimaryPosition return an error
				env.mysqld.PrimaryPositionError = assert.AnError
			},
			wantErr:         true,
			wantErrContains: "failed to get position after clone",
		},
		{
			name:            "success",
			cloneFromTablet: "cell1-100",
			wantErr:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := createCloneFromDonorTestEnv(t, donorHost, donorPort)

			// Save and restore global flags and config
			oldCloneFromPrimary := cloneFromPrimary
			oldCloneFromTablet := cloneFromTablet
			oldCloneUser := dbconfigs.GlobalDBConfigs.CloneUser
			oldMysqlCloneEnabled := mysqlCloneEnabled
			defer func() {
				cloneFromPrimary = oldCloneFromPrimary
				cloneFromTablet = oldCloneFromTablet
				dbconfigs.GlobalDBConfigs.CloneUser = oldCloneUser
				mysqlCloneEnabled = oldMysqlCloneEnabled
			}()

			// Set test flag values
			cloneFromPrimary = tc.cloneFromPrimary
			cloneFromTablet = tc.cloneFromTablet
			mysqlCloneEnabled = true

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
