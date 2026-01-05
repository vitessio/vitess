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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttls"
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
		{
			name: "with escaping",
			executor: &CloneExecutor{
				DonorHost:     "host'one",
				DonorPort:     3310,
				DonorUser:     "user\\name",
				DonorPassword: "pass'word",
				UseSSL:        true,
			},
			expected: fmt.Sprintf("CLONE INSTANCE FROM %s@%s:%d IDENTIFIED BY %s REQUIRE SSL",
				sqltypes.EncodeStringSQL("user\\name"),
				sqltypes.EncodeStringSQL("host'one"),
				3310,
				sqltypes.EncodeStringSQL("pass'word")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.executor.buildCloneCommand()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDonorConnParamsSSLMode(t *testing.T) {
	tests := []struct {
		name     string
		useSSL   bool
		expected vttls.SslMode
	}{
		{
			name:     "ssl required",
			useSSL:   true,
			expected: vttls.Required,
		},
		{
			name:     "ssl disabled",
			useSSL:   false,
			expected: vttls.Disabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &CloneExecutor{
				DonorHost:     "127.0.0.1",
				DonorPort:     3306,
				DonorUser:     "vt_clone",
				DonorPassword: "secret",
				UseSSL:        tt.useSSL,
			}

			params := executor.donorConnParams()
			assert.Equal(t, tt.expected, params.SslMode)
		})
	}
}

func TestIsCloneConnError(t *testing.T) {
	serverGone := sqlerror.NewSQLError(sqlerror.CRServerGone, sqlerror.SSUnknownSQLState, "gone")
	serverLost := sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "lost")
	accessDenied := sqlerror.NewSQLError(sqlerror.ERAccessDeniedError, sqlerror.SSUnknownSQLState, "access denied")

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "server gone",
			err:      serverGone,
			expected: true,
		},
		{
			name:     "server lost",
			err:      serverLost,
			expected: true,
		},
		{
			name:     "wrapped server lost",
			err:      fmt.Errorf("wrapped: %w", serverLost),
			expected: true,
		},
		{
			name:     "access denied",
			err:      accessDenied,
			expected: false,
		},
		{
			name:     "non sql error",
			err:      assert.AnError,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isCloneConnError(tt.err))
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
		version      string
		pluginQuery  *sqltypes.Result
		expectError  bool
		errorContain string
	}{
		{
			name:    "valid MySQL 8.0.32 with clone plugin",
			version: "8.0.32",
			pluginQuery: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar"),
				"ACTIVE",
			),
			expectError: false,
		},
		{
			name:         "MySQL version too old",
			version:      "8.0.16",
			expectError:  true,
			errorContain: "requires version 8.0.17",
		},
		{
			name:         "clone plugin not installed",
			version:      "8.0.32",
			pluginQuery:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("PLUGIN_STATUS", "varchar")),
			expectError:  true,
			errorContain: "clone plugin is not installed",
		},
		{
			name:    "clone plugin not active",
			version: "8.0.32",
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

			fmd.Version = tt.version
			if tt.pluginQuery != nil {
				fmd.FetchSuperQueryMap = map[string]*sqltypes.Result{
					"SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'clone'": tt.pluginQuery,
				}
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
