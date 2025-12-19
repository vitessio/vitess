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

	"vitess.io/vitess/go/sqltypes"
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
