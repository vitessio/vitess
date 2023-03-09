/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
)

var throttlerConfigTimeout = 60 * time.Second

// CheckCancelAllMigrations cancels all pending migrations. There is no validation for affected migrations.
func CheckCancelAllMigrationsViaVtctl(t *testing.T, vtctlclient *cluster.VtctlClientProcess, keyspace string) {
	cancelQuery := "alter vitess_migration cancel all"

	_, err := vtctlclient.ApplySchemaWithOutput(keyspace, cancelQuery, cluster.VtctlClientParams{SkipPreflight: true})
	assert.NoError(t, err)
}

// UpdateThrottlerTopoConfig runs vtctlclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func UpdateThrottlerTopoConfig(clusterInstance *cluster.LocalProcessCluster, enable bool, disable bool, threshold float64, metricsQuery string, viaVtctldClient bool) (result string, err error) {
	args := []string{}
	clientfunc := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput
	if !viaVtctldClient {
		args = append(args, "--")
		clientfunc = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput
	}
	args = append(args, "UpdateThrottlerConfig")
	if enable {
		args = append(args, "--enable")
	}
	if disable {
		args = append(args, "--disable")
	}
	if threshold > 0 {
		args = append(args, "--threshold", fmt.Sprintf("%f", threshold))
	}
	if metricsQuery != "" {
		args = append(args, "--custom-query", metricsQuery)
		args = append(args, "--check-as-check-self")
	} else {
		args = append(args, "--check-as-check-shard")
	}
	args = append(args, clusterInstance.Keyspaces[0].Name)

	ctx, cancel := context.WithTimeout(context.Background(), throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = clientfunc(args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v. Last seen value: %+v, error: %v", throttlerConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}
