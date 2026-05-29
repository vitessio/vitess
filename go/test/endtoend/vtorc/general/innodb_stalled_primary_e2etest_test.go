//go:build e2etest

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

package general

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

// TestInnoDBStalledPrimary verifies the InnoDBStalledPrimary detection +
// recovery path end-to-end. When vttablet is built with the `e2etest` tag
// (EXTRA_BUILD_TAGS=e2etest make build), HasRecentInnoDBLongSemaphoreWait
// queries mysqlctl.InnoDBLogTable — a writable test table — instead of the
// real performance_schema.error_log view (which mysqld backs with an
// in-process log buffer and isn't writable from SQL).
//
// Does NOT cover: mysqld's own MY-012985 emission path on a real InnoDB
// latch stall — that's unfalsifiable in CI.
func TestInnoDBStalledPrimary(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	fakeLogTable := mysqlctl.InnoDBLogTable

	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, cluster.DefaultVtorcsByCell, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	primary, _, _ := waitForPrimaryAndPick(t, keyspace, shard0)
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	utils.WaitForSuccessfulRecoveryCount(t, vtorc, logic.ElectNewPrimaryRecoveryName, keyspace.Name, shard0.Name, 1)

	ersBefore := utils.GetSuccessfulRecoveryCount(t, vtorc, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name)

	// Create the override table and inject a MY-012985 warning row on the
	// primary only. SET sql_log_bin = 0 keeps both statements off the
	// binlog so the table and row don't replicate — once ERS reparents,
	// the new primary's analyser query hits ER_NO_SUCH_TABLE (soft-fail
	// to false), which prevents a second ERS firing on the same row.
	err := utils.RunSQLs(t, []string{
		"SET sql_log_bin = 0",
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (logged DATETIME(6), prio VARCHAR(16), subsystem VARCHAR(16), error_code VARCHAR(16))", fakeLogTable),
		fmt.Sprintf("INSERT INTO %s (logged, prio, subsystem, error_code) VALUES (NOW(6), 'Warning', 'InnoDB', 'MY-012985')", fakeLogTable),
	}, primary, "")
	require.NoError(t, err)

	utils.WaitForDetectedProblems(t, vtorc, string(inst.InnoDBStalledPrimary), primary.Alias, keyspace.Name, shard0.Name, 1)
	utils.WaitForSuccessfulRecoveryCount(t, vtorc, logic.RecoverDeadPrimaryRecoveryName, keyspace.Name, shard0.Name, ersBefore+1)
}
