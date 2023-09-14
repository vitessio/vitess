/*
Copyright 2023 The Vitess Authors.

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

package foreignkey

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// getTestName prepends whether the test is for a sharded keyspace or not to the test name.
func getTestName(testName string, testSharded bool) string {
	if testSharded {
		return "Sharded - " + testName
	}
	return "Unsharded - " + testName
}

// waitForSchemaTrackingForFkTables waits for schema tracking to have run and seen the tables used
// for foreign key tests.
func waitForSchemaTrackingForFkTables(t *testing.T) {
	err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, shardedKs, "fk_t11", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t1", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t18", "col")
	require.NoError(t, err)
	err = utils.WaitForColumn(t, clusterInstance.VtgateProcess, unshardedKs, "fk_t11", "col")
	require.NoError(t, err)
}
