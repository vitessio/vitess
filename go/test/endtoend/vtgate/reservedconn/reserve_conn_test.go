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

package reservedconn

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestServingChange(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// enable reserved connection.
	checkedExec(t, conn, "set enable_system_settings = true")
	checkedExec(t, conn, fmt.Sprintf("use %s@rdonly", keyspaceName))
	checkedExec(t, conn, "set sql_mode = ''")

	// this will create reserved connection on rdonly on -80 and 80- shards.
	checkedExec(t, conn, "select * from test")

	// changing rdonly tablet to spare (non serving).
	rdonlyTablet := clusterInstance.Keyspaces[0].Shards[0].Rdonly()
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", rdonlyTablet.Alias, "spare")
	require.NoError(t, err)

	// this should fail as there is no rdonly present
	_, err = exec(t, conn, "select * from test")
	require.Error(t, err)

	// changing replica tablet to rdonly to make rdonly available for serving.
	replicaTablet := clusterInstance.Keyspaces[0].Shards[0].Replica()
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "rdonly")
	require.NoError(t, err)

	// this should pass now as there is rdonly present
	_, err = exec(t, conn, "select * from test")
	require.NoError(t, err)

	// This test currently failed with error: vttablet: rpc error: code = FailedPrecondition desc = operation not allowed in state NOT_SERVING (errno 1105) (sqlstate HY000) during query: select * from test
}

func TestTabletChange(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// enable reserved connection.
	checkedExec(t, conn, "set enable_system_settings = true")
	checkedExec(t, conn, fmt.Sprintf("use %s@master", keyspaceName))
	checkedExec(t, conn, "set sql_mode = ''")

	// this will create reserved connection on master on -80 and 80- shards.
	checkedExec(t, conn, "select * from test")

	// Change Master
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, "-80"))
	require.NoError(t, err)

	// this should pass as there is new master tablet and is serving.
	_, err = exec(t, conn, "select * from test")
	require.NoError(t, err)

	// This test currently failed with error: vttablet: rpc error: code = FailedPrecondition desc = operation not allowed in state NOT_SERVING (errno 1105) (sqlstate HY000) during query: select * from test
}
