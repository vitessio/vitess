/*
Copyright 2019 The Vitess Authors.

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

package reparent

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestReparentGracefulRangeBased(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	for _, tablet := range []cluster.Vttablet{*masterTablet, *replicaTablet} {
		// create database
		err := tablet.VttabletProcess.CreateDB(keyspaceName)
		require.Nil(t, err)
		// Init Tablet
		err = clusterInstance.VtctlclientProcess.InitTablet(&tablet, tablet.Cell, keyspaceName, hostname, shard1Name)
		require.Nil(t, err)
		// Start the tablet
		err = tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*masterTablet, *replicaTablet} {
		err := tablet.VttabletProcess.WaitForTabletTypes([]string{"SERVING", "NOT_SERVING"})
		require.Nil(t, err)
	}

	// Force the replica to reparent assuming that all the datasets are identical.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1Name), masterTablet.Alias)
	require.Nil(t, err)

	// Validate topology
	validateTopology(t, true)

	// create Tables
	runSQL(ctx, t, sqlSchema, masterTablet)

	checkMasterTablet(t, masterTablet)

	validateTopology(t, false)

	// Run this to make sure it succeeds.
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"ShardReplicationPositions", fmt.Sprintf("%s/%s", keyspaceName, shard1Name))
	require.Nil(t, err)
	strArray := strings.Split(output, "\n")
	if strArray[len(strArray)-1] == "" {
		strArray = strArray[:len(strArray)-1] // Truncate slice, remove empty line
	}
	assert.Equal(t, 2, len(strArray))         // one master, one slave
	assert.Contains(t, strArray[0], "master") // master first

	// Perform a graceful reparent operation
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"PlannedReparentShard",
		"-keyspace_shard", fmt.Sprintf("%s/%s", keyspaceName, shard1Name),
		"-new_master", replicaTablet.Alias)
	require.Nil(t, err)

	// Validate topology
	validateTopology(t, false)

	checkMasterTablet(t, replicaTablet)

	// insert data into the new master, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, 1, 1)
	runSQL(ctx, t, insertSQL, replicaTablet)
	err = checkInsertedValues(ctx, t, masterTablet, 1)
	require.Nil(t, err)
}
