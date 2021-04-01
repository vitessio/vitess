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

package vreplication

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func insertInitialDataIntoExternalCluster(t *testing.T, conn *mysql.Conn) {
	t.Run("insertInitialData", func(t *testing.T) {
		fmt.Printf("Inserting initial data\n")
		execVtgateQuery(t, conn, "rating:0", "insert into review(rid, pid, review) values(1, 1, 'review1');")
		execVtgateQuery(t, conn, "rating:0", "insert into review(rid, pid, review) values(2, 1, 'review2');")
		execVtgateQuery(t, conn, "rating:0", "insert into review(rid, pid, review) values(3, 2, 'review3');")
		execVtgateQuery(t, conn, "rating:0", "insert into rating(gid, pid, rating) values(1, 1, 4);")
		execVtgateQuery(t, conn, "rating:0", "insert into rating(gid, pid, rating) values(2, 2, 5);")
	})
}

// TestMigrate runs an e2e test for importing from an external cluster using the Mount and Migrate commands.
// We have an anti-pattern in Vitess: vt executables look for an environment variable VTDATAROOT for certain cluster parameters
// like the log directory when they are created. Until this test we just needed a single cluster for e2e tests.
// However now we need to create an external Vitess cluster. For this we need a different VTDATAROOT and
// hence the VTDATAROOT env variable gets overwritten.
// Each time we need to create vt processes in the "other" cluster we need to set the appropriate VTDATAROOT
func TestMigrate(t *testing.T) {
	defaultCellName := "zone1"
	cells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestMigrate", cells, mainClusterConfig)

	require.NotNil(t, vc)
	defaultReplicas = 0
	defaultRdonly = 0
	defer vc.TearDown()

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)

	// create external cluster
	extCell := "extcell1"
	extCells := []string{extCell}
	extVc := NewVitessCluster(t, "TestMigrateExternal", extCells, externalClusterConfig)
	require.NotNil(t, extVc)
	defer extVc.TearDown()

	extCell2 := extVc.Cells[extCell]
	extVc.AddKeyspace(t, []*Cell{extCell2}, "rating", "0", initialExternalVSchema, initialExternalSchema, 0, 0, 1000)
	extVtgate := extCell2.Vtgates[0]
	require.NotNil(t, extVtgate)

	extVtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "rating", "0"), 1)
	verifyClusterHealth(t, extVc)
	extVtgateConn := getConnection(t, extVc.ClusterConfig.hostname, extVc.ClusterConfig.vtgateMySQLPort)
	insertInitialDataIntoExternalCluster(t, extVtgateConn)

	var err error
	var output, expected string
	ksWorkflow := "product.e1"

	t.Run("mount external cluster", func(t *testing.T) {
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-topo_type=etcd2",
			fmt.Sprintf("-topo_server=localhost:%d", extVc.ClusterConfig.topoPort), "-topo_root=/vitess/global", "ext1"); err != nil {
			t.Fatalf("Mount command failed with %+v : %s\n", err, output)
		}
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-list"); err != nil {
			t.Fatalf("Mount command failed with %+v : %s\n", err, output)
		}
		expected = "ext1\n"
		require.Equal(t, expected, output)
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-show", "ext1"); err != nil {
			t.Fatalf("Mount command failed with %+v : %s\n", err, output)
		}
		expected = `{"ClusterName":"ext1","topo_config":{"topo_type":"etcd2","server":"localhost:12379","root":"/vitess/global"}}` + "\n"
		require.Equal(t, expected, output)
	})

	t.Run("migrate from external cluster", func(t *testing.T) {
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Migrate", "-all", "-cells=extcell1",
			"-source=ext1.rating", "create", ksWorkflow); err != nil {
			t.Fatalf("Migrate command failed with %+v : %s\n", err, output)
		}
		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 1)
		validateCount(t, vtgateConn, "product:0", "rating", 2)
		validateCount(t, vtgateConn, "product:0", "review", 3)
		execVtgateQuery(t, extVtgateConn, "rating", "insert into review(rid, pid, review) values(4, 1, 'review4');")
		execVtgateQuery(t, extVtgateConn, "rating", "insert into rating(gid, pid, rating) values(3, 1, 3);")
		time.Sleep(1 * time.Second) // wait for stream to find row
		validateCount(t, vtgateConn, "product:0", "rating", 3)
		validateCount(t, vtgateConn, "product:0", "review", 4)
		vdiff(t, ksWorkflow, "extcell1")

		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Migrate", "complete", ksWorkflow); err != nil {
			t.Fatalf("Migrate command failed with %+v : %s\n", err, output)
		}

		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 0)
	})
	t.Run("cancel migrate workflow", func(t *testing.T) {
		execVtgateQuery(t, vtgateConn, "product", "drop table review,rating")

		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Migrate", "-all", "-auto_start=false", "-cells=extcell1",
			"-source=ext1.rating", "create", ksWorkflow); err != nil {
			t.Fatalf("Migrate command failed with %+v : %s\n", err, output)
		}
		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 1)
		validateCount(t, vtgateConn, "product:0", "rating", 0)
		validateCount(t, vtgateConn, "product:0", "review", 0)
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Migrate", "cancel", ksWorkflow); err != nil {
			t.Fatalf("Migrate command failed with %+v : %s\n", err, output)
		}
		expectNumberOfStreams(t, vtgateConn, "migrate", "e1", "product:0", 0)
		var found bool
		found, err = checkIfTableExists(t, vc, "zone1-100", "review")
		require.NoError(t, err)
		require.False(t, found)
		found, err = checkIfTableExists(t, vc, "zone1-100", "rating")
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("unmount external cluster", func(t *testing.T) {
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-unmount", "ext1"); err != nil {
			t.Fatalf("Mount command failed with %+v : %s\n", err, output)
		}

		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-list"); err != nil {
			t.Fatalf("Mount command failed with %+v : %s\n", err, output)
		}
		expected = "\n"
		require.Equal(t, expected, output)

		output, err = vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-show", "ext1")
		require.Errorf(t, err, "there is no vitess cluster named ext1")
	})
}
