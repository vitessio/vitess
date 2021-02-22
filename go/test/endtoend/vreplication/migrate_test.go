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

	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("Mount", "-type=vitess", "-topo_type=etcd2",
		fmt.Sprintf("-topo_server=localhost:%d", extVc.ClusterConfig.topoPort), "-topo_root=/vitess/global", "ext1"); err != nil {
		t.Fatalf("Mount command failed with %+v : %s\n", err, output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("Migrate", "-all", "-source=ext1.rating", "create", "product.extwf1"); err != nil {
		t.Fatalf("Migrate command failed with %+v : %s\n", err, output)
	}
	//TODO: add tests to validate
	//Complete/Cancel tests
}
