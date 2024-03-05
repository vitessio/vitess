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

package vreplication

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	//go:embed schema/fkext/source_schema.sql
	FKExtSourceSchema string
	//go:embed schema/fkext/source_vschema.json
	FKExtSourceVSchema string
	//go:embed schema/fkext/target1_vschema.json
	FKExtTarget1VSchema string
	//go:embed schema/fkext/target2_vschema.json
	FKExtTarget2VSchema string
	//go:embed schema/fkext/materialize_schema.sql
	FKExtMaterializeSchema string
)

type fkextConfigType struct {
	*ClusterConfig
	sourceKeyspaceName  string
	target1KeyspaceName string
	target2KeyspaceName string
	cell                string
}

var fkextConfig *fkextConfigType

func initFKExtConfig(t *testing.T) {
	fkextConfig = &fkextConfigType{
		ClusterConfig:       mainClusterConfig,
		sourceKeyspaceName:  "source",
		target1KeyspaceName: "target1",
		target2KeyspaceName: "target2",
		cell:                "zone1",
	}
}

/*
TestFKExt is an end-to-end test for validating the foreign key implementation with respect to, both vreplication
flows and vtgate processing of DMLs for tables with foreign key constraints. It currently:
* Sets up a source keyspace, to simulate the external database, with a parent and child table with a foreign key constraint.
* Creates a target keyspace with two shards, the Vitess keyspace, into which the source data is imported.
* Imports the data using MoveTables. This uses the atomic copy flow
to test that we can import data with foreign key constraints and that data is kept consistent even after the copy phase
since the tables continue to have the FK Constraints.
* Creates a new keyspace with two shards, the Vitess keyspace, into which the data is migrated using MoveTables.
* Materializes the parent and child tables into a different keyspace.
* Reshards the keyspace from 2 shards to 3 shards.
* Reshards the keyspace from 3 shards to 1 shard.

We drop constraints from the tables from some replicas to simulate a replica that is not doing cascades in
innodb, to confirm that vtgate's fkmanaged mode is working properly.
*/

func TestFKExt(t *testing.T) {
	setSidecarDBName("_vt")

	// Ensure that there are multiple copy phase cycles per table.
	extraVTTabletArgs = append(extraVTTabletArgs, "--vstream_packet_size=256", "--queryserver-config-schema-change-signal")
	extraVTGateArgs = append(extraVTGateArgs, "--schema_change_signal=true", "--planner-version", "Gen4")
	defer func() { extraVTTabletArgs = nil }()
	initFKExtConfig(t)

	cellName := fkextConfig.cell
	cells := []string{cellName}
	vc = NewVitessCluster(t, &clusterOptions{
		cells:         cells,
		clusterConfig: fkextConfig.ClusterConfig,
	})
	defaultCell := vc.Cells[vc.CellNames[0]]
	cell := vc.Cells[cellName]

	defer vc.TearDown()

	sourceKeyspace := fkextConfig.sourceKeyspaceName
	vc.AddKeyspace(t, []*Cell{cell}, sourceKeyspace, "0", FKExtSourceVSchema, FKExtSourceSchema, 0, 0, 100, nil)

	verifyClusterHealth(t, vc)

	lg = &SimpleLoadGenerator{}
	lg.Init(context.Background(), vc)
	lg.SetDBStrategy("vtgate", fkextConfig.sourceKeyspaceName)
	if lg.Load() != nil {
		t.Fatal("Load failed")
	}
	if lg.Start() != nil {
		t.Fatal("Start failed")
	}
	t.Run("Import from external db", func(t *testing.T) {
		// Import data into vitess from sourceKeyspace to target1Keyspace, both unsharded.
		importIntoVitess(t)
	})

	t.Run("MoveTables from unsharded to sharded keyspace", func(t *testing.T) {
		// Migrate data from target1Keyspace to the sharded target2Keyspace. Drops constraints from
		// replica to simulate a replica that is not doing cascades in innodb to test vtgate's fkmanaged mode.
		// The replica with dropped constraints is used as source for the next workflow called in materializeTables().
		moveKeyspace(t)
	})

	t.Run("Materialize parent and copy tables without constraints", func(t *testing.T) {
		// Materialize the tables from target2Keyspace to target1Keyspace. Stream only from replicas, one
		// shard with constraints dropped.
		materializeTables(t)
	})
	lg.SetDBStrategy("vtgate", fkextConfig.target2KeyspaceName)
	if lg.Start() != nil {
		t.Fatal("Start failed")
	}
	threeShards := "-40,40-c0,c0-"
	keyspaceName := fkextConfig.target2KeyspaceName
	ks := vc.Cells[fkextConfig.cell].Keyspaces[keyspaceName]
	numReplicas := 1

	t.Run("Reshard keyspace from 2 shards to 3 shards", func(t *testing.T) {
		tabletID := 500
		require.NoError(t, vc.AddShards(t, []*Cell{defaultCell}, ks, threeShards, numReplicas, 0, tabletID, nil))
		tablets := make(map[string]*cluster.VttabletProcess)
		for i, shard := range strings.Split(threeShards, ",") {
			tablets[shard] = vc.Cells[cellName].Keyspaces[keyspaceName].Shards[shard].Tablets[fmt.Sprintf("%s-%d", cellName, tabletID+i*100)].Vttablet
		}
		sqls := strings.Split(FKExtSourceSchema, "\n")
		for _, sql := range sqls {
			output, err := vc.VtctlClient.ExecuteCommandWithOutput("ApplySchema", "--",
				"--ddl_strategy=direct", "--sql", sql, keyspaceName)
			require.NoErrorf(t, err, output)
		}
		doReshard(t, fkextConfig.target2KeyspaceName, "reshard2to3", "-80,80-", threeShards, tablets)
	})
	t.Run("Reshard keyspace from 3 shards to 1 shard", func(t *testing.T) {
		tabletID := 800
		shard := "0"
		require.NoError(t, vc.AddShards(t, []*Cell{defaultCell}, ks, shard, numReplicas, 0, tabletID, nil))
		tablets := make(map[string]*cluster.VttabletProcess)
		tablets[shard] = vc.Cells[cellName].Keyspaces[keyspaceName].Shards[shard].Tablets[fmt.Sprintf("%s-%d", cellName, tabletID)].Vttablet
		sqls := strings.Split(FKExtSourceSchema, "\n")
		for _, sql := range sqls {
			output, err := vc.VtctlClient.ExecuteCommandWithOutput("ApplySchema", "--",
				"--ddl_strategy=direct", "--sql", sql, keyspaceName)
			require.NoErrorf(t, err, output)
		}
		doReshard(t, fkextConfig.target2KeyspaceName, "reshard3to1", threeShards, "0", tablets)
	})
	lg.Stop()
	waitForLowLag(t, fkextConfig.target1KeyspaceName, "mat")
	t.Run("Validate materialize counts at end of test", func(t *testing.T) {
		validateMaterializeRowCounts(t)
	})

}

// checkRowCounts checks that the parent and child tables in the source and target shards have the same number of rows.
func checkRowCounts(t *testing.T, keyspace string, sourceShards, targetShards []string) bool {
	sourceTabs := make(map[string]*cluster.VttabletProcess)
	targetTabs := make(map[string]*cluster.VttabletProcess)
	for _, shard := range sourceShards {
		sourceTabs[shard] = vc.getPrimaryTablet(t, keyspace, shard)
	}
	for _, shard := range targetShards {
		targetTabs[shard] = vc.getPrimaryTablet(t, keyspace, shard)
	}

	getCount := func(tab *cluster.VttabletProcess, table string) (int64, error) {
		qr, err := tab.QueryTablet(fmt.Sprintf("select count(*) from %s", table), keyspace, true)
		if err != nil {
			return 0, err
		}
		return qr.Rows[0][0].ToInt64()
	}

	var sourceParentCount, sourceChildCount int64
	var targetParentCount, targetChildCount int64
	for _, tab := range sourceTabs {
		count, _ := getCount(tab, "parent")
		sourceParentCount += count
		count, _ = getCount(tab, "child")
		sourceChildCount += count
	}
	for _, tab := range targetTabs {
		count, _ := getCount(tab, "parent")
		targetParentCount += count
		count, _ = getCount(tab, "child")
		targetChildCount += count
	}
	log.Infof("Source parent count: %d, child count: %d, target parent count: %d, child count: %d.",
		sourceParentCount, sourceChildCount, targetParentCount, targetChildCount)
	if sourceParentCount != targetParentCount || sourceChildCount != targetChildCount {
		log.Infof("Row counts do not match for keyspace %s, source shards: %v, target shards: %v", keyspace, sourceShards, targetShards)
		return false
	}
	return true
}

// compareRowCounts compares the row counts for the parent and child tables in the source and target shards. In addition to vdiffs,
// it is another check to ensure that both tables have the same number of rows in the source and target shards after load generation
// has stopped.
func compareRowCounts(t *testing.T, keyspace string, sourceShards, targetShards []string) error {
	log.Infof("Comparing row counts for keyspace %s, source shards: %v, target shards: %v", keyspace, sourceShards, targetShards)
	lg.Stop()
	defer lg.Start()
	if err := waitForCondition("load generator to stop", func() bool { return lg.State() == LoadGeneratorStateStopped }, 10*time.Second); err != nil {
		return err
	}
	if err := waitForCondition("matching row counts", func() bool { return checkRowCounts(t, keyspace, sourceShards, targetShards) }, 30*time.Second); err != nil {
		return err
	}

	return nil
}

func doReshard(t *testing.T, keyspace, workflowName, sourceShards, targetShards string, targetTabs map[string]*cluster.VttabletProcess) {
	rs := newReshard(vc, &reshardWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: keyspace,
		},
		sourceShards:   sourceShards,
		targetShards:   targetShards,
		skipSchemaCopy: true,
	}, workflowFlavorVtctl)
	rs.Create()
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, targetTab := range targetTabs {
		catchup(t, targetTab, workflowName, "Reshard")
	}
	vdiff(t, keyspace, workflowName, fkextConfig.cell, false, true, nil)
	rs.SwitchReadsAndWrites()
	//if lg.WaitForAdditionalRows(100) != nil {
	//	t.Fatal("WaitForAdditionalRows failed")
	//}
	waitForLowLag(t, keyspace, workflowName+"_reverse")
	if compareRowCounts(t, keyspace, strings.Split(sourceShards, ","), strings.Split(targetShards, ",")) != nil {
		t.Fatal("Row counts do not match")
	}
	vdiff(t, keyspace, workflowName+"_reverse", fkextConfig.cell, true, false, nil)

	rs.ReverseReadsAndWrites()
	//if lg.WaitForAdditionalRows(100) != nil {
	//	t.Fatal("WaitForAdditionalRows failed")
	//}
	waitForLowLag(t, keyspace, workflowName)
	if compareRowCounts(t, keyspace, strings.Split(targetShards, ","), strings.Split(sourceShards, ",")) != nil {
		t.Fatal("Row counts do not match")
	}
	vdiff(t, keyspace, workflowName, fkextConfig.cell, false, true, nil)
	lg.Stop()

	rs.SwitchReadsAndWrites()
	rs.Complete()
}

func areRowCountsEqual(t *testing.T) bool {
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	parentRowCount := getRowCount(t, vtgateConn, "target2.parent")
	childRowCount := getRowCount(t, vtgateConn, "target2.child")
	parentCopyRowCount := getRowCount(t, vtgateConn, "target1.parent_copy")
	childCopyRowCount := getRowCount(t, vtgateConn, "target1.child_copy")
	log.Infof("Post-materialize row counts are parent: %d, child: %d, parent_copy: %d, child_copy: %d",
		parentRowCount, childRowCount, parentCopyRowCount, childCopyRowCount)
	if parentRowCount != parentCopyRowCount || childRowCount != childCopyRowCount {
		return false
	}
	return true
}

// validateMaterializeRowCounts expects the Load generator to be stopped before calling it.
func validateMaterializeRowCounts(t *testing.T) {
	if lg.State() != LoadGeneratorStateStopped {
		t.Fatal("Load generator was unexpectedly still running when validateMaterializeRowCounts was called -- this will produce unreliable results.")
	}
	areRowCountsEqual2 := func() bool {
		return areRowCountsEqual(t)
	}
	require.NoError(t, waitForCondition("row counts to be equal", areRowCountsEqual2, defaultTimeout))
}

const fkExtMaterializeSpec = `
{"workflow": "%s", "source_keyspace": "%s", "target_keyspace": "%s", 
"table_settings": [ {"target_table": "parent_copy", "source_expression": "select * from parent"  },{"target_table": "child_copy", "source_expression": "select * from child"  }],
"tablet_types": "replica"}`

func materializeTables(t *testing.T) {
	wfName := "mat"
	err := vc.VtctlClient.ExecuteCommand("ApplySchema", "--", "--ddl_strategy=direct",
		"--sql", FKExtMaterializeSchema, fkextConfig.target1KeyspaceName)
	require.NoError(t, err, fmt.Sprintf("ApplySchema Error: %s", err))
	materializeSpec := fmt.Sprintf(fkExtMaterializeSpec, "mat", fkextConfig.target2KeyspaceName, fkextConfig.target1KeyspaceName)
	err = vc.VtctlClient.ExecuteCommand("Materialize", materializeSpec)
	require.NoError(t, err, "Materialize")
	tab := vc.getPrimaryTablet(t, fkextConfig.target1KeyspaceName, "0")
	catchup(t, tab, wfName, "Materialize")
	validateMaterializeRowCounts(t)
}

func moveKeyspace(t *testing.T) {
	targetTabs := newKeyspace(t, fkextConfig.target2KeyspaceName, "-80,80-", FKExtTarget2VSchema, FKExtSourceSchema, 300, 1)
	shard := "-80"
	tabletId := fmt.Sprintf("%s-%d", fkextConfig.cell, 301)
	replicaTab := vc.Cells[fkextConfig.cell].Keyspaces[fkextConfig.target2KeyspaceName].Shards[shard].Tablets[tabletId].Vttablet
	dropReplicaConstraints(t, fkextConfig.target2KeyspaceName, replicaTab)
	doMoveTables(t, fkextConfig.target1KeyspaceName, fkextConfig.target2KeyspaceName, "move", "replica", targetTabs, false)
}

func newKeyspace(t *testing.T, keyspaceName, shards, vschema, schema string, tabletId, numReplicas int) map[string]*cluster.VttabletProcess {
	tablets := make(map[string]*cluster.VttabletProcess)
	cell := vc.Cells[fkextConfig.cell]
	vtgate := cell.Vtgates[0]
	vc.AddKeyspace(t, []*Cell{cell}, keyspaceName, shards, vschema, schema, numReplicas, 0, tabletId, nil)
	err := vc.VtctldClient.ExecuteCommand("RebuildVSchemaGraph")
	require.NoError(t, err)
	require.NoError(t, waitForColumn(t, vtgate, keyspaceName, "parent", "id"))
	require.NoError(t, waitForColumn(t, vtgate, keyspaceName, "child", "parent_id"))
	return tablets
}

func doMoveTables(t *testing.T, sourceKeyspace, targetKeyspace, workflowName, tabletTypes string, targetTabs map[string]*cluster.VttabletProcess, atomicCopy bool) {
	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
			tabletTypes:    tabletTypes,
		},
		sourceKeyspace: sourceKeyspace,
		atomicCopy:     atomicCopy,
	}, workflowFlavorRandom)
	mt.Create()

	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())

	for _, targetTab := range targetTabs {
		catchup(t, targetTab, workflowName, "MoveTables")
	}
	vdiff(t, targetKeyspace, workflowName, fkextConfig.cell, false, true, nil)
	lg.Stop()
	lg.SetDBStrategy("vtgate", targetKeyspace)
	if lg.Start() != nil {
		t.Fatal("Start failed")
	}

	mt.SwitchReadsAndWrites()

	if lg.WaitForAdditionalRows(100) != nil {
		t.Fatal("WaitForAdditionalRows failed")
	}

	waitForLowLag(t, sourceKeyspace, workflowName+"_reverse")
	vdiff(t, sourceKeyspace, workflowName+"_reverse", fkextConfig.cell, false, true, nil)
	if lg.WaitForAdditionalRows(100) != nil {
		t.Fatal("WaitForAdditionalRows failed")
	}

	mt.ReverseReadsAndWrites()
	if lg.WaitForAdditionalRows(100) != nil {
		t.Fatal("WaitForAdditionalRows failed")
	}
	waitForLowLag(t, targetKeyspace, workflowName)
	time.Sleep(5 * time.Second)
	vdiff(t, targetKeyspace, workflowName, fkextConfig.cell, false, true, nil)
	lg.Stop()
	mt.SwitchReadsAndWrites()
	mt.Complete()
	if err := vc.VtctldClient.ExecuteCommand("ApplyRoutingRules", "--rules={}"); err != nil {
		t.Fatal(err)
	}
}

func importIntoVitess(t *testing.T) {
	targetTabs := newKeyspace(t, fkextConfig.target1KeyspaceName, "0", FKExtTarget1VSchema, FKExtSourceSchema, 200, 1)
	doMoveTables(t, fkextConfig.sourceKeyspaceName, fkextConfig.target1KeyspaceName, "import", "primary", targetTabs, true)
}

const getConstraintsQuery = `
SELECT CONSTRAINT_NAME, TABLE_NAME 
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
WHERE TABLE_SCHEMA = '%s' AND REFERENCED_TABLE_NAME IS NOT NULL;
`

// dropReplicaConstraints drops all foreign key constraints on replica tables for a given keyspace/shard.
// We do this so that we can replay binlogs from a replica which is not doing cascades but just replaying
// the binlogs created by the primary. This will confirm that vtgate is doing the cascades correctly.
func dropReplicaConstraints(t *testing.T, keyspaceName string, tablet *cluster.VttabletProcess) {
	var dropConstraints []string
	require.Equal(t, "replica", strings.ToLower(tablet.TabletType))
	dbName := "vt_" + keyspaceName
	qr, err := tablet.QueryTablet(fmt.Sprintf(getConstraintsQuery, dbName), keyspaceName, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range qr.Rows {
		constraintName := row[0].ToString()
		tableName := row[1].ToString()
		dropConstraints = append(dropConstraints, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP FOREIGN KEY `%s`",
			dbName, tableName, constraintName))
	}
	prefixQueries := []string{
		"set sql_log_bin=0",
		"SET @@global.super_read_only=0",
	}
	suffixQueries := []string{
		"SET @@global.super_read_only=1",
		"set sql_log_bin=1",
	}
	queries := append(prefixQueries, dropConstraints...)
	queries = append(queries, suffixQueries...)
	require.NoError(t, tablet.QueryTabletMultiple(queries, keyspaceName, true))
}
