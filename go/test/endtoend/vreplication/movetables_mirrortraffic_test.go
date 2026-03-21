/*
Copyright 2024 The Vitess Authors.

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

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func testMoveTablesMirrorTraffic(t *testing.T, flavor workflowFlavor) {
	setSidecarDBName("_vt")
	ogReplicas := defaultReplicas
	ogRdOnly := defaultRdonly
	defer func() {
		defaultReplicas = ogReplicas
		defaultRdonly = ogRdOnly
	}()
	defaultRdonly = 0
	defaultReplicas = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	workflowName := "wf1"
	tables := []string{"customer", "loadtest", "customer2"}

	_ = setupMinimalTargetKeyspace(t)

	mtwf := &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: defaultTargetKs,
		},
		sourceKeyspace: defaultSourceKs,
		tables:         "customer,loadtest,customer2",
		mirrorFlags:    []string{"--percent", "25"},
	}
	mt := newMoveTables(vc, mtwf, flavor)

	// Mirror rules do not exist by default.
	mt.Create()
	confirmNoMirrorRules(t)

	waitForWorkflowState(t, vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	// Mirror rules can be created after a MoveTables workflow is created.
	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)
	expectMirrorRules(t, defaultSourceKs, defaultTargetKs, tables, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}, 25)

	// Mirror rules can be adjusted after mirror rules are in place.
	mtwf.mirrorFlags[1] = "50"
	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)
	expectMirrorRules(t, defaultSourceKs, defaultTargetKs, tables, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}, 50)

	// Mirror rules can be adjusted multiple times after mirror rules are in
	// place.
	mtwf.mirrorFlags[1] = "75"
	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)
	expectMirrorRules(t, defaultSourceKs, defaultTargetKs, tables, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}, 75)

	lg := newLoadGenerator(t, vc)
	go func() {
		lg.start()
	}()
	lg.waitForCount(1000)

	mt.SwitchReads()
	confirmMirrorRulesExist(t)

	// Mirror rules can be adjusted for writes after reads have been switched.
	mtwf.mirrorFlags[1] = "100"
	mtwf.mirrorFlags = append(mtwf.mirrorFlags, "--tablet-types", "primary")
	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)
	expectMirrorRules(t, defaultSourceKs, defaultTargetKs, tables, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
	}, 100)

	// Mirror rules are removed after writes are switched.
	mt.SwitchWrites()
	confirmNoMirrorRules(t)
}

func TestMoveTablesMirrorTraffic(t *testing.T) {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	t.Run(workflowFlavorNames[workflowFlavorVtctld], func(t *testing.T) {
		testMoveTablesMirrorTraffic(t, workflowFlavorVtctld)
	})
}

// TestMoveTablesMirrorTraffic_AllowReads verifies that when MirrorTraffic is enabled,
// read-only queries (SELECT) can execute against denied tables on the target, while
// write queries (INSERT, UPDATE, DELETE) are still blocked.
func TestMoveTablesMirrorTraffic_AllowReads(t *testing.T) {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	setSidecarDBName("_vt")
	ogReplicas := defaultReplicas
	ogRdOnly := defaultRdonly
	defer func() {
		defaultReplicas = ogReplicas
		defaultRdonly = ogRdOnly
	}()
	defaultRdonly = 0
	defaultReplicas = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	workflowName := "wf1"

	_ = setupMinimalTargetKeyspace(t)

	mtwf := &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: defaultTargetKs,
		},
		sourceKeyspace: defaultSourceKs,
		tables:         "customer,loadtest",
		mirrorFlags:    []string{"--percent", "100"},
	}
	mt := newMoveTables(vc, mtwf, workflowFlavorVtctld)

	mt.Create()
	waitForWorkflowState(t, vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	// Before mirroring: verify tables are in the deny list on target
	validateTableInDenyList(t, vc, fmt.Sprintf("%s:-80", defaultTargetKs), "customer", true)

	// Enable MirrorTraffic - this should set allow_reads=true on denied tables
	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)

	// Use shard-targeted queries to bypass vtgate routing rules and test
	// vttablet-level denied tables behavior directly.
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	_, err := vtgateConn.ExecuteFetch(fmt.Sprintf("use `%s:-80`", defaultTargetKs), 0, false)
	require.NoError(t, err)

	// Test 1: SELECT queries should succeed on denied tables when allow_reads=true
	qr, err := vtgateConn.ExecuteFetch("SELECT count(*) FROM customer", 1, false)
	require.NoError(t, err, "SELECT should succeed on denied table when allow_reads=true")
	require.NotZero(t, len(qr.Rows))

	// Test 2: INSERT should be blocked on denied tables
	_, err = vtgateConn.ExecuteFetch("INSERT INTO customer(cid, name) VALUES (999, 'test')", 1, false)
	require.Error(t, err, "INSERT should fail on denied table even with allow_reads=true")

	// Test 3: UPDATE should be blocked on denied tables
	_, err = vtgateConn.ExecuteFetch("UPDATE customer SET name = 'test' WHERE cid = 1", 1, false)
	require.Error(t, err, "UPDATE should fail on denied table even with allow_reads=true")

	// Test 4: DELETE should be blocked on denied tables
	_, err = vtgateConn.ExecuteFetch("DELETE FROM customer WHERE cid = 999", 1, false)
	require.Error(t, err, "DELETE should fail on denied table even with allow_reads=true")

	// Clean up: switch traffic and verify mirror rules are removed
	mt.SwitchReadsAndWrites()
	confirmNoMirrorRules(t)
}
