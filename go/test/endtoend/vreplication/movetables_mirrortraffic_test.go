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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
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

	require.NoError(t, waitForWorkflowState(vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String()))

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
	require.NoError(t, waitForWorkflowState(vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String()))

	// Before mirroring: verify tables are in the deny list on target
	validateTableInDenyList(t, vc, defaultTargetKs+":-80", "customer", true)

	// Enable MirrorTraffic - this should set allow_reads=true on denied tables
	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)

	// Probe the target shard primary's queryservice directly to test
	// vttablet-level denied tables behavior. Going through vtgate (even
	// shard-targeted) would entangle the probes with its failover resilience:
	// the expect-error DMLs get buffered or retried for the full buffer
	// window / retry deadline before the denied-tables error surfaces.
	execOnTargetShardPrimary := func(query string) (*sqltypes.Result, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		tablet, err := vc.VtctldClient.GetTablet(targetTab1.Name)
		require.NoError(t, err)
		conn, err := tabletconn.GetDialer()(ctx, tablet, grpcclient.FailFast(false))
		require.NoError(t, err)
		defer conn.Close(ctx)
		session := executorcontext.NewSafeSession(&vtgatepb.Session{})
		return conn.Execute(ctx, session, &querypb.Target{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		}, query, nil, 0, 0, nil)
	}

	// Test 1: SELECT queries should succeed on denied tables when allow_reads=true
	qr, err := execOnTargetShardPrimary("SELECT count(*) FROM customer")
	require.NoError(t, err, "SELECT should succeed on denied table when allow_reads=true")
	require.NotEmpty(t, qr.Rows)

	// Test 2: INSERT should be blocked on denied tables
	_, err = execOnTargetShardPrimary("INSERT INTO customer(cid, name) VALUES (999, 'test')")
	require.ErrorContains(t, err, "disallowed due to rule", "INSERT should fail on denied table even with allow_reads=true")

	// Test 3: UPDATE should be blocked on denied tables
	_, err = execOnTargetShardPrimary("UPDATE customer SET name = 'test' WHERE cid = 1")
	require.ErrorContains(t, err, "disallowed due to rule", "UPDATE should fail on denied table even with allow_reads=true")

	// Test 4: DELETE should be blocked on denied tables
	_, err = execOnTargetShardPrimary("DELETE FROM customer WHERE cid = 999")
	require.ErrorContains(t, err, "disallowed due to rule", "DELETE should fail on denied table even with allow_reads=true")

	// Clean up: switch traffic and verify mirror rules are removed
	mt.SwitchReadsAndWrites()
	confirmNoMirrorRules(t)
}
