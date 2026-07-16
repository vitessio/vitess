/*
Copyright 2022 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance *vitesst.Cluster
	tmClient        *tmc.Client
	primaryTablet   *vitesst.Tablet
	replicaTablet   *vitesst.Tablet
	keyspaceName    = "ks"
	shardName       = "0"
	cell            = "zone1"
	sqlSchema       = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB DEFAULT CHARSET=utf8;
	CREATE VIEW v1 AS SELECT id, value FROM t1;
`

	vSchema = `
	{
    "sharded": false,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
  }`
)

const (
	// tabletMySQLPort is the mysqld port every tablet starts on inside its
	// container.
	tabletMySQLPort = 3306

	// resurrectedMySQLPort is the port the primary's mysqld moves to when the
	// tablet comes back up. Every tablet has a container to itself, so the port
	// only has to be free inside that one container.
	resurrectedMySQLPort = 3307
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithCells(cell),
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc("--clusters-to-watch", keyspaceName),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primaryTablet = shard.Primary()
	replicaTablet = shard.Replicas()[0]
	tmClient = tmc.NewClient()
}

func tmcGetReplicationStatus(ctx context.Context, tablet *vitesst.Tablet) (*replicationdatapb.Status, error) {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}
	return tmClient.ReplicationStatus(ctx, vtablet)
}

// resurrectTablet is used to resurrect the given tablet on the given mysqld port
func resurrectTablet(ctx context.Context, t *testing.T, tab *vitesst.Tablet, mysqlPort int) {
	// initialize config again to regenerate the my.cnf file which has the port to use
	exitCode, output, err := tab.Exec(ctx, "mysqlctl",
		"--tablet-uid", strconv.Itoa(tab.UID),
		"--mysql-port", strconv.Itoa(mysqlPort),
		"init_config")
	require.NoError(t, err)
	require.Zero(t, exitCode, output)

	err = tab.StartMySQL(ctx)
	require.NoError(t, err)

	// Start the tablet
	err = tab.StartVttablet(ctx)
	require.NoError(t, err)
}

// stopTablet stops the tablet
func stopTablet(ctx context.Context, t *testing.T, tab *vitesst.Tablet) {
	err := tab.StopVttablet(ctx)
	require.NoError(t, err)
	err = tab.StopMySQL(ctx)
	require.NoError(t, err)
}

// primaryDownAnalyses are the VTOrc analyses that report a primary as gone, any
// of which makes VTOrc reparent the shard once recoveries are on again.
var primaryDownAnalyses = []string{
	string(inst.InvalidPrimary),
	string(inst.DeadPrimary),
	string(inst.DeadPrimaryWithoutReplicas),
	string(inst.DeadPrimaryAndReplicas),
	string(inst.DeadPrimaryAndSomeReplicas),
	string(inst.UnreachablePrimary),
	string(inst.UnreachablePrimaryWithLaggingReplicas),
	string(inst.UnreachablePrimaryWithBrokenReplicas),
	string(inst.PrimaryTabletDeleted),
	string(inst.ClusterHasNoPrimary),
}

// waitForVTOrcToSeePrimaryAlive blocks until VTOrc has reached the given primary
// again, so that recoveries turned back on cannot act on an analysis left over
// from the time the primary was down.
func waitForVTOrcToSeePrimaryAlive(ctx context.Context, vtorc *vitesst.VTOrc, tablet *vitesst.Tablet) error {
	alias := topoproto.TabletAliasString(&tabletpb.TabletAlias{Cell: tablet.Cell, Uid: uint32(tablet.UID)})

	_, _, err := vtorc.MakeAPICallRetry(ctx, "/api/detection-analysis", 60*time.Second, func(status int, body string) bool {
		if status != http.StatusOK {
			return false
		}

		var analyses []struct {
			AnalyzedInstanceAlias string
			Analysis              string
		}
		if err := json.Unmarshal([]byte(body), &analyses); err != nil {
			return false
		}
		for _, analysis := range analyses {
			if analysis.AnalyzedInstanceAlias == alias && slices.Contains(primaryDownAnalyses, analysis.Analysis) {
				return false
			}
		}
		return true
	})
	return err
}

func waitForSourcePort(ctx context.Context, tablet *vitesst.Tablet, expectedPort int32) error {
	timeout := time.Now().Add(15 * time.Second)
	for time.Now().Before(timeout) {
		// Check that initially replication is setup correctly on the replica tablet
		replicaStatus, err := tmcGetReplicationStatus(ctx, tablet)
		if err == nil && replicaStatus.SourcePort == expectedPort {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "time out before source port became %d for %s", expectedPort, tablet.Alias())
}

func getSidecarDBDDLQueryCount(ctx context.Context, tablet *vitesst.Tablet) (int64, error) {
	vars, err := tablet.GetVars(ctx)
	if err != nil {
		return 0, err
	}
	key := sidecardb.StatsKeyQueryCount
	val, ok := vars[key]
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s not found in debug/vars", key)
	}
	return int64(val.(float64)), nil
}

func TestReplicationRepairAfterPrimaryTabletChange(t *testing.T) {
	setup(t)

	ctx := t.Context()
	// Check that initially replication is setup correctly on the replica tablet
	err := waitForSourcePort(ctx, replicaTablet, tabletMySQLPort)
	require.NoError(t, err)

	sidecarDDLCount, err := getSidecarDBDDLQueryCount(ctx, primaryTablet)
	require.NoError(t, err)
	// sidecar db should create all _vt tables when vttablet started
	require.Greater(t, sidecarDDLCount, int64(0))

	// The primary is taken down for maintenance and comes back on another port,
	// so hold VTOrc off until it is up again. Left to run, VTOrc reads the
	// shutdown primary as dead and promotes the replica, and the shard the test
	// is about is gone.
	vtorc := clusterInstance.VTOrc()
	err = vtorc.DisableGlobalRecoveries(ctx)
	require.NoError(t, err)

	// Stop the primary tablet
	stopTablet(ctx, t, primaryTablet)

	// Start the primary tablet again, on a different MySQL port
	resurrectTablet(ctx, t, primaryTablet, resurrectedMySQLPort)

	err = waitForVTOrcToSeePrimaryAlive(ctx, vtorc, primaryTablet)
	require.NoError(t, err)

	err = vtorc.EnableGlobalRecoveries(ctx)
	require.NoError(t, err)

	// Let the replication be repaired against the primary's new port
	err = waitForSourcePort(ctx, replicaTablet, resurrectedMySQLPort)
	require.NoError(t, err)

	sidecarDDLCount, err = getSidecarDBDDLQueryCount(ctx, primaryTablet)
	require.NoError(t, err)
	// sidecardb should find the desired _vt schema and not apply any new creates or upgrades when the tablet comes up again
	require.Equal(t, sidecarDDLCount, int64(0))
}

func TestReparentJournalInfo(t *testing.T) {
	setup(t)

	for _, vttablet := range clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets() {
		tablet, err := vttablet.TabletProto(t.Context())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		length, err := tmClient.ReadReparentJournalInfo(ctx, tablet)
		cancel()
		require.NoError(t, err)
		require.EqualValues(t, 1, length)
	}
}
