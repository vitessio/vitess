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

package tabletmanager

import (
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	// Register the grpc queryservice dialer so StreamHealth and Execute work.
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

var (
	topoFlavor = flag.String("topo-flavor", "etcd2", "choose a topo server from etcd2, zk2 or consul")

	clusterInstance                  *vitesst.Cluster
	tmClient                         *tmc.Client
	primaryTabletParams              mysql.ConnParams
	replicaTabletParams              mysql.ConnParams
	primaryTablet                    *vitesst.Tablet
	replicaTablet                    *vitesst.Tablet
	rdonlyTablet                     *vitesst.Tablet
	permissionsMu                    sync.Mutex
	hostname                         = "localhost"
	keyspaceName                     = "ks"
	shardName                        = "0"
	keyspaceShard                    = "ks/" + shardName
	dbName                           = "vt_" + keyspaceName
	username                         = "vt_dba"
	cell                             = "zone1"
	tabletHealthcheckRefreshInterval = 5 * time.Second
	tabletUnhealthyThreshold         = tabletHealthcheckRefreshInterval * 2
	vttabletStateTimeout             = 60 * time.Second
	vttabletExtraArgs                = []string{
		"--lock-tables-timeout", "5s",
		"--heartbeat-enable",
		"--health-check-interval", tabletHealthcheckRefreshInterval.String(),
		"--unhealthy-threshold", tabletUnhealthyThreshold.String(),
		"--twopc-abandon-age", "200",
	}
	sqlSchema = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB DEFAULT CHARSET=utf8;
	CREATE VIEW v1 AS SELECT id, value FROM t1;
`

	vSchema = `
	{
    "sharded": true,
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
	// hookScriptPath is the repository's sample hook, and hookContainerPath is
	// where vttablet looks for hooks inside the tablet containers.
	hookScriptPath    = "../../../../vthook/test.sh"
	hookContainerPath = "/vt/vthook/test.sh"

	// tabletMySQLPort is the mysqld port inside every tablet container.
	tabletMySQLPort = "3306"

	// hostAccessGrants lets a vttablet reach another tablet's mysqld over the
	// network with the users it opens its connection pools with.
	hostAccessGrants = `CREATE USER 'vt_app'@'%';
GRANT ALL ON *.* TO 'vt_app'@'%';
CREATE USER 'vt_appdebug'@'%';
GRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'%';
CREATE USER 'vt_allprivs'@'%';
GRANT ALL ON *.* TO 'vt_allprivs'@'%';
CREATE USER 'vt_filtered'@'%';
GRANT ALL ON *.* TO 'vt_filtered'@'%';`
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithCells(cell),
		vitesst.WithTopo(*topoFlavor),
		vitesst.WithVTOrc("--clusters-to-watch", keyspaceName),
		// List of users authorized to execute vschema ddl operations
		vitesst.WithVTGateArgs(
			"--vschema-ddl-authorized-users=%",
			"--enable-views",
			"--discovery-low-replication-lag", tabletUnhealthyThreshold.String(),
		),
		// Set extra tablet args for lock timeout
		vitesst.WithVTTabletArgs(vttabletExtraArgs...),
		vitesst.WithTabletFiles(vitesst.ContainerFile{
			HostPath:      hookScriptPath,
			ContainerPath: hookContainerPath,
			Mode:          0o755,
		}),
		vitesst.WithVTCtldFiles(
			vitesst.ContainerFile{
				Content:       []byte(emptyCustomRules),
				ContainerPath: topoCustomRuleEmptyFile,
			},
			vitesst.ContainerFile{
				Content:       []byte(denySelectCustomRules),
				ContainerPath: topoCustomRuleDenyFile,
			},
		),
		vitesst.WithInitDBSQLExtra(hostAccessGrants),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithRDOnly(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster

	// Collect the tablets of the shard
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primaryTablet = shard.Primary()
	replicaTablet = shard.Replicas()[0]
	rdonlyTablet = shard.RDOnly()[0]

	// Set mysql tablet params
	primaryTabletParams, err = primaryTablet.DBAConnParams(ctx, dbName)
	require.NoError(t, err)
	replicaTabletParams, err = replicaTablet.DBAConnParams(ctx, dbName)
	require.NoError(t, err)

	// create tablet manager client
	tmClient = tmc.NewClient()
}

func tmcLockTables(ctx context.Context, tablet *vitesst.Tablet) error {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return err
	}
	return tmClient.LockTables(ctx, vtablet)
}

func tmcUnlockTables(ctx context.Context, tablet *vitesst.Tablet) error {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return err
	}
	return tmClient.UnlockTables(ctx, vtablet)
}

func tmcStopReplication(ctx context.Context, tablet *vitesst.Tablet) error {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return err
	}
	return tmClient.StopReplication(ctx, vtablet)
}

func tmcStartReplication(ctx context.Context, tablet *vitesst.Tablet) error {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return err
	}
	return tmClient.StartReplication(ctx, vtablet, false)
}

func tmcResetReplicationParameters(ctx context.Context, tablet *vitesst.Tablet) error {
	vttablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return err
	}
	return tmClient.ResetReplicationParameters(ctx, vttablet)
}

func tmcPrimaryPosition(ctx context.Context, tablet *vitesst.Tablet) (string, error) {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return "", err
	}
	return tmClient.PrimaryPosition(ctx, vtablet)
}

func tmcGetGlobalStatusVars(ctx context.Context, tablet *vitesst.Tablet, variables []string) (map[string]string, error) {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}
	return tmClient.GetGlobalStatusVars(ctx, vtablet, variables)
}

func tmcStartReplicationUntilAfter(ctx context.Context, tablet *vitesst.Tablet, positon string, waittime time.Duration) error {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return err
	}
	return tmClient.StartReplicationUntilAfter(ctx, vtablet, positon, waittime)
}

func tmcFullStatus(ctx context.Context, tablet *vitesst.Tablet) (*replicationdatapb.FullStatus, error) {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}
	return tmClient.FullStatus(ctx, vtablet)
}

func tmcStopReplicationAndGetStatus(ctx context.Context, tablet *vitesst.Tablet, mode replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
	vtablet, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}
	return tmClient.StopReplicationAndGetStatus(ctx, vtablet, mode)
}

// getTabletRecord returns a tablet's topology record.
func getTabletRecord(ctx context.Context, alias string) (*tabletpb.Tablet, error) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablet", alias)
	if err != nil {
		return nil, err
	}

	record := &tabletpb.Tablet{}
	if err := protojson.Unmarshal([]byte(out), record); err != nil {
		return nil, err
	}
	return record, nil
}

// execOnTablet executes a query on a tablet's query service and returns the
// result.
func execOnTablet(ctx context.Context, tablet *vitesst.Tablet, sql string, binds map[string]any, opts *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	bindvars, err := sqltypes.BuildBindVariables(binds)
	if err != nil {
		return nil, err
	}

	proto, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(ctx, proto, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	txID, reservedID := 0, 0

	session := executorcontext.NewSafeSession(&vtgatepb.Session{Options: opts})
	return conn.Execute(ctx, session, &querypb.Target{
		Keyspace:   proto.Keyspace,
		Shard:      proto.Shard,
		TabletType: proto.Type,
	}, sql, bindvars, int64(txID), int64(reservedID), opts)
}

// streamTabletHealth invokes a HealthStream on a tablet and returns the
// responses. It returns an error if the stream ends with fewer than count
// responses.
func streamTabletHealth(ctx context.Context, tablet *vitesst.Tablet, count int) (responses []*querypb.StreamHealthResponse, err error) {
	proto, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(ctx, proto, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}

	i := 0
	err = conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		responses = append(responses, shr)

		i++
		if i >= count {
			return io.EOF
		}

		return nil
	})

	switch {
	case err != nil:
		return nil, err
	case len(responses) < count:
		return nil, errors.New("stream ended early")
	}

	return responses, nil
}

// tabletStatus returns the tablet's state name as reported by /debug/vars.
func tabletStatus(t *testing.T, tablet *vitesst.Tablet) string {
	t.Helper()
	vars, err := tablet.GetVars(t.Context())
	require.NoError(t, err)
	status, _ := vars["TabletStateName"].(string)
	return status
}

// statusDetails returns the tablet's /debug/status_details body.
func statusDetails(t *testing.T, tablet *vitesst.Tablet) string {
	t.Helper()
	_, body, err := tablet.MakeAPICall(t.Context(), "/debug/status_details")
	require.NoError(t, err)
	return body
}

// disableVTOrcRecoveries stops the VTOrcs from running any recoveries.
func disableVTOrcRecoveries(t *testing.T) {
	t.Helper()
	for _, vtorc := range clusterInstance.VTOrcs() {
		_, _, err := vtorc.MakeAPICallRetry(t.Context(), "/api/disable-global-recoveries", 30*time.Second,
			func(status int, body string) bool { return status == http.StatusOK })
		require.NoError(t, err)
	}
}

// enableVTOrcRecoveries allows the VTOrcs to run recoveries again.
func enableVTOrcRecoveries(t *testing.T) {
	t.Helper()
	for _, vtorc := range clusterInstance.VTOrcs() {
		_, _, err := vtorc.MakeAPICallRetry(t.Context(), "/api/enable-global-recoveries", 30*time.Second,
			func(status int, body string) bool { return status == http.StatusOK })
		require.NoError(t, err)
	}
}
