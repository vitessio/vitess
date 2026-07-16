/*
Copyright 2026 The Vitess Authors.

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

package emergencyreparent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	// Register the grpc queryservice dialer so StreamHealth works.
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

const (
	keyspaceName  = "ks"
	shardName     = "0"
	keyspaceShard = keyspaceName + "/" + shardName
	dbName        = "vt_" + keyspaceName
	cell1         = "zone1"
	cell2         = "zone2"
)

var (
	insertVal              = 1
	insertSQL              = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	replicationWaitTimeout = 15 * time.Second
	sqlSchema              = `
	create table vt_insert_test (
	id bigint,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB
`
)

// setupReparentCluster brings up a cluster with three tablets in zone1 and one
// in zone2, elects the first zone1 tablet as primary, and applies the schema.
func setupReparentCluster(t *testing.T, durability string) *vitesst.Cluster {
	t.Helper()
	ctx := t.Context()

	cells := []string{cell1, cell1, cell1, cell2}
	uids := []int{101, 102, 103, 201}
	idx := 0

	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(3).
		WithDurabilityPolicy(durability).
		WithSchema(sqlSchema).
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.Cell = cells[idx]
			spec.UID = uids[idx]
			idx++
		})

	clusterInstance, err := vitesst.NewCluster(
		vitesst.WithCells(cell1, cell2),
		vitesst.WithoutVTGate(),
		vitesst.WithVTTabletArgs(
			"--lock-tables-timeout", "5s",
			"--track-schema-versions=true",
			// The tablets take no backups. A tablet that starts with the restore
			// path enabled and already has data skips replication initialization,
			// so a tablet brought back up after its mysqld was stopped would never
			// replicate from the current primary.
			"--restore-from-backup=false",
			// disabling online-ddl for reparent tests. This is done to reduce flakiness.
			// All the tests in this package reparent frequently between different tablets
			// This means that Promoting a tablet to primary is sometimes immediately followed by a DemotePrimary call.
			// In this case, the close method and initSchema method of the onlineDDL executor race.
			// If the initSchema acquires the lock, then it takes about 30 seconds for it to run during which time the
			// DemotePrimary rpc is stalled!
			"--queryserver-enable-online-ddl=false",
		),
		keyspace,
	)
	require.NoError(t, err)

	startCluster(t, clusterInstance)

	tablets := clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()
	validateTopology(ctx, t, clusterInstance, true)
	checkPrimaryTablet(ctx, t, clusterInstance, tablets[0])
	validateTopology(ctx, t, clusterInstance, false)

	waitForReplicationToStart(ctx, t, clusterInstance, keyspaceName, shardName, 4)
	return clusterInstance
}

// startCluster starts the cluster and registers teardown, dumping diagnostics
// when the test fails.
func startCluster(t *testing.T, clusterInstance *vitesst.Cluster) {
	t.Helper()
	ctx := t.Context()

	cleanup, err := clusterInstance.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		if t.Failed() {
			clusterInstance.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)
}

// region database queries

// runSQL runs a SQL command directly on the MySQL instance of a vttablet.
func runSQL(ctx context.Context, t *testing.T, sql string, tablet *vitesst.Tablet) *sqltypes.Result {
	t.Helper()
	conn, err := vitesst.GetMySQLConn(ctx, tablet, dbName)
	require.NoError(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(sql, 1000, true)
	require.NoError(t, err)
	return qr
}

// endregion

// region ers, prs

// prs runs PRS.
func prs(ctx context.Context, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet) (string, error) {
	return prsWithTimeout(ctx, clusterInstance, tab, false, "", "")
}

// prsWithTimeout runs PRS.
func prsWithTimeout(ctx context.Context, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, avoid bool, actionTimeout, waitTimeout string) (string, error) {
	args := []string{
		"PlannedReparentShard",
		keyspaceShard,
	}
	if actionTimeout != "" {
		args = append(args, "--action-timeout", actionTimeout)
	}
	if waitTimeout != "" {
		args = append(args, "--wait-replicas-timeout", waitTimeout)
	}
	if avoid {
		args = append(args, "--avoid-primary")
	} else {
		args = append(args, "--new-primary")
	}
	args = append(args, tab.Alias())
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
}

// ers runs the ERS.
func ers(ctx context.Context, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, totalTimeout, waitReplicasTimeout string) (string, error) {
	return ersIgnoreTablet(ctx, clusterInstance, tab, totalTimeout, waitReplicasTimeout, nil, false)
}

// ersIgnoreTablet is used to run ERS.
func ersIgnoreTablet(ctx context.Context, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, timeout, waitReplicasTimeout string, tabletsToIgnore []*vitesst.Tablet, preventCrossCellPromotion bool) (string, error) {
	var args []string
	if timeout != "" {
		args = append(args, "--action-timeout", timeout)
	}
	args = append(args, "EmergencyReparentShard", keyspaceShard)
	if tab != nil {
		args = append(args, "--new-primary", tab.Alias())
	}
	if waitReplicasTimeout != "" {
		args = append(args, "--wait-replicas-timeout", waitReplicasTimeout)
	}
	if preventCrossCellPromotion {
		args = append(args, "--prevent-cross-cell-promotion")
	}
	if len(tabletsToIgnore) != 0 {
		tabsString := ""
		for _, vttablet := range tabletsToIgnore {
			if tabsString == "" {
				tabsString = vttablet.Alias()
			} else {
				tabsString = tabsString + "," + vttablet.Alias()
			}
		}
		args = append(args, "--ignore-replicas", tabsString)
	}
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
}

// ersWithVtctldClient runs ERS via a vtctldclient binary.
func ersWithVtctldClient(ctx context.Context, clusterInstance *vitesst.Cluster) (string, error) {
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "EmergencyReparentShard", keyspaceShard)
}

// endregion

// region validations

// validateTopology is used to validate the topology.
func validateTopology(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, pingTablets bool) {
	args := []string{"Validate"}

	if pingTablets {
		args = append(args, "--ping-tablets")
	}
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
	require.Contains(t, out, "no issues found")
	require.NoError(t, err)
}

// confirmReplication confirms that the replication is working properly.
func confirmReplication(t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet) int {
	ctx := t.Context()
	insertVal++
	n := insertVal // unique value ...
	// insert data into the new primary, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, n, n)
	runSQL(ctx, t, insertSQL, primary)
	for _, tab := range replicas {
		err := checkInsertedValues(ctx, t, tab, n)
		require.NoError(t, err)
	}
	return n
}

// confirmOldPrimaryIsHangingAround confirms that the old primary is hanging around.
func confirmOldPrimaryIsHangingAround(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "Validate")
	require.Error(t, err)
	require.Contains(t, out, "already has primary")
}

// checkPrimaryTablet makes sure the tablet type is primary, and its health check agrees.
func checkPrimaryTablet(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) {
	typ, err := tabletTopoType(ctx, clusterInstance, tablet)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, typ)

	// make sure the health stream is updated
	shrs, err := streamTabletHealth(ctx, tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tabletType)
}

// isHealthyPrimaryTablet will return if tablet is primary AND healthy.
func isHealthyPrimaryTablet(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) bool {
	typ, err := tabletTopoType(ctx, clusterInstance, tablet)
	require.NoError(t, err)
	if typ != topodatapb.TabletType_PRIMARY {
		return false
	}

	// make sure the health stream is updated
	shrs, err := streamTabletHealth(ctx, tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	return tabletType == topodatapb.TabletType_PRIMARY
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

// tabletTopoType returns the tablet type recorded in the topology server.
func tabletTopoType(ctx context.Context, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) (topodatapb.TabletType, error) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablet", tablet.Alias())
	if err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}
	record := &topodatapb.Tablet{}
	if err := protojson.Unmarshal([]byte(out), record); err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}
	return record.GetType(), nil
}

// checkInsertedValues checks that the given value is present in the given tablet.
func checkInsertedValues(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, index int) error {
	query := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
	tabletParams, err := tablet.DBAConnParams(ctx, dbName)
	if err != nil {
		return err
	}
	var conn *mysql.Conn

	// wait until it gets the data
	timeout := time.Now().Add(replicationWaitTimeout)
	i := 0
	for time.Now().Before(timeout) {
		// We start with no connection to MySQL
		if conn == nil {
			// Try connecting to MySQL
			mysqlConn, err := mysql.Connect(ctx, &tabletParams)
			// This can fail if the database create hasn't been replicated yet.
			// We ignore this failure and try again later
			if err == nil {
				// If we succeed, then we store the connection
				// and reuse it for checking the rows in the table.
				conn = mysqlConn
				defer conn.Close()
			}
		}
		if conn != nil {
			// We'll get a mysql.ERNoSuchTable (1146) error if the CREATE TABLE has not replicated yet and
			// it's possible that we get other ephemeral errors too, so we make the tests more robust by
			// retrying with the timeout.
			qr, err := conn.ExecuteFetch(query, 1, true)
			if err == nil && len(qr.Rows) == 1 {
				return nil
			}
		}
		d := time.Duration(300 * i)
		time.Sleep(d * time.Millisecond)
		i++
	}
	return fmt.Errorf("data did not get replicated on tablet %s within the timeout of %v", tablet.Alias(), replicationWaitTimeout)
}

func checkSemiSyncSetupCorrectly(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, semiSyncVal string) {
	semisyncType, err := semiSyncExtensionLoaded(ctx, tablet)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		require.Equal(t, semiSyncVal, getDBVar(ctx, t, tablet, "rpl_semi_sync_replica_enabled"))
	case mysql.SemiSyncTypeMaster:
		require.Equal(t, semiSyncVal, getDBVar(ctx, t, tablet, "rpl_semi_sync_slave_enabled"))
	default:
		require.Fail(t, "Unknown semi sync type")
	}
}

// getDBVar reads the value of a MySQL variable from the given tablet.
func getDBVar(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, varName string) string {
	qr := runSQL(ctx, t, fmt.Sprintf("show variables like '%s'", varName), tablet)
	require.Len(t, qr.Rows, 1)
	return qr.Rows[0][1].ToString()
}

// checkCountOfInsertedValues checks that the number of inserted values matches the given count on the given tablet.
func checkCountOfInsertedValues(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, count int) error {
	selectSQL := "select * from vt_insert_test"
	qr := runSQL(ctx, t, selectSQL, tablet)
	if len(qr.Rows) == count {
		return nil
	}
	return fmt.Errorf("count does not match on the tablet %s", tablet.Alias())
}

// endregion

// region tablet operations

// stopTablet stops the tablet.
func stopTablet(ctx context.Context, t *testing.T, tab *vitesst.Tablet, stopDatabase bool) {
	err := tab.StopVttablet(ctx)
	require.NoError(t, err)
	if stopDatabase {
		err = tab.StopMySQL(ctx)
		require.NoError(t, err)
	}
}

// restartTablet brings the tablet's mysqld back up. The vttablet is left down.
func restartTablet(ctx context.Context, t *testing.T, tab *vitesst.Tablet) {
	err := tab.StartMySQL(ctx)
	require.NoError(t, err)
}

// resurrectTablet is used to resurrect the given tablet. It brings back both
// mysqld and the vttablet, which re-creates the tablet record in the topology
// and comes up as a replica of the current primary.
func resurrectTablet(ctx context.Context, t *testing.T, tab *vitesst.Tablet) {
	err := tab.StartMySQL(ctx)
	require.NoError(t, err)
	err = tab.StartVttablet(ctx)
	require.NoError(t, err)

	err = checkInsertedValues(ctx, t, tab, insertVal)
	require.NoError(t, err)
}

// deleteTablet is used to delete the given tablet.
func deleteTablet(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet) {
	err := clusterInstance.Vtctld().ExecuteCommand(ctx,
		"DeleteTablets",
		"--allow-primary",
		tab.Alias())
	require.NoError(t, err)
}

// endregion

// region get info

// getNewPrimary is used to find the new primary of the cluster.
func getNewPrimary(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster) *vitesst.Tablet {
	var newPrimary *vitesst.Tablet
	for _, tablet := range clusterInstance.Keyspace(keyspaceName).Shard(shardName).Tablets()[1:] {
		if isHealthyPrimaryTablet(ctx, t, clusterInstance, tablet) {
			newPrimary = tablet
			break
		}
	}
	require.NotNil(t, newPrimary)
	return newPrimary
}

// getShardReplicationPositions gets the shards replication positions.
func getShardReplicationPositions(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, keyspaceName, shardName string) []string {
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx,
		"ShardReplicationPositions", fmt.Sprintf("%s/%s", keyspaceName, shardName))
	require.NoError(t, err)
	strArray := strings.Split(output, "\n")
	if strArray[len(strArray)-1] == "" {
		strArray = strArray[:len(strArray)-1] // Truncate slice, remove empty line
	}
	return strArray
}

func waitForReplicationToStart(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, keyspaceName, shardName string, tabletCnt int) {
	tkr := time.NewTicker(500 * time.Millisecond)
	defer tkr.Stop()
	for {
		select {
		case <-tkr.C:
			strArray := getShardReplicationPositions(ctx, t, clusterInstance, keyspaceName, shardName)
			if len(strArray) == tabletCnt && strings.Contains(strArray[0], "primary") { // primary first
				return
			}
		case <-time.After(replicationWaitTimeout):
			require.FailNow(t, fmt.Sprintf("replication did not start everywhere in %s/%s within the timeout of %v",
				keyspaceName, shardName, replicationWaitTimeout))
			return
		}
	}
}

// endregion

// checkReplicationStatus checks that the replication for sql and io threads is setup as expected.
func checkReplicationStatus(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, sqlThreadRunning bool, ioThreadRunning bool) {
	res := runSQL(ctx, t, "show replica status", tablet)
	if ioThreadRunning {
		require.Equal(t, "Yes", res.Rows[0][10].ToString())
	} else {
		require.Equal(t, "No", res.Rows[0][10].ToString())
	}

	if sqlThreadRunning {
		require.Equal(t, "Yes", res.Rows[0][11].ToString())
	} else {
		require.Equal(t, "No", res.Rows[0][11].ToString())
	}
}

func semiSyncExtensionLoaded(ctx context.Context, tablet *vitesst.Tablet) (mysql.SemiSyncType, error) {
	conn, err := vitesst.GetMySQLConn(ctx, tablet, dbName)
	if err != nil {
		return mysql.SemiSyncTypeUnknown, err
	}
	defer conn.Close()
	return conn.SemiSyncExtensionLoaded()
}

// waitForQueryWithStateInProcesslist waits for a query to be present in the processlist with a specific state.
func waitForQueryWithStateInProcesslist(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, sql, state string, timeout time.Duration) {
	require.Eventually(t, func() bool {
		qr := runSQL(ctx, t, "select Command, State, Info from information_schema.processlist", tablet)
		for _, row := range qr.Rows {
			if len(row) != 3 {
				continue
			}
			if strings.EqualFold(row[0].ToString(), "Query") {
				continue
			}
			if strings.EqualFold(row[1].ToString(), state) && strings.EqualFold(row[2].ToString(), sql) {
				return true
			}
		}
		return false
	}, timeout, time.Second, "query with state not in processlist")
}
