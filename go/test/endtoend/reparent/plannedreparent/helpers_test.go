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

package plannedreparent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/grpcclient"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	// Register the grpc queryservice dialer so StreamHealth works.
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

var (
	KeyspaceName = "ks"
	dbName       = "vt_" + KeyspaceName
	insertVal    = 1
	insertSQL    = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	sqlSchema    = `
	create table vt_insert_test (
	id bigint,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB
`
	cell1                  = "zone1"
	cell2                  = "zone2"
	ShardName              = "0"
	replicationWaitTimeout = time.Duration(15 * time.Second)

	tmClient = tmc.NewClient()

	cleanupsMu sync.Mutex
	cleanups   = map[*vitesst.Cluster]func(context.Context) error{}
)

// tabletMySQLPort is the mysqld port every tablet container listens on inside
// the cluster network.
const tabletMySQLPort = 3306

// tabletPlacement pins a tablet's cell and UID, so a shard's tablets land in
// the same cells and carry the same UIDs the tests expect.
type tabletPlacement struct {
	cell string
	uid  int
}

// region cluster setup/teardown

// SetupReparentCluster is used to setup the reparent cluster
func SetupReparentCluster(t *testing.T, durability string) *vitesst.Cluster {
	return setupCluster(t.Context(), t, ShardName, []string{cell1, cell2}, []int{3, 1}, durability)
}

// SetupRangeBasedCluster sets up the range based cluster
func SetupRangeBasedCluster(ctx context.Context, t *testing.T) *vitesst.Cluster {
	return setupCluster(ctx, t, ShardName, []string{cell1}, []int{2}, "semi_sync")
}

// TeardownCluster is used to teardown the reparent cluster.
func TeardownCluster(t *testing.T, clusterInstance *vitesst.Cluster) {
	ctx := context.WithoutCancel(t.Context())
	if t.Failed() {
		clusterInstance.DumpDiagnostics(ctx, t.Logf)
	}

	cleanupsMu.Lock()
	cleanup := cleanups[clusterInstance]
	delete(cleanups, clusterInstance)
	cleanupsMu.Unlock()

	if cleanup != nil {
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}
}

func setupCluster(ctx context.Context, t *testing.T, shardName string, cells []string, numTablets []int, durability string) *vitesst.Cluster {
	var placements []tabletPlacement
	for numCell := range len(cells) {
		for i := 1; i <= numTablets[numCell]; i++ {
			placements = append(placements, tabletPlacement{cell: cells[numCell], uid: 100*(numCell+1) + i})
		}
	}

	next := 0
	keyspace := vitesst.WithKeyspace(KeyspaceName).
		WithShardNames(shardName).
		WithReplicas(len(placements) - 1).
		WithDurabilityPolicy(durability).
		WithSchema(sqlSchema).
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.Cell = placements[next].cell
			spec.UID = placements[next].uid
			next++
		})

	clusterInstance, err := vitesst.NewCluster(
		vitesst.WithCells(cells...),
		vitesst.WithoutVTGate(),
		vitesst.WithVTTabletArgs(
			"--lock-tables-timeout", "5s",
			"--track-schema-versions=true",
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

	cleanup, err := clusterInstance.Start(ctx)
	require.NoError(t, err)

	cleanupsMu.Lock()
	cleanups[clusterInstance] = cleanup
	cleanupsMu.Unlock()

	WaitForReplicationToStart(t, clusterInstance, KeyspaceName, shardName, len(placements), true)
	return clusterInstance
}

// shardTablets returns the shard's tablets in placement order: primary first,
// then the replicas in the order they were created.
func shardTablets(clusterInstance *vitesst.Cluster) []*vitesst.Tablet {
	return clusterInstance.Keyspace(KeyspaceName).Shard(ShardName).Tablets()
}

// StartNewVTTablet starts a new vttablet instance and joins it to the shard.
func StartNewVTTablet(t *testing.T, clusterInstance *vitesst.Cluster) *vitesst.Tablet {
	tablet, err := clusterInstance.AddTablet(t.Context(), "", KeyspaceName, ShardName, "replica")
	require.NoError(t, err)
	return tablet
}

// endregion

// region database queries

// RunSQLs is used to run SQL commands directly on the MySQL instance of a vttablet. All commands are
// run in a single connection.
func RunSQLs(ctx context.Context, t *testing.T, sqls []string, tablet *vitesst.Tablet) (results []*sqltypes.Result) {
	conn := tabletConn(ctx, t, tablet)
	defer conn.Close()

	for _, sql := range sqls {
		result := execute(t, conn, sql)
		results = append(results, result)
	}
	return results
}

// RunSQL is used to run a SQL command directly on the MySQL instance of a vttablet
func RunSQL(ctx context.Context, t *testing.T, sql string, tablet *vitesst.Tablet) *sqltypes.Result {
	conn := tabletConn(ctx, t, tablet)
	defer conn.Close()
	return execute(t, conn, sql)
}

func tabletConn(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) *mysql.Conn {
	params, err := tablet.DBAConnParams(ctx, dbName)
	require.NoError(t, err)
	conn, err := mysql.Connect(ctx, &params)
	require.Nil(t, err)
	return conn
}

func execute(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}

// endregion

// region ers, prs

// Prs runs PRS
func Prs(t *testing.T, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, extraArgs ...string) (string, error) {
	return PrsWithTimeout(t, clusterInstance, tab, false, "", "", extraArgs...)
}

// PrsAvoid runs PRS
func PrsAvoid(t *testing.T, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, extraArgs ...string) (string, error) {
	return PrsWithTimeout(t, clusterInstance, tab, true, "", "", extraArgs...)
}

// PrsWithTimeout runs PRS
func PrsWithTimeout(t *testing.T, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, avoid bool, actionTimeout, waitTimeout string, extraArgs ...string) (string, error) {
	args := []string{
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", KeyspaceName, ShardName),
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
	args = append(args, extraArgs...)
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), args...)
}

// endregion

// region validations

// ValidateTopology is used to validate the topology
func ValidateTopology(t *testing.T, clusterInstance *vitesst.Cluster, pingTablets bool) {
	args := []string{"Validate"}

	if pingTablets {
		args = append(args, "--ping-tablets")
	}
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), args...)
	require.Contains(t, out, "no issues found")
	require.NoError(t, err)
}

// ConfirmReplication confirms that the replication is working properly
func ConfirmReplication(t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet) int {
	ctx := t.Context()
	insertVal++
	n := insertVal // unique value ...
	// insert data into the new primary, check the connected replica work
	insertSQL := fmt.Sprintf(insertSQL, n, n)
	RunSQL(ctx, t, insertSQL, primary)
	for _, tab := range replicas {
		err := CheckInsertedValues(ctx, t, tab, n)
		require.NoError(t, err)
	}
	return n
}

// CheckPrimaryTablet makes sure the tablet type is primary, and its health check agrees.
func CheckPrimaryTablet(t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) {
	tabletInfo, err := getTablet(t.Context(), clusterInstance, tablet.Alias())
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tabletInfo.GetType())

	// make sure the health stream is updated
	shrs, err := streamTabletHealth(context.Background(), clusterInstance, tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tabletType)
}

// CheckInsertedValues checks that the given value is present in the given tablet
func CheckInsertedValues(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, index int) error {
	query := fmt.Sprintf("select msg from vt_insert_test where id=%d", index)
	var conn *mysql.Conn

	// wait until it gets the data
	timeout := time.Now().Add(replicationWaitTimeout)
	i := 0
	for time.Now().Before(timeout) {
		// We start with no connection to MySQL
		if conn == nil {
			// Try connecting to MySQL
			params, err := tablet.DBAConnParams(ctx, dbName)
			if err == nil {
				mysqlConn, err := mysql.Connect(ctx, &params)
				// This can fail if the database create hasn't been replicated yet.
				// We ignore this failure and try again later
				if err == nil {
					// If we succeed, then we store the connection
					// and reuse it for checking the rows in the table.
					conn = mysqlConn
					defer conn.Close()
				}
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

func CheckSemiSyncSetupCorrectly(t *testing.T, tablet *vitesst.Tablet, semiSyncVal string) {
	ctx := t.Context()
	semisyncType, err := semiSyncExtensionLoaded(ctx, tablet)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		dbVar := getDBVar(ctx, t, tablet, "rpl_semi_sync_replica_enabled")
		require.Equal(t, semiSyncVal, dbVar)
	case mysql.SemiSyncTypeMaster:
		dbVar := getDBVar(ctx, t, tablet, "rpl_semi_sync_slave_enabled")
		require.Equal(t, semiSyncVal, dbVar)
	default:
		require.Fail(t, "Unknown semi sync type")
	}
}

// getDBVar returns first matching database variable's value
func getDBVar(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, varName string) string {
	qr := RunSQL(ctx, t, fmt.Sprintf("show variables like '%s'", varName), tablet)
	if len(qr.Rows) == 0 {
		return ""
	}
	return qr.Rows[0][1].ToString()
}

// endregion

// region tablet operations

// StopTablet stops the tablet
func StopTablet(t *testing.T, tab *vitesst.Tablet, stopDatabase bool) {
	ctx := t.Context()
	err := tab.StopVttablet(ctx)
	require.NoError(t, err)
	if stopDatabase {
		err = tab.StopMySQL(ctx)
		require.NoError(t, err)
	}
}

// DeleteTablet is used to delete the given tablet
func DeleteTablet(t *testing.T, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet) {
	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(),
		"DeleteTablets",
		"--allow-primary",
		tab.Alias())
	require.NoError(t, err)
}

// endregion

// region get info

// canonicalAlias formats a tablet's alias in the zero-padded form used in
// topology records and Vitess-internal error messages.
func canonicalAlias(tablet *vitesst.Tablet) string {
	return topoproto.TabletAliasString(&topodatapb.TabletAlias{Cell: tablet.Cell, Uid: uint32(tablet.UID)})
}

// getTablet fetches a tablet's topology record.
func getTablet(ctx context.Context, clusterInstance *vitesst.Cluster, alias string) (*topodatapb.Tablet, error) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablet", alias)
	if err != nil {
		return nil, err
	}
	tablet := &topodatapb.Tablet{}
	if err := json2.UnmarshalPB([]byte(out), tablet); err != nil {
		return nil, err
	}
	return tablet, nil
}

// streamTabletHealth invokes a HealthStream on a tablet and returns the
// responses. It returns an error if the stream ends with fewer than count
// responses.
func streamTabletHealth(ctx context.Context, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet, count int) (responses []*querypb.StreamHealthResponse, err error) {
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

// GetShardReplicationPositions gets the shards replication positions.
func GetShardReplicationPositions(t *testing.T, clusterInstance *vitesst.Cluster, keyspaceName, shardName string, doPrint bool) []string {
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(),
		"ShardReplicationPositions", fmt.Sprintf("%s/%s", keyspaceName, shardName))
	require.NoError(t, err)
	strArray := strings.Split(output, "\n")
	if strArray[len(strArray)-1] == "" {
		strArray = strArray[:len(strArray)-1] // Truncate slice, remove empty line
	}
	if doPrint {
		t.Logf("Positions:")
		for _, pos := range strArray {
			t.Logf("\t%s", pos)
		}
	}
	return strArray
}

func WaitForReplicationToStart(t *testing.T, clusterInstance *vitesst.Cluster, keyspaceName, shardName string, tabletCnt int, doPrint bool) {
	tkr := time.NewTicker(500 * time.Millisecond)
	defer tkr.Stop()
	for {
		select {
		case <-tkr.C:
			strArray := GetShardReplicationPositions(t, clusterInstance, keyspaceName, shardName, doPrint)
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

// CheckReplicaStatus checks the replication status and asserts that the replication is stopped
func CheckReplicaStatus(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) {
	qr := RunSQL(ctx, t, "show replica status", tablet)
	IOThreadRunning := fmt.Sprintf("%v", qr.Rows[0][10])
	SQLThreadRunning := fmt.Sprintf("%v", qr.Rows[0][10])
	assert.Equal(t, IOThreadRunning, "VARCHAR(\"No\")")
	assert.Equal(t, SQLThreadRunning, "VARCHAR(\"No\")")
}

// CheckReparentFromOutside checks that cluster was reparented from outside
func CheckReparentFromOutside(t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet, downPrimary bool, baseTime int64) {
	ctx := t.Context()
	result, err := getShardReplication(ctx, clusterInstance, KeyspaceName, ShardName, cell1)
	require.Nil(t, err, "error should be Nil")
	require.NotNil(t, result[cell1], "result should not be nil")
	if !downPrimary {
		assert.Len(t, result[cell1].Nodes, 3)
	} else {
		assert.Len(t, result[cell1].Nodes, 2)
	}

	// make sure the primary status page says it's the primary
	_, status, err := tablet.MakeAPICall(ctx, "/debug/status")
	require.NoError(t, err)
	assert.Contains(t, status, "Tablet Type: PRIMARY")

	// make sure the primary health stream says it's the primary too
	// (health check is disabled on these servers, force it first)
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", tablet.Alias())
	require.NoError(t, err)

	shrs, err := streamTabletHealth(context.Background(), clusterInstance, tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.Equal(t, streamHealthResponse.Target.TabletType, topodatapb.TabletType_PRIMARY)
	assert.True(t, streamHealthResponse.PrimaryTermStartTimestamp >= baseTime)
}

// getShardReplication fetches the shard replication records for the given cell.
func getShardReplication(ctx context.Context, clusterInstance *vitesst.Cluster, keyspace, shard, cell string) (map[string]*topodatapb.ShardReplication, error) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetShardReplication", keyspace+"/"+shard, cell)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetShardReplicationResponse
	err = json2.UnmarshalPB([]byte(out), &resp)
	return resp.ShardReplicationByCell, err
}

// WaitForReplicationPosition waits for tablet B to catch up to the replication position of tablet A.
func WaitForReplicationPosition(t *testing.T, tabletA *vitesst.Tablet, tabletB *vitesst.Tablet) error {
	ctx := t.Context()
	_, gtIDA := primaryPosition(t, tabletA)
	timeout := time.Now().Add(replicationWaitTimeout)
	for time.Now().Before(timeout) {
		if positionAtLeast(ctx, t, tabletB, gtIDA) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.New("failed to catch up on replication position")
}

// primaryPosition gets the executed replication position of the given tablet.
func primaryPosition(t *testing.T, tablet *vitesst.Tablet) (string, string) {
	ctx := t.Context()
	proto, err := tablet.TabletProto(ctx)
	require.NoError(t, err)
	pos, err := tmClient.PrimaryPosition(ctx, proto)
	require.NoError(t, err)
	gtID := strings.SplitAfter(pos, "/")[1]
	return pos, gtID
}

// positionAtLeast reports whether the tablet's executed GTID set already
// contains the given GTID set.
func positionAtLeast(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, gtID string) bool {
	qr := RunSQL(ctx, t, fmt.Sprintf("SELECT GTID_SUBSET('%s', @@global.gtid_executed)", gtID), tablet)
	return qr.Rows[0][0].ToString() == "1"
}

func CheckSemisyncEnabled(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, enabled bool) {
	conn := tabletConn(ctx, t, tablet)
	defer conn.Close()

	status := "OFF"
	if enabled {
		status = "ON"
	}

	semisyncType, err := semiSyncExtensionLoaded(ctx, tablet)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		qr := execute(t, conn, "show variables like 'rpl_semi_sync_replica_enabled'")
		got := fmt.Sprintf("%v", qr.Rows)
		want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", "rpl_semi_sync_replica_enabled", status)
		assert.Equal(t, want, got)
	case mysql.SemiSyncTypeMaster:
		qr := execute(t, conn, "show variables like 'rpl_semi_sync_slave_enabled'")
		got := fmt.Sprintf("%v", qr.Rows)
		want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", "rpl_semi_sync_slave_enabled", status)
		assert.Equal(t, want, got)
	}
}

func CheckSemisyncStatus(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, enabled bool) {
	conn := tabletConn(ctx, t, tablet)
	defer conn.Close()

	status := "OFF"
	if enabled {
		status = "ON"
	}

	semisyncType, err := semiSyncExtensionLoaded(ctx, tablet)
	require.NoError(t, err)
	switch semisyncType {
	case mysql.SemiSyncTypeSource:
		qr := execute(t, conn, "show status like 'Rpl_semi_sync_replica_status'")
		got := fmt.Sprintf("%v", qr.Rows)
		want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", "Rpl_semi_sync_replica_status", status)
		assert.Equal(t, want, got)
	case mysql.SemiSyncTypeMaster:
		qr := execute(t, conn, "show status like 'Rpl_semi_sync_slave_status'")
		got := fmt.Sprintf("%v", qr.Rows)
		want := fmt.Sprintf("[[VARCHAR(\"%s\") VARCHAR(\"%s\")]]", "Rpl_semi_sync_slave_status", status)
		assert.Equal(t, want, got)
	default:
		assert.Fail(t, "unknown semi-sync type")
	}
}

func semiSyncExtensionLoaded(ctx context.Context, tablet *vitesst.Tablet) (mysql.SemiSyncType, error) {
	conn := connectTablet(ctx, tablet)
	if conn == nil {
		return mysql.SemiSyncTypeUnknown, errors.New("could not connect to tablet mysqld")
	}
	defer conn.Close()
	return conn.SemiSyncExtensionLoaded()
}

func connectTablet(ctx context.Context, tablet *vitesst.Tablet) *mysql.Conn {
	params, err := tablet.DBAConnParams(ctx, dbName)
	if err != nil {
		return nil
	}
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return nil
	}
	return conn
}

// resetBinaryLogsCommand returns the command to reset binary logs on the tablet.
func resetBinaryLogsCommand(ctx context.Context, tablet *vitesst.Tablet) (string, error) {
	conn := connectTablet(ctx, tablet)
	if conn == nil {
		return "", errors.New("could not connect to tablet mysqld")
	}
	defer conn.Close()
	return conn.ResetBinaryLogsCommand(), nil
}
