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

package newfeaturetest

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
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	// Register the grpc queryservice dialer so StreamHealth works.
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

var (
	KeyspaceName            = "ks"
	dbName                  = "vt_" + KeyspaceName
	insertVal               = 1
	insertSQL               = "insert into vt_insert_test(id, msg) values (%d, 'test %d')"
	insertSQLMultipleValues = "insert into vt_insert_test(id, msg) values (%d, 'test %d'), (%d, 'test %d'), (%d, 'test %d'), (%d, 'test %d')"
	sqlSchema               = `
	create table vt_insert_test (
	id bigint,
	msg varchar(64),
	primary key (id)
	) Engine=InnoDB
`
	shardedVSchema = `{"sharded": true, "vindexes": {"hash_index": {"type": "hash"}}, "tables": {"vt_insert_test": {"column_vindexes": [{"column": "id", "name": "hash_index"}]}}}`

	cell1     = "zone1"
	cell2     = "zone2"
	ShardName = "0"
	// The tests drive replication with the SQL statements MySQL 8.0 accepts.
	mysqlVersion           = "8.0"
	replicationWaitTimeout = time.Duration(15 * time.Second)

	cleanupsMu sync.Mutex
	cleanups   = map[*vitesst.Cluster]func(context.Context) error{}
)

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

// SetupShardedReparentCluster is used to setup a sharded cluster for testing
func SetupShardedReparentCluster(t *testing.T, durability string, extraVttabletFlags []string) *vitesst.Cluster {
	ctx := t.Context()

	tabletArgs := []string{
		"--lock-tables-timeout", "5s",
		// Fast health checks help find corner cases.
		"--health-check-interval", "1s",
		"--track-schema-versions=true",
		"--queryserver-enable-online-ddl=false",
	}
	tabletArgs = append(tabletArgs, extraVttabletFlags...)

	keyspace := vitesst.WithKeyspace(KeyspaceName).
		WithShardNames("-40", "40-80", "80-").
		WithReplicas(2).
		WithDurabilityPolicy(durability).
		WithSchema(sqlSchema).
		WithVSchema(shardedVSchema)

	clusterInstance, err := vitesst.NewCluster(t,
		vitesst.WithMySQLVersion(mysqlVersion),
		vitesst.WithVTTabletArgs(tabletArgs...),
		vitesst.WithVTGateArgs(
			"--enable-buffer",
			// Long timeout in case failover is slow.
			"--buffer-window", "10m",
			"--buffer-max-failover-duration", "10m",
			"--buffer-min-time-between-failovers", "20m",
		),
		vitesst.WithVTOrc(),
		keyspace,
	)
	require.NoError(t, err)

	cleanup, err := clusterInstance.Start(t, ctx)
	require.NoError(t, err)

	cleanupsMu.Lock()
	cleanups[clusterInstance] = cleanup
	cleanupsMu.Unlock()

	return clusterInstance
}

// GetInsertQuery returns a built insert query to insert a row.
func GetInsertQuery(idx int) string {
	return fmt.Sprintf(insertSQL, idx, idx)
}

// GetInsertMultipleValuesQuery returns a built insert query to insert multiple rows at once.
func GetInsertMultipleValuesQuery(idx1, idx2, idx3, idx4 int) string {
	return fmt.Sprintf(insertSQLMultipleValues, idx1, idx1, idx2, idx2, idx3, idx3, idx4, idx4)
}

// TeardownCluster is used to teardown the reparent cluster.
func TeardownCluster(t *testing.T, clusterInstance *vitesst.Cluster) {
	ctx := context.WithoutCancel(t.Context())

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

	clusterInstance, err := vitesst.NewCluster(t,
		vitesst.WithMySQLVersion(mysqlVersion),
		vitesst.WithCells(cells...),
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

	cleanup, err := clusterInstance.Start(t, ctx)
	require.NoError(t, err)

	cleanupsMu.Lock()
	cleanups[clusterInstance] = cleanup
	cleanupsMu.Unlock()

	ValidateTopology(t, clusterInstance, true)
	CheckPrimaryTablet(t, clusterInstance, clusterInstance.Keyspace(KeyspaceName).Shard(shardName).Tablets()[0])
	ValidateTopology(t, clusterInstance, false)

	WaitForReplicationToStart(t, clusterInstance, KeyspaceName, shardName, len(placements), true)
	return clusterInstance
}

// shardTablets returns the tablets of the single-shard reparent cluster in
// placement order: primary first, then the replicas in the order they were
// created.
func shardTablets(clusterInstance *vitesst.Cluster) []*vitesst.Tablet {
	return clusterInstance.Keyspace(KeyspaceName).Shard(ShardName).Tablets()
}

// restartTablet starts the tablet's vttablet again after a StopTablet and
// blocks until it reports a serving state.
func restartTablet(t *testing.T, tablet *vitesst.Tablet) {
	require.NoError(t, tablet.StartVttablet(t.Context()))
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

// PlannedReparentShard reparents the given shard to the given tablet.
func PlannedReparentShard(ctx context.Context, clusterInstance *vitesst.Cluster, keyspace, shard, alias string) error {
	return clusterInstance.Vtctld().ExecuteCommand(
		ctx,
		"PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspace, shard),
		"--new-primary", alias,
		"--wait-replicas-timeout", "30s",
	)
}

// Ers runs the ERS
func Ers(ctx context.Context, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, totalTimeout, waitReplicasTimeout string) (string, error) {
	return ErsIgnoreTablet(ctx, clusterInstance, tab, totalTimeout, waitReplicasTimeout, nil, false)
}

// ErsIgnoreTablet is used to run ERS
func ErsIgnoreTablet(ctx context.Context, clusterInstance *vitesst.Cluster, tab *vitesst.Tablet, timeout, waitReplicasTimeout string, tabletsToIgnore []*vitesst.Tablet, preventCrossCellPromotion bool) (string, error) {
	var args []string
	if timeout != "" {
		args = append(args, "--action-timeout", timeout)
	}
	args = append(args, "EmergencyReparentShard", fmt.Sprintf("%s/%s", KeyspaceName, ShardName))
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
		for _, tablet := range tabletsToIgnore {
			if tabsString == "" {
				tabsString = tablet.Alias()
			} else {
				tabsString = tabsString + "," + tablet.Alias()
			}
		}
		args = append(args, "--ignore-replicas", tabsString)
	}
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
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

// isHealthyPrimaryTablet will return if tablet is primary AND healthy.
func isHealthyPrimaryTablet(t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) bool {
	tabletInfo, err := getTablet(t.Context(), clusterInstance, tablet.Alias())
	require.Nil(t, err)
	if tabletInfo.GetType() != topodatapb.TabletType_PRIMARY {
		return false
	}

	// make sure the health stream is updated
	shrs, err := streamTabletHealth(context.Background(), clusterInstance, tablet, 1)
	require.NoError(t, err)
	streamHealthResponse := shrs[0]

	assert.True(t, streamHealthResponse.GetServing())
	tabletType := streamHealthResponse.GetTarget().GetTabletType()
	return tabletType == topodatapb.TabletType_PRIMARY
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

// GetNewPrimary is used to find the new primary of the cluster.
func GetNewPrimary(t *testing.T, clusterInstance *vitesst.Cluster) *vitesst.Tablet {
	var newPrimary *vitesst.Tablet
	for _, tablet := range shardTablets(clusterInstance)[1:] {
		if isHealthyPrimaryTablet(t, clusterInstance, tablet) {
			newPrimary = tablet
			break
		}
	}
	require.NotNil(t, newPrimary)
	return newPrimary
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

// SemiSyncExtensionLoaded reports the semi-sync plugin flavor loaded on the
// given tablet's mysqld.
func SemiSyncExtensionLoaded(ctx context.Context, tablet *vitesst.Tablet) (mysql.SemiSyncType, error) {
	params, err := tablet.DBAConnParams(ctx, dbName)
	if err != nil {
		return mysql.SemiSyncTypeUnknown, err
	}
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return mysql.SemiSyncTypeUnknown, err
	}
	defer conn.Close()
	return conn.SemiSyncExtensionLoaded()
}
