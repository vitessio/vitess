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

package sharding

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	lotRange1 uint64 = 0xA000000000000000
	lotRange2 uint64 = 0xE000000000000000
	// InsertTabletTemplateKsID common insert format to be used for different tests
	InsertTabletTemplateKsID = `insert into %s (id, msg) values (%d, '%s') /* id:%d */`
)

const (
	maxRowsToFetch = 10000
)

// CheckSrvKeyspace verifies the schema with expectedPartition
func CheckSrvKeyspace(t *testing.T, cell string, ksname string, shardingCol string, colType topodata.KeyspaceIdType, expectedPartition map[topodata.TabletType][]string, ci cluster.LocalProcessCluster) {
	srvKeyspace := GetSrvKeyspace(t, cell, ksname, ci)
	if shardingCol != "" {
		assert.Equal(t, srvKeyspace.ShardingColumnName, shardingCol)
	}
	if colType != 0 {
		assert.Equal(t, srvKeyspace.ShardingColumnType, colType)
	}

	currentPartition := map[topodata.TabletType][]string{}

	for _, partition := range srvKeyspace.Partitions {
		currentPartition[partition.ServedType] = []string{}
		for _, shardRef := range partition.ShardReferences {
			currentPartition[partition.ServedType] = append(currentPartition[partition.ServedType], shardRef.Name)
		}
	}

	assert.True(t, reflect.DeepEqual(currentPartition, expectedPartition))
}

// GetSrvKeyspace return the Srv Keyspace structure
func GetSrvKeyspace(t *testing.T, cell string, ksname string, ci cluster.LocalProcessCluster) *topodata.SrvKeyspace {
	output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspace", cell, ksname)
	require.Nil(t, err)
	var srvKeyspace topodata.SrvKeyspace

	err = json2.Unmarshal([]byte(output), &srvKeyspace)
	require.Nil(t, err)
	return &srvKeyspace
}

// VerifyTabletHealth checks that the tablet URL is reachable.
func VerifyTabletHealth(t *testing.T, vttablet cluster.Vttablet, hostname string) {
	tabletURL := fmt.Sprintf("http://%s:%d/healthz", hostname, vttablet.HTTPPort)
	resp, err := http.Get(tabletURL)
	require.Nil(t, err)
	assert.Equal(t, resp.StatusCode, 200)
}

// CheckValues check value from sql query to table with expected values
func CheckValues(t *testing.T, vttablet cluster.Vttablet, id uint64, msg string, exists bool, tableName string, ks string, keyType querypb.Type, dbConn *mysql.Conn) bool {
	query := fmt.Sprintf("select id, msg from %s where id = %d", tableName, id)
	if keyType == querypb.Type_VARBINARY {
		query = fmt.Sprintf("select id, msg from %s where id = '%d'", tableName, id)
	}
	var result *sqltypes.Result
	if dbConn != nil {
		r1, err := dbConn.ExecuteFetch(query, maxRowsToFetch, true)
		require.Nil(t, err)
		result = r1
	} else {
		r2, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
		require.Nil(t, err)
		result = r2
	}

	isFound := false
	if exists && len(result.Rows) > 0 {
		if keyType == querypb.Type_VARBINARY {
			isFound = assert.Equal(t, fmt.Sprintf("%v", result.Rows), fmt.Sprintf(`[[VARBINARY("%d") VARCHAR("%s")]]`, id, msg))
		} else {
			isFound = assert.Equal(t, fmt.Sprintf("%v", result.Rows), fmt.Sprintf(`[[UINT64(%d) VARCHAR("%s")]]`, id, msg))
		}

	} else {
		assert.Equal(t, len(result.Rows), 0)
	}
	return isFound
}

// CheckDestinationMaster performs multiple checks on a destination master.
func CheckDestinationMaster(t *testing.T, vttablet cluster.Vttablet, sourceShards []string, ci cluster.LocalProcessCluster) {
	_ = vttablet.VttabletProcess.WaitForBinLogPlayerCount(len(sourceShards))
	CheckBinlogPlayerVars(t, vttablet, sourceShards, 0)
	checkStreamHealthEqualsBinlogPlayerVars(t, vttablet, len(sourceShards), ci)
}

// CheckBinlogPlayerVars Checks the binlog player variables are correctly exported.
func CheckBinlogPlayerVars(t *testing.T, vttablet cluster.Vttablet, sourceShards []string, secondBehindMaster int64) {
	tabletVars := vttablet.VttabletProcess.GetVars()

	assert.Contains(t, tabletVars, "VReplicationStreamCount")
	assert.Contains(t, tabletVars, "VReplicationSecondsBehindMasterMax")
	assert.Contains(t, tabletVars, "VReplicationSecondsBehindMaster")
	assert.Contains(t, tabletVars, "VReplicationSource")
	assert.Contains(t, tabletVars, "VReplicationSourceTablet")

	streamCountStr := fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationStreamCount"]))
	streamCount, _ := strconv.Atoi(streamCountStr)
	assert.Equal(t, streamCount, len(sourceShards))

	replicationSourceObj := reflect.ValueOf(tabletVars["VReplicationSource"])
	replicationSourceValue := []string{}

	assert.Equal(t, len(replicationSourceObj.MapKeys()), len(reflect.ValueOf(tabletVars["VReplicationSourceTablet"]).MapKeys()))

	for _, key := range replicationSourceObj.MapKeys() {
		replicationSourceValue = append(replicationSourceValue,
			fmt.Sprintf("%v", replicationSourceObj.MapIndex(key)))
	}

	for _, shard := range sourceShards {
		assert.Containsf(t, replicationSourceValue, shard, "Source shard is not matched with vReplication shard value")
	}

	if secondBehindMaster != 0 {
		secondBehindMaserMaxStr := fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationSecondsBehindMasterMax"]))
		secondBehindMaserMax, _ := strconv.ParseFloat(secondBehindMaserMaxStr, 64)

		assert.True(t, secondBehindMaserMax < float64(secondBehindMaster))

		replicationSecondBehindMasterObj := reflect.ValueOf(tabletVars["VReplicationSecondsBehindMaster"])
		for _, key := range replicationSourceObj.MapKeys() {
			str := fmt.Sprintf("%v", replicationSecondBehindMasterObj.MapIndex(key))
			flt, _ := strconv.ParseFloat(str, 64)
			assert.True(t, flt < float64(secondBehindMaster))
		}
	}
}

// checkStreamHealthEqualsBinlogPlayerVars - Checks the variables exported by streaming health check match vars.
func checkStreamHealthEqualsBinlogPlayerVars(t *testing.T, vttablet cluster.Vttablet, count int, ci cluster.LocalProcessCluster) {
	tabletVars := vttablet.VttabletProcess.GetVars()

	streamCountStr := fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationStreamCount"]))
	streamCount, _ := strconv.Atoi(streamCountStr)

	secondBehindMaserMaxStr := fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationSecondsBehindMasterMax"]))
	secondBehindMaserMax, _ := strconv.ParseFloat(secondBehindMaserMaxStr, 64)

	assert.Equal(t, streamCount, count)
	// Enforce health check because it's not running by default as
	// tablets may not be started with it, or may not run it in time.
	_ = ci.VtctlclientProcess.ExecuteCommand("RunHealthCheck", vttablet.Alias)
	streamHealth, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", vttablet.Alias)
	require.Nil(t, err)

	var streamHealthResponse querypb.StreamHealthResponse
	err = json2.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	require.Nil(t, err, "error should be Nil")
	assert.Equal(t, streamHealthResponse.Serving, false)
	assert.NotNil(t, streamHealthResponse.RealtimeStats)
	assert.Equal(t, streamHealthResponse.RealtimeStats.HealthError, "")
	assert.NotNil(t, streamHealthResponse.RealtimeStats.BinlogPlayersCount)

	assert.Equal(t, streamCount, int(streamHealthResponse.RealtimeStats.BinlogPlayersCount))
	assert.Equal(t, secondBehindMaserMax, float64(streamHealthResponse.RealtimeStats.SecondsBehindMasterFilteredReplication))
}

// CheckBinlogServerVars checks the binlog server variables are correctly exported.
func CheckBinlogServerVars(t *testing.T, vttablet cluster.Vttablet, minStatement int, minTxn int, isVerticalSplit bool) {
	resultMap := vttablet.VttabletProcess.GetVars()
	skey := "UpdateStreamKeyRangeStatements"
	tkey := "UpdateStreamKeyRangeTransactions"
	if isVerticalSplit {
		skey = "UpdateStreamTablesStatements"
		tkey = "UpdateStreamTablesTransactions"
	}
	assert.Contains(t, resultMap, skey)
	assert.Contains(t, resultMap, tkey)
	if minStatement > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap[skey]))
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minStatement, fmt.Sprintf("only got %d < %d statements", iValue, minStatement))
	}
	if minTxn > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap[tkey]))
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minTxn, fmt.Sprintf("only got %d < %d transactions", iValue, minTxn))
	}
}

// InsertLots inserts multiple values to vttablet
func InsertLots(t *testing.T, count uint64, vttablet cluster.Vttablet, table string, ks string) {
	var query1, query2 string
	var i uint64
	dbConn := getDBConnFromTablet(t, &vttablet, ks)
	defer dbConn.Close()
	for i = 0; i < count; i++ {
		query1 = fmt.Sprintf(InsertTabletTemplateKsID, table, lotRange1+i, fmt.Sprintf("msg-range1-%d", 10000+i), lotRange1+i)
		query2 = fmt.Sprintf(InsertTabletTemplateKsID, table, lotRange2+i, fmt.Sprintf("msg-range2-%d", 20000+i), lotRange2+i)

		// insert first query
		executeQueryInTransaction(t, query1, dbConn)
		executeQueryInTransaction(t, query2, dbConn)
	}
}

func executeQueryInTransaction(t *testing.T, query string, dbConn *mysql.Conn) {
	dbConn.ExecuteFetch("begin", maxRowsToFetch, true)
	_, err := dbConn.ExecuteFetch(query, maxRowsToFetch, true)
	require.NoError(t, err)
	dbConn.ExecuteFetch("commit", maxRowsToFetch, true)
}

// ExecuteOnTablet executes a write query on specified vttablet
// It should always be called with a master tablet for the keyspace/shard
func ExecuteOnTablet(t *testing.T, query string, vttablet cluster.Vttablet, ks string, expectFail bool) {
	_, _ = vttablet.VttabletProcess.QueryTablet("begin", ks, true)
	_, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
	if expectFail {
		require.Error(t, err)
	} else {
		require.Nil(t, err)
	}
	_, _ = vttablet.VttabletProcess.QueryTablet("commit", ks, true)
}

// InsertMultiValues inserts a multiple values to vttablet
func InsertMultiValues(t *testing.T, tablet cluster.Vttablet, keyspaceName string, tableName string,
	fixedParentID int, ids []int, msgs []string, ksIDs []uint64) {
	queryStr := fmt.Sprintf("insert into %s (parent_id, id, msg, custom_ksid_col) values", tableName)
	valueSQL := ""
	keyspaceIds := ""
	valueIds := ""
	for i := range ids {
		valueSQL += fmt.Sprintf(`(%d, %d, "%s", %d)`, fixedParentID, ids[i], msgs[i], ksIDs[i])
		keyspaceIds += fmt.Sprintf("%d", ksIDs[i])
		valueIds += fmt.Sprintf("%d", ids[i])
		if i < len(ids)-1 {
			valueSQL += ","
			keyspaceIds += ","
			valueIds += ","
		}
	}

	queryStr += valueSQL
	queryStr += fmt.Sprintf(" /* vtgate:: keyspace_id:%s */", keyspaceIds)
	queryStr += fmt.Sprintf(" /* id:%s */", valueIds)
	ExecuteOnTablet(t, queryStr, tablet, keyspaceName, false)
}

// CheckLotsTimeout waits till all values are inserted
func CheckLotsTimeout(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, ks string, keyType querypb.Type, pctFound int) bool {
	timeout := time.Now().Add(10 * time.Second)
	var percentFound float64
	for time.Now().Before(timeout) {
		percentFound = checkLots(t, vttablet, count, table, ks, keyType)
		if int(math.Round(percentFound)) == pctFound {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	log.Infof("expected pct %d, got pct %f", pctFound, percentFound)
	return false
}

func checkLots(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, ks string, keyType querypb.Type) float64 {
	var isFound bool
	var totalFound int
	var i uint64
	dbConn := getDBConnFromTablet(t, &vttablet, ks)
	defer dbConn.Close()

	for i = 0; i < count; i++ {
		isFound = CheckValues(t, vttablet,
			lotRange1+i, fmt.Sprintf("msg-range1-%d", 10000+i), true, table, ks, keyType, dbConn)
		if isFound {
			totalFound++
		}

		isFound = CheckValues(t, vttablet,
			lotRange2+i, fmt.Sprintf("msg-range2-%d", 20000+i), true, table, ks, keyType, dbConn)
		if isFound {
			totalFound++
		}
	}
	log.Infof("Total found %d", totalFound)
	return float64(float64(totalFound) * 100 / float64(count) / 2)
}

// CheckTabletQueryServices check that the query service is enabled or disabled on the specified tablets.
func CheckTabletQueryServices(t *testing.T, vttablets []cluster.Vttablet, expectedStatus string, tabletControlEnabled bool, ci cluster.LocalProcessCluster) {
	for _, tablet := range vttablets {
		CheckTabletQueryService(t, tablet, expectedStatus, tabletControlEnabled, ci)
	}
}

// CheckTabletQueryService check that the query service is enabled or disabled on the tablet
func CheckTabletQueryService(t *testing.T, vttablet cluster.Vttablet, expectedStatus string, tabletControlEnabled bool, ci cluster.LocalProcessCluster) {
	tabletStatus := vttablet.VttabletProcess.GetTabletStatus()
	assert.Equal(t, tabletStatus, expectedStatus)

	queryServiceDisabled := "TabletControl.DisableQueryService set"
	status := vttablet.VttabletProcess.GetStatus()
	if tabletControlEnabled {
		assert.Contains(t, status, queryServiceDisabled)
	} else {
		assert.NotContains(t, status, queryServiceDisabled)
	}

	if vttablet.Type == "rdonly" {
		// Run RunHealthCheck to be sure the tablet doesn't change its serving state.
		_ = ci.VtctlclientProcess.ExecuteCommand("RunHealthCheck", vttablet.Alias)
		tabletStatus = vttablet.VttabletProcess.GetTabletStatus()
		assert.Equal(t, tabletStatus, expectedStatus)
	}
}

// CheckShardQueryServices checks DisableQueryService for all shards
func CheckShardQueryServices(t *testing.T, ci cluster.LocalProcessCluster, shards []cluster.Shard, cell string,
	keyspaceName string, tabletType topodata.TabletType, expectedState bool) {
	for _, shard := range shards {
		CheckShardQueryService(t, ci, cell, keyspaceName, shard.Name, tabletType, expectedState)
	}
}

// CheckShardQueryService checks DisableQueryService in the shard record's TabletControlMap.
func CheckShardQueryService(t *testing.T, ci cluster.LocalProcessCluster, cell string, keyspaceName string,
	shardName string, tabletType topodata.TabletType, expectedState bool) {
	// We assume that query service should be enabled unless
	// DisableQueryService is explicitly True
	queryServiceEnabled := true
	srvKeyspace := GetSrvKeyspace(t, cell, keyspaceName, ci)
	for _, partition := range srvKeyspace.Partitions {
		tType := partition.GetServedType()
		if tabletType != tType {
			continue
		}
		for _, shardTabletControl := range partition.GetShardTabletControls() {
			if shardTabletControl.GetName() == shardName {
				if shardTabletControl.GetQueryServiceDisabled() {
					queryServiceEnabled = false
				}
			}
		}
	}

	assert.True(t, queryServiceEnabled == expectedState,
		fmt.Sprintf("shard %s does not have the correct query service state: got %t but expected %t",
			shardName, queryServiceEnabled, expectedState))

}

// GetShardInfo return the Shard information
func GetShardInfo(t *testing.T, shard1Ks string, ci cluster.LocalProcessCluster) *topodata.Shard {
	output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", shard1Ks)
	require.Nil(t, err)
	var shard topodata.Shard
	err = json2.Unmarshal([]byte(output), &shard)
	require.Nil(t, err)
	return &shard
}

// checkThrottlerServiceMaxRates Checks the vtctl ThrottlerMaxRates and ThrottlerSetRate commands.
func checkThrottlerServiceMaxRates(t *testing.T, server string, names []string, rate int, ci cluster.LocalProcessCluster) {
	// Avoid flakes by waiting for all throttlers. (Necessary because filtered
	// replication on vttablet will register the throttler asynchronously.)
	var output string
	var err error
	startTime := time.Now()
	msg := fmt.Sprintf("%d active throttler(s)", len(names))
	for {
		output, err = ci.VtctlclientProcess.ExecuteCommandWithOutput("ThrottlerMaxRates", "--server", server)
		require.Nil(t, err)
		if strings.Contains(output, msg) || (time.Now().After(startTime.Add(2 * time.Minute))) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	assert.Contains(t, output, msg)

	for _, name := range names {
		str := fmt.Sprintf("| %s | %d |", name, rate)
		assert.Contains(t, output, str)
	}

	// Check that it's possible to change the max rate on the throttler.
	newRate := "unlimited"
	output, err = ci.VtctlclientProcess.ExecuteCommandWithOutput("ThrottlerSetMaxRate", "--server", server, newRate)
	require.Nil(t, err)
	assert.Contains(t, output, msg)

	output, err = ci.VtctlclientProcess.ExecuteCommandWithOutput("ThrottlerMaxRates", "--server", server)
	require.Nil(t, err)
	for _, name := range names {
		str := fmt.Sprintf("| %s | %s |", name, newRate)
		assert.Contains(t, output, str)
	}
	assert.Contains(t, output, msg)
}

// checkThrottlerServiceConfiguration checks the vtctl (Get|Update|Reset)ThrottlerConfiguration commands.
func checkThrottlerServiceConfiguration(t *testing.T, server string, names []string, ci cluster.LocalProcessCluster) {
	output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput(
		"UpdateThrottlerConfiguration", "--server", server,
		"--copy_zero_values",
		"target_replication_lag_sec:12345 "+
			"max_replication_lag_sec:65789 "+
			"initial_rate:3 max_increase:0.4 "+
			"emergency_decrease:0.5 "+
			"min_duration_between_increases_sec:6 "+
			"max_duration_between_increases_sec:7 "+
			"min_duration_between_decreases_sec:8 "+
			"spread_backlog_across_sec:9 "+
			"ignore_n_slowest_replicas:0 "+
			"ignore_n_slowest_rdonlys:0 "+
			"age_bad_rate_after_sec:12 "+
			"bad_rate_increase:0.13 "+
			"max_rate_approach_threshold: 0.9 ",
	)
	require.Nil(t, err)
	msg := fmt.Sprintf("%d active throttler(s)", len(names))
	assert.Contains(t, output, msg)

	output, err = ci.VtctlclientProcess.ExecuteCommandWithOutput("GetThrottlerConfiguration", "--server", server)
	require.Nil(t, err)
	for _, name := range names {
		str := fmt.Sprintf("| %s | target_replication_lag_sec:12345 ", name)
		assert.Contains(t, output, str)
		assert.NotContains(t, output, "ignore_n_slowest_replicas")
	}
	assert.Contains(t, output, msg)

	// Reset clears our configuration values.
	output, err = ci.VtctlclientProcess.ExecuteCommandWithOutput("ResetThrottlerConfiguration", "--server", server)
	require.Nil(t, err)
	assert.Contains(t, output, msg)

	// Check that the reset configuration no longer has our values.
	output, err = ci.VtctlclientProcess.ExecuteCommandWithOutput("GetThrottlerConfiguration", "--server", server)
	require.Nil(t, err)
	assert.NotContains(t, output, "target_replication_lag_sec:12345")
	assert.Contains(t, output, msg)

}

// CheckThrottlerService runs checkThrottlerServiceMaxRates and checkThrottlerServiceConfigs
func CheckThrottlerService(t *testing.T, server string, names []string, rate int, ci cluster.LocalProcessCluster) {
	checkThrottlerServiceMaxRates(t, server, names, rate, ci)
	checkThrottlerServiceConfiguration(t, server, names, ci)
}

func getDBConnFromTablet(t *testing.T, vttablet *cluster.Vttablet, ks string) *mysql.Conn {
	dbParams := cluster.NewConnParams(vttablet.VttabletProcess.DbPort, vttablet.VttabletProcess.DbPassword, path.Join(vttablet.VttabletProcess.Directory, "mysql.sock"), ks)
	dbConn, err := mysql.Connect(context.Background(), &dbParams)
	require.NoError(t, err)
	return dbConn
}
