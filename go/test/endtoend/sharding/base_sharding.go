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
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
)

var (
	lotRange1 uint64 = 0xA000000000000000
	lotRange2 uint64 = 0xE000000000000000
	// InsertTabletTemplateKsID common insert format to be used for different tests
	InsertTabletTemplateKsID = `insert into %s (parent_id, id, msg, custom_ksid_col) values (%d, %d, '%s', 0x%x) /* vtgate:: keyspace_id:%016X */ /* id:%d */`
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
	assert.Nil(t, err)
	var srvKeyspace topodata.SrvKeyspace

	err = json2.Unmarshal([]byte(output), &srvKeyspace)
	assert.Nil(t, err)
	return &srvKeyspace
}

// VerifyTabletHealth checks that the tablet URL is reachable.
func VerifyTabletHealth(t *testing.T, vttablet cluster.Vttablet, hostname string) {
	tabletUrl := fmt.Sprintf("http://%s:%d/healthz", hostname, vttablet.HTTPPort)
	resp, err := http.Get(tabletUrl)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, 200)
}

// VerifyReconciliationCounters checks that the reconciliation Counters have the expected values.
func VerifyReconciliationCounters(t *testing.T, vtworkerURL string, availabilityType string, table string,
	inserts int, updates int, deletes int, equals int) {
	resp, err := http.Get(vtworkerURL)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, 200)

	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err = json2.Unmarshal(respByte, &resultMap)
	assert.Nil(t, err)

	value := getValueFromJSON(resultMap, "Worker"+availabilityType+"InsertsCounters", table)
	if inserts == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", inserts))
	}

	value = getValueFromJSON(resultMap, "Worker"+availabilityType+"UpdatesCounters", table)
	if updates == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", updates))
	}

	value = getValueFromJSON(resultMap, "Worker"+availabilityType+"DeletesCounters", table)
	if deletes == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", deletes))
	}

	value = getValueFromJSON(resultMap, "Worker"+availabilityType+"EqualRowsCounters", table)
	if equals == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", equals))
	}
}

func getValueFromJSON(jsonMap map[string]interface{}, keyname string, tableName string) string {
	object := reflect.ValueOf(jsonMap[keyname])
	if object.Kind() == reflect.Map {
		for _, key := range object.MapKeys() {
			if key.String() == tableName {
				return fmt.Sprintf("%v", object.MapIndex(key))
			}
		}
	}
	return ""
}

// CheckValues check value from sql query to table with expected values
func CheckValues(t *testing.T, vttablet cluster.Vttablet, values []string, id uint64, exists bool, tableName string, parentID int, ks string, keyType topodata.KeyspaceIdType) bool {
	query := fmt.Sprintf("select parent_id, id, msg, custom_ksid_col from %s where parent_id = %d and id = %d", tableName, parentID, id)
	result, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
	assert.Nil(t, err)
	isFound := false
	if exists && len(result.Rows) > 0 {
		isFound = assert.Equal(t, result.Rows[0][0].String(), values[0])
		isFound = isFound && assert.Equal(t, result.Rows[0][1].String(), values[1])
		isFound = isFound && assert.Equal(t, result.Rows[0][2].String(), values[2])
		if keyType == topodata.KeyspaceIdType_BYTES {
			byteResult := result.Rows[0][3].ToBytes()
			isFound = isFound && assert.Equal(t, fmt.Sprintf("%016x", binary.BigEndian.Uint64(byteResult[:])), values[3])
		} else {
			isFound = isFound && assert.Equal(t, result.Rows[0][3].String(), values[3])
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

	assert.Equal(t,
		fmt.Sprintf("%v", replicationSourceObj.MapKeys()),
		fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationSourceTablet"]).MapKeys()))

	for _, key := range replicationSourceObj.MapKeys() {
		replicationSourceValue = append(replicationSourceValue,
			fmt.Sprintf("%v", replicationSourceObj.MapIndex(key)))
	}

	assert.True(t, reflect.DeepEqual(replicationSourceValue, sourceShards))

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
	assert.Nil(t, err)

	var streamHealthResponse querypb.StreamHealthResponse
	err = json2.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	assert.Nil(t, err, "error should be Nil")
	assert.Equal(t, streamHealthResponse.Serving, false)
	assert.NotNil(t, streamHealthResponse.RealtimeStats)
	assert.Equal(t, streamHealthResponse.RealtimeStats.HealthError, "")
	assert.NotNil(t, streamHealthResponse.RealtimeStats.BinlogPlayersCount)

	assert.Equal(t, streamCount, int(streamHealthResponse.RealtimeStats.BinlogPlayersCount))
	assert.Equal(t, secondBehindMaserMax, float64(streamHealthResponse.RealtimeStats.SecondsBehindMasterFilteredReplication))
}

// CheckBinlogServerVars checks the binlog server variables are correctly exported.
func CheckBinlogServerVars(t *testing.T, vttablet cluster.Vttablet, minStatement int, minTxn int) {
	resultMap := vttablet.VttabletProcess.GetVars()
	assert.Contains(t, resultMap, "UpdateStreamKeyRangeStatements")
	assert.Contains(t, resultMap, "UpdateStreamKeyRangeTransactions")
	if minStatement > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap["UpdateStreamKeyRangeStatements"]))
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minStatement, fmt.Sprintf("only got %d < %d statements", iValue, minStatement))
	}

	if minTxn > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap["UpdateStreamKeyRangeStatements"]))
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minTxn, fmt.Sprintf("only got %d < %d transactions", iValue, minTxn))
	}
}

// InsertLots inserts multiple values to vttablet
func InsertLots(count uint64, base uint64, vttablet cluster.Vttablet, table string, parentID int, ks string) {
	ctx := context.Background()
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.VttabletProcess.Directory, "mysql.sock"),
		DbName:     "vt_" + ks,
	}
	dbConn, _ := mysql.Connect(ctx, &dbParams)
	defer dbConn.Close()

	var query1, query2 string
	var i uint64
	for i = 0; i < count; i++ {
		query1 = fmt.Sprintf(InsertTabletTemplateKsID, table, parentID, 10000+base+i,
			fmt.Sprintf("msg-range1-%d", 10000+base+i), lotRange1+base+i, lotRange1+base+i, 10000+base+i)
		query2 = fmt.Sprintf(InsertTabletTemplateKsID, table, parentID, 20000+base+i,
			fmt.Sprintf("msg-range2-%d", 20000+base+i), lotRange2+base+i, lotRange2+base+i, 20000+base+i)

		InsertToTabletUsingSameConn(query1, vttablet, ks, dbConn)
		InsertToTabletUsingSameConn(query2, vttablet, ks, dbConn)
	}
}

// InsertToTablet inserts a single row to vttablet
func InsertToTablet(query string, vttablet cluster.Vttablet, ks string) {
	_, _ = vttablet.VttabletProcess.QueryTablet("begin", ks, true)
	_, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
	_, _ = vttablet.VttabletProcess.QueryTablet("commit", ks, true)
	if err != nil {
		fmt.Println(err)
	}
}

// InsertToTabletUsingSameConn inserts a single row to vttablet using existing connection
func InsertToTabletUsingSameConn(query string, vttablet cluster.Vttablet, ks string, dbConn *mysql.Conn) {
	_, err := dbConn.ExecuteFetch(query, 1000, true)
	if err != nil {
		fmt.Println(err)
	}
}

// InsertMultiValueToTablet inserts a multiple values to vttablet
func InsertMultiValueToTablet(tablet cluster.Vttablet, keyspaceName string, tableName string,
	fixedParentID int, ids []int, msgs []string, ksIDs []uint64) {
	queryStr := fmt.Sprintf("insert into %s (parent_id, id, msg, custom_ksid_col) values", tableName)
	valueSQL := ""
	keyspaceIds := ""
	valueIds := ""
	for i := range ids {
		valueSQL += fmt.Sprintf(`(%d, %d, "%s", 0x%x)`, fixedParentID, ids[i], msgs[i], ksIDs[i])
		keyspaceIds += fmt.Sprintf("%016X", ksIDs[i])
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
	InsertToTablet(queryStr, tablet, keyspaceName)
}

// CheckLotsTimeout waits till all values are inserted
func CheckLotsTimeout(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, parentID int, ks string, keyType topodata.KeyspaceIdType) bool {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		percentFound := checkLots(t, vttablet, count, table, parentID, ks, keyType)
		if percentFound == 100 {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

// CheckLotsNotPresent verifies that no rows should be present in vttablet
func CheckLotsNotPresent(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, parentID int, ks string, keyType topodata.KeyspaceIdType) {
	var i uint64
	for i = 0; i < count; i++ {
		assert.False(t, CheckValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+i),
			HexToDbStr(lotRange1+i, keyType)},
			10000+i, true, table, parentID, ks, keyType))

		assert.False(t, CheckValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+i),
			HexToDbStr(lotRange2+i, keyType)},
			20000+i, true, table, parentID, ks, keyType))
	}
}

func checkLots(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, parentID int, ks string, keyType topodata.KeyspaceIdType) float32 {
	var isFound bool
	var totalFound int
	var i uint64

	for i = 0; i < count; i++ {
		// "INT64(1)" `VARCHAR("msg1")`,
		isFound = CheckValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+i),
			HexToDbStr(lotRange1+i, keyType)},
			10000+i, true, table, parentID, ks, keyType)
		if isFound {
			totalFound++
		}

		isFound = CheckValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+i),
			HexToDbStr(lotRange2+i, keyType)},
			20000+i, true, table, parentID, ks, keyType)
		if isFound {
			totalFound++
		}
	}
	return float32(totalFound * 100 / int(count) / 2)
}

// CheckRunningBinlogPlayer Checks binlog player is running and showing in status
func CheckRunningBinlogPlayer(t *testing.T, vttablet cluster.Vttablet, numberOfQueries int, numberOfTxns int) {
	status := vttablet.VttabletProcess.GetStatus()
	assert.Contains(t, status, "VReplication state: Open")
	assert.Contains(t, status, fmt.Sprintf("<td><b>All</b>: %d<br><b>Query</b>: %d<br><b>Transaction</b>: %d<br></td>", numberOfQueries+numberOfTxns, numberOfQueries, numberOfTxns))
	assert.Contains(t, status, "</html>")
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

	queryServiceDisabled := "Query Service disabled: TabletControl.DisableQueryService set"
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

// CheckShardQueryServices checks DisableQueryService in the shard record's TabletControlMap.
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

// HexToDbStr converts number to comparable string we got after querying to database.
func HexToDbStr(number uint64, keyType topodata.KeyspaceIdType) string {
	fmtValue := "UINT64(%d)"
	if keyType == topodata.KeyspaceIdType_BYTES {
		fmtValue = "%016x"
	}
	return fmt.Sprintf(fmtValue, number)
}

func GetShardInfo(t *testing.T, shard1Ks string, ci cluster.LocalProcessCluster) *topodata.Shard {
	output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", shard1Ks)
	assert.Nil(t, err)
	var shard topodata.Shard
	err = json2.Unmarshal([]byte(output), &shard)
	assert.Nil(t, err)
	return &shard
}

// checkThrottlerServiceMaxRates Checks the vtctl ThrottlerMaxRates and ThrottlerSetRate commands.
func checkThrottlerServiceMaxRates(t *testing.T, server string, ci cluster.LocalProcessCluster) {
	// Avoid flakes by waiting for all throttlers. (Necessary because filtered
	// replication on vttablet will register the throttler asynchronously.)
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("ThrottlerMaxRates", "--server", server)
		assert.Nil(t, err)
		fmt.Println(output)
		// TODO: To be complete
	}
	//return false
}

func CheckThrottlerService(t *testing.T, server string, names []string, rate int, ci cluster.LocalProcessCluster) {
	checkThrottlerServiceMaxRates(t, server, ci)
}
