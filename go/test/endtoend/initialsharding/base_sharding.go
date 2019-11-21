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

package initialsharding

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

var (
	lotRange1                uint64 = 0xA000000000000000
	lotRange2                uint64 = 0xE000000000000000
	insertTabletTemplateKsID        = `insert into %s (parent_id, id, msg, custom_ksid_col) values (%d, %d, '%s', 0x%x) /* vtgate:: keyspace_id:%016X */ /* id:%d */`
)

func checkSrvKeyspace(t *testing.T, cell string, ksname string, shardingCol string, colType topodata.KeyspaceIdType, expectedPartition map[topodata.TabletType][]string, ci cluster.LocalProcessCluster) {
	srvKeyspace := getSrvKeyspace(t, cell, ksname, ci)
	// TODO: when we define sharding column v3 way, it is not reflected here
	//assert.Equal(t, srvKeyspace.ShardingColumnName, shardingCol)
	//assert.Equal(t, srvKeyspace.ShardingColumnType, colType)

	currentPartition := map[topodata.TabletType][]string{}

	for _, partition := range srvKeyspace.Partitions {
		currentPartition[partition.ServedType] = []string{}
		for _, shardRef := range partition.ShardReferences {
			currentPartition[partition.ServedType] = append(currentPartition[partition.ServedType], shardRef.Name)
		}
	}

	assert.True(t, reflect.DeepEqual(currentPartition, expectedPartition))
}

func getSrvKeyspace(t *testing.T, cell string, ksname string, ci cluster.LocalProcessCluster) *topodata.SrvKeyspace {
	output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspace", cell, ksname)
	assert.Nil(t, err)
	var srvKeyspace topodata.SrvKeyspace

	err = json.Unmarshal([]byte(output), &srvKeyspace)
	assert.Nil(t, err)
	return &srvKeyspace
}

func verifyReconciliationCounters(t *testing.T, vtworkerURL string, availabilityType string, table string,
	inserts int, updates int, deletes int, equals int) {
	resp, err := http.Get(vtworkerURL)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, 200)

	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(respByte, &resultMap)
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

// checkValues check value from sql query to table with expected values
func checkValues(t *testing.T, vttablet cluster.Vttablet, values []string, id uint64, exists bool, tableName string, parentID int, ks string) bool {
	query := fmt.Sprintf("select parent_id, id, msg, custom_ksid_col from %s where parent_id = %d and id = %d", tableName, parentID, id)
	result, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
	assert.Nil(t, err)
	isFound := false
	if exists && len(result.Rows) > 0 {
		isFound = assert.Equal(t, result.Rows[0][0].String(), values[0])
		isFound = isFound && assert.Equal(t, result.Rows[0][1].String(), values[1])
		isFound = isFound && assert.Equal(t, result.Rows[0][2].String(), values[2])
		isFound = isFound && assert.Equal(t, result.Rows[0][3].String(), values[3])
	} else {
		assert.Equal(t, len(result.Rows), 0)
	}
	return isFound
}

func checkDestinationMaster(t *testing.T, vttablet cluster.Vttablet, sourceShards []string, ci cluster.LocalProcessCluster) {
	_ = vttablet.VttabletProcess.WaitForBinLogPlayerCount(len(sourceShards))
	checkBinlogPlayerVars(t, vttablet, sourceShards, 0)
	checkStreamHealthEqualsBinlogPlayerVars(t, vttablet, len(sourceShards), ci)
}

// checkBinlogPlayerVars Checks the binlog player variables are correctly exported.
func checkBinlogPlayerVars(t *testing.T, vttablet cluster.Vttablet, sourceShards []string, secondBehindMaster int64) {
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
	err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	assert.Nil(t, err, "error should be Nil")
	assert.Equal(t, streamHealthResponse.Serving, false)
	assert.NotNil(t, streamHealthResponse.RealtimeStats)
	assert.Equal(t, streamHealthResponse.RealtimeStats.HealthError, "")
	assert.NotNil(t, streamHealthResponse.RealtimeStats.BinlogPlayersCount)

	assert.Equal(t, streamCount, int(streamHealthResponse.RealtimeStats.BinlogPlayersCount))
	assert.Equal(t, secondBehindMaserMax, float64(streamHealthResponse.RealtimeStats.SecondsBehindMasterFilteredReplication))
}

func checkBinlogServerVars(t *testing.T, vttablet cluster.Vttablet, minStatement int, minTxn int) {
	resultMap := vttablet.VttabletProcess.GetVars()
	assert.Contains(t, resultMap, "UpdateStreamKeyRangeStatements")
	assert.Contains(t, resultMap, "UpdateStreamKeyRangeTransactions")
	if minStatement > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap["UpdateStreamKeyRangeStatements"]))
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minStatement)
	}

	if minTxn > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap["UpdateStreamKeyRangeStatements"]))
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minTxn)
	}
}

func insertLots(count uint64, vttablet cluster.Vttablet, table string, parentID int, ks string) {
	var query1, query2 string
	var i uint64
	for i = 0; i < count; i++ {
		query1 = fmt.Sprintf(insertTabletTemplateKsID, table, parentID, 10000+i, fmt.Sprintf("msg-range1-%d", 10000+i), lotRange1+i, lotRange1+i, 10000+i)
		query2 = fmt.Sprintf(insertTabletTemplateKsID, table, parentID, 20000+i, fmt.Sprintf("msg-range2-%d", 20000+i), lotRange2+i, lotRange2+i, 20000+i)

		insertToTablet(query1, vttablet, ks)
		insertToTablet(query2, vttablet, ks)
	}
}

func insertToTablet(query string, vttablet cluster.Vttablet, ks string) {
	_, _ = vttablet.VttabletProcess.QueryTablet("begin", ks, true)
	_, _ = vttablet.VttabletProcess.QueryTablet(query, ks, true)
	_, _ = vttablet.VttabletProcess.QueryTablet("commit", ks, true)
}

func checkLotsTimeout(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, parentID int, ks string) bool {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		percentFound := checkLots(t, vttablet, count, table, parentID, ks)
		if percentFound == 100 {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func checkLotsNotPresent(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, parentID int, ks string) {
	var i uint64
	for i = 0; i < count; i++ {
		assert.False(t, checkValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+i),
			fmt.Sprintf("UINT64(%d)", lotRange1+i)},
			10000+i, true, table, parentID, ks))

		assert.False(t, checkValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+i),
			fmt.Sprintf("UINT64(%d)", lotRange2+i)},
			20000+i, true, table, parentID, ks))
	}
}

func checkLots(t *testing.T, vttablet cluster.Vttablet, count uint64, table string, parentID int, ks string) float32 {
	var isFound bool
	var totalFound int
	var i uint64
	for i = 0; i < count; i++ {
		// "INT64(1)" `VARCHAR("msg1")`,
		isFound = checkValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+i),
			fmt.Sprintf("UINT64(%d)", lotRange1+i)},
			10000+i, true, table, parentID, ks)
		if isFound {
			totalFound++
		}

		isFound = checkValues(t, vttablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+i),
			fmt.Sprintf("UINT64(%d)", lotRange2+i)},
			20000+i, true, table, parentID, ks)
		if isFound {
			totalFound++
		}
	}
	return float32(totalFound * 100 / int(count) / 2)
}

func checkRunningBinlogPlayer(t *testing.T, vttablet cluster.Vttablet, numberOfQueries int, numberOfTxns int) {
	status := vttablet.VttabletProcess.GetStatus()
	assert.Contains(t, status, "VReplication state: Open")
	assert.Contains(t, status, fmt.Sprintf("<td><b>All</b>: %d<br><b>Query</b>: %d<br><b>Transaction</b>: %d<br></td>", numberOfQueries+numberOfTxns, numberOfQueries, numberOfTxns))
	assert.Contains(t, status, "</html>")
}

func checkTabletQueryServices(t *testing.T, vttablets []cluster.Vttablet, expectedStatus string, tabletControlEnabled bool, ci cluster.LocalProcessCluster) {
	for _, tablet := range vttablets {
		checkTabletQueryService(t, tablet, expectedStatus, tabletControlEnabled, ci)
	}
}

func checkTabletQueryService(t *testing.T, vttablet cluster.Vttablet, expectedStatus string, tabletControlEnabled bool, ci cluster.LocalProcessCluster) {
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

func hexToDbStr(number uint64) string {
	return fmt.Sprintf("UINT64(%d)", number)
}
