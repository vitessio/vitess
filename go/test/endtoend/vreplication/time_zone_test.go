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

package vreplication

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestMoveTablesTZ(t *testing.T) {
	allCellNames = "zone1"
	defaultCellName := "zone1"
	vc = NewVitessCluster(t, "TestCellAliasVreplicationWorkflow", []string{"zone1"}, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCell = vc.Cells[defaultCellName]
	cells := []*Cell{defaultCell}

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, "product", "0", initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	// Add cell alias containing only zone2
	result, err := vc.VtctlClient.ExecuteCommandWithOutput("AddCellsAlias", "--", "--cells", "zone2", "alias")
	require.NoError(t, err, "command failed with output: %v", result)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	timeZoneSQL, _ := os.ReadFile("tz.sql")
	_, err = productTab.QueryTabletWithDB(string(timeZoneSQL), "mysql")
	require.NoError(t, err)
	time.Sleep(5 * time.Second) // todo: replace sleep with actual check
	_, err = productTab.QueryTablet("SET GLOBAL time_zone = 'US/Pacific';", "", false)
	require.NoError(t, err)

	insertInitialData(t)

	workflow := "tz"
	sourceKs := "product"
	targetKs := "customer"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	if _, err := vc.AddKeyspace(t, cells, "customer", "0", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200, targetKsOpts); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "0"), 1); err != nil {
		t.Fatal(err)
	}

	defaultCell := vc.Cells["zone1"]
	custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
	customerTab := custKs.Shards["0"].Tablets["zone1-200"].Vttablet

	customerTab.QueryTabletWithDB(string(timeZoneSQL), "mysql")
	require.NoError(t, err)
	time.Sleep(5 * time.Second) // todo: replace sleep with actual check
	_, err = customerTab.QueryTablet("SET GLOBAL time_zone = 'UTC';", "", false)
	require.NoError(t, err)

	tables := "datze"

	output, err := vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--", "--source", sourceKs, "--tables", tables, "--source_time_zone", "US/Pacific", "Create", ksWorkflow)
	require.NoError(t, err, output)

	catchup(t, customerTab, workflow, "MoveTables")

	time.Sleep(2 * time.Second)
	_, err = vtgateConn.ExecuteFetch("insert into datze(id, dt2) values (11, '2022-01-01 10:20:30')", 1, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch("insert into datze(id, dt2) values (12, '2022-04-01 5:06:07')", 1, false)
	require.NoError(t, err)

	vdiff(t, ksWorkflow, "")

	query := "select * from datze"
	qrSourceUSPacific, err := productTab.QueryTablet(query, "product", true)
	require.NoError(t, err)
	require.NotNil(t, qrSourceUSPacific)

	qrTargetUTC, err := customerTab.QueryTablet(query, "customer", true)
	require.NoError(t, err)
	require.NotNil(t, qrTargetUTC)

	require.Equal(t, len(qrSourceUSPacific.Rows), len(qrTargetUTC.Rows))

	pst, err := time.LoadLocation("US/Pacific")
	require.NoError(t, err)

	// for reference the columns in the test are as follows:
	//  * dt1 datetime default current_timestamp, constant for all rows
	//  * dt2 datetime, different values. First row is in standard time, rest with daylight savings including times around the time zone switch
	//  * ts1 timestamp default current_timestamp, constant for all rows
	for i, row := range qrSourceUSPacific.Named().Rows {
		// source and UTC results must differ since source is in US/Pacific
		require.NotEqual(t, row.AsString("dt1", ""), qrTargetUTC.Named().Rows[i].AsString("dt1", ""))
		require.NotEqual(t, row.AsString("dt2", ""), qrTargetUTC.Named().Rows[i].AsString("dt2", ""))
		require.NotEqual(t, row.AsString("ts1", ""), qrTargetUTC.Named().Rows[i].AsString("ts1", ""))

		// now compare times b/w source and target (actual). VDiff has already compared, but we want to validate that vdiff is right too!
		dt2a, err := time.Parse("2006-01-02 15:04:05", qrTargetUTC.Named().Rows[i].AsString("dt2", ""))
		require.NoError(t, err)
		targetUTCTUnix := dt2a.Unix()

		dt2b, err := time.Parse("2006-01-02 15:04:05", qrSourceUSPacific.Named().Rows[i].AsString("dt2", ""))
		require.NoError(t, err)
		sourceUSPacific := dt2b.Unix()

		dtt := dt2b.In(pst)
		zone, _ := dtt.Zone()
		var hoursBehind int64
		if zone == "PDT" { // daylight savings is on
			hoursBehind = 7
		} else {
			hoursBehind = 8
		}
		// extra logging, so that we can spot any issues in CI test runs
		log.Infof("times are %s, %s, hours behind %d", dt2a, dt2b, hoursBehind)
		require.Equal(t, int64(hoursBehind*3600), targetUTCTUnix-sourceUSPacific)
	}

	// user should be either running this query or have set their location in their driver to map from the time in Vitess/UTC to local
	query = "select id, convert_tz(dt1, 'UTC', 'US/Pacific') dt1, convert_tz(dt2, 'UTC', 'US/Pacific') dt2, convert_tz(ts1, 'UTC', 'US/Pacific') ts1 from datze"
	qrTargetUSPacific, err := customerTab.QueryTablet(query, "customer", true)
	require.NoError(t, err)
	require.NotNil(t, qrTargetUSPacific)
	require.Equal(t, len(qrSourceUSPacific.Rows), len(qrTargetUSPacific.Rows))

	for i, row := range qrSourceUSPacific.Named().Rows {
		// source and target results must match since source is in US/Pacific and we are converting target columns explicitly to US/Pacific
		require.Equal(t, row.AsString("dt1", ""), qrTargetUSPacific.Named().Rows[i].AsString("dt1", ""))
		require.Equal(t, row.AsString("dt2", ""), qrTargetUSPacific.Named().Rows[i].AsString("dt2", ""))
		require.Equal(t, row.AsString("ts1", ""), qrTargetUSPacific.Named().Rows[i].AsString("ts1", ""))
	}
}
