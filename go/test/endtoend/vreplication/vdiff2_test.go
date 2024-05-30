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
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

type testCase struct {
	name, typ, sourceKs, targetKs string
	sourceShards, targetShards    string
	tables                        string
	workflow                      string
	tabletBaseID                  int
	autoRetryError                bool // if true, test auto retry on error against this workflow
	// If testing auto retry on error, what new rows should be diff'd. These rows must have a PK > all initial rows.
	retryInsert string
	resume      bool // test resume functionality with this workflow
	// If testing resume, what new rows should be diff'd. These rows must have a PK > all initial rows and retry rows.
	resumeInsert        string
	stop                bool // test stop functionality with this workflow
	testCLIErrors       bool // test CLI errors against this workflow (only needs to be done once)
	testCLICreateWait   bool // test CLI create and wait until done against this workflow (only needs to be done once)
	testCLIFlagHandling bool // test vtctldclient flag handling from end-to-end
	extraVDiffFlags     map[string]string
	vdiffCount          int64 // Keep track of the number of vdiffs created to test the stats
}

const (
	sqlSimulateError = `update %s.vdiff as vd, %s.vdiff_table as vdt set vd.state = 'error', vdt.state = 'error', vd.completed_at = NULL,
						vd.last_error = 'vttablet: rpc error: code = Unknown desc = (errno 1213) (sqlstate 40001): Deadlock found when trying to get lock; try restarting transaction'
						where vd.vdiff_uuid = %s and vd.id = vdt.vdiff_id`
	sqlAnalyzeTable = `analyze table %s`
)

var testCases = []*testCase{
	{
		name:                "MoveTables/unsharded to two shards",
		workflow:            "p1c2",
		typ:                 "MoveTables",
		sourceKs:            "product",
		targetKs:            "customer",
		sourceShards:        "0",
		targetShards:        "-80,80-",
		tabletBaseID:        200,
		tables:              "customer,Lead,Lead-1,nopk",
		autoRetryError:      true,
		retryInsert:         `insert into customer(cid, name, typ) values(2005149100, 'Testy McTester', 'soho')`,
		resume:              true,
		resumeInsert:        `insert into customer(cid, name, typ) values(2005149200, 'Testy McTester (redux)', 'enterprise')`,
		testCLIErrors:       true, // test for errors in the simplest workflow
		testCLICreateWait:   true, // test wait on create feature against simplest workflow
		testCLIFlagHandling: true, // test flag handling end-to-end against simplest workflow
		extraVDiffFlags: map[string]string{
			"--max-diff-duration": "2s",
		},
	},
	{
		name:           "Reshard Merge/split 2 to 3",
		workflow:       "c2c3",
		typ:            "Reshard",
		sourceKs:       "customer",
		targetKs:       "customer",
		sourceShards:   "-80,80-",
		targetShards:   "-40,40-a0,a0-",
		tabletBaseID:   400,
		autoRetryError: true,
		retryInsert:    `insert into customer(cid, name, typ) values(2005149300, 'Testy McTester Jr', 'enterprise'), (2005149350, 'Testy McTester II', 'enterprise')`,
		resume:         true,
		resumeInsert:   `insert into customer(cid, name, typ) values(2005149400, 'Testy McTester III', 'enterprise')`,
		stop:           true,
	},
	{
		name:           "Reshard/merge 3 to 1",
		workflow:       "c3c1",
		typ:            "Reshard",
		sourceKs:       "customer",
		targetKs:       "customer",
		sourceShards:   "-40,40-a0,a0-",
		targetShards:   "0",
		tabletBaseID:   700,
		autoRetryError: true,
		retryInsert:    `insert into customer(cid, name, typ) values(2005149500, 'Testy McTester IV', 'enterprise')`,
		resume:         true,
		resumeInsert:   `insert into customer(cid, name, typ) values(2005149600, 'Testy McTester V', 'enterprise'), (2005149650, 'Testy McTester VI', 'enterprise')`,
		stop:           true,
	},
}

func checkVDiffCountStat(t *testing.T, tablet *cluster.VttabletProcess, expectedCount int64) {
	countStr, err := getDebugVar(t, tablet.Port, []string{"VDiffCount"})
	require.NoError(t, err, "failed to get VDiffCount stat from %s-%d tablet: %v", tablet.Cell, tablet.TabletUID, err)
	count, err := strconv.Atoi(countStr)
	require.NoError(t, err, "failed to convert VDiffCount stat string to int: %v", err)
	require.Equal(t, expectedCount, int64(count), "expected VDiffCount stat to be %d but got %d", expectedCount, count)
}

func TestVDiff2(t *testing.T) {
	cellNames := "zone5,zone1,zone2,zone3,zone4"
	sourceKs := "product"
	sourceShards := []string{"0"}
	targetKs := "customer"
	targetShards := []string{"-80", "80-"}
	extraVTTabletArgs = []string{
		// This forces us to use multiple vstream packets even with small test tables.
		"--vstream_packet_size=1",
		// Test VPlayer batching mode.
		fmt.Sprintf("--vreplication_experimental_flags=%d",
			vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching),
	}

	vc = NewVitessCluster(t, &clusterOptions{cells: strings.Split(cellNames, ",")})
	defer vc.TearDown()

	zone1 := vc.Cells["zone1"]
	zone2 := vc.Cells["zone2"]
	zone3 := vc.Cells["zone3"]

	// The primary tablet is only added in the first cell.
	// We ONLY add primary tablets in this test.
	_, err := vc.AddKeyspace(t, []*Cell{zone2, zone1, zone3}, sourceKs, strings.Join(sourceShards, ","), initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)
	require.NoError(t, err)

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	insertInitialData(t)
	// Insert null and empty enum values for testing vdiff comparisons for those values.
	// If we add this to the initial data list, the counts in several other tests will need to change
	query := `insert into customer(cid, name, typ, sport) values(1001, null, 'soho','')`
	execVtgateQuery(t, vtgateConn, fmt.Sprintf("%s:%s", sourceKs, sourceShards[0]), query)

	generateMoreCustomers(t, sourceKs, 1000)

	// Create rows in the nopk table using the customer names and random ages between 20 and 100.
	query = "insert into nopk(name, age) select name, floor(rand()*80)+20 from customer"
	execVtgateQuery(t, vtgateConn, fmt.Sprintf("%s:%s", sourceKs, sourceShards[0]), query)

	// The primary tablet is only added in the first cell.
	// We ONLY add primary tablets in this test.
	tks, err := vc.AddKeyspace(t, []*Cell{zone3, zone1, zone2}, targetKs, strings.Join(targetShards, ","), customerVSchema, customerSchema, 0, 0, 200, targetKsOpts)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Primary tablets for any new shards are added in the first cell.
			testWorkflow(t, vc, tc, tks, []*Cell{zone3, zone2, zone1})
		})
	}

	statsTablet := vc.getPrimaryTablet(t, targetKs, targetShards[0])

	// We diffed X rows so confirm that the global total is > 0.
	countStr, err := getDebugVar(t, statsTablet.Port, []string{"VDiffRowsComparedTotal"})
	require.NoError(t, err, "failed to get VDiffRowsComparedTotal stat from %s-%d tablet: %v", statsTablet.Cell, statsTablet.TabletUID, err)
	count, err := strconv.Atoi(countStr)
	require.NoError(t, err, "failed to convert VDiffRowsComparedTotal stat string to int: %v", err)
	require.Greater(t, count, 0, "expected VDiffRowsComparedTotal stat to be greater than 0 but got %d", count)

	// The VDiffs should all be cleaned up so the VDiffRowsCompared value, which
	// is produced from controller info, should be empty.
	vdrc, err := getDebugVar(t, statsTablet.Port, []string{"VDiffRowsCompared"})
	require.NoError(t, err, "failed to get VDiffRowsCompared stat from %s-%d tablet: %v", statsTablet.Cell, statsTablet.TabletUID, err)
	require.Equal(t, "{}", vdrc, "expected VDiffRowsCompared stat to be empty but got %s", vdrc)
}

func testWorkflow(t *testing.T, vc *VitessCluster, tc *testCase, tks *Keyspace, cells []*Cell) {
	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()
	arrTargetShards := strings.Split(tc.targetShards, ",")
	if tc.typ == "Reshard" {
		require.NoError(t, vc.AddShards(t, cells, tks, tc.targetShards, 0, 0, tc.tabletBaseID, targetKsOpts))

	}
	ksWorkflow := fmt.Sprintf("%s.%s", tc.targetKs, tc.workflow)
	statsShard := arrTargetShards[0]
	statsTablet := vc.getPrimaryTablet(t, tc.targetKs, statsShard)
	var args []string
	args = append(args, tc.typ, "--")
	args = append(args, "--source", tc.sourceKs)
	if tc.typ == "Reshard" {
		args = append(args, "--source_shards", tc.sourceShards, "--target_shards", tc.targetShards)
	}
	allCellNames := getCellNames(nil)
	args = append(args, "--cells", allCellNames)
	args = append(args, "--tables", tc.tables)
	args = append(args, "Create")
	args = append(args, ksWorkflow)
	err := vc.VtctlClient.ExecuteCommand(args...)
	require.NoError(t, err)

	waitForShardsToCatchup := func() {
		for _, shard := range arrTargetShards {
			tab := vc.getPrimaryTablet(t, tc.targetKs, shard)
			catchup(t, tab, tc.workflow, tc.typ)
		}
	}

	// Wait for the workflow to finish the copy phase and initially catch up.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	waitForShardsToCatchup()

	if diffDuration, ok := tc.extraVDiffFlags["--max-diff-duration"]; ok {
		if !strings.Contains(tc.tables, "customer") {
			require.Fail(t, "customer table must be included in the table list to test --max-diff-duration")
		}
		// Generate enough customer table data so that the table diff gets restarted.
		dur, err := time.ParseDuration(diffDuration)
		require.NoError(t, err, "could not parse --max-diff-duration %q: %v", diffDuration, err)
		seconds := int64(dur.Seconds())
		chunkSize := int64(100000)
		// Take the test host/runner vCPU count into account when generating rows.
		perVCpuCount := int64(100000)
		// Cap it at 1M rows per second so that we will create betweeen 100,000 and 1,000,000
		// rows for each second in the diff duration, depending on the test host vCPU count.
		perSecondCount := int64(math.Min(float64(perVCpuCount*int64(runtime.NumCPU())), 1000000))
		totalRowsToCreate := seconds * perSecondCount
		log.Infof("Test host has %d vCPUs. Generating %d rows in the customer table to test --max-diff-duration", runtime.NumCPU(), totalRowsToCreate)
		for i := int64(0); i < totalRowsToCreate; i += chunkSize {
			generateMoreCustomers(t, sourceKs, chunkSize)
		}

		// Wait for the workflow to catch up after all the inserts.
		waitForShardsToCatchup()

		// This flag is only implemented in vtctldclient.
		doVtctldclientVDiff(t, tc.targetKs, tc.workflow, allCellNames, nil, "--max-diff-duration", diffDuration)

		// Confirm that the customer table diff was restarted but not others.
		tablet := vc.getPrimaryTablet(t, tc.targetKs, arrTargetShards[0])
		stat, err := getDebugVar(t, tablet.Port, []string{"VDiffRestartedTableDiffsCount"})
		require.NoError(t, err, "failed to get VDiffRestartedTableDiffsCount stat: %v", err)
		customerRestarts := gjson.Parse(stat).Get("customer").Int()
		require.Greater(t, customerRestarts, int64(0), "expected VDiffRestartedTableDiffsCount stat to be greater than 0 for the customer table, got %d", customerRestarts)
		leadRestarts := gjson.Parse(stat).Get("lead").Int()
		require.Equal(t, int64(0), leadRestarts, "expected VDiffRestartedTableDiffsCount stat to be 0 for the Lead table, got %d", leadRestarts)

		// Cleanup the created customer records so as not to slow down the rest of the test.
		delstmt := fmt.Sprintf("delete from %s.customer order by cid desc limit %d", sourceKs, chunkSize)
		for i := int64(0); i < totalRowsToCreate; i += chunkSize {
			_, err := vtgateConn.ExecuteFetch(delstmt, int(chunkSize), false)
			require.NoError(t, err, "failed to cleanup added customer records: %v", err)
		}
		// Wait for the workflow to catch up again on the deletes.
		waitForShardsToCatchup()
		tc.vdiffCount++ // We only did vtctldclient vdiff create
	} else {
		vdiff(t, tc.targetKs, tc.workflow, allCellNames, true, true, nil)
		tc.vdiffCount += 2 // We did vtctlclient AND vtctldclient vdiff create
	}
	checkVDiffCountStat(t, statsTablet, tc.vdiffCount)

	// Confirm that the VDiffRowsCompared stat -- which is a running count of the rows
	// compared by vdiff per table at the controller level -- works as expected.
	vdrc, err := getDebugVar(t, statsTablet.Port, []string{"VDiffRowsCompared"})
	require.NoError(t, err, "failed to get VDiffRowsCompared stat from %s-%d tablet: %v", statsTablet.Cell, statsTablet.TabletUID, err)
	uuid, jsout := performVDiff2Action(t, false, ksWorkflow, allCellNames, "show", "last", false, "--verbose")
	expect := gjson.Get(jsout, fmt.Sprintf("Reports.customer.%s", statsShard)).Int()
	got := gjson.Get(vdrc, fmt.Sprintf("%s.%s.%s", tc.workflow, uuid, "customer")).Int()
	require.Equal(t, expect, got, "expected VDiffRowsCompared stat to be %d, but got %d", expect, got)

	if tc.autoRetryError {
		testAutoRetryError(t, tc, allCellNames)
	}

	if tc.resume {
		testResume(t, tc, allCellNames)
	}

	checkVDiffCountStat(t, statsTablet, tc.vdiffCount)

	// These are done here so that we have a valid workflow to test the commands against.
	if tc.stop {
		testStop(t, ksWorkflow, allCellNames)
		tc.vdiffCount++ // We did either vtctlclient OR vtctldclient vdiff create
	}
	if tc.testCLICreateWait {
		testCLICreateWait(t, ksWorkflow, allCellNames)
		tc.vdiffCount++ // We did either vtctlclient OR vtctldclient vdiff create
	}
	if tc.testCLIErrors {
		testCLIErrors(t, ksWorkflow, allCellNames)
	}
	if tc.testCLIFlagHandling {
		testCLIFlagHandling(t, tc.targetKs, tc.workflow, cells[0])
		tc.vdiffCount++ // We did either vtctlclient OR vtctldclient vdiff create
	}

	checkVDiffCountStat(t, statsTablet, tc.vdiffCount)

	testDelete(t, ksWorkflow, allCellNames)
	tc.vdiffCount = 0 // All vdiffs are deleted, so reset the count and check
	checkVDiffCountStat(t, statsTablet, tc.vdiffCount)

	// Create another VDiff record to confirm it gets deleted when the workflow is completed.
	ts := time.Now()
	uuid, _ = performVDiff2Action(t, false, ksWorkflow, allCellNames, "create", "", false)
	waitForVDiff2ToComplete(t, false, ksWorkflow, allCellNames, uuid, ts)
	tc.vdiffCount++
	checkVDiffCountStat(t, statsTablet, tc.vdiffCount)

	err = vc.VtctlClient.ExecuteCommand(tc.typ, "--", "SwitchTraffic", ksWorkflow)
	require.NoError(t, err)
	err = vc.VtctlClient.ExecuteCommand(tc.typ, "--", "Complete", ksWorkflow)
	require.NoError(t, err)

	// Confirm the VDiff data is deleted for the workflow.
	testNoOrphanedData(t, tc.targetKs, tc.workflow, arrTargetShards)
	tc.vdiffCount = 0 // All vdiffs are deleted, so reset the count and check
	checkVDiffCountStat(t, statsTablet, tc.vdiffCount)
}

func testCLIErrors(t *testing.T, ksWorkflow, cells string) {
	t.Run("Client error handling", func(t *testing.T) {
		_, output := performVDiff2Action(t, false, ksWorkflow, cells, "badcmd", "", true)
		require.Contains(t, output, "Usage:")
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "create", "invalid_uuid", true)
		require.Contains(t, output, "invalid UUID provided")
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "resume", "invalid_uuid", true)
		require.Contains(t, output, "invalid UUID provided")
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "delete", "invalid_uuid", true)
		require.Contains(t, output, "invalid argument provided")
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "show", "invalid_uuid", true)
		require.Contains(t, output, "invalid argument provided")
		uuid, _ := performVDiff2Action(t, false, ksWorkflow, cells, "show", "last", false)
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "create", uuid, true)
		require.Contains(t, output, "already exists")
	})
}

// testCLIFlagHandling tests that the vtctldclient CLI flags are handled correctly
// from vtctldclient->vtctld->vttablet->mysqld.
func testCLIFlagHandling(t *testing.T, targetKs, workflowName string, cell *Cell) {
	expectedOptions := &tabletmanagerdatapb.VDiffOptions{
		CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
			MaxRows:               999,
			MaxExtraRowsToCompare: 777,
			AutoRetry:             true,
			UpdateTableStats:      true,
			TimeoutSeconds:        60,
			MaxDiffSeconds:        333,
		},
		PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{
			SourceCell:  "zone1,zone2,zone3,zonefoosource",
			TargetCell:  "zone1,zone2,zone3,zonefootarget",
			TabletTypes: "replica,primary,rdonly",
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			MaxSampleRows: 888,
			OnlyPks:       true,
		},
	}

	t.Run("Client flag handling", func(t *testing.T) {
		res, err := vc.VtctldClient.ExecuteCommandWithOutput("vdiff", "--target-keyspace", targetKs, "--workflow", workflowName,
			"create",
			"--limit", fmt.Sprintf("%d", expectedOptions.CoreOptions.MaxRows),
			"--max-report-sample-rows", fmt.Sprintf("%d", expectedOptions.ReportOptions.MaxSampleRows),
			"--max-extra-rows-to-compare", fmt.Sprintf("%d", expectedOptions.CoreOptions.MaxExtraRowsToCompare),
			"--filtered-replication-wait-time", fmt.Sprintf("%v", time.Duration(expectedOptions.CoreOptions.TimeoutSeconds)*time.Second),
			"--max-diff-duration", fmt.Sprintf("%v", time.Duration(expectedOptions.CoreOptions.MaxDiffSeconds)*time.Second),
			"--source-cells", expectedOptions.PickerOptions.SourceCell,
			"--target-cells", expectedOptions.PickerOptions.TargetCell,
			"--tablet-types", expectedOptions.PickerOptions.TabletTypes,
			fmt.Sprintf("--update-table-stats=%t", expectedOptions.CoreOptions.UpdateTableStats),
			fmt.Sprintf("--auto-retry=%t", expectedOptions.CoreOptions.AutoRetry),
			fmt.Sprintf("--only-pks=%t", expectedOptions.ReportOptions.OnlyPks),
			"--tablet-types-in-preference-order=false", // So tablet_types should not start with "in_order:", which is the default
			"--format=json") // So we can easily grab the UUID
		require.NoError(t, err, "vdiff command failed: %s", res)
		jsonRes := gjson.Parse(res)
		vduuid, err := uuid.Parse(jsonRes.Get("UUID").String())
		require.NoError(t, err, "invalid UUID: %s", jsonRes.Get("UUID").String())

		// Confirm that the options were passed through and saved correctly.
		query := sqlparser.BuildParsedQuery("select options from %s.vdiff where vdiff_uuid = %s",
			sidecarDBIdentifier, encodeString(vduuid.String())).Query
		tablets := vc.getVttabletsInKeyspace(t, cell, targetKs, "PRIMARY")
		require.Greater(t, len(tablets), 0, "no primary tablets found in keyspace %s", targetKs)
		tablet := maps.Values(tablets)[0]
		qres, err := tablet.QueryTablet(query, targetKs, false)
		require.NoError(t, err, "query %q failed: %v", query, err)
		require.NotNil(t, qres, "query %q returned nil result", query) // Should never happen
		require.Equal(t, 1, len(qres.Rows), "query %q returned %d rows, expected 1", query, len(qres.Rows))
		require.Equal(t, 1, len(qres.Rows[0]), "query %q returned %d columns, expected 1", query, len(qres.Rows[0]))
		storedOptions := &tabletmanagerdatapb.VDiffOptions{}
		bytes, err := qres.Rows[0][0].ToBytes()
		require.NoError(t, err, "failed to convert result %+v to bytes: %v", qres.Rows[0], err)
		err = protojson.Unmarshal(bytes, storedOptions)
		require.NoError(t, err, "failed to unmarshal result %s to a %T: %v", string(bytes), storedOptions, err)
		require.True(t, proto.Equal(expectedOptions, storedOptions), "stored options %v != expected options %v", storedOptions, expectedOptions)
	})
}

func testDelete(t *testing.T, ksWorkflow, cells string) {
	t.Run("Delete", func(t *testing.T) {
		// Let's be sure that we have at least 3 unique VDiffs.
		// We have one record in the SHOW output per VDiff, per
		// shard. So we want to get a count of the unique VDiffs
		// by UUID.
		uuidCount := func(uuids []gjson.Result) int64 {
			seen := make(map[string]struct{})
			for _, uuid := range uuids {
				seen[uuid.String()] = struct{}{}
			}
			return int64(len(seen))
		}
		_, output := performVDiff2Action(t, false, ksWorkflow, cells, "show", "all", false)
		initialVDiffCount := uuidCount(gjson.Get(output, "#.UUID").Array())
		for ; initialVDiffCount < 3; initialVDiffCount++ {
			_, _ = performVDiff2Action(t, false, ksWorkflow, cells, "create", "", false)
		}

		// Now let's confirm that we have at least 3 unique VDiffs.
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "show", "all", false)
		require.GreaterOrEqual(t, uuidCount(gjson.Get(output, "#.UUID").Array()), int64(3))
		// And that our initial count is what we expect.
		require.Equal(t, initialVDiffCount, uuidCount(gjson.Get(output, "#.UUID").Array()))

		// Test show last with verbose too as a side effect.
		uuid, output := performVDiff2Action(t, false, ksWorkflow, cells, "show", "last", false, "--verbose")
		// The TableSummary is only present with --verbose.
		require.Contains(t, output, `"TableSummary":`)

		// Now let's delete one of the VDiffs.
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "delete", uuid, false)
		require.Equal(t, "completed", gjson.Get(output, "Status").String())
		// And confirm that our unique VDiff count has only decreased by one.
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "show", "all", false)
		require.Equal(t, initialVDiffCount-1, uuidCount(gjson.Get(output, "#.UUID").Array()))

		// Now let's delete all of them.
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "delete", "all", false)
		require.Equal(t, "completed", gjson.Get(output, "Status").String())
		// And finally confirm that we have no more VDiffs.
		_, output = performVDiff2Action(t, false, ksWorkflow, cells, "show", "all", false)
		require.Equal(t, int64(0), gjson.Get(output, "#").Int())
	})
}

func testNoOrphanedData(t *testing.T, keyspace, workflow string, shards []string) {
	t.Run("No orphaned data", func(t *testing.T) {
		query := sqlparser.BuildParsedQuery("select vd.id as vdiff_id, vdt.vdiff_id as vdiff_table_id, vdl.vdiff_id as vdiff_log_id from %s.vdiff as vd inner join %s.vdiff_table as vdt on (vd.id = vdt.vdiff_id) inner join %s.vdiff_log as vdl on (vd.id = vdl.vdiff_id) where vd.keyspace = %s and vd.workflow = %s",
			sidecarDBIdentifier, sidecarDBIdentifier, sidecarDBIdentifier, encodeString(keyspace), encodeString(workflow)).Query
		for _, shard := range shards {
			res, err := vc.getPrimaryTablet(t, keyspace, shard).QueryTablet(query, keyspace, false)
			require.NoError(t, err)
			require.Equal(t, 0, len(res.Rows))
		}
	})
}

func testResume(t *testing.T, tc *testCase, cells string) {
	t.Run("Resume", func(t *testing.T) {
		vtgateConn, closeConn := getVTGateConn()
		defer closeConn()
		ksWorkflow := fmt.Sprintf("%s.%s", tc.targetKs, tc.workflow)

		// Confirm the last VDiff is in the expected completed state.
		uuid, output := performVDiff2Action(t, false, ksWorkflow, cells, "show", "last", false)
		jsonOutput := getVDiffInfo(output)
		require.Equal(t, "completed", jsonOutput.State)
		// Save the number of rows compared in previous runs.
		rowsCompared := jsonOutput.RowsCompared
		ogTime := time.Now() // The completed_at should be later than this after resuming

		expectedNewRows := int64(0)
		if tc.resumeInsert != "" {
			res := execVtgateQuery(t, vtgateConn, tc.sourceKs, tc.resumeInsert)
			expectedNewRows = int64(res.RowsAffected)
		}
		expectedRows := rowsCompared + expectedNewRows

		// confirm that the VDiff was resumed, able to complete, and we compared the
		// expected number of rows in total (original run and resume)
		_, _ = performVDiff2Action(t, false, ksWorkflow, cells, "resume", uuid, false)
		info := waitForVDiff2ToComplete(t, false, ksWorkflow, cells, uuid, ogTime)
		require.NotNil(t, info)
		require.False(t, info.HasMismatch)
		require.Equal(t, expectedRows, info.RowsCompared)
	})
}

func testStop(t *testing.T, ksWorkflow, cells string) {
	t.Run("Stop", func(t *testing.T) {
		// create a new VDiff and immediately stop it
		uuid, _ := performVDiff2Action(t, false, ksWorkflow, cells, "create", "", false)
		_, _ = performVDiff2Action(t, false, ksWorkflow, cells, "stop", uuid, false)
		// confirm the VDiff is in the expected stopped state
		_, output := performVDiff2Action(t, false, ksWorkflow, cells, "show", uuid, false)
		jsonOutput := getVDiffInfo(output)
		require.Equal(t, "stopped", jsonOutput.State)
		// confirm that the context cancelled error was also cleared
		require.False(t, strings.Contains(output, `"Errors":`))
	})
}

func testAutoRetryError(t *testing.T, tc *testCase, cells string) {
	t.Run("Auto retry on error", func(t *testing.T) {
		vtgateConn, closeConn := getVTGateConn()
		defer closeConn()
		ksWorkflow := fmt.Sprintf("%s.%s", tc.targetKs, tc.workflow)

		// Confirm the last VDiff is in the expected completed state.
		uuid, output := performVDiff2Action(t, false, ksWorkflow, cells, "show", "last", false)
		jsonOutput := getVDiffInfo(output)
		require.Equal(t, "completed", jsonOutput.State)
		// Save the number of rows compared in the first run.
		rowsCompared := jsonOutput.RowsCompared
		ogTime := time.Now() // The completed_at should be later than this upon retry

		// Create new data since original VDiff run -- if requested -- to confirm that the rows
		// compared is cumulative.
		expectedNewRows := int64(0)
		if tc.retryInsert != "" {
			res := execVtgateQuery(t, vtgateConn, tc.sourceKs, tc.retryInsert)
			expectedNewRows = int64(res.RowsAffected)
		}
		expectedRows := rowsCompared + expectedNewRows

		// Update the VDiff to simulate an ephemeral error having occurred.
		for _, shard := range strings.Split(tc.targetShards, ",") {
			tab := vc.getPrimaryTablet(t, tc.targetKs, shard)
			res, err := tab.QueryTabletWithDB(sqlparser.BuildParsedQuery(sqlSimulateError, sidecarDBIdentifier, sidecarDBIdentifier, encodeString(uuid)).Query, "vt_"+tc.targetKs)
			require.NoError(t, err)
			// Should have updated the vdiff record and at least one vdiff_table record.
			require.GreaterOrEqual(t, int(res.RowsAffected), 2)
		}

		// Confirm that the VDiff was retried, able to complete, and we compared the expected
		// number of rows in total (original run and retry).
		info := waitForVDiff2ToComplete(t, false, ksWorkflow, cells, uuid, ogTime)
		require.NotNil(t, info)
		require.False(t, info.HasMismatch)
		require.Equal(t, expectedRows, info.RowsCompared)
	})
}

func testCLICreateWait(t *testing.T, ksWorkflow string, cells string) {
	t.Run("vtctl create and wait", func(t *testing.T) {
		chCompleted := make(chan bool)
		go func() {
			_, output := performVDiff2Action(t, false, ksWorkflow, cells, "create", "", false, "--wait", "--wait-update-interval=1s")
			completed := false
			// We don't try to parse the JSON output as it may contain a series of outputs
			// that together do not form a valid JSON document. We can change this in the
			// future if we want to by printing them out as an array of JSON objects.
			if strings.Contains(output, `"State": "completed"`) {
				completed = true
			}
			chCompleted <- completed
		}()

		tmr := time.NewTimer(vdiffTimeout)
		defer tmr.Stop()
		select {
		case completed := <-chCompleted:
			require.Equal(t, true, completed)
		case <-tmr.C:
			require.Fail(t, "timeout waiting for vdiff to complete")
		}
	})
}
