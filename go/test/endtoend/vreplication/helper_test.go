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
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	defaultTick          = 1 * time.Second
	defaultTimeout       = 30 * time.Second
	workflowStateTimeout = 90 * time.Second
	workflowStateCopying = "Copying" // nolint
	workflowStateRunning = "Running" // nolint
	workflowStateStopped = "Stopped" // nolint
	workflowStateError   = "Error"   // nolint
)

func execMultipleQueries(t *testing.T, conn *mysql.Conn, database string, lines string) {
	queries := strings.Split(lines, "\n")
	for _, query := range queries {
		if strings.HasPrefix(query, "--") {
			continue
		}
		execVtgateQuery(t, conn, database, string(query))
	}
}
func execQuery(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	qr, err := conn.ExecuteFetch(query, 1000, false)
	require.NoError(t, err)
	return qr
}

func getConnection(t *testing.T, hostname string, port int) *mysql.Conn {
	vtParams := mysql.ConnParams{
		Host:  hostname,
		Port:  port,
		Uname: "vt_dba",
	}
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	return conn
}

func execVtgateQuery(t *testing.T, conn *mysql.Conn, database string, query string) *sqltypes.Result {
	if strings.TrimSpace(query) == "" {
		return nil
	}
	if database != "" {
		execQuery(t, conn, "use `"+database+"`;")
	}
	execQuery(t, conn, "begin")
	qr := execQuery(t, conn, query)
	execQuery(t, conn, "commit")
	return qr
}

func checkHealth(t *testing.T, url string) bool {
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func waitForQueryResult(t *testing.T, conn *mysql.Conn, database string, query string, want string) {
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		qr := execVtgateQuery(t, conn, database, query)
		require.NotNil(t, qr)
		if want == fmt.Sprintf("%v", qr.Rows) {
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, fmt.Sprintf("query %q on database %q did not return the expected result of %v before the timeout of %s; last seen result: %v",
				query, database, want, defaultTimeout, qr.Rows))
		default:
			time.Sleep(defaultTick)
		}
	}
}

// waitForTabletThrottlingStatus waits for the tablet to return the provided HTTP code for
// the provided app name in its self check.
func waitForTabletThrottlingStatus(t *testing.T, tablet *cluster.VttabletProcess, appName string, wantCode int64) {
	var gotCode int64
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		output, err := throttlerCheckSelf(tablet, appName)
		require.NoError(t, err)

		gotCode, err = jsonparser.GetInt([]byte(output), "StatusCode")
		require.NoError(t, err)
		if wantCode == gotCode {
			// Wait for any cached check values to be cleared and the new
			// status value to be in effect everywhere before returning.
			time.Sleep(500 * time.Millisecond)
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, fmt.Sprintf("tablet %q did not return expected status of %d for application %q before the timeout of %s; last seen status: %d",
				tablet.Name, wantCode, appName, defaultTimeout, gotCode))
		default:
			time.Sleep(defaultTick)
		}
	}
}

// waitForNoWorkflowLag waits for the VReplication workflow's MaxVReplicationTransactionLag
// value to be 0.
func waitForNoWorkflowLag(t *testing.T, vc *VitessCluster, keyspace, worfklow string) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, worfklow)
	lag := int64(0)
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", "--", ksWorkflow, "show")
		require.NoError(t, err)
		lag, err = jsonparser.GetInt([]byte(output), "MaxVReplicationTransactionLag")
		require.NoError(t, err)
		if lag == 0 {
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, fmt.Sprintf("workflow %q did not eliminate VReplication lag before the timeout of %s; last seen MaxVReplicationTransactionLag: %d",
				ksWorkflow, defaultTimeout, lag))
		default:
			time.Sleep(defaultTick)
		}
	}
}

// verifyNoInternalTables can e.g. be used to confirm that no internal tables were
// copied from a source to a target during a MoveTables or Reshard operation.
func verifyNoInternalTables(t *testing.T, conn *mysql.Conn, keyspaceShard string) {
	qr := execVtgateQuery(t, conn, keyspaceShard, "show tables")
	require.NotNil(t, qr)
	require.NotNil(t, qr.Rows)
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		assert.False(t, schema.IsInternalOperationTableName(tableName), "found internal table %q in shard %q", tableName, keyspaceShard)
	}
}

func waitForRowCount(t *testing.T, conn *mysql.Conn, database string, table string, want int) {
	query := fmt.Sprintf("select count(*) from %s", table)
	wantRes := fmt.Sprintf("[[INT64(%d)]]", want)
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		qr := execVtgateQuery(t, conn, database, query)
		require.NotNil(t, qr)
		if wantRes == fmt.Sprintf("%v", qr.Rows) {
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, fmt.Sprintf("table %q did not reach the expected number of rows (%d) before the timeout of %s; last seen result: %v",
				table, want, defaultTimeout, qr.Rows))
		default:
			time.Sleep(defaultTick)
		}
	}
}

func waitForRowCountInTablet(t *testing.T, vttablet *cluster.VttabletProcess, database string, table string, want int) {
	query := fmt.Sprintf("select count(*) from %s", table)
	wantRes := fmt.Sprintf("[[INT64(%d)]]", want)
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		qr, err := vttablet.QueryTablet(query, database, true)
		require.NoError(t, err)
		require.NotNil(t, qr)
		if wantRes == fmt.Sprintf("%v", qr.Rows) {
			return
		}
		select {
		case <-timer.C:
			require.FailNow(t, fmt.Sprintf("table %q did not reach the expected number of rows (%d) on tablet %q before the timeout of %s; last seen result: %v",
				table, want, vttablet.Name, defaultTimeout, qr.Rows))
		default:
			time.Sleep(defaultTick)
		}
	}
}

func validateThatQueryExecutesOnTablet(t *testing.T, conn *mysql.Conn, tablet *cluster.VttabletProcess, ksName string, query string, matchQuery string) bool {
	count := getQueryCount(tablet.QueryzURL, matchQuery)
	qr := execVtgateQuery(t, conn, ksName, query)
	require.NotNil(t, qr)
	newCount := getQueryCount(tablet.QueryzURL, matchQuery)
	return newCount == count+1
}

// waitForWorkflowState waits for all of the given workflow's
// streams to reach the provided state. You can pass optional
// key value pairs of the form "key==value" to also wait for
// additional stream sub-state such as "Message==for vdiff".
// Invalid checks are ignored.
func waitForWorkflowState(t *testing.T, vc *VitessCluster, ksWorkflow string, wantState string, fieldEqualityChecks ...string) {
	done := false
	timer := time.NewTimer(workflowStateTimeout)
	log.Infof("Waiting for workflow %q to fully reach %q state", ksWorkflow, wantState)
	for {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksWorkflow, "show")
		require.NoError(t, err)
		done = true
		state := ""
		result := gjson.Get(output, "ShardStatuses")
		result.ForEach(func(tabletId, tabletStreams gjson.Result) bool { // for each participating tablet
			tabletStreams.ForEach(func(streamId, streamInfos gjson.Result) bool { // for each stream
				if streamId.String() == "PrimaryReplicationStatuses" {
					streamInfos.ForEach(func(attributeKey, attributeValue gjson.Result) bool { // for each attribute in the stream
						// we need to wait for all streams to have the desired state
						state = attributeValue.Get("State").String()
						if state == wantState {
							for i := 0; i < len(fieldEqualityChecks); i++ {
								if kvparts := strings.Split(fieldEqualityChecks[i], "=="); len(kvparts) == 2 {
									key := kvparts[0]
									val := kvparts[1]
									res := attributeValue.Get(key).String()
									if !strings.EqualFold(res, val) {
										done = false
									}
								}
							}
						} else {
							done = false
						}
						return true
					})
				}
				return true
			})
			return true
		})
		if done {
			log.Infof("Workflow %q has fully reached the desired state of %q", ksWorkflow, wantState)
			return
		}
		select {
		case <-timer.C:
			var extraRequirements string
			if len(fieldEqualityChecks) > 0 {
				extraRequirements = fmt.Sprintf(" with the additional requirements of \"%v\"", fieldEqualityChecks)
			}
			require.FailNowf(t, "workflow state not reached",
				"Workflow %q did not fully reach the expected state of %q%s before the timeout of %s; last seen output: %s",
				ksWorkflow, wantState, extraRequirements, workflowStateTimeout, output)
		default:
			time.Sleep(defaultTick)
		}
	}
}

// confirmTablesHaveSecondaryKeys confirms that the tables provided
// as a CSV have secondary keys. This is useful when testing the
// --defer-secondary-keys flag to confirm that the secondary keys
// were re-added by the time the workflow hits the running phase.
// For a Reshard workflow, where no tables are specififed, pass
// an empty string for the tables and all tables in the target
// keyspace will be checked.
func confirmTablesHaveSecondaryKeys(t *testing.T, tablets []*cluster.VttabletProcess, ksName string, tables string) {
	require.NotNil(t, tablets)
	require.NotNil(t, tablets[0])
	var tableArr []string
	if strings.TrimSpace(tables) != "" {
		tableArr = strings.Split(tables, ",")
	}
	if len(tableArr) == 0 { // We don't specify any for Reshard.
		// In this case we check all of them.
		res, err := tablets[0].QueryTablet("show tables", ksName, true)
		require.NoError(t, err)
		require.NotNil(t, res)
		for _, row := range res.Rows {
			tableArr = append(tableArr, row[0].ToString())
		}
	}
	for _, tablet := range tablets {
		for _, table := range tableArr {
			if schema.IsInternalOperationTableName(table) {
				continue
			}
			table := strings.TrimSpace(table)
			secondaryKeys := 0
			res, err := tablet.QueryTablet(fmt.Sprintf("show create table %s", sqlescape.EscapeID(table)), ksName, true)
			require.NoError(t, err)
			require.NotNil(t, res)
			row := res.Named().Row()
			tableSchema := row["Create Table"].ToString()
			parsedDDL, err := sqlparser.ParseStrictDDL(tableSchema)
			require.NoError(t, err)
			createTable, ok := parsedDDL.(*sqlparser.CreateTable)
			require.True(t, ok)
			require.NotNil(t, createTable)
			require.NotNil(t, createTable.GetTableSpec())
			for _, index := range createTable.GetTableSpec().Indexes {
				if !index.Info.Primary {
					secondaryKeys++
				}
			}
			require.Greater(t, secondaryKeys, 0, "Table %s does not have any secondary keys", table)
		}
	}
}

func getHTTPBody(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		log.Infof("http Get returns %+v", err)
		return ""
	}
	if resp.StatusCode != 200 {
		log.Infof("http Get returns status %d", resp.StatusCode)
		return ""
	}
	respByte, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	body := string(respByte)
	return body
}

func getQueryCount(url string, query string) int {
	var headings, row []string
	var rows [][]string
	body := getHTTPBody(url)
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		log.Infof("goquery parsing returns %+v\n", err)
		return 0
	}

	var queryIndex, countIndex, count int
	queryIndex = -1
	countIndex = -1

	doc.Find("table").Each(func(index int, tablehtml *goquery.Selection) {
		tablehtml.Find("tr").Each(func(indextr int, rowhtml *goquery.Selection) {
			rowhtml.Find("th").Each(func(indexth int, tableheading *goquery.Selection) {
				heading := tableheading.Text()
				if heading == "Query" {
					queryIndex = indexth
				}
				if heading == "Count" {
					countIndex = indexth
				}
				headings = append(headings, heading)
			})
			rowhtml.Find("td").Each(func(indexth int, tablecell *goquery.Selection) {
				row = append(row, tablecell.Text())
			})
			rows = append(rows, row)
			row = nil
		})
	})
	if queryIndex == -1 || countIndex == -1 {
		log.Infof("Queryz response is incorrect")
		return 0
	}
	for _, row := range rows {
		if len(row) != len(headings) {
			continue
		}
		filterChars := []string{"_", "`"}
		//Queries seem to include non-printable characters at times and hence equality fails unless these are removed
		re := regexp.MustCompile("[[:^ascii:]]")
		foundQuery := re.ReplaceAllLiteralString(row[queryIndex], "")
		cleanQuery := re.ReplaceAllLiteralString(query, "")
		for _, filterChar := range filterChars {
			foundQuery = strings.ReplaceAll(foundQuery, filterChar, "")
			cleanQuery = strings.ReplaceAll(cleanQuery, filterChar, "")
		}
		if foundQuery == cleanQuery || strings.Contains(foundQuery, cleanQuery) {
			count, _ = strconv.Atoi(row[countIndex])
		}
	}
	return count
}

func validateDryRunResults(t *testing.T, output string, want []string) {
	t.Helper()
	require.NotEmpty(t, output)
	gotDryRun := strings.Split(output, "\n")
	require.True(t, len(gotDryRun) > 3)
	startRow := 3
	if strings.Contains(gotDryRun[0], "deprecated") {
		startRow = 4
	}
	gotDryRun = gotDryRun[startRow : len(gotDryRun)-1]
	if len(want) != len(gotDryRun) {
		t.Fatalf("want and got: lengths don't match, \nwant\n%s\n\ngot\n%s", strings.Join(want, "\n"), strings.Join(gotDryRun, "\n"))
	}
	var match, fail bool
	fail = false
	for i, w := range want {
		w = strings.TrimSpace(w)
		g := strings.TrimSpace(gotDryRun[i])
		if w[0] == '/' {
			w = strings.TrimSpace(w[1:])
			result := strings.HasPrefix(g, w)
			match = result
			//t.Logf("Partial match |%v|%v|%v\n", w, g, match)
		} else {
			match = g == w
		}
		if !match {
			fail = true
			t.Fatalf("want %s, got %s\n", w, gotDryRun[i])
		}
	}
	if fail {
		t.Fatalf("Dry run results don't match, want %s, got %s", want, gotDryRun)
	}
}

func checkIfTableExists(t *testing.T, vc *VitessCluster, tabletAlias string, table string) (bool, error) {
	var output string
	var err error
	found := false

	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetSchema", "--", "--tables", table, tabletAlias); err != nil {
		return false, err
	}
	jsonparser.ArrayEach([]byte(output), func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		t, _ := jsonparser.GetString(value, "name")
		if t == table {
			found = true
		}
	}, "table_definitions")
	return found, nil
}

func checkIfDenyListExists(t *testing.T, vc *VitessCluster, ksShard string, table string) (bool, error) {
	var output string
	var err error
	found := false
	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetShard", ksShard); err != nil {
		t.Fatalf("%v %v", err, output)
		return false, err
	}
	jsonparser.ArrayEach([]byte(output), func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if string(value) == table {
			found = true
		}
	}, "tablet_controls", "[0]", "denied_tables")
	return found, nil
}

func expectNumberOfStreams(t *testing.T, vtgateConn *mysql.Conn, name string, workflow string, database string, want int) {
	query := sqlparser.BuildParsedQuery("select count(*) from %s.vreplication where workflow='%s'", sidecarDBIdentifier, workflow).Query
	waitForQueryResult(t, vtgateConn, database, query, fmt.Sprintf(`[[INT64(%d)]]`, want))
}

func printShardPositions(vc *VitessCluster, ksShards []string) {
	for _, ksShard := range ksShards {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ShardReplicationPositions", ksShard)
		if err != nil {
			fmt.Printf("Error in ShardReplicationPositions: %v, output %v", err, output)
		} else {
			fmt.Printf("Position of %s: %s", ksShard, output)
		}
	}
}

func printRoutingRules(t *testing.T, vc *VitessCluster, msg string) error {
	var output string
	var err error
	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetRoutingRules"); err != nil {
		return err
	}
	fmt.Printf("Routing Rules::%s:\n%s\n", msg, output)
	return nil
}

func osExec(t *testing.T, command string, args []string) (string, error) {
	cmd := exec.Command(command, args...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func getDebugVar(t *testing.T, port int, varPath []string) (string, error) {
	var val []byte
	var err error
	url := fmt.Sprintf("http://localhost:%d/debug/vars", port)
	log.Infof("url: %s, varPath: %s", url, strings.Join(varPath, ":"))
	body := getHTTPBody(url)
	val, _, _, err = jsonparser.Get([]byte(body), varPath...)
	require.NoError(t, err)
	return string(val), nil
}

func confirmWorkflowHasCopiedNoData(t *testing.T, targetKS, workflow string) {
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	ksWorkflow := fmt.Sprintf("%s.%s", targetKS, workflow)
	for {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksWorkflow, "show")
		require.NoError(t, err)
		result := gjson.Get(output, "ShardStatuses")
		result.ForEach(func(tabletId, tabletStreams gjson.Result) bool { // for each source tablet
			tabletStreams.ForEach(func(streamId, streamInfos gjson.Result) bool { // for each stream
				if streamId.String() == "PrimaryReplicationStatuses" {
					streamInfos.ForEach(func(attributeKey, attributeValue gjson.Result) bool { // for each attribute in the stream
						state := attributeValue.Get("State").String()
						pos := attributeValue.Get("Pos").String()
						// If we've actually copied anything then we'll have a position in the stream
						if (state == workflowStateRunning || state == workflowStateCopying) && pos != "" {
							require.FailNowf(t, "Unexpected data copied in workflow",
								"The MoveTables workflow %q copied data in less than %s when it should have been waiting. Show output: %s",
								ksWorkflow, defaultTimeout, output)
						}
						return true // end attribute loop
					})
				}
				return true // end stream loop
			})
			return true // end tablet loop
		})
		select {
		case <-timer.C:
			return
		default:
			time.Sleep(defaultTick)
		}
	}
}

// getShardRoutingRules returns the shard routing rules stored in the
// topo. It returns the rules sorted by shard,to_keyspace and with all
// newlines and whitespace removed so that we have predictable,
// compact, and easy to compare results for tests.
func getShardRoutingRules(t *testing.T) string {
	output, err := osExec(t, "vtctldclient", []string{"--server", getVtctldGRPCURL(), "GetShardRoutingRules"})
	log.Infof("GetShardRoutingRules err: %+v, output: %+v", err, output)
	require.Nilf(t, err, output)
	require.NotNil(t, output)

	// Sort the rules by shard,to_keyspace
	jsonOutput := gjson.Parse(output)
	rules := jsonOutput.Get("rules").Array()
	sort.Slice(rules, func(i, j int) bool {
		shardI := rules[i].Get("shard").String()
		shardJ := rules[j].Get("shard").String()
		if shardI == shardJ {
			return rules[i].Get("to_keyspace").String() < rules[j].Get("to_keyspace").String()
		}
		return shardI < shardJ
	})
	sb := strings.Builder{}
	for i := 0; i < len(rules); i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(rules[i].String())
	}
	output = fmt.Sprintf(`{"rules":[%s]}`, sb.String())

	// Remove newlines and whitespace
	re := regexp.MustCompile(`[\n\s]+`)
	output = re.ReplaceAllString(output, "")
	output = strings.TrimSpace(output)
	return output
}

func verifyCopyStateIsOptimized(t *testing.T, tablet *cluster.VttabletProcess) {
	// Update information_schem with the latest data
	_, err := tablet.QueryTablet(sqlparser.BuildParsedQuery("analyze table %s.copy_state", sidecarDBIdentifier).Query, "", false)
	require.NoError(t, err)

	// Verify that there's no delete marked rows and we reset the auto-inc value.
	// MySQL doesn't always immediately update information_schema so we wait.
	tmr := time.NewTimer(defaultTimeout)
	defer tmr.Stop()
	query := sqlparser.BuildParsedQuery("select data_free, auto_increment from information_schema.tables where table_schema='%s' and table_name='copy_state'", sidecarDBName).Query
	var dataFree, autoIncrement int64
	for {
		res, err := tablet.QueryTablet(query, "", false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, len(res.Rows))
		dataFree, err = res.Rows[0][0].ToInt64()
		require.NoError(t, err)
		autoIncrement, err = res.Rows[0][1].ToInt64()
		require.NoError(t, err)
		if dataFree == 0 && autoIncrement == 1 {
			return
		}

		select {
		case <-tmr.C:
			require.FailNowf(t, "timed out waiting for copy_state table to be optimized",
				"data_free should be 0 and auto_increment should be 1, last seen values were %d and %d respectively",
				dataFree, autoIncrement)
		default:
			time.Sleep(defaultTick)
		}
	}
}
