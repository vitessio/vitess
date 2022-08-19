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
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/schema"

	"github.com/PuerkitoBio/goquery"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

var (
	defaultTick    = 500 * time.Millisecond
	defaultTimeout = 30 * time.Second
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
	if err != nil || resp.StatusCode != 200 {
		return false
	}
	return true
}

func waitForQueryResult(t *testing.T, conn *mysql.Conn, database string, query string, want string) {
	ticker := time.NewTicker(defaultTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			qr := execVtgateQuery(t, conn, database, query)
			require.NotNil(t, qr)
			if want == fmt.Sprintf("%v", qr.Rows) {
				return
			}
		case <-time.After(defaultTimeout):
			require.FailNow(t, "query %s on database %s did not return the expected result of %v before the timeout of %s",
				query, database, want, defaultTimeout)
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
		assert.False(t, schema.IsInternalOperationTableName(tableName), "found internal table %s in shard %s", tableName, keyspaceShard)
	}
}

func waitForRowCount(t *testing.T, conn *mysql.Conn, database string, table string, want int) {
	query := fmt.Sprintf("select count(*) from %s", table)
	wantRes := fmt.Sprintf("[[INT64(%d)]]", want)
	ticker := time.NewTicker(defaultTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			qr := execVtgateQuery(t, conn, database, query)
			require.NotNil(t, qr)
			require.Equal(t, fmt.Sprintf("[[INT64(%d)]]", want), fmt.Sprintf("%v", qr.Rows))
			if wantRes == fmt.Sprintf("%v", qr.Rows) {
				return
			}
		case <-time.After(defaultTimeout):
			require.FailNow(t, "table %s did not reach the expected number of rows (%d) before the timeout of %s", table, want, defaultTimeout)
		}
	}
}

func waitForRowCountInTablet(t *testing.T, vttablet *cluster.VttabletProcess, database string, table string, want int) {
	query := fmt.Sprintf("select count(*) from %s", table)
	wantRes := fmt.Sprintf("[[INT64(%d)]]", want)
	ticker := time.NewTicker(defaultTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			qr, err := vttablet.QueryTablet(query, database, true)
			require.NoError(t, err)
			require.NotNil(t, qr)
			require.Equal(t, fmt.Sprintf("[[INT64(%d)]]", want), fmt.Sprintf("%v", qr.Rows))
			if wantRes == fmt.Sprintf("%v", qr.Rows) {
				return
			}
		case <-time.After(defaultTimeout):
			require.FailNow(t, "table %s did not reach the expected number of rows (%d) on the %s tablet before the timeout of %s",
				table, want, vttablet.Name, defaultTimeout)
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
	query := fmt.Sprintf("select count(*) from _vt.vreplication where workflow='%s';", workflow)
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

func clearRoutingRules(t *testing.T, vc *VitessCluster) error {
	if _, err := vc.VtctlClient.ExecuteCommandWithOutput("ApplyRoutingRules", "--", "--rules={}"); err != nil {
		return err
	}
	return nil
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
	body := getHTTPBody(url)
	val, _, _, err = jsonparser.Get([]byte(body), varPath...)
	require.NoError(t, err)
	return string(val), nil
}
