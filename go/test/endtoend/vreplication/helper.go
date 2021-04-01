package vreplication

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/PuerkitoBio/goquery"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func execMultipleQueries(t *testing.T, conn *mysql.Conn, database string, lines string) {
	queries := strings.Split(lines, "\n")
	for _, query := range queries {
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
	execQuery(t, conn, "use `"+database+"`;")
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

func validateCount(t *testing.T, conn *mysql.Conn, database string, table string, want int) {
	qr := execVtgateQuery(t, conn, database, fmt.Sprintf("select count(*) from %s", table))
	require.NotNil(t, qr)
	require.NotNil(t, qr.Rows)
	require.Equal(t, fmt.Sprintf("[[INT64(%d)]]", want), fmt.Sprintf("%v", qr.Rows))
}

func validateQuery(t *testing.T, conn *mysql.Conn, database string, query string, want string) {
	qr := execVtgateQuery(t, conn, database, query)
	require.NotNil(t, qr)
	require.Equal(t, want, fmt.Sprintf("%v", qr.Rows))
}

func validateCountInTablet(t *testing.T, vttablet *cluster.VttabletProcess, database string, table string, want int) {
	query := fmt.Sprintf("select count(*) from %s", table)
	qr, err := vttablet.QueryTablet(query, database, true)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("[[INT64(%d)]]", want), fmt.Sprintf("%v", qr.Rows))
}

func validateThatQueryExecutesOnTablet(t *testing.T, conn *mysql.Conn, tablet *cluster.VttabletProcess, ksName string, query string, matchQuery string) bool {
	count := getQueryCount(tablet.QueryzURL, matchQuery)
	qr := execVtgateQuery(t, conn, ksName, query)
	require.NotNil(t, qr)
	newCount := getQueryCount(tablet.QueryzURL, matchQuery)
	return newCount == count+1
}

func getQueryCount(url string, query string) int {
	var headings, row []string
	var rows [][]string
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("http Get returns %+v\n", err)
		return 0
	}
	if resp.StatusCode != 200 {
		fmt.Printf("http Get returns status %d\n", resp.StatusCode)
		return 0
	}
	respByte, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	body := string(respByte)
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		fmt.Printf("goquery parsing returns %+v\n", err)
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
		fmt.Println("Queryz response is incorrect")
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
	gotDryRun = gotDryRun[3 : len(gotDryRun)-1]
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
			t.Logf("want %s, got %s\n", w, gotDryRun[i])
		}
	}
	if fail {
		t.Fatal("Dry run results don't match")
	}
}

func checkIfTableExists(t *testing.T, vc *VitessCluster, tabletAlias string, table string) (bool, error) {
	var output string
	var err error
	found := false

	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetSchema", "-tables", table, tabletAlias); err != nil {
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

func checkIfBlacklistExists(t *testing.T, vc *VitessCluster, ksShard string, table string) (bool, error) {
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
	}, "tablet_controls", "[0]", "blacklisted_tables")
	return found, nil
}

func expectNumberOfStreams(t *testing.T, vtgateConn *mysql.Conn, name string, workflow string, database string, want int) {
	query := fmt.Sprintf("select count(*) from _vt.vreplication where workflow='%s';", workflow)
	validateQuery(t, vtgateConn, database, query, fmt.Sprintf(`[[INT64(%d)]]`, want))
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
	if _, err := vc.VtctlClient.ExecuteCommandWithOutput("ApplyRoutingRules", "-rules={}"); err != nil {
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
