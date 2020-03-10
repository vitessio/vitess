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

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/PuerkitoBio/goquery"
	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)
	return qr
}

func getConnection(t *testing.T, port int) *mysql.Conn {
	vtParams := mysql.ConnParams{
		Host:  globalConfig.hostname,
		Port:  port,
		Uname: "vt_dba",
	}
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	if err != nil || resp.StatusCode != 200 {
		return false
	}
	return true
}

func validateCount(t *testing.T, conn *mysql.Conn, database string, table string, want int) string {
	qr := execVtgateQuery(t, conn, database, fmt.Sprintf("select count(*) from %s", table))
	assert.NotNil(t, qr)
	assert.NotNil(t, qr.Rows)
	if got, want := fmt.Sprintf("%v", qr.Rows), fmt.Sprintf("[[INT64(%d)]]", want); got != want {
		return fmt.Sprintf("select:\n%v want\n%v", got, want)
	}
	return ""
}

func validateQuery(t *testing.T, conn *mysql.Conn, database string, query string, want string) string {
	qr := execVtgateQuery(t, conn, database, query)
	assert.NotNil(t, qr)
	if got, want := fmt.Sprintf("%v", qr.Rows), want; got != want {
		return fmt.Sprintf("got:\n%v want\n%v", got, want)
	}
	return ""
}

func validateQueryInTablet(t *testing.T, vttablet *cluster.VttabletProcess, database string, query string, want string) string {
	qr, err := vttablet.QueryTabletWithDB(query, database)
	if err != nil {
		return err.Error()
	}
	if got, want := fmt.Sprintf("%v", qr.Rows), want; got != want {
		return fmt.Sprintf("got:\n%v want\n%v", got, want)
	}
	return ""
}

//TODO
func getShardCounts(t *testing.T, vindexType string, conn *mysql.Conn, database string, table string, pkColumn string, shards []string) {
	if vindexType != "reverse_bits" {
		t.Fatal("Only reverse_bits supported for getShardCounts")
	}
	query := fmt.Sprintf("select %s from %s", pkColumn, table)
	qr := execVtgateQuery(t, conn, database, query)
	assert.NotNil(t, qr)
	assert.True(t, len(qr.Rows) > 0)
	//var rangeStart, rangeEnd uint64
	for _, shard := range shards {
		if string(shard[0]) == "-" {
			//rangeStart = 0
		}
	}
	//select keyspace_id from reverse_bits where id = 102
}

func validateCountInTablet(t *testing.T, vttablet *cluster.VttabletProcess, database string, table string, want int) string {
	query := fmt.Sprintf("select count(*) from %s", table)
	qr, err := vttablet.QueryTablet(query, database, true)
	if err != nil {
		return err.Error()
	}
	if got, want := fmt.Sprintf("%v", qr.Rows), fmt.Sprintf("[[INT64(%d)]]", want); got != want {
		return fmt.Sprintf("want\n%v,got\n%v", want, got)
	}
	return ""
}

func validateThatQueryExecutesOnTablet(t *testing.T, conn *mysql.Conn, tablet *cluster.VttabletProcess, ksName string, query string, matchQuery string) bool {
	count := getQueryCount(tablet.QueryzURL, matchQuery)
	qr := execVtgateQuery(t, conn, ksName, query)
	assert.NotNil(t, qr)
	newCount := getQueryCount(tablet.QueryzURL, matchQuery)
	if newCount == count+1 {
		return true
	}
	return false
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
		//Queries seem to include non-printable characters at times and hence equality fails unless these are removed
		re := regexp.MustCompile("[[:^ascii:]]")
		foundQuery := re.ReplaceAllLiteralString(row[queryIndex], "")
		cleanQuery := re.ReplaceAllLiteralString(query, "")
		if foundQuery == cleanQuery {
			count, err = strconv.Atoi(row[countIndex])
		} else {
			//fmt.Printf(">> %s %s %d %d\n", foundQuery, cleanQuery, len(foundQuery), len(cleanQuery))
		}
	}
	return count
}
