package vreplication

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"

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
	execQuery(t, conn, "begin")
	execQuery(t, conn, "use `"+database+"`;")
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
		return fmt.Sprintf("select:\n%v want\n%v", got, want)
	}
	return ""
}

func validateQueryInTablet(t *testing.T, vttablet *cluster.VttabletProcess, database string, query string, want string) string {
	qr, err := vttablet.QueryTabletWithDB(query, database)
	if err != nil {
		return err.Error()
	}
	if got, want := fmt.Sprintf("%v", qr.Rows), want; got != want {
		return fmt.Sprintf("select:\n%v want\n%v", got, want)
	}
	return ""
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
