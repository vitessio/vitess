package vreplication

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func execMultipleQueries(t *testing.T, conn *mysql.Conn, database string, lines string) {
	t.Helper()
	queries := strings.Split(lines, "\n")
	for _, query := range queries {
		execVtgateQuery(t, conn, database, string(query))
	}

}
func execQuery(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, false)
	assert.Nil(t, err)
	return qr
}

func getConnection(t *testing.T) *mysql.Conn {
	t.Helper()
	vtParams := mysql.ConnParams{
		Host: globalConfig.hostname,
		Port: globalConfig.vtgateMySQLPort,
	}
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	assert.Nil(t, err)
	return conn
}

func execVtgateQuery(t *testing.T, conn *mysql.Conn, database string, query string) {
	t.Helper()
	if strings.TrimSpace(query) == "" {
		return
	}
	execQuery(t, conn, "begin")
	execQuery(t, conn, "use `"+database+"`;")
	execQuery(t, conn, query)
	execQuery(t, conn, "commit")
}
