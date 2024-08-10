package utils

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

// ClearOutTable deletes everything from a table. Sometimes the table might have more rows than allowed in a single delete query,
// so we have to do the deletions iteratively.
func ClearOutTable(t *testing.T, vtParams mysql.ConnParams, tableName string) {
	ctx := context.Background()
	for {
		conn, err := mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)

		res, err := conn.ExecuteFetch(fmt.Sprintf("SELECT count(*) FROM %v", tableName), 1, false)
		if err != nil {
			log.Errorf("Error in selecting - %v", err)
			conn.Close()
			continue
		}
		require.Len(t, res.Rows, 1)
		require.Len(t, res.Rows[0], 1)
		rowCount, err := res.Rows[0][0].ToInt()
		require.NoError(t, err)
		if rowCount == 0 {
			conn.Close()
			return
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("DELETE FROM %v LIMIT 10000", tableName), 10000, false)
		if err != nil {
			log.Errorf("Error in cleanup deletion - %v", err)
			conn.Close()
			continue
		}
	}
}
