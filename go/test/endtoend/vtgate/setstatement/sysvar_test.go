package setstatement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSetSysVar(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	type queriesWithExpectations struct {
		query        string
		expectedRows string
		rowsAffected int
		errMsg       string
	}

	queries := []queriesWithExpectations{{
		query:        `set @@debug = 'T'`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:        `set @@sql_mode = @@sql_mode`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:        `set @@sql_mode = concat(@@sql_mode,"")`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:  `set @@sql_mode = concat(@@sql_mode,"ALLOW_INVALID_DATES")`,
		errMsg: "Modification not allowed using set construct for: sql_mode",
	}}

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q.query), func(t *testing.T) {
			qr, err := exec(t, conn, q.query)
			if q.errMsg != "" {
				require.Contains(t, err.Error(), q.errMsg)
			} else {
				require.Nil(t, err)
				require.Equal(t, uint64(q.rowsAffected), qr.RowsAffected, "rows affected wrong for query: %s", q.query)
				if q.expectedRows != "" {
					result := fmt.Sprintf("%v", qr.Rows)
					if diff := cmp.Diff(q.expectedRows, result); diff != "" {
						t.Errorf("%s\nfor query: %s", diff, q.query)
					}
				}
			}
		})
	}
}
