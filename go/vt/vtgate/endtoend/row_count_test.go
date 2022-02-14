package endtoend

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestRowCount(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	type tc struct {
		query    string
		expected int
	}
	tests := []tc{{
		query:    "insert into t1_row_count(id, id1) values(1, 1), (2, 1), (3, 3), (4, 3)",
		expected: 4,
	}, {
		query:    "select * from t1_row_count",
		expected: -1,
	}, {
		query:    "update t1_row_count set id1 = 500 where id in (1,3)",
		expected: 2,
	}, {
		query:    "show tables",
		expected: -1,
	}, {
		query:    "set @x = 24",
		expected: 0,
	}, {
		query:    "delete from t1_row_count",
		expected: 4,
	}}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			exec(t, conn, test.query)
			qr := exec(t, conn, "select row_count()")
			require.Equal(t, fmt.Sprintf("INT64(%d)", test.expected), qr.Rows[0][0].String())
		})
	}
}
