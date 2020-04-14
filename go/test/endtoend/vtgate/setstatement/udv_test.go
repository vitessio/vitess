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

func TestSetUDV(t *testing.T) {
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
	}

	queries := []queriesWithExpectations{{
		query:        "set @foo = 'abc', @bar = 42, @baz = 30.5",
		expectedRows: "", rowsAffected: 0,
	}, {
		query:        "select @foo, @bar, @baz",
		expectedRows: `[[VARCHAR("abc") INT64(42) DECIMAL(30.5)]]`, rowsAffected: 1,
	}, {
		query:        "insert into test(id, val1, val2, val3) values(1, @foo, null, null), (2, null, @bar, null), (3, null, null, @baz)",
		expectedRows: ``, rowsAffected: 3,
	}, {
		query:        "select id, val1, val2, val3 from test order by id",
		expectedRows: `[[INT64(1) VARCHAR("abc") NULL NULL] [INT64(2) NULL INT32(42) NULL] [INT64(3) NULL NULL FLOAT32(30.5)]]`, rowsAffected: 3,
	}, {
		query:        "select id, val1 from test where val1=@foo",
		expectedRows: `[[INT64(1) VARCHAR("abc")]]`, rowsAffected: 1,
	}, {
		query:        "select id, val2 from test where val2=@bar",
		expectedRows: `[[INT64(2) INT32(42)]]`, rowsAffected: 1,
	}, {
		query:        "select id, val3 from test where val3=@baz",
		expectedRows: `[[INT64(3) FLOAT32(30.5)]]`, rowsAffected: 1,
	}, {
		query:        "delete from test where val2 = @bar",
		expectedRows: ``, rowsAffected: 1,
	}, {
		query:        "select id, val2 from test where val2=@bar",
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:        "update test set val2 = @bar where val1 = @foo",
		expectedRows: ``, rowsAffected: 1,
	}, {
		query:        "select id, val1, val2 from test where val1=@foo",
		expectedRows: `[[INT64(1) VARCHAR("abc") INT32(42)]]`, rowsAffected: 1,
	}, {
		query:        "delete from test",
		expectedRows: ``, rowsAffected: 2,
	}}

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q.query), func(t *testing.T) {
			qr, err := exec(t, conn, q.query)
			require.Nil(t, err)
			require.Equal(t, uint64(q.rowsAffected), qr.RowsAffected, "rows affected wrong for query: %s", q.query)
			if q.expectedRows != "" {
				result := fmt.Sprintf("%v", qr.Rows)
				if diff := cmp.Diff(q.expectedRows, result); diff != "" {
					t.Errorf("%s\nfor query: %s", diff, q.query)
				}
			}
		})
	}
}
