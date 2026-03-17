package connpool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

func BenchmarkExecOnceHappyPath(b *testing.B) {
	db := fakesqldb.New(b)
	defer db.Close()

	sql := "select 1"
	db.AddQuery(sql, &sqltypes.Result{})

	connPool := newPool()
	params := dbconfigs.New(db.ConnParams())
	connPool.Open(params, params, params)
	defer connPool.Close()

	dbConn, err := newPooledConn(context.Background(), connPool, params)
	require.NoError(b, err)
	defer dbConn.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, err := dbConn.execOnce(ctx, sql, 1, false, false)
		if err != nil {
			b.Fatal(err)
		}
	}
}
