package vtgate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

/*
1. s a, begin,exec,rollback to a, err
1. s a, begin,exec,rollback, rollback to a, err
1. s a, begin,exec,commit, rollback to a, err
1. begin, s a, exec,rollback to a, err
1. s a, begin, s b
*/
func TestSavepoint(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "delete from t1")

	exec(t, conn, "start transaction")
	exec(t, conn, "insert into t1(id1, id2) values(1,1)")
	exec(t, conn, "savepoint a")
	exec(t, conn, "insert into t1(id1, id2) values(2,2)")
	qr := exec(t, conn, "select id1, id2 from t1")
	require.Equal(t, `[[INT64(1) INT64(1)]]`, fmt.Sprintf("%v", qr.Rows))
	exec(t, conn, "rollback work to savepoint a")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select id1, id2 from t1")
	require.Zero(t, len(qr.Rows))

}
