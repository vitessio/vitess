package vtgate

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSavepoint(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "delete from t1")

	exec(t, conn, "start transaction")
	exec(t, conn, "start transaction")
	exec(t, conn, "start transaction")
}
