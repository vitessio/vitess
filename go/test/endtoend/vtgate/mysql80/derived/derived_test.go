package derived

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestDerivedTableColumns(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `delete from t1`)
	defer utils.Exec(t, conn, `delete from t1`)

	utils.Exec(t, conn, "insert into t1(id1, id2) values (0,10),(1,9),(2,8),(3,7),(4,6),(5,5)")
	utils.AssertMatches(t, conn, `SELECT /*vt+ PLANNER=gen4 */ t.id FROM (SELECT id2 FROM t1) AS t(id) ORDER BY t.id DESC`, `[[INT64(10)] [INT64(9)] [INT64(8)] [INT64(7)] [INT64(6)] [INT64(5)]]`)
}
