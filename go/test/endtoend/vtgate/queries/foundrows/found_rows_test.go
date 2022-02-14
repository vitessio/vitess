package foundrows

import (
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/vtgate/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestFoundRows(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from t2")
	defer utils.Exec(t, conn, "delete from t2")

	utils.Exec(t, conn, "insert into t2(id3,id4) values(1,2), (2,2), (3,3), (4,3), (5,3)")

	runTests := func(workload string) {
		utils.AssertFoundRowsValue(t, conn, "select * from t2", workload, 5)
		utils.AssertFoundRowsValue(t, conn, "select * from t2 order by id3 limit 2", workload, 2)
		utils.AssertFoundRowsValue(t, conn, "select SQL_CALC_FOUND_ROWS * from t2 order by id3 limit 2", workload, 5)
		utils.AssertFoundRowsValue(t, conn, "select SQL_CALC_FOUND_ROWS * from t2 where id3 = 4 order by id3 limit 2", workload, 1)
		utils.AssertFoundRowsValue(t, conn, "select SQL_CALC_FOUND_ROWS * from t2 where id4 = 3 order by id3 limit 2", workload, 3)
		utils.AssertFoundRowsValue(t, conn, "select SQL_CALC_FOUND_ROWS id4, count(id3) from t2 where id3 = 3 group by id4 limit 1", workload, 1)
	}

	runTests("oltp")
	utils.Exec(t, conn, "set workload = olap")
	runTests("olap")

	// cleanup test data
	utils.Exec(t, conn, "set workload = oltp")
	utils.Exec(t, conn, "delete from t2")
	utils.Exec(t, conn, "delete from t2_id4_idx")
}
