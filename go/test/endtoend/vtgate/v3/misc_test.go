/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestSQLSelectLimit(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a'), (2, 'b'), (3, null), (4, 'a'), (5, 'a'), (6, 'b')")
	defer utils.Exec(t, conn, "delete from t7_xxhash")

	for _, workload := range []string{"olap", "oltp"} {
		utils.Exec(t, conn, fmt.Sprintf("set workload = %s", workload))
		utils.Exec(t, conn, "set sql_select_limit = 2")
		utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		utils.AssertMatches(t, conn, "(select uid, msg from t7_xxhash order by uid)", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid limit 4", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL] [VARCHAR("4") VARCHAR("a")]]`)
		/*
			planner does not support query with order by in union query. without order by the results are not deterministic for testing purpose
			utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash order by uid", ``)
			utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash order by uid limit 3", ``)
		*/

		//	without order by the results are not deterministic for testing purpose. Checking row count only.
		qr := utils.Exec(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash")
		assert.Equal(t, 2, len(qr.Rows))

		qr = utils.Exec(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash limit 3")
		assert.Equal(t, 3, len(qr.Rows))
	}
}

func TestSQLSelectLimitWithPlanCache(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a'), (2, 'b'), (3, null)")
	defer utils.Exec(t, conn, "delete from t7_xxhash")

	tcases := []struct {
		limit int
		out   string
	}{{
		limit: -1,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL]]`,
	}, {
		limit: 1,
		out:   `[[VARCHAR("1") VARCHAR("a")]]`,
	}, {
		limit: 2,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`,
	}, {
		limit: 3,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL]]`,
	}, {
		limit: 4,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL]]`,
	}}
	for _, workload := range []string{"olap", "oltp"} {
		utils.Exec(t, conn, fmt.Sprintf("set workload = %s", workload))
		for _, tcase := range tcases {
			utils.Exec(t, conn, fmt.Sprintf("set sql_select_limit = %d", tcase.limit))
			utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", tcase.out)
		}
	}
}
