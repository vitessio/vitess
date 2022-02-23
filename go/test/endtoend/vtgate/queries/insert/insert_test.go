/*
Copyright 2021 The Vitess Authors.

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

package insert

import (
	"context"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSimpleInsertSelect(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from s_tbl`)
	defer utils.Exec(t, conn, `delete from u_tbl`)

	utils.Exec(t, conn, "insert into s_tbl(id, num) values (1,2),(3,4)")
	utils.Exec(t, conn, "insert into u_tbl(id, num) values (1,2),(3,4)")

	qr := utils.Exec(t, conn, "insert into s_tbl(id, num) select id*10, num*20 from s_tbl")
	require.EqualValues(t, 2, qr.RowsAffected)
	utils.Exec(t, conn, "insert into u_tbl(id, num) select id*10, num*20 from u_tbl")
	require.EqualValues(t, 2, qr.RowsAffected)

	utils.AssertMatches(t, conn, `select id, num from s_tbl order by id`, `[[INT64(1) INT64(2)] [INT64(3) INT64(4)] [INT64(10) INT64(40)] [INT64(30) INT64(80)]]`)
	utils.AssertMatches(t, conn, `select id, num from u_tbl order by id`, `[[INT64(1) INT64(2)] [INT64(3) INT64(4)] [INT64(10) INT64(40)] [INT64(30) INT64(80)]]`)
}
