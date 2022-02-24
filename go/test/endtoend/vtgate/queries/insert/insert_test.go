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

	"vitess.io/vitess/go/test/endtoend/utils"

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

	utils.AssertMatches(t, conn, `select num from num_vdx_tbl order by num`, `[[INT64(2)] [INT64(4)] [INT64(40)] [INT64(80)]]`)
}

func TestFailureInsertSelect(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from s_tbl`)
	defer utils.Exec(t, conn, `delete from u_tbl`)

	utils.Exec(t, conn, "insert into s_tbl(id, num) values (1,2),(3,4)")
	utils.Exec(t, conn, "insert into u_tbl(id, num) values (1,2),(3,4)")

	// primary key same
	utils.AssertContainsError(t, conn, "insert into s_tbl(id, num) select id, num*20 from s_tbl where id = 1", `Duplicate entry '1' for key 's_tbl.PRIMARY' (errno 1062) (sqlstate 23000)`)
	// lookup key same
	utils.AssertContainsError(t, conn, "insert into s_tbl(id, num) select id*20, num from s_tbl where id = 1", `lookup.Create: Code: ALREADY_EXISTS`)
	// mismatch column count
	utils.AssertContainsError(t, conn, "insert into s_tbl(id, num) select 100,200,300", `Column count doesn't match value count at row 1`)
	utils.AssertContainsError(t, conn, "insert into s_tbl(id, num) select 100", `Column count doesn't match value count at row 1`)
}

func TestAutoIncInsertSelect(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from user_tbl`)

	utils.Exec(t, conn, "insert into user_tbl(region_id, name) values (1,'A'),(3,'C')")

	qr := utils.Exec(t, conn, "insert into user_tbl(region_id, name) select region_id, name from user_tbl")
	require.EqualValues(t, 2, qr.RowsAffected)
	require.EqualValues(t, 3, qr.InsertID)

	qr = utils.Exec(t, conn, "insert into user_tbl(id, region_id, name) select null, region_id, name from user_tbl where id = 1")
	require.EqualValues(t, 1, qr.RowsAffected)
	require.EqualValues(t, 5, qr.InsertID)

	qr = utils.Exec(t, conn, "insert into user_tbl(id, region_id, name) select 100, region_id, name from user_tbl where id = 1")
	require.EqualValues(t, 1, qr.RowsAffected)
	require.EqualValues(t, 0, qr.InsertID)

	utils.AssertMatches(t, conn, `select id from user_tbl order by id`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(100)]]`)
}

func TestUnownedVindexInsertSelect(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from order_tbl`)
	defer utils.Exec(t, conn, `delete from oevent_tbl`)

	utils.Exec(t, conn, "insert into order_tbl(oid, region_id) values (100,1),(300,2),(600,3),(700,4)")

	qr := utils.Exec(t, conn, "insert into oevent_tbl(oid, ename) select oid, 'dispatched' from order_tbl")
	require.EqualValues(t, 4, qr.RowsAffected)

	utils.Exec(t, conn, "use `sks/-80`")
	utils.AssertMatches(t, conn, `select count(*) from order_tbl o join oevent_tbl oe on o.oid = oe.oid`, `[[INT64(3)]]`)
	utils.Exec(t, conn, "use `sks/80-`")
	utils.AssertMatches(t, conn, `select count(*) from order_tbl o join oevent_tbl oe on o.oid = oe.oid`, `[[INT64(1)]]`)

	// resetting the target
	utils.Exec(t, conn, "use `sks`")

	// inserting non-existing record in order_tbl.
	utils.AssertContainsError(t, conn, "insert into oevent_tbl(oid, ename) select 1000, 'dispatched'", `could not map [INT64(1000)] to a keyspace id`)

	// id is the sharding column, oid is unknowned lookup which points to region_id,
	// the verify step should pass as we are inserting the same region_id to id
	qr = utils.Exec(t, conn, "insert into oextra_tbl(id, oid) select region_id, oid from order_tbl")
	require.EqualValues(t, 4, qr.RowsAffected)

	// mismatch value, verify step will fail. oid 100 is mapped to region_id 1, in this test trying to insert 100 with 2.
	utils.AssertContainsError(t, conn, "insert into oextra_tbl(id, oid) select 2, 100", `values [[INT64(100)]] for column [oid] does not map to keyspace ids`)
}
