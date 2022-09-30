/*
Copyright 2022 The Vitess Authors.

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

package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestSimpleInsertSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into s_tbl(id, num) values (1,2),(3,4)")
	mcmp.Exec("insert into u_tbl(id, num) values (1,2),(3,4)")

	qr := mcmp.Exec("insert into s_tbl(id, num) select id*10, num*20 from s_tbl")
	assert.EqualValues(t, 2, qr.RowsAffected)
	qr = mcmp.Exec("insert into u_tbl(id, num) select id*10, num*20 from u_tbl")
	assert.EqualValues(t, 2, qr.RowsAffected)

	mcmp.AssertMatches(`select id, num from s_tbl order by id`, `[[INT64(1) INT64(2)] [INT64(3) INT64(4)] [INT64(10) INT64(40)] [INT64(30) INT64(80)]]`)
	mcmp.AssertMatches(`select id, num from u_tbl order by id`, `[[INT64(1) INT64(2)] [INT64(3) INT64(4)] [INT64(10) INT64(40)] [INT64(30) INT64(80)]]`)

	utils.AssertMatches(t, mcmp.VtConn, `select num from num_vdx_tbl order by num`, `[[INT64(2)] [INT64(4)] [INT64(40)] [INT64(80)]]`)
}

func TestFailureInsertSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into s_tbl(id, num) values (1,2),(3,4)")
	mcmp.Exec("insert into u_tbl(id, num) values (1,2),(3,4)")

	// primary key same
	mcmp.AssertContainsError("insert into s_tbl(id, num) select id, num*20 from s_tbl where id = 1", `AlreadyExists desc = Duplicate entry '1' for key`)
	// lookup key same (does not fail on MySQL as there is no lookup)
	utils.AssertContainsError(t, mcmp.VtConn, "insert into s_tbl(id, num) select id*20, num from s_tbl where id = 1", `lookup.Create: Code: ALREADY_EXISTS`)
	utils.Exec(t, mcmp.MySQLConn, "insert into s_tbl(id, num) select id*20, num from s_tbl where id = 1")
	// mismatch column count
	mcmp.AssertContainsError("insert into s_tbl(id, num) select 100,200,300", `Column count doesn't match value count at row 1`)
	mcmp.AssertContainsError("insert into s_tbl(id, num) select 100", `Column count doesn't match value count at row 1`)
}

func TestAutoIncInsertSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// comparison with MySQL ignored in this test as it tests auto increment

	utils.Exec(t, mcmp.VtConn, "insert into user_tbl(region_id, name) values (1,'A'),(3,'C')")

	qr := utils.Exec(t, mcmp.VtConn, "insert into user_tbl(region_id, name) select region_id, name from user_tbl")
	assert.EqualValues(t, 2, qr.RowsAffected)
	assert.EqualValues(t, 3, qr.InsertID)

	qr = utils.Exec(t, mcmp.VtConn, "insert into user_tbl(id, region_id, name) select null, region_id, name from user_tbl where id = 1")
	assert.EqualValues(t, 1, qr.RowsAffected)
	assert.EqualValues(t, 5, qr.InsertID)

	qr = utils.Exec(t, mcmp.VtConn, "insert into user_tbl(id, region_id, name) select 100, region_id, name from user_tbl where id = 1")
	assert.EqualValues(t, 1, qr.RowsAffected)
	assert.EqualValues(t, 0, qr.InsertID)

	utils.AssertMatches(t, mcmp.VtConn, `select id from user_tbl order by id`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(100)]]`)

	// auto-inc column as sharding column
	qr = utils.Exec(t, mcmp.VtConn, "insert into auto_tbl(id) select 10 union select null")
	assert.EqualValues(t, 2, qr.RowsAffected)
	assert.EqualValues(t, 666, qr.InsertID)

	qr = utils.Exec(t, mcmp.VtConn, "insert into auto_tbl(unq_col) select null")
	assert.EqualValues(t, 1, qr.RowsAffected)
	assert.EqualValues(t, 667, qr.InsertID)
}

func TestUnownedVindexInsertSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, "insert into order_tbl(oid, region_id) values (100,1),(300,2),(600,3),(700,4)")

	qr := utils.Exec(t, mcmp.VtConn, "insert into oevent_tbl(oid, ename) select oid, 'dispatched' from order_tbl")
	assert.EqualValues(t, 4, qr.RowsAffected)

	utils.Exec(t, mcmp.VtConn, "use `sks/-80`")
	utils.AssertMatches(t, mcmp.VtConn, `select count(*) from order_tbl o join oevent_tbl oe on o.oid = oe.oid`, `[[INT64(3)]]`)
	utils.Exec(t, mcmp.VtConn, "use `sks/80-`")
	utils.AssertMatches(t, mcmp.VtConn, `select count(*) from order_tbl o join oevent_tbl oe on o.oid = oe.oid`, `[[INT64(1)]]`)

	// resetting the target
	utils.Exec(t, mcmp.VtConn, "use `sks`")

	// inserting non-existing record in oevent_tbl.
	utils.AssertContainsError(t, mcmp.VtConn, "insert into oevent_tbl(oid, ename) select 1000, 'dispatched'", `could not map [INT64(1000)] to a keyspace id`)

	// id is the sharding column, oid is unknowned lookup which points to region_id,
	// the verify step should pass as we are inserting the same region_id to id
	qr = utils.Exec(t, mcmp.VtConn, "insert into oextra_tbl(id, oid) select region_id, oid from order_tbl")
	assert.EqualValues(t, 4, qr.RowsAffected)

	// mismatch value, verify step will fail. oid 100 is mapped to region_id 1, in this test trying to insert 100 with 2.
	utils.AssertContainsError(t, mcmp.VtConn, "insert into oextra_tbl(id, oid) select 2, 100", `values [[INT64(100)]] for column [oid] does not map to keyspace ids`)

	// null to oid is allowed so verify ksids will ignore it.
	utils.Exec(t, mcmp.VtConn, "insert into oextra_tbl(id, oid) select 5, null")
}

func TestLookupCasesIncInsertSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// all lookup vdx cols present
	mcmp.Exec("insert into auto_tbl(id, unq_col, nonunq_col) select 1,2,3")
	// unique lookup vdx cols present
	mcmp.Exec("insert into auto_tbl(id, nonunq_col) select 2,3")
	// non-unique lookup vdx cols present
	mcmp.Exec("insert into auto_tbl(id, unq_col) select 3,3")

	mcmp.AssertMatches(`select id, unq_col, nonunq_col from auto_tbl order by id`,
		`[[INT64(1) INT64(2) INT64(3)] [INT64(2) NULL INT64(3)] [INT64(3) INT64(3) NULL]]`)
	utils.AssertMatches(t, mcmp.VtConn, `select unq_col from unq_idx order by unq_col`,
		`[[INT64(2)] [INT64(3)]]`)
	utils.AssertMatches(t, mcmp.VtConn, `select nonunq_col, id from nonunq_idx order by nonunq_col, id`,
		`[[INT64(3) INT64(1)] [INT64(3) INT64(2)]]`)
}

func TestIgnoreInsertSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into order_tbl(region_id, oid, cust_no) values (1,1,100),(1,2,200),(1,3,300)")

	// inserting same rows, throws error.
	mcmp.AssertContainsError("insert into order_tbl(region_id, oid, cust_no) select region_id, oid, cust_no from order_tbl", `lookup.Create: Code: ALREADY_EXISTS`)
	// inserting same rows with ignore
	qr := mcmp.Exec("insert ignore into order_tbl(region_id, oid, cust_no) select region_id, oid, cust_no from order_tbl")
	assert.EqualValues(t, 0, qr.RowsAffected)
	mcmp.AssertMatches("select count(*) from order_tbl", `[[INT64(3)]]`)

	// inserting row with ignore with cust_no non-unique
	qr = mcmp.Exec("insert ignore into order_tbl(region_id, oid, cust_no) select 1, 4, 100 from order_tbl")
	assert.EqualValues(t, 0, qr.RowsAffected)

	// inserting row with ignore with cust_no unique
	qr = mcmp.Exec("insert ignore into order_tbl(region_id, oid, cust_no) select 1, 4, 400 from order_tbl")
	assert.EqualValues(t, 1, qr.RowsAffected)
	mcmp.AssertMatches("select count(*) from order_tbl", `[[INT64(4)]]`)

	mcmp.AssertMatches("select oid, cust_no from order_tbl where region_id = 1 order by oid", `[[INT64(1) INT64(100)] [INT64(2) INT64(200)] [INT64(3) INT64(300)] [INT64(4) INT64(400)]]`)

	// inserting row with on dup with cust_no non-unique
	qr = utils.Exec(t, mcmp.VtConn, "insert into order_tbl(region_id, oid, cust_no) select region_id, oid, cust_no from order_tbl where oid = 4 on duplicate key update cust_no = cust_no + 1")
	assert.EqualValues(t, 2, qr.RowsAffected)
	utils.AssertMatches(t, mcmp.VtConn, "select count(*) from order_tbl", `[[INT64(4)]]`)

	utils.AssertMatches(t, mcmp.VtConn, "select oid, cust_no from order_tbl order by oid", `[[INT64(1) INT64(100)] [INT64(2) INT64(200)] [INT64(3) INT64(300)] [INT64(4) INT64(401)]]`)

	// inserting on dup trying to update vindex throws error.
	utils.AssertContainsError(t, mcmp.VtConn, "insert into order_tbl(region_id, oid, cust_no) select 1, 10, 1000 on duplicate key update region_id = region_id + 1", `unsupported: DML cannot change vindex column`)
	utils.AssertContainsError(t, mcmp.VtConn, "insert into order_tbl(region_id, oid, cust_no) select 1, 10, 1000 on duplicate key update oid = oid + 100", `unsupported: DML cannot change vindex column`)
}

func TestInsertSelectUnshardedUsingSharded(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into s_tbl(id, num) values (1,2),(3,4)")

	qr := mcmp.Exec("insert into u_tbl(id, num) select id, num from s_tbl where s_tbl.id in (1,3)")
	assert.EqualValues(t, 2, qr.RowsAffected)
	mcmp.AssertMatches(`select id, num from u_tbl order by id`, `[[INT64(1) INT64(2)] [INT64(3) INT64(4)]]`)
}
