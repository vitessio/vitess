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

func TestMultiEqual(t *testing.T) {
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("test uses multiple keyspaces, test framework only supports partial keyspace testing for a single keyspace")
	}
	mcmp, closer := start(t)
	defer closer()

	// initial rows
	mcmp.Exec("insert into user_tbl(id, region_id) values (1,2),(3,4)")

	// multi equal query
	qr := mcmp.Exec("update user_tbl set id = 2 where (id, region_id) in ((1,1), (3,4))")
	assert.EqualValues(t, 1, qr.RowsAffected) // multi equal query

	qr = mcmp.Exec("delete from user_tbl where (id, region_id) in ((1,1), (2,4))")
	assert.EqualValues(t, 1, qr.RowsAffected)
}

// TestMultiTableDelete executed multi-table delete queries
func TestMultiTableDelete(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")

	mcmp, closer := start(t)
	defer closer()

	// initial rows
	mcmp.Exec("insert into order_tbl(region_id, oid, cust_no) values (1,1,4), (1,2,2), (2,3,5), (2,4,55)")
	mcmp.Exec("insert into oevent_tbl(oid, ename) values (1,'a'), (2,'b'), (3,'a'), (4,'c')")

	// check rows
	mcmp.AssertMatches(`select region_id, oid, cust_no from order_tbl order by oid`,
		`[[INT64(1) INT64(1) INT64(4)] [INT64(1) INT64(2) INT64(2)] [INT64(2) INT64(3) INT64(5)] [INT64(2) INT64(4) INT64(55)]]`)
	mcmp.AssertMatches(`select oid, ename from oevent_tbl order by oid`,
		`[[INT64(1) VARCHAR("a")] [INT64(2) VARCHAR("b")] [INT64(3) VARCHAR("a")] [INT64(4) VARCHAR("c")]]`)

	// multi table delete
	qr := mcmp.Exec(`delete o from order_tbl o join oevent_tbl ev where o.oid = ev.oid and ev.ename = 'a'`)
	assert.EqualValues(t, 2, qr.RowsAffected)

	// check rows
	mcmp.AssertMatches(`select region_id, oid, cust_no from order_tbl order by oid`,
		`[[INT64(1) INT64(2) INT64(2)] [INT64(2) INT64(4) INT64(55)]]`)
	mcmp.AssertMatches(`select oid, ename from oevent_tbl order by oid`,
		`[[INT64(1) VARCHAR("a")] [INT64(2) VARCHAR("b")] [INT64(3) VARCHAR("a")] [INT64(4) VARCHAR("c")]]`)

	qr = mcmp.Exec(`delete o from order_tbl o join oevent_tbl ev where o.cust_no = ev.oid`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// check rows
	mcmp.AssertMatches(`select region_id, oid, cust_no from order_tbl order by oid`,
		`[[INT64(2) INT64(4) INT64(55)]]`)
	mcmp.AssertMatches(`select oid, ename from oevent_tbl order by oid`,
		`[[INT64(1) VARCHAR("a")] [INT64(2) VARCHAR("b")] [INT64(3) VARCHAR("a")] [INT64(4) VARCHAR("c")]]`)
}
