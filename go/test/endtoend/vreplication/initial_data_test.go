/*
Copyright 2023 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/log"
)

func insertInitialData(t *testing.T) {
	t.Run("insertInitialData", func(t *testing.T) {
		vtgateConn, closeConn := getVTGateConn()
		defer closeConn()
		log.Infof("Inserting initial data")
		lines, _ := os.ReadFile("unsharded_init_data.sql")
		execMultipleQueries(t, vtgateConn, "product:0", string(lines))
		execVtgateQuery(t, vtgateConn, "product:0", "insert into customer_seq(id, next_id, cache) values(0, 100, 100);")
		execVtgateQuery(t, vtgateConn, "product:0", "insert into order_seq(id, next_id, cache) values(0, 100, 100);")
		execVtgateQuery(t, vtgateConn, "product:0", "insert into customer_seq2(id, next_id, cache) values(0, 100, 100);")
		log.Infof("Done inserting initial data")

		waitForRowCount(t, vtgateConn, "product:0", "product", 2)
		waitForRowCount(t, vtgateConn, "product:0", "customer", 3)
		waitForQueryResult(t, vtgateConn, "product:0", "select * from merchant",
			`[[VARCHAR("Monoprice") VARCHAR("eléctronics")] [VARCHAR("newegg") VARCHAR("elec†ronics")]]`)

		insertJSONValues(t)
	})
}

const NumJSONRows = 100

func insertJSONValues(t *testing.T) {
	// insert null value combinations
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j3) values(1, \"{}\")")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j1, j3) values(2, \"{}\", \"{}\")")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j2, j3) values(3, \"{}\", \"{}\")")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j1, j2, j3) values(4, NULL, 'null', '\"null\"')")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j3) values(5, JSON_QUOTE('null'))")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j3) values(6, '{}')")

	id := 8 // 6 inserted above and one after copy phase is done

	q := "insert into json_tbl(id, j1, j2, j3) values(%d, '%s', '%s', '{}')"
	numJsonValues := len(jsonValues)
	for id <= NumJSONRows {
		id++
		j1 := rand.IntN(numJsonValues)
		j2 := rand.IntN(numJsonValues)
		query := fmt.Sprintf(q, id, jsonValues[j1], jsonValues[j2])
		execVtgateQuery(t, vtgateConn, "product:0", query)
	}
}

// insertMoreCustomers creates additional customers.
// Note: this will only work when the customer sequence is in place.
func insertMoreCustomers(t *testing.T, numCustomers int) {
	// Let's first be sure that the sequence is working.
	// We reserve all of the sequence values we need for
	// the number of customer records we are going to
	// create. The value we get back is the max value
	// that we reserved.
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	maxID := waitForSequenceValue(t, vtgateConn, "product", "customer_seq", numCustomers)
	// So we need to calculate the first value we reserved
	// from the max.
	cid := maxID - int64(numCustomers)

	// Now let's insert the records using the sequence
	// values we reserved.
	sql := "insert into customer (cid, name) values "
	for i := 1; i <= numCustomers; i++ {
		sql += fmt.Sprintf("(%d, 'customer%d')", cid, i)
		if i != numCustomers {
			sql += ","
		}
		cid++
	}
	execVtgateQuery(t, vtgateConn, "customer", sql)
}

func insertMoreProducts(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	sql := "insert into product(pid, description) values(3, 'cpu'),(4, 'camera'),(5, 'mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

func insertMoreProductsForSourceThrottler(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	sql := "insert into product(pid, description) values(103, 'new-cpu'),(104, 'new-camera'),(105, 'new-mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

func insertMoreProductsForTargetThrottler(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	sql := "insert into product(pid, description) values(203, 'new-cpu'),(204, 'new-camera'),(205, 'new-mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

var blobTableQueries = []string{
	"insert into `blüb_tbl`(id, val1, txt1) values (1, 'Jøhn \"❤️\" Paül','Jøhn \"❤️\" Paül keyböard ⌨️ jo˙n')",
	"insert into `blüb_tbl`(id, val1, `blöb1`, `bl@b2`) values (2, 'val1_aaa', 'blb1_aaa', 'blb2_AAAA')",
	"update `blüb_tbl` set val1 = 'val1_bbb', `bl@b2` = 'blb2_bbb' where id = 1",
	"insert into `blüb_tbl`(id, val2, txt1, txt2, blb4) values (3, 'val2_ccc', 'txt1_ccc', 'txt2_ccc', 'blb4_CCC')",
	"update `blüb_tbl` set txt1 = 'txt1_ddd'",
	"update `blüb_tbl` set blb3 = 'blb3_eee'",
	"delete from `blüb_tbl` where id = 2",
	"insert into `blüb_tbl`(id, val2, txt1, txt2, blb4) values (4, 'val2_fff', 'txt1_fff', 'txt2_fff', 'blb4_FFF')",
	"update `blüb_tbl` set txt1 = 'txt1_eee', blb3 = 'blb3_eee' where id = 4",
}

func insertIntoBlobTable(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	for _, query := range blobTableQueries {
		execVtgateQuery(t, vtgateConn, "product:0", query)
	}
}
