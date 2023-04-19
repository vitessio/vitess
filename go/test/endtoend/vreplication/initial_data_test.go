package vreplication

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/log"
)

func insertInitialData(t *testing.T) {
	t.Run("insertInitialData", func(t *testing.T) {
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
		insertIntoBlobTable(t)
	})
}

const NumJSONRows = 100

func insertJSONValues(t *testing.T) {
	// insert null value combinations
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id) values(1)")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j1) values(2, \"{}\")")
	execVtgateQuery(t, vtgateConn, "product:0", "insert into json_tbl(id, j2) values(3, \"{}\")")

	id := 4
	q := "insert into json_tbl(id, j1, j2) values(%d, '%s', '%s')"
	numJsonValues := len(jsonValues)
	for id <= NumJSONRows {
		id++
		j1 := rand.Intn(numJsonValues)
		j2 := rand.Intn(numJsonValues)
		query := fmt.Sprintf(q, id, jsonValues[j1], jsonValues[j2])
		execVtgateQuery(t, vtgateConn, "product:0", query)
	}
}

// insertMoreCustomers creates additional customers.
// Note: this will only work when the customer sequence is in place.
func insertMoreCustomers(t *testing.T, numCustomers int) {
	sql := "insert into customer (name) values "
	i := 0
	for i < numCustomers {
		i++
		sql += fmt.Sprintf("('customer%d')", i)
		if i != numCustomers {
			sql += ","
		}
	}
	execVtgateQuery(t, vtgateConn, "customer", sql)
}

func insertMoreProducts(t *testing.T) {
	sql := "insert into product(pid, description) values(3, 'cpu'),(4, 'camera'),(5, 'mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

func insertMoreProductsForSourceThrottler(t *testing.T) {
	sql := "insert into product(pid, description) values(103, 'new-cpu'),(104, 'new-camera'),(105, 'new-mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

func insertMoreProductsForTargetThrottler(t *testing.T) {
	sql := "insert into product(pid, description) values(203, 'new-cpu'),(204, 'new-camera'),(205, 'new-mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

// create table blob_tbl (id int, val1 varchar(20), blb1 blob, val2 varbinary(20), blb2 longblob, txt1 text, blb3 tinyblob, txt2 longtext, blb4 mediumblob, primary key(id));
var blobTableQueries = []string{
	"insert into blob_tbl(id, val1, txt1) values (1, '   ', '                ')",
	"insert into blob_tbl(id, val1, blb1, blb2) values (2, 'val1_aaa', 'blb1_aaaaaaaaaa', 'blb2_AAAAAAAAAAAAAAA')",
	"update blob_tbl set val1 = 'val1_uuuuu', blb2 = 'blb2_uuuuuuuuuuuu' where id = 1",
	"insert into blob_tbl(id, val2, txt1, txt2, blb4) values (3, 'val2_bbbbbb', 'txt1_bbbbbbbbbbb', 'txt2_bbbbbbbbbbb', 'blb4_BBBBBBBBBBBBBBBBBB')",
	"update blob_tbl set txt1 = 'txt1_wwwwwwwwww', blb3 = 'blb3_wwwwwwwwww'",
}

func insertIntoBlobTable(t *testing.T) {
	for _, query := range blobTableQueries {
		execVtgateQuery(t, vtgateConn, "product:0", query)
	}
}
