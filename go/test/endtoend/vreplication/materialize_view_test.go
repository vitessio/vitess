package vreplication

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

// select oid, c.cid, p.pid, mname, qty, price, name, description from orders o join customer c on o.cid = c.cid join product p on p.pid = o.pid;
const mvSchema = `
	CREATE TABLE orders_view (
		oid int NOT NULL,
		cid int NOT NULL,
		pid int NOT NULL,
		mname varchar(128) NOT NULL,
		qty int NOT NULL,
		price int NOT NULL,
		name varchar(128) NOT NULL,
		description varbinary(128) NOT NULL,
		PRIMARY KEY (oid),
		KEY idx_cid (cid),
		KEY idx_pid (pid)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

func TestMaterializeView(t *testing.T) {
	setSidecarDBName("_vt")
	// Don't create RDONLY tablets to reduce number of tablets created to reduce resource requirements for the test.
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	execVtgateQuery(t, vtgateConn, "product", mvSchema)

	workflow := "wf1"
	sourceKeyspace := "product"
	targetKeyspace := "product"
	viewQuery := "select /*vt+ view=orders_view */ orders.oid, customer.cid, product.pid, orders.mname, orders.qty, orders.price, customer.name, product.description from orders join customer on orders.cid = customer.cid join product on product.pid = orders.pid;"
	tableSettings := fmt.Sprintf("[ {\"target_table\": \"orders_view\", \"source_expression\": \"%s\"  }]", viewQuery)

	output, err := vc.VtctldClient.ExecuteCommandWithOutput("materialize", "--workflow", workflow, "--target-keyspace", targetKeyspace,
		"create", "--source-keyspace", sourceKeyspace, "--table-settings", tableSettings, "--stop-after-copy=false")
	log.Infof("materialize output: %s", output)
	require.NoError(t, err, "Materialize")

	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflow), binlogdatapb.VReplicationWorkflowState_Running.String())
	diffViews(t, vtgateConn, viewQuery, "After copy phase")

	queries := []string{
		"insert into orders (oid, cid, pid, mname, qty, price) values (11, 1, 1, 'mname1', 1, 1100)",
		"update orders set qty = 2 where oid = 11",
		"insert into orders (oid, cid, pid, mname, qty, price) values (12, 1, 1, 'mname1', 1, 1200)",
	}
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, "product", query)
	}
	time.Sleep(3 * time.Second)
	diffViews(t, vtgateConn, viewQuery, "After DMLs")

	queries = []string{
		"delete from orders where oid = 11",
	}
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, "product", query)
	}
	time.Sleep(3 * time.Second)
	diffViews(t, vtgateConn, viewQuery, "After Delete")

	queries = []string{
		"update orders set qty = 3 where oid = 12",
	}
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, "product", query)
	}
	time.Sleep(3 * time.Second)
	diffViews(t, vtgateConn, viewQuery, "After Update")
}

func diffViews(t *testing.T, vtgateConn *mysql.Conn, viewQuery, message string) {
	rsMaterializedView := execVtgateQuery(t, vtgateConn, "product", "select * from orders_view order by oid")
	rsViewQuery := execVtgateQuery(t, vtgateConn, "product", strings.Replace(viewQuery, ";", " order by oid;", 1))
	diffResults(t, rsViewQuery, rsMaterializedView, message)
}

func diffResults(t *testing.T, rs1, rs2 *sqltypes.Result, message string) {
	if len(rs1.Rows) != len(rs2.Rows) {
		t.Fatalf("%s:: Row count mismatch: %d != %d", message, len(rs1.Rows), len(rs2.Rows))
	}
	for i := range rs1.Rows {
		for j := range rs1.Rows[i] {
			if rs1.Rows[i][j].RawStr() != rs2.Rows[i][j].RawStr() {
				t.Fatalf("%s:: Row %d, Column %d: %v != %v", message, i, j, rs1.Rows[i][j].RawStr(), rs2.Rows[i][j].RawStr())
			}
		}
	}
}
