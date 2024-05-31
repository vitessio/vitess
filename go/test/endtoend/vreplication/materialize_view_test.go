package vreplication

import (
	"fmt"
	"testing"
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
		PRIMARY KEY (oid)
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
	viewQuery := "select /*vt+ view=orders_view */ o.oid, c.cid, p.pid, o.mname, o.qty, o.price, c.name, p.description from orders o join customer c on o.cid = c.cid join product p on p.pid = o.pid;"
	tableSettings := fmt.Sprintf("[ {\"target_table\": \"orders_view\", \"source_expression\": \"%s\"  }]", viewQuery)

	output, err := vc.VtctldClient.ExecuteCommandWithOutput("materialize", "--workflow", workflow, "--target-keyspace", targetKeyspace,
		"create", "--source-keyspace", sourceKeyspace, "--table-settings", tableSettings, "--stop-after-copy=false")
	log.Infof("materialize output: %s", output)
	require.NoError(t, err, "Materialize")

	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflow), binlogdatapb.VReplicationWorkflowState_Running.String())

	rsMaterializedView := execVtgateQuery(t, vtgateConn, "product", "select * from orders_view")
	rsViewQuery := execVtgateQuery(t, vtgateConn, "product", viewQuery)
	diffResults(t, rsMaterializedView, rsViewQuery)

}

func diffResults(t *testing.T, rs1, rs2 *sqltypes.Result) {
	for i := range rs1.Rows {
		for j := range rs1.Rows[i] {
			if rs1.Rows[i][j].RawStr() != rs2.Rows[i][j].RawStr() {
				t.Fatalf("Row %d, Column %d: %v != %v", i, j, rs1.Rows[i][j].RawStr(), rs2.Rows[i][j].RawStr())
			}
		}
	}
}
