package vreplication

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

// select oid, c.cid, p.pid, mname, qty, price, name, description from orders o join customer c on o.cid = c.cid join product p on p.pid = o.pid;
const mvSchema = `
	CREATE TABLE orders_view (
		oid bigint NOT NULL,
		cid bigint NOT NULL,
		pid bigint NOT NULL,
		mname varchar(128) NOT NULL,
		qty int NOT NULL,
		price decimal(10,2) NOT NULL,
		name varchar(128) NOT NULL,
		description varchar(128) NOT NULL,
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
	tableSettings := "[ {\"target_table\": \"orders_view\", \"source_expression\": \"select oid, c.cid, p.pid, mname, qty, price, name, description from orders o join customer c on o.cid = c.cid join product p on p.pid = o.pid;\"  }]"

	output, err := vc.VtctldClient.ExecuteCommandWithOutput("materialize", "--workflow", workflow, "--target-keyspace", targetKeyspace,
		"create", "--source-keyspace", sourceKeyspace, "--table-settings", tableSettings, "--stop-after-copy=false")
	log.Infof("materialize output: %s", output)
	require.NoError(t, err, "Materialize")

}
