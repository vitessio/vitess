package vreplication

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestTZ(t *testing.T) {
	allCellNames = "zone1"
	defaultCellName := "zone1"
	vc = NewVitessCluster(t, "TestCellAliasVreplicationWorkflow", []string{"zone1"}, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCell = vc.Cells[defaultCellName]
	cells := []*Cell{defaultCell}

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, "product", "0", initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	// Add cell alias containing only zone2
	result, err := vc.VtctlClient.ExecuteCommandWithOutput("AddCellsAlias", "--", "--cells", "zone2", "alias")
	require.NoError(t, err, "command failed with output: %v", result)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	timeZoneSQL, _ := os.ReadFile("tz.sql")
	_, err = productTab.QueryTabletWithDB(string(timeZoneSQL), "mysql")
	require.NoError(t, err)
	time.Sleep(5 * time.Second) // todo: replace sleep with actual check
	_, err = productTab.QueryTablet("SET GLOBAL time_zone = 'US/Pacific';", "", false)
	require.NoError(t, err)

	insertInitialData(t)

	workflow := "tz"
	sourceKs := "product"
	targetKs := "customer"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	if _, err := vc.AddKeyspace(t, cells, "customer", "0", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200, targetKsOpts); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "0"), 1); err != nil {
		t.Fatal(err)
	}

	defaultCell := vc.Cells["zone1"]
	custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
	customerTab := custKs.Shards["0"].Tablets["zone1-200"].Vttablet

	customerTab.QueryTabletWithDB(string(timeZoneSQL), "mysql")
	require.NoError(t, err)
	time.Sleep(5 * time.Second) // todo: replace sleep with actual check

	tables := "datze"
	moveTables(t, "zone1", workflow, sourceKs, targetKs, tables)

	catchup(t, customerTab, workflow, "MoveTables")

	vdiff(t, ksWorkflow, "")

	query := "select * from datze"
	qr, err := productTab.QueryTablet(query, "product", true)
	require.NoError(t, err)
	require.NotNil(t, qr)
	log.Infof("Product US/Pacific:\n")
	for _, row := range qr.Rows {
		log.Infof("%+v", row)
	}

	qr, err = customerTab.QueryTablet(query, "customer", true)
	require.NoError(t, err)
	require.NotNil(t, qr)
	log.Infof("Customer UTC:\n")
	for _, row := range qr.Rows {
		log.Infof("%+v", row)
	}

	setStatement := "set session time_zone='US/Pacific'"
	query = "select id, convert_tz(dt1, 'UTC', 'US/Pacific') dt1, convert_tz(dt2, 'UTC', 'US/Pacific') dt2, ts1 from datze"
	qr, err = customerTab.QueryTabletWithSet(query, "customer", true, setStatement)
	require.NoError(t, err)
	require.NotNil(t, qr)
	log.Infof("Customer US/Pacific:\n")
	for _, row := range qr.Rows {
		log.Infof("%+v", row)
	}
}
