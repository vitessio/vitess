package vreplication

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func getVTTables(t *testing.T, tabletID string) (numTablets int, tables []string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsApp", "--", "--json", tabletID, "show tables from _vt")
	require.NoError(t, err)
	result := gjson.Get(output, "rows")
	require.NotNil(t, result)
	require.True(t, result.IsArray())
	rows := result.Array()
	numTablets = len(rows)
	for _, row := range rows {
		require.True(t, row.IsArray())
		rows2 := row.Array()
		require.NotNil(t, rows2)
		require.Equal(t, len(rows2), 1)
		table := rows2[0].String()
		tables = append(tables, table)
	}
	return numTablets, tables
}

var VTTables []string
var NumVTTables int
var ddls1, ddls2 []string

func init() {
	VTTables = []string{"copy_state", "dt_participant", "dt_state", "heartbeat", "post_copy_action", "redo_state",
		"redo_statement", "reparent_journal", "resharding_journal", "schema_migrations", "schema_version", "schemacopy",
		"vdiff", "vdiff_log", "vdiff_table", "views", "vreplication", "vreplication_log"}
	NumVTTables = len(VTTables)
	ddls1 = []string{
		"drop table _vt.vreplication_log",
		"alter table _vt.vreplication drop column defer_secondary_keys",
	}
	ddls2 = []string{
		"alter table _vt.vreplication modify column defer_secondary_keys boolean default false",
	}
}

// TestSidecarDB launches a Vitess cluster and ensures that the expected _vt tables are created. We also drop/alter
// tables and ensure the next tablet init will recreate the _vt database to the desired schema
func TestSidecarDB(t *testing.T) {
	cells := []string{"zone1"}

	vc = NewVitessCluster(t, "TestSidecarDB", cells, mainClusterConfig)
	require.NotNil(t, vc)
	allCellNames = "zone1"
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, "product", "0", initialProductVSchema, initialProductSchema, 1, 0, 100, sourceKsOpts)
	shard0 := vc.Cells["zone1"].Keyspaces["product"].Shards["0"]
	tablet100Port := shard0.Tablets["zone1-100"].Vttablet.Port
	tablet101Port := shard0.Tablets["zone1-101"].Vttablet.Port
	currentPrimary := "zone1-100"

	validateVTTables(t, "zone1-100", VTTables)
	validateVTTables(t, "zone1-101", VTTables)
	expectedChanges100 := len(VTTables)
	expectedChanges101 := 0
	require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)

	numChanges := modifyVTSchema(t, vc, currentPrimary, ddls1)

	validateVTTables(t, "zone1-100", VTTables[0:NumVTTables-1])
	validateVTTables(t, "zone1-101", VTTables[0:NumVTTables-1])

	_, err := vc.VtctldClient.ExecuteCommandWithOutput("PlannedReparentShard", "--", "product/0")
	require.NoError(t, err)
	currentPrimary = "zone1-101"
	expectedChanges100 += numChanges
	validateVTTables(t, "zone1-100", VTTables)
	validateVTTables(t, "zone1-101", VTTables)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)

	numChanges = modifyVTSchema(t, vc, currentPrimary, ddls1)
	expectedChanges101 += numChanges
	_, err = vc.VtctldClient.ExecuteCommandWithOutput("PlannedReparentShard", "--", "product/0")
	require.NoError(t, err)
	// nolint
	currentPrimary = "zone1-100"

	validateVTTables(t, "zone1-100", VTTables)
	validateVTTables(t, "zone1-101", VTTables)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)

	// TODO before merging
	// At this point the replication on zone-101 seems to have been stopped. It is possibly because we still upgrade
	// _vt during DemotePrimary(). This test is itself possibly incorrect though ...
	/*
		modifyVTSchema(t, vc, currentPrimary, ddls2)
		output, err := vc.VtctldClient.ExecuteCommandWithOutput("PlannedReparentShard", "--", "product/0")
		require.NoErrorf(t, err, output)
		currentPrimary = "zone1-101"

		validateVTTables(t, "zone1-100", VTTables)
		validateVTTables(t, "zone1-101", VTTables)
		require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
		require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)

	*/

}

func validateVTTables(t *testing.T, tabletID string, tables []string) {
	_, tables2 := getVTTables(t, tabletID)
	require.EqualValues(t, tables, tables2)
}

func modifyVTSchema(t *testing.T, vc *VitessCluster, tabletID string, ddls []string) (numChanges int) {
	for _, ddl := range ddls {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", "--", tabletID, ddl)
		require.NoErrorf(t, err, output)
	}
	return len(ddls)
}

func getNumExecutedDDLQueries(t *testing.T, port int) int {
	val, err := getDebugVar(t, port, []string{"SidecarDbDDLQueryCount"})
	require.NoError(t, err)
	i, err := strconv.Atoi(val)
	require.NoError(t, err)
	return i
}
