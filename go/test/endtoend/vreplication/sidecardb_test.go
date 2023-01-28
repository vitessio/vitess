package vreplication

import (
	"fmt"
	"strconv"
	"testing"

	"vitess.io/vitess/go/vt/sidecardb"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func getSidecarDBTables(t *testing.T, tabletID string) (numTablets int, tables []string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", "--", "--json", tabletID, sidecardb.GetCurrentTablesQuery)
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

var sidecarDBTables []string
var numSidecarDBTables int
var ddls1, ddls2 []string

func init() {
	sidecarDBTables = []string{"copy_state", "dt_participant", "dt_state", "heartbeat", "post_copy_action", "redo_state",
		"redo_statement", "reparent_journal", "resharding_journal", "schema_migrations", "schema_version", "schemacopy",
		"vdiff", "vdiff_log", "vdiff_table", "views", "vreplication", "vreplication_log"}
	numSidecarDBTables = len(sidecarDBTables)
	ddls1 = []string{
		"drop table _vt.vreplication_log",
		"alter table _vt.vreplication drop column defer_secondary_keys",
	}
	ddls2 = []string{
		"alter table _vt.vreplication modify column defer_secondary_keys boolean default false",
	}
}

func prs(t *testing.T, keyspace, shard string) {
	_, err := vc.VtctldClient.ExecuteCommandWithOutput("PlannedReparentShard", "--", fmt.Sprintf("%s/%s", keyspace, shard))
	require.NoError(t, err)

}

// TestSidecarDB launches a Vitess cluster and ensures that the expected sidecar tables are created. We also drop/alter
// tables and ensure the next tablet init will recreate the sidecar database to the desired schema.
func TestSidecarDB(t *testing.T) {
	cells := []string{"zone1"}

	vc = NewVitessCluster(t, "TestSidecarDB", cells, mainClusterConfig)
	require.NotNil(t, vc)
	allCellNames = "zone1"
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown(t)

	keyspace := "product"
	shard := "0"

	cell1 := vc.Cells[defaultCellName]
	tablet100ID := fmt.Sprintf("%s-100", defaultCellName)
	tablet101ID := fmt.Sprintf("%s-101", defaultCellName)
	vc.AddKeyspace(t, []*Cell{cell1}, keyspace, shard, initialProductVSchema, initialProductSchema, 1, 0, 100, sourceKsOpts)
	shard0 := vc.Cells[defaultCellName].Keyspaces[keyspace].Shards[shard]
	tablet100Port := shard0.Tablets[tablet100ID].Vttablet.Port
	tablet101Port := shard0.Tablets[tablet101ID].Vttablet.Port
	currentPrimary := tablet100ID

	validateSidecarDBTables(t, tablet100ID, sidecarDBTables)
	validateSidecarDBTables(t, tablet101ID, sidecarDBTables)
	expectedChanges100 := len(sidecarDBTables)
	expectedChanges101 := 0
	require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)

	numChanges := modifySidecarDBSchema(t, vc, currentPrimary, ddls1)

	validateSidecarDBTables(t, tablet100ID, sidecarDBTables[0:numSidecarDBTables-1])
	validateSidecarDBTables(t, tablet101ID, sidecarDBTables[0:numSidecarDBTables-1])

	prs(t, keyspace, shard)
	currentPrimary = tablet101ID
	expectedChanges100 += numChanges
	validateSidecarDBTables(t, tablet100ID, sidecarDBTables)
	validateSidecarDBTables(t, tablet101ID, sidecarDBTables)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)

	numChanges = modifySidecarDBSchema(t, vc, currentPrimary, ddls1)
	expectedChanges101 += numChanges
	prs(t, keyspace, shard)
	// nolint
	currentPrimary = tablet100ID

	validateSidecarDBTables(t, tablet100ID, sidecarDBTables)
	validateSidecarDBTables(t, tablet101ID, sidecarDBTables)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet100Port), expectedChanges100)
	require.Equal(t, getNumExecutedDDLQueries(t, tablet101Port), expectedChanges101)
}

func validateSidecarDBTables(t *testing.T, tabletID string, tables []string) {
	_, tables2 := getSidecarDBTables(t, tabletID)
	require.EqualValues(t, tables, tables2)
}

func modifySidecarDBSchema(t *testing.T, vc *VitessCluster, tabletID string, ddls []string) (numChanges int) {
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
