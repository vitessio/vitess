package plan_tests

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

func readJSONTests(filename string) []planbuilder.PlanTest {
	var output []planbuilder.PlanTest
	file, err := os.Open(locateFile(filename))
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(file)
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

func locateFile(name string) string {
	return "../../../../../vt/vtgate/planbuilder/testdata/" + name
}

func TestPlan(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	tests := readJSONTests("select_cases.json")
	for _, test := range tests {
		mcmp.Run(test.Query, func(mcmp *utils.MySQLCompare) {
			mcmp.Exec(test.Query)
		})
	}
}

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	keyspaceName    = "user"
	cell            = "test_aggr"
)

func readFile(filename string) string {
	schema, err := os.ReadFile(locateFile(filename))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return string(schema)
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	return mcmp, func() {
		mcmp.Close()
	}
}

func extractUserKS(jsonString string) string {
	var result map[string]any
	if err := json.Unmarshal([]byte(jsonString), &result); err != nil {
		panic(err.Error())
	}

	keyspaces, ok := result["keyspaces"].(map[string]any)
	if !ok {
		panic("Keyspaces not found")
	}

	user, ok := keyspaces["user"].(map[string]any)
	if !ok {
		panic("User keyspaces not found")
	}

	tables, ok := user["tables"].(map[string]any)
	if !ok {
		panic("Tables not found")
	}

	userTbl, ok := tables["user"].(map[string]any)
	if !ok {
		panic("User table not found")
	}

	delete(userTbl, "auto_increment") // TODO: we should have an unsharded keyspace where this could live

	// Marshal the inner part back to JSON string
	userJson, err := json.Marshal(user)
	if err != nil {
		panic(err.Error())
	}

	return string(userJson)
}

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)

	schemaSQL := readFile("vschemas/schema.sql")
	vschema := extractUserKS(readFile("vschemas/schema.json"))

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// TODO: (@GuptaManan100/@systay): Also run the tests with normalizer on.
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--normalize_queries=false",
			"--schema_change_signal=false",
		)

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(keyspaceName)

		// create mysql instance and connection parameters
		conn, closer, err := utils.NewMySQL(clusterInstance, keyspaceName, schemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		mysqlParams = conn

		return m.Run()
	}()
	os.Exit(exitCode)
}
