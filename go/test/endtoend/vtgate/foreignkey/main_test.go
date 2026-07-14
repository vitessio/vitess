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

package foreignkey

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	clusterInstance      *vitesst.Cluster
	vtParams             mysql.ConnParams
	mysqlParams          mysql.ConnParams
	shardedKs            = "ks"
	shardScopedKs        = "sks"
	unshardedKs          = "uks"
	unshardedUnmanagedKs = "unmanaged_uks"

	//go:embed schema.sql
	schemaSQL string

	//go:embed sharded_vschema.json
	shardedVSchema string

	//go:embed shard_scoped_vschema.json
	shardScopedVSchema string

	//go:embed unsharded_vschema.json
	unshardedVSchema string

	//go:embed unsharded_unmanaged_vschema.json
	unshardedUnmanagedVSchema string

	fkTables = []string{
		"fk_t1", "fk_t2", "fk_t3", "fk_t4", "fk_t5", "fk_t6", "fk_t7",
		"fk_t10", "fk_t11", "fk_t12", "fk_t13", "fk_t15", "fk_t16", "fk_t17", "fk_t18", "fk_t19", "fk_t20",
		"fk_multicol_t1", "fk_multicol_t2", "fk_multicol_t3", "fk_multicol_t4", "fk_multicol_t5", "fk_multicol_t6", "fk_multicol_t7",
		"fk_multicol_t10", "fk_multicol_t11", "fk_multicol_t12", "fk_multicol_t13", "fk_multicol_t15", "fk_multicol_t16", "fk_multicol_t17", "fk_multicol_t18", "fk_multicol_t19",
	}
	fkReferences = []fkReference{
		{parentTable: "fk_t1", childTable: "fk_t2"},
		{parentTable: "fk_t2", childTable: "fk_t7"},
		{parentTable: "fk_t2", childTable: "fk_t3"},
		{parentTable: "fk_t3", childTable: "fk_t4"},
		{parentTable: "fk_t3", childTable: "fk_t6"},
		{parentTable: "fk_t4", childTable: "fk_t5"},
		{parentTable: "fk_t10", childTable: "fk_t11"},
		{parentTable: "fk_t11", childTable: "fk_t12"},
		{parentTable: "fk_t11", childTable: "fk_t13"},
		{parentTable: "fk_t15", childTable: "fk_t16"},
		{parentTable: "fk_t16", childTable: "fk_t17"},
		{parentTable: "fk_t17", childTable: "fk_t18"},
		{parentTable: "fk_t17", childTable: "fk_t19"},
		{parentTable: "fk_multicol_t1", childTable: "fk_multicol_t2"},
		{parentTable: "fk_multicol_t2", childTable: "fk_multicol_t7"},
		{parentTable: "fk_multicol_t2", childTable: "fk_multicol_t3"},
		{parentTable: "fk_multicol_t3", childTable: "fk_multicol_t4"},
		{parentTable: "fk_multicol_t3", childTable: "fk_multicol_t6"},
		{parentTable: "fk_multicol_t4", childTable: "fk_multicol_t5"},
		{parentTable: "fk_multicol_t10", childTable: "fk_multicol_t11"},
		{parentTable: "fk_multicol_t11", childTable: "fk_multicol_t12"},
		{parentTable: "fk_multicol_t11", childTable: "fk_multicol_t13"},
		{parentTable: "fk_multicol_t15", childTable: "fk_multicol_t16"},
		{parentTable: "fk_multicol_t16", childTable: "fk_multicol_t17"},
		{parentTable: "fk_multicol_t17", childTable: "fk_multicol_t18"},
		{parentTable: "fk_multicol_t17", childTable: "fk_multicol_t19"},
	}
)

const (
	// mysqlVersion is the MySQL version the cluster and the comparison mysqld run.
	mysqlVersion = "8.4"

	// extraMyCnfPath is where extra_my.cnf lands inside the containers whose
	// mysqld reads it.
	extraMyCnfPath = "/vt/files/extra_my.cnf"
)

// fkReference stores a foreign key reference from one table to another.
type fkReference struct {
	parentTable string
	childTable  string
}

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithMySQLVersion(mysqlVersion),
			vitesst.WithTabletFiles(vitesst.ContainerFile{
				HostPath:      "extra_my.cnf",
				ContainerPath: extraMyCnfPath,
			}),
			vitesst.WithTabletEnv(map[string]string{"EXTRA_MY_CNF": extraMyCnfPath}),
			vitesst.WithKeyspace(shardedKs).
				WithShardNames("-80", "80-").
				WithReplicas(1).
				WithSchema(schemaSQL).
				WithVSchema(shardedVSchema),
			vitesst.WithKeyspace(shardScopedKs).
				WithShardNames("-80", "80-").
				WithReplicas(1).
				WithSchema(schemaSQL).
				WithVSchema(shardScopedVSchema),
			vitesst.WithKeyspace(unshardedKs).
				WithShardNames("0").
				WithReplicas(1).
				WithSchema(schemaSQL).
				WithVSchema(unshardedVSchema),
			vitesst.WithKeyspace(unshardedUnmanagedKs).
				WithShardNames("0").
				WithReplicas(1).
				WithSchema(schemaSQL).
				WithVSchema(unshardedUnmanagedVSchema),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		if err = cluster.Vtctld().ExecuteCommand(ctx, "RebuildVSchemaGraph"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")

		connParams, closer, err := newComparisonMySQL(ctx, cluster, shardedKs, schemaSQL)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := closer(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "comparison mysqld teardown:", err)
			}
		}()
		mysqlParams = connParams
		return m.Run()
	}()
	os.Exit(exitCode)
}

// newComparisonMySQL starts the standalone mysqld the tests compare Vitess
// against. It reads extra_my.cnf, so foreign keys that reference non-standard
// keys are accepted, and it applies the given schema to the database.
func newComparisonMySQL(ctx context.Context, cluster *vitesst.Cluster, dbName string, schema string) (mysql.ConnParams, func(context.Context) error, error) {
	const (
		mysqlUID    = 1
		mysqlPort   = 3306
		dataRoot    = "/vt/vtdataroot"
		initPath    = "/vt/files/init_compare.sql"
		dbaUser     = "vt_dba"
		startupWait = 3 * time.Minute
	)

	initSQL := fmt.Sprintf(`SET GLOBAL super_read_only='OFF';
CREATE DATABASE IF NOT EXISTS %s;
CREATE USER '%s'@'%%';
GRANT ALL ON *.* TO '%s'@'%%' WITH GRANT OPTION;
`, dbName, dbaUser, dbaUser)

	socket := fmt.Sprintf("%s/vt_%010d/mysql.sock", dataRoot, mysqlUID)
	script := fmt.Sprintf(
		"mysqlctl --tablet-uid %d --mysql-port %d --log-format text init --init-db-sql-file %s && sleep infinity",
		mysqlUID, mysqlPort, initPath,
	)
	probe := []string{"mysql", "--socket", socket, "-u", dbaUser, "-e", "SELECT 1"}

	ctr, err := testcontainers.Run(ctx, vitesst.Image(mysqlVersion),
		testcontainers.WithEntrypoint("bash", "-c", script),
		testcontainers.WithEnv(map[string]string{"EXTRA_MY_CNF": extraMyCnfPath}),
		testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", mysqlPort)),
		testcontainers.WithTmpfs(map[string]string{dataRoot: "uid=999,gid=999"}),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{HostFilePath: "extra_my.cnf", ContainerFilePath: extraMyCnfPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(initSQL), ContainerFilePath: initPath, FileMode: 0o644},
		),
		testcontainers.WithWaitStrategyAndDeadline(startupWait,
			wait.ForExec(probe).
				WithStartupTimeout(startupWait).
				WithPollInterval(100*time.Millisecond),
		),
	)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	cleanup := func(ctx context.Context) error {
		return testcontainers.TerminateContainer(ctr, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0))
	}

	fail := func(err error) (mysql.ConnParams, func(context.Context) error, error) {
		_ = cleanup(ctx)
		return mysql.ConnParams{}, nil, err
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return fail(err)
	}
	mapped, err := ctr.MappedPort(ctx, fmt.Sprintf("%d/tcp", mysqlPort))
	if err != nil {
		return fail(err)
	}

	params := mysql.ConnParams{
		Host:   host,
		Port:   int(mapped.Num()),
		Uname:  dbaUser,
		DbName: dbName,
	}
	if err = applyComparisonSchema(ctx, params, schema); err != nil {
		return fail(err)
	}
	return params, cleanup, nil
}

// applyComparisonSchema applies the schema statements to the comparison mysqld.
func applyComparisonSchema(ctx context.Context, params mysql.ConnParams, schema string) error {
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return err
	}
	defer conn.Close()

	statements, err := sqlparser.NewTestParser().SplitStatementToPieces(schema)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := conn.ExecuteFetch(statement, 1, false); err != nil {
			return err
		}
	}
	return nil
}

func start(t *testing.T) (vitesst.MySQLCompare, func()) {
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		clearOutAllData(t, mcmp.VtConn, mcmp.MySQLConn)
		_ = vitesst.Exec(t, mcmp.VtConn, "use `ks`")
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}

func startBenchmark(b *testing.B) {
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(b, err)
	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(b, err)

	clearOutAllData(b, vtConn, mysqlConn)
}

func clearOutAllData(t testing.TB, vtConn *mysql.Conn, mysqlConn *mysql.Conn) {
	tables := []string{"t4", "t3", "t2", "t1", "multicol_tbl2", "multicol_tbl1"}
	tables = append(tables, fkTables...)
	keyspaces := []string{`ks/-80`, `ks/80-`, `sks/-80`, `sks/80-`}
	for _, keyspace := range keyspaces {
		_ = vitesst.Exec(t, vtConn, fmt.Sprintf("use `%v`", keyspace))
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(t, vtConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
			_, _ = vitesst.ExecAllowError(t, mysqlConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
		}
	}

	tables = []string{"u_t1", "u_t2", "u_t3"}
	tables = append(tables, fkTables...)
	keyspaces = []string{`uks`, `unmanaged_uks`}
	for _, keyspace := range keyspaces {
		_ = vitesst.Exec(t, vtConn, fmt.Sprintf("use `%v`", keyspace))
		for _, table := range tables {
			_, _ = vitesst.ExecAllowError(t, vtConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
			_, _ = vitesst.ExecAllowError(t, mysqlConn, "delete /*+ SET_VAR(foreign_key_checks=OFF) */ from "+table)
		}
	}
}
