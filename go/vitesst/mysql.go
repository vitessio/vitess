/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"
)

// comparisonMySQLUID is the tablet UID the standalone comparison mysqld runs
// under inside its container.
const comparisonMySQLUID = 1

// NewMySQL starts a standalone mysqld container from the same image as the
// cluster, for MySQL-comparison tests. It returns connection parameters and a
// cleanup that terminates the container. The schema statements are applied to
// the given database before returning.
func NewMySQL(ctx context.Context, cluster *Cluster, dbName string, schemaSQL ...string) (mysql.ConnParams, func(context.Context) error, error) {
	params, cleanup, err := newMySQLContainer(ctx, cluster, dbName)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	if err := applySchema(ctx, params, schemaSQL); err != nil {
		if cleanupErr := cleanup(ctx); cleanupErr != nil {
			cluster.logf("cleaning up comparison mysqld: %v", cleanupErr)
		}
		return mysql.ConnParams{}, nil, err
	}

	return params, cleanup, nil
}

// newMySQLContainer starts the standalone mysqld container.
func newMySQLContainer(ctx context.Context, cluster *Cluster, dbName string) (mysql.ConnParams, func(context.Context) error, error) {
	initSQL := fmt.Sprintf(`SET GLOBAL super_read_only='OFF';
CREATE DATABASE IF NOT EXISTS %s;
CREATE USER '%s'@'%%';
GRANT ALL ON *.* TO '%s'@'%%' WITH GRANT OPTION;
`, dbName, dbaUser, dbaUser)

	initPath := containerFilesDir + "/init_compare.sql"
	script := fmt.Sprintf(
		"mysqlctl --tablet-uid %d --mysql-port %d --log-format text init --init-db-sql-file %s && sleep infinity",
		comparisonMySQLUID, tabletMySQLPort, initPath,
	)

	// The wait probe connects over the socket as the image user, proving
	// both that mysqld is up and that init SQL ran.
	socket := tabletDirForUID(comparisonMySQLUID) + "/mysql.sock"
	probe := []string{"mysql", "--socket", socket, "-u", dbaUser, "-e", "SELECT 1"}

	name := cluster.name("mysql-compare-" + dbName)
	ctr, err := testcontainers.Run(
		ctx, cluster.image,
		testcontainers.WithEntrypoint("bash", "-c", script),
		testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", tabletMySQLPort)),
		testcontainers.WithTmpfs(map[string]string{vtDataRoot: "uid=999,gid=999"}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(initSQL),
			ContainerFilePath: initPath,
			FileMode:          0o644,
		}),
		testcontainers.WithLogConsumers(cluster.newLogConsumer(name)),
		testcontainers.WithWaitStrategyAndDeadline(
			tabletStartupTimeout,
			wait.ForExec(probe).
				WithStartupTimeout(tabletStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return mysql.ConnParams{}, nil, fmt.Errorf("starting comparison mysqld: %w", err)
	}
	cleanup := func(ctx context.Context) error {
		return testcontainers.TerminateContainer(ctr, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0))
	}

	fail := func(err error) (mysql.ConnParams, func(context.Context) error, error) {
		if cleanupErr := cleanup(ctx); cleanupErr != nil {
			cluster.logf("cleaning up comparison mysqld: %v", cleanupErr)
		}
		return mysql.ConnParams{}, nil, fmt.Errorf("resolving comparison mysqld address: %w", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return fail(err)
	}
	mapped, err := ctr.MappedPort(ctx, fmt.Sprintf("%d/tcp", tabletMySQLPort))
	if err != nil {
		return fail(err)
	}

	params := mysql.ConnParams{
		Host:   host,
		Port:   int(mapped.Num()),
		Uname:  dbaUser,
		DbName: dbName,
	}
	return params, cleanup, nil
}

// applySchema splits each schemaSQL element into statements and applies them.
func applySchema(ctx context.Context, params mysql.ConnParams, schemaSQL []string) error {
	if len(schemaSQL) == 0 {
		return nil
	}

	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return fmt.Errorf("connecting to comparison mysqld: %w", err)
	}
	defer conn.Close()

	parser := sqlparser.NewTestParser()
	for _, sql := range schemaSQL {
		statements, err := parser.SplitStatementToPieces(sql)
		if err != nil {
			return fmt.Errorf("splitting schema SQL: %w", err)
		}
		for _, statement := range statements {
			if _, err := conn.ExecuteFetch(statement, 1, false); err != nil {
				return fmt.Errorf("applying schema statement %q: %w", statement, err)
			}
		}
	}
	return nil
}

// VTParams returns connection parameters for the cluster's first vtgate. The
// database name is set only when given, so unqualified tables resolve through
// vtgate global routing by default.
func (c *Cluster) VTParams(ctx context.Context, dbName string) mysql.ConnParams {
	// Failures surface on stderr unconditionally: this runs in TestMain
	// before any testing.T exists, and returning a zero ConnParams silently
	// would make every test fail with opaque connection errors instead.
	vtgate := c.VTGate()
	if vtgate == nil {
		fmt.Fprintln(os.Stderr, "vitesst: VTParams: no vtgate is running")
		return mysql.ConnParams{}
	}

	addr, err := vtgate.MySQLAddr(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vitesst: VTParams: resolving vtgate mysql address: %v\n", err)
		return mysql.ConnParams{}
	}

	host, portStr, _ := strings.Cut(addr, ":")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vitesst: VTParams: parsing vtgate mysql port from %q: %v\n", addr, err)
		return mysql.ConnParams{}
	}

	return mysql.ConnParams{
		Host:   host,
		Port:   port,
		DbName: dbName,
	}
}

// Connect returns a new MySQL connection to the cluster's first vtgate with
// no default database selected.
func (c *Cluster) Connect(t testing.TB) *mysql.Conn {
	t.Helper()
	return c.connectDB(t, "")
}

// connectDB returns a new MySQL connection to the cluster's first vtgate with
// the given default database (a keyspace name, optionally with a tablet-type
// suffix such as "ks@replica").
func (c *Cluster) connectDB(t testing.TB, dbName string) *mysql.Conn {
	t.Helper()

	ctx := t.Context()
	params := c.VTParams(ctx, dbName)
	require.NotEmpty(t, params.Host, "no vtgate to connect to")

	conn, err := mysql.Connect(ctx, &params)
	require.NoError(t, err, "connecting to vtgate")
	return conn
}
