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

package sessionvariable

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vt/schema"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	migrationWaitTimeout  = 60 * time.Second

	// Zero-date defaults are rejected under the default MySQL sql_mode. Setting
	// sql_mode via --session-variable is what makes this CREATE succeed.
	createZeroDateTable = `
		CREATE TABLE %s (
			id INT NOT NULL,
			d DATE DEFAULT '0000-00-00',
			PRIMARY KEY (id)
		) ENGINE=InnoDB`
	createBaseTable = `
		CREATE TABLE %s (
			id INT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB`
	// Adding a zero-date default must fail during VReplication shadow-table ALTER
	// unless sql_mode allows invalid dates.
	alterAddZeroDateColumn = `ALTER TABLE %s ADD COLUMN d DATE DEFAULT '0000-00-00'`
	dropTable              = `DROP TABLE IF EXISTS %s`

	sessionVariableStrategy = "direct --session-variable sql_mode=ALLOW_INVALID_DATES"
	onlineSessionStrategy   = "vitess --session-variable sql_mode=ALLOW_INVALID_DATES"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0o700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"--schema-change-dir", schemaChangeDirectory,
			"--schema-change-controller", "local",
			"--schema-change-check-interval", "1s",
		}
		clusterInstance.VtTabletExtraArgs = []string{
			"--heartbeat-interval", "250ms",
			"--migration-check-interval", "2s",
		}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		keyspace := &cluster.Keyspace{Name: keyspaceName}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false, cell); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	os.Exit(exitcode)
}

// TestVtctldclientDirectSessionVariable verifies ApplySchema --ddl-strategy with
// --session-variable applies SESSION sql_mode before direct DDL.
func TestVtctldclientDirectSessionVariable(t *testing.T) {
	tableName := "vtctldclient_session_var"
	createSQL := fmt.Sprintf(createZeroDateTable, tableName)
	dropSQL := fmt.Sprintf(dropTable, tableName)
	t.Cleanup(func() {
		_, _ = clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
			keyspaceName,
			dropSQL,
			cluster.ApplySchemaParams{DDLStrategy: "direct"},
		)
	})

	t.Run("without session variable", func(t *testing.T) {
		output, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
			keyspaceName,
			createSQL,
			cluster.ApplySchemaParams{DDLStrategy: "direct"},
		)
		require.Error(t, err)
		assert.True(t,
			strings.Contains(output, "Invalid default value") || strings.Contains(err.Error(), "Invalid default value"),
			"expected zero-date rejection, got output=%q err=%v", output, err,
		)
	})

	t.Run("with session variable", func(t *testing.T) {
		_, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
			keyspaceName,
			createSQL,
			cluster.ApplySchemaParams{DDLStrategy: sessionVariableStrategy},
		)
		require.NoError(t, err)
		assertTableExists(t, tableName)
	})
}

// TestVtgateOnlineSessionVariable verifies @@ddl_strategy --session-variable is
// applied on the VReplication shadow-table path (initVreplicationOriginalMigration),
// not only on CREATE TABLE which executes directly.
func TestVtgateOnlineSessionVariable(t *testing.T) {
	require.NoError(t, clusterInstance.WaitForTabletsToHealthyInVtgate())
	shards = clusterInstance.Keyspaces[0].Shards

	tableName := "vtgate_session_var"
	createSQL := fmt.Sprintf(createBaseTable, tableName)
	alterSQL := fmt.Sprintf(alterAddZeroDateColumn, tableName)
	dropSQL := fmt.Sprintf(dropTable, tableName)
	t.Cleanup(func() {
		_, _ = clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
			keyspaceName,
			dropSQL,
			cluster.ApplySchemaParams{DDLStrategy: "direct"},
		)
	})

	_, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
		keyspaceName,
		createSQL,
		cluster.ApplySchemaParams{DDLStrategy: "direct"},
	)
	require.NoError(t, err)
	assertTableExists(t, tableName)

	t.Run("without session variable", func(t *testing.T) {
		uuid := submitOnlineDDL(t, "vitess", alterSQL)
		status := onlineddl.WaitForMigrationStatus(
			t, &vtParams, shards, uuid, migrationWaitTimeout,
			schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed,
		)
		require.Equal(t, schema.OnlineDDLStatusFailed, status)
		assertColumnMissing(t, tableName, "d")
	})

	t.Run("with session variable", func(t *testing.T) {
		uuid := submitOnlineDDL(t, onlineSessionStrategy, alterSQL)
		status := onlineddl.WaitForMigrationStatus(
			t, &vtParams, shards, uuid, migrationWaitTimeout,
			schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed,
		)
		require.Equal(t, schema.OnlineDDLStatusComplete, status)
		assertColumnExists(t, tableName, "d")
	})
}

func submitOnlineDDL(t *testing.T, ddlStrategy, sql string) string {
	t.Helper()
	row := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, sql, "").Named().Row()
	require.NotNil(t, row)
	uuid := strings.TrimSpace(row.AsString("uuid", ""))
	require.NotEmpty(t, uuid)
	return uuid
}

func assertTableExists(t *testing.T, tableName string) {
	t.Helper()
	qr, err := onlineddl.VtgateExecQuery(t.Context(), &vtParams, "show tables like '"+tableName+"'")
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1, "expected table %s to exist", tableName)
}

func assertColumnExists(t *testing.T, tableName, columnName string) {
	t.Helper()
	qr, err := onlineddl.VtgateExecQuery(
		t.Context(),
		&vtParams,
		fmt.Sprintf("show columns from %s like '%s'", tableName, columnName),
	)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1, "expected column %s.%s to exist", tableName, columnName)
}

func assertColumnMissing(t *testing.T, tableName, columnName string) {
	t.Helper()
	qr, err := onlineddl.VtgateExecQuery(
		t.Context(),
		&vtParams,
		fmt.Sprintf("show columns from %s like '%s'", tableName, columnName),
	)
	require.NoError(t, err)
	require.Empty(t, qr.Rows, "expected column %s.%s to be missing", tableName, columnName)
}
