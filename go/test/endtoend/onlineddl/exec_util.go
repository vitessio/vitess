/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/schema"
)

var (
	NormalMigrationWait   = 30 * time.Second
	ExtendedMigrationWait = 60 * time.Second
)

// CreateTempScript creates a script in the temporary directory with given content
func CreateTempScript(t *testing.T, content string) (fileName string) {
	f, err := os.CreateTemp("", "onlineddl-test-")
	require.NoError(t, err)

	_, err = f.WriteString(content)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	return f.Name()
}

// MysqlClientExecFile runs a file through the mysql client
func MysqlClientExecFile(t *testing.T, mysqlParams *mysql.ConnParams, testDataPath, testName string, fileName string) (output string) {
	t.Helper()

	bashPath, err := exec.LookPath("bash")
	require.NoError(t, err)
	mysqlPath, err := exec.LookPath("mysql")
	require.NoError(t, err)

	filePath := fileName
	if !filepath.IsAbs(fileName) {
		filePath, _ = filepath.Abs(path.Join(testDataPath, testName, fileName))
	}
	bashCommand := fmt.Sprintf(`%s -u%s --socket=%s --database=%s -s -s < %s 2> /tmp/error.log`, mysqlPath, mysqlParams.Uname, mysqlParams.UnixSocket, mysqlParams.DbName, filePath)
	cmd, err := exec.Command(
		bashPath,
		"-c",
		bashCommand,
	).Output()

	require.NoError(t, err)
	return string(cmd)
}

// TestOnlineDDLStatement runs an online DDL, ALTER statement and waits for it as appropriate based
// on the strategy and expectations.
// If no expected statuses are provided then this waits for the migration to be:
//   - 'completed' when no expected error is specified and the strategy does not include postponement
//   - 'running' when no expected error is specified and the strategy includes postponement
// If you expect some other status(es) then you should specify. For example, if you expect the migration
// to be throttled then you should expect schema.OnlineDDLStatusRunning.
func TestOnlineDDLStatement(t *testing.T, clusterInstance *cluster.LocalProcessCluster, alterStatement string, ddlStrategy string, providedUUIDList string, providedMigrationContext string, executeStrategy string, expectColumn string, expectError string, expectStatuses ...schema.OnlineDDLStatus) (uuid string) {
	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(alterStatement, tableName)
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	if executeStrategy == "vtgate" {
		row := VtgateExecDDL(t, &vtParams, ddlStrategy, sqlQuery, "").Named().Row()
		if row != nil {
			uuid = row.AsString("uuid", "")
		}
	} else {
		params := cluster.VtctlClientParams{DDLStrategy: ddlStrategy, UUIDList: providedUUIDList, MigrationContext: providedMigrationContext}
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(clusterInstance.Keyspaces[0].Name, sqlQuery, params)
		if expectError == "" {
			assert.NoError(t, err)
			uuid = output
		} else {
			assert.Error(t, err)
			assert.Contains(t, output, expectError)
		}
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	assert.NoError(t, err)

	waitFor := "migration"

	// In the case of a direct DDL there's no migration context to wait for.
	// We should instead wait for the table to be created by checking the
	// tablet(s) directly.
	if strategySetting.Strategy.IsDirect() {
		waitFor = "tablet"
	}
	if strategySetting.IsPostponeCompletion() {
		// We expect it to be running by default
		if len(expectStatuses) == 0 {
			expectStatuses = []schema.OnlineDDLStatus{schema.OnlineDDLStatusRunning}
		}
	}
	if expectError == "" {
		if waitFor == "migration" {
			// Unless otherwise specified, the migration should be completed
			if len(expectStatuses) == 0 {
				expectStatuses = []schema.OnlineDDLStatus{schema.OnlineDDLStatusComplete}
			}
			status := WaitForMigrationStatus(t, &vtParams, clusterInstance.Keyspaces[0].Shards, uuid, NormalMigrationWait, expectStatuses...)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			if expectColumn != "" {
				for _, expectedStatus := range expectStatuses {
					if expectedStatus == schema.OnlineDDLStatusComplete {
						CheckMigratedTable(t, clusterInstance, tableName, expectColumn)
						break
					}
				}
			}
		} else if waitFor == "tablet" && expectColumn != "" {
			WaitForTableOnTablets(t, clusterInstance, tableName, expectColumn)
		}
	}
	return uuid
}

// CheckMigratedTables checks the CREATE STATEMENT of a table after migration
func CheckMigratedTable(t *testing.T, cluster *cluster.LocalProcessCluster, tableName, expectColumn string) {
	for i := range cluster.Keyspaces[0].Shards {
		createStatement := GetCreateTableStatement(t, cluster.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectColumn)
	}
}

// GetCreateTableStatement returns the CREATE TABLE statement for a given table
func GetCreateTableStatement(t *testing.T, tablet *cluster.Vttablet, tableName string) (statement string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), tablet.VttabletProcess.Keyspace, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

// WaitForTableOnTablets checks the CREATE STATEMENT of a table and waits for
// it to appear until we hit the timeout.
func WaitForTableOnTablets(t *testing.T, cluster *cluster.LocalProcessCluster, tableName, expectColumn string) {
	tkr := time.NewTicker(time.Second)
	defer tkr.Stop()
	for {
		select {
		case <-tkr.C:
			for i := range cluster.Keyspaces[0].Shards {
				res, err := cluster.Keyspaces[0].Shards[i].Vttablets[0].VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), cluster.Keyspaces[0].Name, true)
				if err == nil && len(res.Rows) == 1 {
					createTableStmt := res.Rows[0][1].ToString()
					if strings.Contains(createTableStmt, expectColumn) {
						return
					}
				}
			}
		case <-time.After(NormalMigrationWait):
			require.Fail(t, fmt.Sprintf("the %s table did not include the expected %s column before hitting the timeout of %v",
				tableName, expectColumn, NormalMigrationWait))
			return
		}
	}
}
