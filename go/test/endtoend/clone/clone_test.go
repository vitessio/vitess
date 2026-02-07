/*
Copyright 2025 The Vitess Authors.

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

package clone

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	donorTablet     *cluster.Vttablet
	recipientTablet *cluster.Vttablet
	hostname        = "localhost"
	cell            = "zone1"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		// Check MySQL version first - skip entire test suite if not supported
		versionStr, err := mysqlctl.GetVersionString()
		if err != nil {
			log.Infof("Skipping clone tests: unable to get MySQL version: %v", err)
			return 0
		}
		log.Infof("Detected MySQL version: %s", versionStr)

		flavor, version, err := mysqlctl.ParseVersionString(versionStr)
		if err != nil {
			log.Infof("Skipping clone tests: unable to parse MySQL version: %v", err)
			return 0
		}
		log.Infof("Parsed flavor: %v, version: %d.%d.%d", flavor, version.Major, version.Minor, version.Patch)

		// Clone is only supported on MySQL 8.0.17+
		if flavor != mysqlctl.FlavorMySQL && flavor != mysqlctl.FlavorPercona {
			log.Infof("Skipping clone tests: MySQL CLONE requires MySQL or Percona, got flavor: %v", flavor)
			return 0
		}
		if version.Major < 8 || (version.Major == 8 && version.Minor == 0 && version.Patch < 17) {
			log.Infof("Skipping clone tests: MySQL CLONE requires version 8.0.17+, got: %d.%d.%d", version.Major, version.Minor, version.Patch)
			return 0
		}

		// Verify clone capability using the clean version string
		cleanVersion := fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch)
		capableOf := mysql.ServerVersionCapableOf(cleanVersion)
		if capableOf == nil {
			log.Infof("Skipping clone tests: unable to get capability checker for version %s", cleanVersion)
			return 0
		}
		hasClone, err := capableOf(capabilities.MySQLClonePluginFlavorCapability)
		if err != nil || !hasClone {
			log.Infof("Skipping clone tests: MySQL version %s does not support CLONE plugin", cleanVersion)
			return 0
		}
		log.Infof("MySQL version %s supports CLONE plugin, proceeding with tests", cleanVersion)

		// Setup EXTRA_MY_CNF for clone plugin
		if err := setupExtraMyCnf(); err != nil {
			log.Errorf("Failed to setup extra MySQL config: %v", err)
			return 1
		}

		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			log.Errorf("Failed to start topo: %v", err)
			return 1
		}

		// Initialize cluster with 2 tablets for clone testing
		if err := initClusterForClone(); err != nil {
			log.Errorf("Failed to init cluster: %v", err)
			return 1
		}

		// Clean up MySQL processes explicitly since we don't register them with the cluster
		defer func() {
			for _, tablet := range []*cluster.Vttablet{donorTablet, recipientTablet} {
				if tablet != nil {
					if err := tablet.MysqlctlProcess.Stop(); err != nil {
						log.Errorf("Failed to stop MySQL for tablet %d: %v", tablet.TabletUID, err)
					}
				}
			}
		}()

		return m.Run()
	}()
	os.Exit(exitCode)
}

// setupExtraMyCnf sets EXTRA_MY_CNF to include clone plugin configuration
func setupExtraMyCnf() error {
	cloneCnfPath := path.Join(os.Getenv("VTROOT"), "config", "mycnf", "clone.cnf")
	if _, err := os.Stat(cloneCnfPath); os.IsNotExist(err) {
		return fmt.Errorf("clone.cnf not found at %s", cloneCnfPath)
	}

	// Check if EXTRA_MY_CNF is already set
	existing := os.Getenv("EXTRA_MY_CNF")
	if existing != "" {
		// Append clone.cnf to existing
		if err := os.Setenv("EXTRA_MY_CNF", existing+":"+cloneCnfPath); err != nil {
			return fmt.Errorf("failed to set EXTRA_MY_CNF: %v", err)
		}
	} else {
		if err := os.Setenv("EXTRA_MY_CNF", cloneCnfPath); err != nil {
			return fmt.Errorf("failed to set EXTRA_MY_CNF: %v", err)
		}
	}

	log.Infof("Set EXTRA_MY_CNF to include clone plugin: %s", os.Getenv("EXTRA_MY_CNF"))
	return nil
}

// initClusterForClone sets up two MySQL instances for clone testing
func initClusterForClone() error {
	// Create a combined init file that includes clone user
	initDBWithClone, err := createInitDBWithCloneUser()
	if err != nil {
		return fmt.Errorf("failed to create init DB file: %v", err)
	}
	log.Infof("Created combined init file at: %s", initDBWithClone)

	var mysqlCtlProcessList []*exec.Cmd

	// Create donor tablet (will be the clone source)
	donorTablet = &cluster.Vttablet{
		TabletUID: clusterInstance.GetAndReserveTabletUID(),
		HTTPPort:  clusterInstance.GetAndReservePort(),
		GrpcPort:  clusterInstance.GetAndReservePort(),
		MySQLPort: clusterInstance.GetAndReservePort(),
		Type:      "primary",
	}
	donorTablet.Alias = fmt.Sprintf("%s-%010d", clusterInstance.Cell, donorTablet.TabletUID)

	// Create recipient tablet (will receive cloned data)
	recipientTablet = &cluster.Vttablet{
		TabletUID: clusterInstance.GetAndReserveTabletUID(),
		HTTPPort:  clusterInstance.GetAndReservePort(),
		GrpcPort:  clusterInstance.GetAndReservePort(),
		MySQLPort: clusterInstance.GetAndReservePort(),
		Type:      "replica",
	}
	recipientTablet.Alias = fmt.Sprintf("%s-%010d", clusterInstance.Cell, recipientTablet.TabletUID)

	// Start MySQL for both tablets with custom init file that includes clone user
	for _, tablet := range []*cluster.Vttablet{donorTablet, recipientTablet} {
		mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(
			tablet.TabletUID,
			tablet.MySQLPort,
			clusterInstance.TmpDirectory,
		)
		if err != nil {
			return fmt.Errorf("failed to create mysqlctl for tablet %d: %v", tablet.TabletUID, err)
		}
		// Use our custom init file with clone user
		mysqlctlProcess.InitDBFile = initDBWithClone
		tablet.MysqlctlProcess = *mysqlctlProcess

		proc, err := tablet.MysqlctlProcess.StartProcess()
		if err != nil {
			return fmt.Errorf("failed to start MySQL for tablet %d: %v", tablet.TabletUID, err)
		}
		mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
	}

	// Wait for MySQL processes to be ready
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			return fmt.Errorf("MySQL process failed to start: %v", err)
		}
	}
	log.Infof("MySQL processes started successfully")

	// Note: We intentionally do NOT register tablets with shards/keyspaces
	// because we only start MySQL processes (not vttablets). The standard
	// Teardown would try to stop VttabletProcess which we never started.
	// Instead, we clean up MySQL explicitly in TestMain.

	return nil
}

// createInitDBWithCloneUser creates a combined init_db.sql that includes clone user setup.
// It uses the official {{custom_sql}} marker in init_db.sql to inject the clone user SQL.
func createInitDBWithCloneUser() (string, error) {
	initDBPath := path.Join(os.Getenv("VTROOT"), "config", "init_db.sql")
	initClonePath := path.Join(os.Getenv("VTROOT"), "config", "init_clone.sql")

	initDB, err := os.ReadFile(initDBPath)
	if err != nil {
		return "", fmt.Errorf("failed to read init_db.sql: %v", err)
	}

	initClone, err := os.ReadFile(initClonePath)
	if err != nil {
		return "", fmt.Errorf("failed to read init_clone.sql: %v", err)
	}

	// Use the official {{custom_sql}} marker pattern to inject clone user SQL
	combined, err := utils.GetInitDBSQL(string(initDB), string(initClone), "")
	if err != nil {
		return "", fmt.Errorf("failed to inject clone SQL: %v", err)
	}

	// Write to temp file
	combinedPath := path.Join(clusterInstance.TmpDirectory, "init_db_with_clone.sql")
	if err := os.WriteFile(combinedPath, []byte(combined), 0o666); err != nil {
		return "", fmt.Errorf("failed to write combined init file: %v", err)
	}

	return combinedPath, nil
}

// connectToTablet creates a MySQL connection to the given tablet
func connectToTablet(ctx context.Context, tablet *cluster.Vttablet) (*mysql.Conn, error) {
	socketPath := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID), "mysql.sock")
	params := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: socketPath,
	}
	return mysql.Connect(ctx, &params)
}

// createMysqldForTablet creates a Mysqld instance for CloneExecutor
func createMysqldForTablet(tablet *cluster.Vttablet) *mysqlctl.Mysqld {
	socketPath := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID), "mysql.sock")

	dbcfgs := dbconfigs.NewTestDBConfigs(mysql.ConnParams{
		UnixSocket: socketPath,
		Uname:      "vt_dba",
	}, mysql.ConnParams{
		UnixSocket: socketPath,
		Uname:      "vt_app",
	}, "")

	return mysqlctl.NewMysqld(dbcfgs)
}

// TestCloneRemote tests MySQL CLONE INSTANCE functionality
func TestCloneRemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// Connect to donor and insert test data
	donorConn, err := connectToTablet(ctx, donorTablet)
	require.NoError(t, err, "Failed to connect to donor")
	defer donorConn.Close()

	// Disable super_read_only so we can create test data
	_, err = donorConn.ExecuteFetch("SET GLOBAL super_read_only = OFF", 0, false)
	require.NoError(t, err, "Failed to disable super_read_only")
	_, err = donorConn.ExecuteFetch("SET GLOBAL read_only = OFF", 0, false)
	require.NoError(t, err, "Failed to disable read_only")

	// Create test database and table on donor
	_, err = donorConn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS test_clone", 0, false)
	require.NoError(t, err, "Failed to create test database")

	_, err = donorConn.ExecuteFetch(`
		CREATE TABLE IF NOT EXISTS test_clone.clone_test (
			id INT AUTO_INCREMENT PRIMARY KEY,
			msg VARCHAR(255)
		) ENGINE=InnoDB
	`, 0, false)
	require.NoError(t, err, "Failed to create test table")

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err = donorConn.ExecuteFetch(fmt.Sprintf(
			"INSERT INTO test_clone.clone_test (msg) VALUES ('test message %d')", i), 0, false)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}

	// Verify donor has the data
	qr, err := donorConn.ExecuteFetch("SELECT COUNT(*) FROM test_clone.clone_test", 1, false)
	require.NoError(t, err, "Failed to count rows on donor")
	require.Len(t, qr.Rows, 1)
	require.Equal(t, "10", qr.Rows[0][0].ToString(), "Donor should have 10 rows")

	// Pre-clone verification: ensure recipient does NOT have the test database
	// This proves the clone actually transfers data, not that it was already there
	recipientConnPreClone, err := connectToTablet(ctx, recipientTablet)
	require.NoError(t, err, "Failed to connect to recipient for pre-clone check")
	qr, err = recipientConnPreClone.ExecuteFetch("SHOW DATABASES LIKE 'test_clone'", 1, false)
	require.NoError(t, err, "Failed to check for test_clone database on recipient")
	require.Len(t, qr.Rows, 0, "Recipient should NOT have test_clone database before clone")
	recipientConnPreClone.Close()

	// Create Mysqld instance for recipient (needed by CloneExecutor)
	recipientMysqld := createMysqldForTablet(recipientTablet)
	defer recipientMysqld.Close()

	// Enable MySQL CLONE for the test
	mysqlctl.SetMySQLCloneEnabled(true)
	defer mysqlctl.SetMySQLCloneEnabled(false)

	// Execute clone
	executor := &mysqlctl.CloneExecutor{
		DonorHost:     "127.0.0.1",
		DonorPort:     donorTablet.MySQLPort,
		DonorUser:     "vt_clone",
		DonorPassword: "",
		UseSSL:        false,
	}

	err = executor.ExecuteClone(ctx, recipientMysqld, 5*time.Minute)
	require.NoError(t, err, "Clone operation failed")

	// Connect to recipient and verify data
	recipientConn, err := connectToTablet(ctx, recipientTablet)
	require.NoError(t, err, "Failed to connect to recipient after clone")
	defer recipientConn.Close()

	// Verify clone succeeded at MySQL level via performance_schema.clone_status
	qr, err = recipientConn.ExecuteFetch(
		"SELECT STATE, ERROR_NO, ERROR_MESSAGE FROM performance_schema.clone_status ORDER BY ID DESC LIMIT 1", 1, false)
	require.NoError(t, err, "Failed to query clone_status")
	require.Len(t, qr.Rows, 1, "Expected one clone_status row")
	cloneState := qr.Rows[0][0].ToString()
	cloneErrorNo := qr.Rows[0][1].ToString()
	cloneErrorMsg := qr.Rows[0][2].ToString()
	require.Equal(t, "Completed", cloneState, "Clone state should be Completed")
	require.Equal(t, "0", cloneErrorNo, "Clone should have no error, got: %s", cloneErrorMsg)

	// Verify recipient has the cloned data
	qr, err = recipientConn.ExecuteFetch("SELECT COUNT(*) FROM test_clone.clone_test", 1, false)
	require.NoError(t, err, "Failed to count rows on recipient")
	require.Len(t, qr.Rows, 1)
	require.Equal(t, "10", qr.Rows[0][0].ToString(), "Recipient should have 10 rows after clone")

	// Verify actual data content matches
	donorData, err := donorConn.ExecuteFetch("SELECT id, msg FROM test_clone.clone_test ORDER BY id", 100, false)
	require.NoError(t, err)

	recipientData, err := recipientConn.ExecuteFetch("SELECT id, msg FROM test_clone.clone_test ORDER BY id", 100, false)
	require.NoError(t, err)

	require.Equal(t, len(donorData.Rows), len(recipientData.Rows), "Row counts should match")
	for i := range donorData.Rows {
		assert.Equal(t, donorData.Rows[i][0].ToString(), recipientData.Rows[i][0].ToString(), "IDs should match at row %d", i)
		assert.Equal(t, donorData.Rows[i][1].ToString(), recipientData.Rows[i][1].ToString(), "Messages should match at row %d", i)
	}

	t.Logf("Clone test passed: successfully cloned 10 rows from donor (tablet %d) to recipient (tablet %d)",
		donorTablet.TabletUID, recipientTablet.TabletUID)
}
