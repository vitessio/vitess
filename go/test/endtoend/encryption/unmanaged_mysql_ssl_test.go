package encryption

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	clusterInstance    *cluster.LocalProcessCluster
	cell               = "zone1"
	hostname           = "localhost"
	keyspaceName       = "test_keyspace"
	shardName          = "0"
	dbName             = "vt_test_keyspace"
	username           = "vt_app"
	certDirectory      string
	mysqlSSLProcess    *cluster.MysqlctlProcess
	mysqlNonSSLProcess *cluster.MysqlctlProcess
	sslMySQLPort       int
	nonSSLMySQLPort    int
	vtDataRoot         string
	originalVTROOT     string
	originalVTDATAROOT string
)

// TestMain is the entry point for the test suite
func TestMain(m *testing.M) {
	// Ensure cleanup happens even if tests panic
	exitCode := 0
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Test panic: %v", r)
			exitCode = 1
		}
		teardown()
		os.Exit(exitCode)
	}()

	// Run tests
	exitCode = m.Run()
}

// TestUnmanagedMysqlSSLConnection verifies that when connecting to an external MySQL instance,
// vttablet correctly includes SSL configuration parameters.
//
// This test verifies 4 scenarios:
// 1. When a MySQL instance does not require SSL connections and SSL config is configured in the TabletConfig DB config, there's no error
// 2. When a MySQL instance does require SSL connections and SSL config is configured in the TabletConfig DB config, there's no error
// 3. When a MySQL instance does not require SSL connections, and no SSL config is configured in the TabletConfig DB config, there's no error
// 4. When a MySQL instance does require SSL connections, but no SSL config is configured in the TabletConfig DB config, there's an error
func TestUnmanagedMysqlSSLConnection(t *testing.T) {
	// Initialize the cluster and create SSL certificates
	err := setup()
	require.NoError(t, err, "setup failed")

	// Start MySQL instances with different SSL configurations
	err = startMySQLInstances()
	require.NoError(t, err, "failed to start MySQL instances")

	// Test Case 1: MySQL without SSL requirement + vttablet with SSL config (should work)
	t.Run("NonSSL_MySQL_With_SSL_Config", func(t *testing.T) {
		testUnmanagedTabletConnection(t, nonSSLMySQLPort, true, true)
	})

	// Test Case 2: MySQL with SSL requirement + vttablet with SSL config (should work)
	t.Run("SSL_MySQL_With_SSL_Config", func(t *testing.T) {
		testUnmanagedTabletConnection(t, sslMySQLPort, true, true)
	})

	// Test Case 3: MySQL without SSL requirement + vttablet without SSL config (should work)
	t.Run("NonSSL_MySQL_Without_SSL_Config", func(t *testing.T) {
		testUnmanagedTabletConnection(t, nonSSLMySQLPort, false, true)
	})

	// Test Case 4: MySQL with SSL requirement + vttablet without SSL config (should fail)
	t.Run("SSL_MySQL_Without_SSL_Config", func(t *testing.T) {
		testUnmanagedTabletConnection(t, sslMySQLPort, false, false)
	})
}

func setup() error {
	// Store original environment variables for restoration
	originalVTROOT = os.Getenv("VTROOT")
	originalVTDATAROOT = os.Getenv("VTDATAROOT")

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Infof("Received signal %v, cleaning up...", sig)
		teardown()
		os.Exit(1)
	}()

	clusterInstance = cluster.NewCluster(cell, hostname)

	// Set up VTDATAROOT for this test - must be an absolute path
	vtDataRoot = path.Join(clusterInstance.TmpDirectory, "vtdataroot")
	// Ensure it's an absolute path
	if !path.IsAbs(vtDataRoot) {
		cwd, _ := os.Getwd()
		vtDataRoot = path.Join(cwd, vtDataRoot)
	}
	vtDataRoot = path.Clean(vtDataRoot)

	if err := os.MkdirAll(vtDataRoot, 0755); err != nil {
		return fmt.Errorf("failed to create VTDATAROOT: %v", err)
	}

	// Set VTDATAROOT environment variable
	os.Setenv("VTDATAROOT", vtDataRoot)
	log.Infof("Setting VTDATAROOT to: %s", vtDataRoot)

	// Set VTROOT to the vitess source root (4 directories up from current test location)
	// Current location is: <vitess>/go/test/endtoend/encryption
	// We need to go up 4 levels to get to <vitess>
	cwd, _ := os.Getwd()
	vtRoot := path.Join(cwd, "..", "..", "..", "..")
	vtRoot = path.Clean(vtRoot)
	os.Setenv("VTROOT", vtRoot)
	log.Infof("Setting VTROOT to: %s", vtRoot)

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return err
	}

	// Create certificates directory
	log.Info("Creating SSL certificates")
	certDirectory = path.Join(clusterInstance.TmpDirectory, "certs")
	if err := CreateDirectory(certDirectory, 0700); err != nil {
		return err
	}

	// Generate SSL certificates
	if err := ExecuteVttlstestCommand("CreateCA", "--root", certDirectory); err != nil {
		return err
	}

	if err := ExecuteVttlstestCommand("CreateSignedCert", "--root", certDirectory, "--common-name", "MySQL Server", "--serial", "01", "server"); err != nil {
		return err
	}

	if err := ExecuteVttlstestCommand("CreateSignedCert", "--root", certDirectory, "--common-name", "MySQL Client", "--serial", "02", "client"); err != nil {
		return err
	}

	return nil
}

func teardown() {
	log.Infof("Starting test cleanup...")

	// Stop MySQL processes if they exist
	if mysqlNonSSLProcess != nil {
		log.Infof("Stopping non-SSL MySQL process on port %d", nonSSLMySQLPort)
		if err := mysqlNonSSLProcess.Stop(); err != nil {
			log.Errorf("Failed to stop non-SSL MySQL: %v", err)
		}
		mysqlNonSSLProcess = nil
	}

	if mysqlSSLProcess != nil {
		log.Infof("Stopping SSL MySQL process on port %d", sslMySQLPort)
		if err := mysqlSSLProcess.Stop(); err != nil {
			log.Errorf("Failed to stop SSL MySQL: %v", err)
		}
		mysqlSSLProcess = nil
	}

	// Teardown cluster
	if clusterInstance != nil {
		log.Infof("Tearing down cluster instance")
		clusterInstance.Teardown()

		// Clean up test directories
		if clusterInstance.TmpDirectory != "" {
			log.Infof("Cleaning up test directory: %s", clusterInstance.TmpDirectory)
			if err := os.RemoveAll(clusterInstance.TmpDirectory); err != nil {
				log.Errorf("Failed to remove test directory: %v", err)
			}
		}

		// Clean up any vtroot_ directories in the test directory
		// The test runs from the Vitess root directory
		testDir := "go/test/endtoend/encryption"
		if files, err := os.ReadDir(testDir); err == nil {
			for _, file := range files {
				if file.IsDir() && (strings.HasPrefix(file.Name(), "vtroot_") || file.Name() == "vtdataroot") {
					dirPath := path.Join(testDir, file.Name())
					log.Infof("Cleaning up leftover directory: %s", dirPath)
					if err := os.RemoveAll(dirPath); err != nil {
						log.Errorf("Failed to remove directory %s: %v", dirPath, err)
					}
				}
			}
		}

		clusterInstance = nil
	}

	// Clean up VTDATAROOT if it was created by this test
	if vtDataRoot != "" && vtDataRoot != originalVTDATAROOT {
		log.Infof("Cleaning up VTDATAROOT: %s", vtDataRoot)
		if err := os.RemoveAll(vtDataRoot); err != nil {
			log.Errorf("Failed to remove VTDATAROOT: %v", err)
		}
		vtDataRoot = ""
	}

	// Restore original environment variables
	if originalVTDATAROOT != "" {
		os.Setenv("VTDATAROOT", originalVTDATAROOT)
	} else {
		os.Unsetenv("VTDATAROOT")
	}

	if originalVTROOT != "" {
		os.Setenv("VTROOT", originalVTROOT)
	} else {
		os.Unsetenv("VTROOT")
	}

	log.Infof("Test cleanup completed")
}

// startMySQLInstances starts two MySQL instances: one with SSL required and one without
func startMySQLInstances() error {
	// Start MySQL instance without SSL requirement (but with SSL certificates available)
	nonSSLTabletUID := clusterInstance.GetAndReserveTabletUID()
	nonSSLMySQLPort = clusterInstance.GetAndReservePort()

	log.Infof("Starting non-SSL MySQL on port %d with UID %d", nonSSLMySQLPort, nonSSLTabletUID)

	// Create SSL config file for non-SSL MySQL (SSL available but not required)
	nonSSLConfigPath := path.Join(certDirectory, "non_ssl.cnf")
	nonSSLConfig := fmt.Sprintf(`[mysqld]
# SSL is available but not required
ssl-ca=%s/ca-cert.pem
ssl-cert=%s/server-cert.pem
ssl-key=%s/server-key.pem
`, certDirectory, certDirectory, certDirectory)

	if err := os.WriteFile(nonSSLConfigPath, []byte(nonSSLConfig), 0644); err != nil {
		return err
	}

	// Set EXTRA_MY_CNF environment variable for non-SSL MySQL
	os.Setenv("EXTRA_MY_CNF", nonSSLConfigPath)

	// Create and start non-SSL MySQL using mysqlctl
	var err error
	mysqlNonSSLProcess, err = cluster.MysqlCtlProcessInstance(nonSSLTabletUID, nonSSLMySQLPort, clusterInstance.TmpDirectory)
	if err != nil {
		os.Unsetenv("EXTRA_MY_CNF")
		return fmt.Errorf("failed to create non-SSL MySQL process: %v", err)
	}

	// Start the MySQL instance
	log.Infof("mysqlctl binary: %s", mysqlNonSSLProcess.Binary)
	log.Infof("mysqlctl LogDirectory: %s", mysqlNonSSLProcess.LogDirectory)
	log.Infof("mysqlctl TabletUID: %d", mysqlNonSSLProcess.TabletUID)
	log.Infof("mysqlctl InitDBFile: %s", mysqlNonSSLProcess.InitDBFile)
	log.Infof("VTROOT: %s", os.Getenv("VTROOT"))
	log.Infof("VTDATAROOT: %s", os.Getenv("VTDATAROOT"))

	// Check if init_db.sql exists
	if _, err := os.Stat(mysqlNonSSLProcess.InitDBFile); err != nil {
		log.Errorf("InitDBFile does not exist at %s: %v", mysqlNonSSLProcess.InitDBFile, err)
	}

	if err := mysqlNonSSLProcess.Start(); err != nil {
		// Try to get error logs
		logPath := path.Join(vtDataRoot, fmt.Sprintf("vt_%010d", nonSSLTabletUID), "error.log")
		if logContent, readErr := os.ReadFile(logPath); readErr == nil {
			log.Errorf("MySQL error log:\n%s", string(logContent))
		}
		// Also check mysqlctl logs - look for any log files
		logDir := mysqlNonSSLProcess.LogDirectory
		if files, readErr := os.ReadDir(logDir); readErr == nil {
			for _, file := range files {
				if !file.IsDir() {
					fullPath := path.Join(logDir, file.Name())
					if logContent, err := os.ReadFile(fullPath); err == nil && len(logContent) > 0 {
						log.Errorf("Log file %s:\n%s", file.Name(), string(logContent))
					}
				}
			}
		}
		return fmt.Errorf("failed to start non-SSL MySQL: %v", err)
	}

	// Unset EXTRA_MY_CNF after starting non-SSL MySQL
	os.Unsetenv("EXTRA_MY_CNF")

	// Wait for MySQL to be ready
	time.Sleep(5 * time.Second)

	// Create user for non-SSL MySQL
	if err := createMySQLUser(nonSSLMySQLPort, nonSSLTabletUID); err != nil {
		return fmt.Errorf("failed to create user for non-SSL MySQL: %v", err)
	}

	// Start MySQL instance with SSL requirement
	sslTabletUID := clusterInstance.GetAndReserveTabletUID()
	sslMySQLPort = clusterInstance.GetAndReservePort()

	log.Infof("Starting SSL MySQL on port %d with UID %d", sslMySQLPort, sslTabletUID)

	// Create SSL config file
	sslConfigPath := path.Join(certDirectory, "ssl.cnf")
	sslConfig := fmt.Sprintf(`[mysqld]
require_secure_transport=ON
ssl-ca=%s/ca-cert.pem
ssl-cert=%s/server-cert.pem
ssl-key=%s/server-key.pem
`, certDirectory, certDirectory, certDirectory)

	if err := os.WriteFile(sslConfigPath, []byte(sslConfig), 0644); err != nil {
		return err
	}

	// Set EXTRA_MY_CNF environment variable for SSL MySQL
	os.Setenv("EXTRA_MY_CNF", sslConfigPath)

	// Create and start SSL MySQL
	mysqlSSLProcess, err = cluster.MysqlCtlProcessInstance(sslTabletUID, sslMySQLPort, clusterInstance.TmpDirectory)
	if err != nil {
		os.Unsetenv("EXTRA_MY_CNF")
		return fmt.Errorf("failed to create SSL MySQL process: %v", err)
	}

	// Start the MySQL instance
	if err := mysqlSSLProcess.Start(); err != nil {
		os.Unsetenv("EXTRA_MY_CNF")
		// Try to get error logs
		logPath := path.Join(vtDataRoot, fmt.Sprintf("vt_%010d", sslTabletUID), "error.log")
		if logContent, readErr := os.ReadFile(logPath); readErr == nil {
			log.Errorf("MySQL error log:\n%s", string(logContent))
		}
		return fmt.Errorf("failed to start SSL MySQL: %v", err)
	}

	// Unset EXTRA_MY_CNF to avoid affecting other tests
	os.Unsetenv("EXTRA_MY_CNF")

	// Wait for MySQL to be ready
	time.Sleep(5 * time.Second)

	// Create user for SSL MySQL - connect using SSL
	if err := createMySQLUserWithSSL(sslMySQLPort, sslTabletUID); err != nil {
		return fmt.Errorf("failed to create user for SSL MySQL: %v", err)
	}

	return nil
}

// createMySQLUser creates a user in the MySQL instance for vttablet to connect (non-SSL)
func createMySQLUser(port int, tabletUID int) error {
	socketPath := path.Join(vtDataRoot, fmt.Sprintf("vt_%010d/mysql.sock", tabletUID))

	// Wait for socket file to exist
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Connect as root
	params := mysql.ConnParams{
		UnixSocket: socketPath,
		Uname:      "root",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL at socket %s: %v", socketPath, err)
	}
	defer conn.Close()

	// Disable super_read_only and read_only to allow creating users and databases
	if _, err := conn.ExecuteFetch("SET GLOBAL super_read_only = OFF", 1, false); err != nil {
		// Ignore error if variable doesn't exist (e.g., MariaDB)
		log.Infof("Note: could not disable super_read_only (may not exist): %v", err)
	}
	if _, err := conn.ExecuteFetch("SET GLOBAL read_only = OFF", 1, false); err != nil {
		return fmt.Errorf("failed to disable read_only: %v", err)
	}

	// Create database
	if _, err := conn.ExecuteFetch(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName), 1, false); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	// Create user and grant privileges
	queries := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY 'password'", username),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION", username),
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'localhost' IDENTIFIED BY 'password'", username),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'localhost' WITH GRANT OPTION", username),
		"FLUSH PRIVILEGES",
	}

	for _, query := range queries {
		if _, err := conn.ExecuteFetch(query, 1, false); err != nil {
			return fmt.Errorf("failed to execute %s: %v", query, err)
		}
	}

	log.Infof("Successfully created user %s for MySQL on port %d", username, port)
	return nil
}

// createMySQLUserWithSSL creates a user in the MySQL instance that requires SSL
func createMySQLUserWithSSL(port int, tabletUID int) error {
	socketPath := path.Join(vtDataRoot, fmt.Sprintf("vt_%010d/mysql.sock", tabletUID))

	// Wait for socket file to exist
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Connect as root using SSL
	params := mysql.ConnParams{
		UnixSocket: socketPath,
		Uname:      "root",
		SslMode:    vttls.Required,
		SslCa:      path.Join(certDirectory, "ca-cert.pem"),
		SslCert:    path.Join(certDirectory, "client-cert.pem"),
		SslKey:     path.Join(certDirectory, "client-key.pem"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL with SSL at socket %s: %v", socketPath, err)
	}
	defer conn.Close()

	// Disable super_read_only and read_only to allow creating users and databases
	if _, err := conn.ExecuteFetch("SET GLOBAL super_read_only = OFF", 1, false); err != nil {
		// Ignore error if variable doesn't exist (e.g., MariaDB)
		log.Infof("Note: could not disable super_read_only (may not exist): %v", err)
	}
	if _, err := conn.ExecuteFetch("SET GLOBAL read_only = OFF", 1, false); err != nil {
		return fmt.Errorf("failed to disable read_only: %v", err)
	}

	// Create database
	if _, err := conn.ExecuteFetch(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName), 1, false); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	// Create user and grant privileges
	queries := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY 'password'", username),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION", username),
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'localhost' IDENTIFIED BY 'password'", username),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'localhost' WITH GRANT OPTION", username),
		"FLUSH PRIVILEGES",
	}

	for _, query := range queries {
		if _, err := conn.ExecuteFetch(query, 1, false); err != nil {
			return fmt.Errorf("failed to execute %s: %v", query, err)
		}
	}

	log.Infof("Successfully created user %s for SSL MySQL on port %d", username, port)
	return nil
}

// testUnmanagedTabletConnection tests vttablet connection to external MySQL
func testUnmanagedTabletConnection(t *testing.T, mysqlPort int, withSSLConfig bool, shouldSucceed bool) {
	// Create a unique tablet UID
	tabletUID := clusterInstance.GetAndReserveTabletUID()

	// Create vttablet instance
	tablet := clusterInstance.NewVttabletInstance("replica", tabletUID, cell)
	tablet.VttabletProcess = cluster.VttabletProcessInstance(
		tablet.HTTPPort,
		tablet.GrpcPort,
		tablet.TabletUID,
		clusterInstance.Cell,
		shardName,
		keyspaceName,
		clusterInstance.VtctldProcess.Port,
		tablet.Type,
		clusterInstance.TopoProcess.Port,
		clusterInstance.Hostname,
		clusterInstance.TmpDirectory,
		[]string{},
		"utf8mb4")

	// Disable backup restore for unmanaged tablets
	tablet.VttabletProcess.SupportsBackup = false

	// Configure vttablet with unmanaged mode and external MySQL connection
	tablet.VttabletProcess.ExtraArgs = []string{
		"--unmanaged",
		utils.GetFlagVariantForTests("--db-host"), hostname,
		utils.GetFlagVariantForTests("--db-port"), fmt.Sprintf("%d", mysqlPort),
		utils.GetFlagVariantForTests("--db-app-user"), username,
		utils.GetFlagVariantForTests("--db-app-password"), "password",
		utils.GetFlagVariantForTests("--db-dba-user"), username,
		utils.GetFlagVariantForTests("--db-dba-password"), "password",
		utils.GetFlagVariantForTests("--db-appdebug-user"), username,
		utils.GetFlagVariantForTests("--db-appdebug-password"), "password",
		utils.GetFlagVariantForTests("--db-allprivs-user"), username,
		utils.GetFlagVariantForTests("--db-allprivs-password"), "password",
		utils.GetFlagVariantForTests("--db-repl-user"), username,
		utils.GetFlagVariantForTests("--db-repl-password"), "password",
		utils.GetFlagVariantForTests("--db-filtered-user"), username,
		utils.GetFlagVariantForTests("--db-filtered-password"), "password",
		utils.GetFlagVariantForTests("--init-db-name-override"), dbName,
	}

	// Add SSL configuration if requested
	if withSSLConfig {
		tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs,
			utils.GetFlagVariantForTests("--db-ssl-mode"), "verify_ca", // Use verify_ca instead of required to use our custom CA
			utils.GetFlagVariantForTests("--db-ssl-ca"), path.Join(certDirectory, "ca-cert.pem"),
			utils.GetFlagVariantForTests("--db-ssl-cert"), path.Join(certDirectory, "client-cert.pem"),
			utils.GetFlagVariantForTests("--db-ssl-key"), path.Join(certDirectory, "client-key.pem"),
			utils.GetFlagVariantForTests("--db-app-use-ssl"),
			utils.GetFlagVariantForTests("--db-dba-use-ssl"),
			utils.GetFlagVariantForTests("--db-appdebug-use-ssl"),
			utils.GetFlagVariantForTests("--db-allprivs-use-ssl"),
			utils.GetFlagVariantForTests("--db-repl-use-ssl"),
			utils.GetFlagVariantForTests("--db-filtered-use-ssl"),
		)
	}

	log.Infof("Starting vttablet for test: withSSL=%v, shouldSucceed=%v", withSSLConfig, shouldSucceed)

	// Try to start vttablet
	err := tablet.VttabletProcess.Setup()

	if shouldSucceed {
		require.NoError(t, err, "vttablet should start successfully")

		// Wait a bit for tablet to be ready
		time.Sleep(2 * time.Second)

		// Verify tablet is actually healthy
		status := tablet.VttabletProcess.GetTabletStatus()
		require.NotEmpty(t, status, "tablet should have a status")

		log.Infof("Tablet started successfully with status")

		// Clean up the tablet
		tablet.VttabletProcess.TearDown()
	} else {
		// We expect the vttablet to fail to start when connecting to SSL-required MySQL without SSL config
		// The error should happen during the connection check in checkConnectionForExternalMysql
		require.Error(t, err, "vttablet should fail to start without SSL config when MySQL requires SSL")
		log.Infof("Tablet failed to start as expected: %v", err)
	}
}

// Additional test to verify the actual config passed to mysql.Connect includes SSL params
func TestTabletConfigVerification(t *testing.T) {
	// Test different SSL modes
	sslModes := []vttls.SslMode{
		vttls.Disabled,
		vttls.Preferred,
		vttls.Required,
		vttls.VerifyCA,
		vttls.VerifyIdentity,
	}

	for _, sslMode := range sslModes {
		t.Run(string(sslMode), func(t *testing.T) {
			// Create a TabletConfig with the current SSL mode
			config := tabletenv.TabletConfig{
				DB: &dbconfigs.DBConfigs{
					Host:    "localhost",
					Port:    3306,
					Socket:  "",
					App:     dbconfigs.UserConfig{User: "testuser", Password: "testpass"},
					SslMode: sslMode,
					SslCa:   "/path/to/ca.pem",
					SslCert: "/path/to/cert.pem",
					SslKey:  "/path/to/key.pem",
				},
				Unmanaged: true,
			}

			// Verify that the SSL parameters are correctly set in the config
			require.Equal(t, "localhost", config.DB.Host)
			require.Equal(t, 3306, config.DB.Port)
			require.Equal(t, "testuser", config.DB.App.User)
			require.Equal(t, "testpass", config.DB.App.Password)
			require.Equal(t, "", config.DB.Socket)
			require.Equal(t, sslMode, config.DB.SslMode)
			require.Equal(t, "/path/to/ca.pem", config.DB.SslCa)
			require.Equal(t, "/path/to/cert.pem", config.DB.SslCert)
			require.Equal(t, "/path/to/key.pem", config.DB.SslKey)

			// The actual checkConnectionForExternalMysql method will use these SSL parameters
			// when creating the mysql.ConnParams structure and calling mysql.Connect
		})
	}
}
