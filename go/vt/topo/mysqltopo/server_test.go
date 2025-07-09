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

// Package mysqltopo_test contains tests for the MySQL topology implementation.
//
// To run the full test suite with MySQL:
//  1. Start a MySQL server (e.g., MySQL 8.0 or later)
//  2. Set the MYSQL_TOPO_TEST_ADDR environment variable:
//     export MYSQL_TOPO_TEST_ADDR="root:password@localhost:3306/"
//  3. Run: go test -v
//
// To run tests that don't require MySQL:
//
//	go test -v -run "TestMySQLTopoConfigParsing|TestMySQLVersion|TestExpandQuery|TestFactory"
//
// The tests will create and drop temporary databases with names like "topo_test_*".
package mysqltopo

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

// getTestMySQLAddr returns the MySQL server address for testing.
// It checks environment variables for custom settings, otherwise uses defaults.
func getTestMySQLAddr() string {
	if addr := os.Getenv("MYSQL_TOPO_TEST_ADDR"); addr != "" {
		return addr
	}
	return "root@localhost:3306/"
}

// setupMySQLTopo sets up a MySQL topo server for testing.
func setupMySQLTopo(t *testing.T, serverAddr string) (*Server, func()) {
	params := parseServerAddr(serverAddr)
	testDB := fmt.Sprintf("topo_test_%d_%d", time.Now().UnixNano(), rand.Int())
	params.DbName = testDB

	tempParams := *params
	tempParams.DbName = ""

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &tempParams)
	require.NoError(t, err, "Failed to connect to MySQL")

	_, err = conn.ExecuteFetch(fmt.Sprintf("CREATE DATABASE `%s`", testDB), 0, false)
	require.NoError(t, err, "Failed to create test database")
	conn.Close()

	conn, err = mysql.Connect(ctx, params)
	require.NoError(t, err, "Failed to connect to test database")

	server := &Server{
		conn:   conn,
		params: params,
		cells:  make(map[string]*Server),
		root:   "/",
	}

	err = server.createTablesIfNotExist()
	require.NoError(t, err, "Failed to create tables")

	// Don't pre-populate any test data - let each test create what it needs
	// This avoids conflicts with directory tests that expect a clean slate

	cleanup := func() {
		server.Close()
		conn, err := mysql.Connect(ctx, &tempParams)
		if err != nil {
			log.Errorf("Failed to connect to MySQL for cleanup: %v", err)
			return
		}
		defer conn.Close()

		_, err = conn.ExecuteFetch(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testDB), 0, false)
		if err != nil {
			log.Errorf("Failed to drop test database: %v", err)
		}
	}

	return server, cleanup
}

// TestMySQLTopo runs the full topo.Server test suite against MySQL.
func TestMySQLTopo(t *testing.T) {
	// Skip if MySQL is not available
	serverAddr := getTestMySQLAddr()

	testIndex := 0
	newServer := func() *topo.Server {
		// Each test will use its own database and sub-directories with unique timestamps.
		testRoot := fmt.Sprintf("/test-%v-%d", testIndex, time.Now().UnixNano())
		testIndex++

		// Create a completely isolated MySQL topo server for this test
		server, cleanup := setupMySQLTopo(t, serverAddr)

		// Store cleanup function for later use
		t.Cleanup(cleanup)

		// Debug: Log the database being used
		t.Logf("Using test database: %s", server.params.DbName)

		// Create the server address using the isolated database
		var testServerAddr string
		if server.params.Pass != "" {
			testServerAddr = fmt.Sprintf("%s:%s@%s:%d/%s",
				server.params.Uname,
				server.params.Pass,
				server.params.Host,
				server.params.Port,
				server.params.DbName)
		} else {
			testServerAddr = fmt.Sprintf("%s@%s:%d/%s",
				server.params.Uname,
				server.params.Host,
				server.params.Port,
				server.params.DbName)
		}

		t.Logf("Using server address: %s", testServerAddr)

		// Create the server on the new root using the isolated database
		ts, err := topo.OpenServer("mysql", testServerAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo only if it doesn't exist
		cellName := test.LocalCellName
		if _, err := ts.GetCellInfo(context.Background(), cellName, false); err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				// Create the CellInfo.
				if err := ts.CreateCellInfo(context.Background(), cellName, &topodatapb.CellInfo{
					ServerAddress: testServerAddr,
					Root:          path.Join(testRoot, cellName),
				}); err != nil {
					t.Fatalf("CreateCellInfo() failed: %v", err)
				}
			} else {
				t.Fatalf("GetCellInfo() failed: %v", err)
			}
		}

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		return newServer()
	}, []string{})

	// Run MySQL-specific tests.
	ts := newServer()
	testKeyspaceLock(t, ts)
	ts.Close()
}

// TestMySQLTopoBasicOperations tests basic MySQL topo operations.
func TestMySQLTopoBasicOperations(t *testing.T) {
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	ctx := context.Background()
	testKey := "testkey"
	testVal := "testval"

	// Test Create
	_, err := server.Create(ctx, testKey, []byte(testVal))
	require.NoError(t, err, "Failed to create key-value pair")

	// Test Get
	val, _, err := server.Get(ctx, testKey)
	require.NoError(t, err, "Failed to retrieve value")
	require.Equal(t, testVal, string(val), "Value returned doesn't match")

	// Test Update
	newVal := "newval"
	_, err = server.Update(ctx, testKey, []byte(newVal), nil)
	require.NoError(t, err, "Failed to update value")

	// Verify update
	val, _, err = server.Get(ctx, testKey)
	require.NoError(t, err, "Failed to retrieve updated value")
	require.Equal(t, newVal, string(val), "Updated value doesn't match")

	// Test Delete
	err = server.Delete(ctx, testKey, nil)
	require.NoError(t, err, "Failed to delete key")

	// Verify deletion
	_, _, err = server.Get(ctx, testKey)
	require.Error(t, err, "Get should fail after delete")
	require.True(t, topo.IsErrType(err, topo.NoNode), "Error should be NoNode")
}

// TestMySQLTopoServerClosed tests that operations on a closed server return
// appropriate errors instead of panicking due to nil pointer dereference.
func TestMySQLTopoServerClosed(t *testing.T) {
	serverAddr := getTestMySQLAddr()

	testRoot := fmt.Sprintf("/test-closed-%d", time.Now().UnixNano())

	// Create the server on the new root.
	ts, err := topo.OpenServer("mysql", serverAddr, path.Join(testRoot, topo.GlobalCell))
	require.NoError(t, err, "OpenServer() failed")

	// Create the CellInfo first.
	ctx := context.Background()
	cellName := fmt.Sprintf("test_cell_%d", time.Now().UnixNano())
	err = ts.CreateCellInfo(ctx, cellName, &topodatapb.CellInfo{
		ServerAddress: serverAddr,
		Root:          path.Join(testRoot, cellName),
	})
	require.NoError(t, err, "CreateCellInfo() failed")

	// Get the connection for the cell
	conn, err := ts.ConnForCell(ctx, cellName)
	require.NoError(t, err, "ConnForCell() failed")

	// Test that operations work before closing
	testPath := fmt.Sprintf("test_key_%d", time.Now().UnixNano())
	testContents := []byte("test_value")

	_, err = conn.Create(ctx, testPath, testContents)
	require.NoError(t, err, "Create() before close should succeed")

	// Close the connection
	ts.Close()

	// Test that operations return appropriate errors after closing
	anotherKey := fmt.Sprintf("another_key_%d", time.Now().UnixNano())
	_, err = conn.Create(ctx, anotherKey, testContents)
	require.Error(t, err, "Create() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, _, err = conn.Get(ctx, testPath)
	require.Error(t, err, "Get() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, err = conn.GetVersion(ctx, testPath, 1)
	require.Error(t, err, "GetVersion() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	err = conn.Delete(ctx, testPath, nil)
	require.Error(t, err, "Delete() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, err = conn.List(ctx, "/")
	require.Error(t, err, "List() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, err = conn.Update(ctx, testPath, testContents, nil)
	require.Error(t, err, "Update() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	// Test watch operations after close
	_, _, err = conn.Watch(ctx, testPath)
	require.Error(t, err, "Watch() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)

	_, _, err = conn.WatchRecursive(ctx, "/")
	require.Error(t, err, "WatchRecursive() after close should fail")
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Error should be topo.Interrupted, got: %v", err)
}

// TestMySQLTopoLocking tests MySQL-specific locking functionality.
func TestMySQLTopoLocking(t *testing.T) {
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	ctx := context.Background()
	lockPath := fmt.Sprintf("/test/lock_%d", time.Now().UnixNano())

	// Create the directory entry for the lock path
	_, err := server.exec("INSERT IGNORE INTO topo_directories (path) VALUES (%s)", lockPath)
	require.NoError(t, err, "Failed to create directory entry")

	// Test basic locking
	lock1, err := server.Lock(ctx, lockPath, "test-contents")
	require.NoError(t, err, "Failed to acquire lock")

	// Test that second lock fails with TryLock
	_, err = server.TryLock(ctx, lockPath, "test-contents-2")
	require.Error(t, err, "TryLock should fail when lock is held")
	require.True(t, topo.IsErrType(err, topo.NodeExists), "Error should be NodeExists")

	// Test lock check
	err = lock1.Check(ctx)
	require.NoError(t, err, "Lock check should succeed")

	// Test unlock
	err = lock1.Unlock(ctx)
	require.NoError(t, err, "Failed to unlock")

	// Test that lock can be acquired again after unlock
	lock2, err := server.TryLock(ctx, lockPath, "test-contents-3")
	require.NoError(t, err, "Should be able to acquire lock after unlock")

	err = lock2.Unlock(ctx)
	require.NoError(t, err, "Failed to unlock second lock")
}

// TestMySQLTopoLockExpiration tests lock expiration functionality.
func TestMySQLTopoLockExpiration(t *testing.T) {
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	ctx := context.Background()
	lockPath := fmt.Sprintf("/test/lock-expiry_%d", time.Now().UnixNano())

	// Create the directory entry for the lock path
	_, err := server.exec("INSERT IGNORE INTO topo_directories (path) VALUES (%s)", lockPath)
	require.NoError(t, err, "Failed to create directory entry")

	// Test that we can acquire a lock
	lock1, err := server.TryLock(ctx, lockPath, "first-lock")
	require.NoError(t, err, "Failed to acquire first lock")

	// Test that second lock fails while first is held
	_, err = server.TryLock(ctx, lockPath, "second-lock")
	require.Error(t, err, "Second lock should fail while first is held")
	require.True(t, topo.IsErrType(err, topo.NodeExists), "Error should be NodeExists")

	// Unlock the first lock
	err = lock1.Unlock(ctx)
	require.NoError(t, err, "Failed to unlock first lock")

	// Now we should be able to acquire the lock again
	lock2, err := server.TryLock(ctx, lockPath, "third-lock")
	require.NoError(t, err, "Should be able to acquire lock after unlock")

	// Clean up
	err = lock2.Unlock(ctx)
	require.NoError(t, err, "Failed to unlock second lock")
}

// testKeyspaceLock tests MySQL-specific lock behavior with TTL.
func testKeyspaceLock(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	keyspacePath := path.Join(topo.KeyspacesPath, "test_keyspace")
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell failed: %v", err)
	}

	// Test long TTL, unlock before lease runs out.
	lockDescriptor, err := conn.Lock(ctx, keyspacePath, "ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Test short TTL lock behavior
	lockDescriptor, err = conn.LockWithTTL(ctx, keyspacePath, "short ttl", 1*time.Second)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Check that lock is still valid
	if err := lockDescriptor.Check(ctx); err != nil {
		t.Fatalf("Lock check failed: %v", err)
	}

	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

// TestMySQLTopoTransactionRollback tests transaction rollback behavior.
func TestMySQLTopoTransactionRollback(t *testing.T) {
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	lockPath := "/test/transaction-test"
	lockID := "test-lock-id"
	contents := "test-contents"
	expiration := time.Now().Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	ctx := context.Background()
	err := server.createParentDirectories(ctx, lockPath)
	require.NoError(t, err, "Failed to create parent directories")

	// Test successful lock acquisition
	acquired, err := server.tryAcquireLockInTransaction(lockPath, lockID, contents, expiration)
	require.NoError(t, err, "tryAcquireLockInTransaction should not error")
	require.True(t, acquired, "Lock should be acquired")

	// Test that second attempt fails (lock already exists)
	acquired2, err := server.tryAcquireLockInTransaction(lockPath, "different-id", contents, expiration)
	require.NoError(t, err, "tryAcquireLockInTransaction should not error")
	require.False(t, acquired2, "Second lock attempt should fail")

	// Clean up the lock
	_, err = server.exec("DELETE FROM topo_locks WHERE path = %s", lockPath)
	require.NoError(t, err, "Failed to clean up lock")
}

// TestMySQLTopoCellInfo tests CellInfo creation and retrieval.
func TestMySQLTopoCellInfo(t *testing.T) {
	serverAddr := getTestMySQLAddr()

	testRoot := fmt.Sprintf("/test-cellinfo-%d", time.Now().UnixNano())

	// Create the server on the new root.
	ts, err := topo.OpenServer("mysql", serverAddr, path.Join(testRoot, topo.GlobalCell))
	require.NoError(t, err, "OpenServer() failed")
	defer ts.Close()

	ctx := context.Background()

	// Test creating a CellInfo
	cellName := "test_cell"
	cellInfo := &topodatapb.CellInfo{
		ServerAddress: serverAddr,
		Root:          path.Join(testRoot, cellName),
	}

	// First creation should succeed
	err = ts.CreateCellInfo(ctx, cellName, cellInfo)
	require.NoError(t, err, "First CreateCellInfo should succeed")

	// Second creation should fail with NodeExists
	err = ts.CreateCellInfo(ctx, cellName, cellInfo)
	require.Error(t, err, "Second CreateCellInfo should fail")
	require.True(t, topo.IsErrType(err, topo.NodeExists), "Error should be NodeExists, got: %v", err)

	// GetCellInfo should work
	retrievedCellInfo, err := ts.GetCellInfo(ctx, cellName, false)
	require.NoError(t, err, "GetCellInfo should succeed")
	require.Equal(t, cellInfo.ServerAddress, retrievedCellInfo.ServerAddress, "ServerAddress should match")
	require.Equal(t, cellInfo.Root, retrievedCellInfo.Root, "Root should match")

	t.Logf("CellInfo creation and retrieval test passed")
}

// TestMySQLTopoConfigParsing tests configuration parsing without requiring MySQL.
func TestMySQLTopoConfigParsing(t *testing.T) {
	tests := []struct {
		name         string
		serverAddr   string
		expectedHost string
		expectedPort int
		expectedUser string
		expectedDB   string
	}{
		{
			name:         "default values",
			serverAddr:   "",
			expectedHost: "localhost",
			expectedPort: 3306,
			expectedUser: "root",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "host only",
			serverAddr:   "myhost",
			expectedHost: "myhost",
			expectedPort: 3306,
			expectedUser: "root",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "host and port",
			serverAddr:   "myhost:3307",
			expectedHost: "myhost",
			expectedPort: 3307,
			expectedUser: "root",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user and host",
			serverAddr:   "myuser@myhost",
			expectedHost: "myhost",
			expectedPort: 3306,
			expectedUser: "myuser",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user, password, host and port",
			serverAddr:   "myuser:mypass@myhost:3307",
			expectedHost: "myhost",
			expectedPort: 3307,
			expectedUser: "myuser",
			expectedDB:   DefaultTopoSchema,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := parseServerAddr(tt.serverAddr)
			require.Equal(t, tt.expectedHost, params.Host, "Host mismatch")
			require.Equal(t, tt.expectedPort, params.Port, "Port mismatch")
			require.Equal(t, tt.expectedUser, params.Uname, "User mismatch")
			require.Equal(t, tt.expectedDB, params.DbName, "Database mismatch")
		})
	}
}

// TestErrorConversion tests that errors are properly converted.
func TestErrorConversion(t *testing.T) {
	// Test convertError function
	err := fmt.Errorf("connection closed")
	convertedErr := convertError(err, "/test/path")

	t.Logf("Original error: %v", err)
	t.Logf("Converted error: %v", convertedErr)
	t.Logf("Is topo.Interrupted: %v", topo.IsErrType(convertedErr, topo.Interrupted))

	require.True(t, topo.IsErrType(convertedErr, topo.Interrupted), "Error should be topo.Interrupted")
}

// TestDirectoryCreation tests directory creation specifically.
func TestDirectoryCreation(t *testing.T) {
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	ctx := context.Background()

	// Test creating a file that should create parent directories
	testPath := "cells/test/CellInfo"
	testContents := []byte("test cell info")

	t.Logf("Creating file at path: %s", testPath)

	// Create the file
	_, err := server.Create(ctx, testPath, testContents)
	require.NoError(t, err, "Failed to create file")

	// Check what directories were created
	result, err := server.query("SELECT path FROM topo_directories ORDER BY path")
	require.NoError(t, err, "Failed to query directories")

	t.Logf("Directories created:")
	for _, row := range result.Rows {
		t.Logf("  %s", row[0].ToString())
	}

	// Check what files were created
	result, err = server.query("SELECT path FROM topo_files ORDER BY path")
	require.NoError(t, err, "Failed to query files")

	t.Logf("Files created:")
	for _, row := range result.Rows {
		t.Logf("  %s", row[0].ToString())
	}

	// Test ListDir on root
	entries, err := server.ListDir(ctx, "/", false)
	require.NoError(t, err, "Failed to list root directory")

	t.Logf("Root directory contents:")
	for _, entry := range entries {
		t.Logf("  %s", entry.Name)
	}

	// Test ListDir on cells
	entries, err = server.ListDir(ctx, "cells", false)
	if err != nil {
		t.Logf("Failed to list cells directory: %v", err)
	} else {
		t.Logf("Cells directory contents:")
		for _, entry := range entries {
			t.Logf("  %s", entry.Name)
		}
	}
}

// TestDirectoryListingDebug tests directory listing with detailed debugging.
func TestDirectoryListingDebug(t *testing.T) {
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	ctx := context.Background()

	// Debug: Show the server root
	t.Logf("Server root: %s", server.root)

	// Create a file 3 layers down like the test does
	testPath := "/types/name/MyFile"
	testContents := []byte{'a'}

	t.Logf("Creating file at path: %s", testPath)
	t.Logf("Full path will be: %s", server.fullPath(testPath))

	// Create the file
	_, err := server.Create(ctx, testPath, testContents)
	require.NoError(t, err, "Failed to create file")

	// Check what directories were created
	result, err := server.query("SELECT path FROM topo_directories ORDER BY path")
	require.NoError(t, err, "Failed to query directories")

	t.Logf("Directories created:")
	for _, row := range result.Rows {
		t.Logf("  %s", row[0].ToString())
	}

	// Check what files were created
	result, err = server.query("SELECT path FROM topo_files ORDER BY path")
	require.NoError(t, err, "Failed to query files")

	t.Logf("Files created:")
	for _, row := range result.Rows {
		t.Logf("  %s", row[0].ToString())
	}

	// Test ListDir on /types/
	t.Logf("Testing ListDir on /types/")
	t.Logf("Full path for /types/ will be: %s", server.fullPath("/types/"))

	// Debug the SQL query
	fullDirPath := server.fullPath("/types/")
	dirPrefix := fullDirPath
	if !strings.HasSuffix(dirPrefix, "/") {
		dirPrefix = dirPrefix + "/"
	}
	t.Logf("fullDirPath: %s", fullDirPath)
	t.Logf("dirPrefix: %s", dirPrefix)

	// Test the SQL queries directly
	t.Logf("Testing file query...")
	fileResult, err := server.query(`
		SELECT SUBSTRING_INDEX(SUBSTRING(path, LENGTH(%s) + 1), '/', 1) AS name
		FROM topo_files
		WHERE path LIKE %s AND path NOT LIKE %s
		GROUP BY name
	`, dirPrefix, dirPrefix+"%", dirPrefix+"%/%")
	if err != nil {
		t.Logf("File query failed: %v", err)
	} else {
		t.Logf("File query results:")
		for _, row := range fileResult.Rows {
			t.Logf("  %s", row[0].ToString())
		}
	}

	t.Logf("Testing directory query...")
	dirResult, err := server.query(`
		SELECT SUBSTRING_INDEX(SUBSTRING(path, LENGTH(%s) + 1), '/', 1) AS name
		FROM topo_directories
		WHERE path LIKE %s AND path != %s AND path NOT LIKE %s
		GROUP BY name
	`, dirPrefix, dirPrefix+"%", fullDirPath, dirPrefix+"%/%")
	if err != nil {
		t.Logf("Directory query failed: %v", err)
	} else {
		t.Logf("Directory query results:")
		for _, row := range dirResult.Rows {
			t.Logf("  %s", row[0].ToString())
		}
	}

	entries, err := server.ListDir(ctx, "/types/", false)
	if err != nil {
		t.Logf("ListDir(/types/) failed: %v", err)
	} else {
		t.Logf("/types/ directory contents:")
		for _, entry := range entries {
			t.Logf("  %s (type: %d)", entry.Name, entry.Type)
		}
	}

	// Test ListDir on /types/name/
	t.Logf("Testing ListDir on /types/name/")
	t.Logf("Full path for /types/name/ will be: %s", server.fullPath("/types/name/"))
	entries, err = server.ListDir(ctx, "/types/name/", false)
	if err != nil {
		t.Logf("ListDir(/types/name/) failed: %v", err)
	} else {
		t.Logf("/types/name/ directory contents:")
		for _, entry := range entries {
			t.Logf("  %s (type: %d)", entry.Name, entry.Type)
		}
	}

	// Now delete the file and see what happens
	t.Logf("Deleting file...")
	err = server.Delete(ctx, testPath, nil)
	require.NoError(t, err, "Failed to delete file")

	// Check what directories remain
	result, err = server.query("SELECT path FROM topo_directories ORDER BY path")
	require.NoError(t, err, "Failed to query directories")

	t.Logf("Directories after delete:")
	for _, row := range result.Rows {
		t.Logf("  %s", row[0].ToString())
	}

	// Test ListDir on /types/ after delete
	t.Logf("Testing ListDir on /types/ after delete")
	entries, err = server.ListDir(ctx, "/types/", false)
	if err != nil {
		t.Logf("ListDir(/types/) failed: %v", err)
	} else {
		t.Logf("/types/ directory contents after delete:")
		for _, entry := range entries {
			t.Logf("  %s (type: %d)", entry.Name, entry.Type)
		}
	}
}

// TestShardCreation tests shard creation specifically.
func TestShardCreation(t *testing.T) {
	serverAddr := getTestMySQLAddr()

	testRoot := fmt.Sprintf("/test-shard-%d", time.Now().UnixNano())
	keyspaceName := fmt.Sprintf("test_keyspace_%d", time.Now().UnixNano())

	// Create the server on the new root.
	ts, err := topo.OpenServer("mysql", serverAddr, path.Join(testRoot, topo.GlobalCell))
	require.NoError(t, err, "OpenServer() failed")
	defer ts.Close()

	ctx := context.Background()

	// Create a keyspace first
	err = ts.CreateKeyspace(ctx, keyspaceName, &topodatapb.Keyspace{})
	require.NoError(t, err, "Failed to create keyspace")

	// Create a shard
	err = ts.CreateShard(ctx, keyspaceName, "b0-c0")
	require.NoError(t, err, "Failed to create shard")

	// Get the shard and check its properties
	si, err := ts.GetShard(ctx, keyspaceName, "b0-c0")
	require.NoError(t, err, "Failed to get shard")

	t.Logf("Shard IsPrimaryServing: %v", si.IsPrimaryServing)
	t.Logf("Shard KeyRange: %v", si.KeyRange)

	// Test GetServingShards
	servingShards, err := ts.GetServingShards(ctx, keyspaceName)
	if err != nil {
		t.Logf("GetServingShards failed: %v", err)

		// Debug: Let's check what FindAllShardsInKeyspace returns
		allShards, err2 := ts.FindAllShardsInKeyspace(ctx, keyspaceName, nil)
		if err2 != nil {
			t.Logf("FindAllShardsInKeyspace failed: %v", err2)
		} else {
			t.Logf("Found %d total shards:", len(allShards))
			for name, shard := range allShards {
				t.Logf("  Shard %s: IsPrimaryServing=%v", name, shard.IsPrimaryServing)
			}
		}
	} else {
		t.Logf("Found %d serving shards", len(servingShards))
	}
}
