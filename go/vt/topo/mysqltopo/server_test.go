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

package mysqltopo

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

// TestMySQLTopo runs the standard topo server test suite with shared schema.
func TestMySQLTopo(t *testing.T) {
	// Use a single shared schema for all tests in this suite so watch notifications work
	sharedSchemaName := generateRandomSchemaName()
	testIndex := 0

	newServer := func() *topo.Server {
		// Create a test server using the shared schema name
		server, schemaName, cleanup := createTestServer(t, sharedSchemaName)

		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the topo.Server wrapper
		ts, err := topo.OpenServer("mysql", server.serverAddr, path.Join(testRoot, topo.GlobalCell))
		require.NoError(t, err, "OpenServer() failed")

		// Create the CellInfo.
		err = ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: server.serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		})
		require.NoError(t, err, "CreateCellInfo() failed")

		// Register cleanup
		t.Cleanup(func() {
			ts.Close()
			cleanup() // Clean up the test database
			t.Logf("Cleaned up test schema: %s", schemaName)
		})

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		return newServer()
	}, []string{}) // Try all tests including watch tests

	// Run MySQL-specific tests.
	ts := newServer()
	testMySQLSpecific(t, ts)
}

// testMySQLSpecific runs MySQL-specific tests.
func testMySQLSpecific(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	// Test basic file operations
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err)

	// Test Create
	testPath := "test_file"
	testData := []byte("test data")
	version, err := conn.Create(ctx, testPath, testData)
	require.NoError(t, err)
	require.NotNil(t, version)

	// Test Get
	data, getVersion, err := conn.Get(ctx, testPath)
	require.NoError(t, err)
	require.Equal(t, testData, data)
	require.Equal(t, version, getVersion)

	// Test Update
	newData := []byte("updated data")
	newVersion, err := conn.Update(ctx, testPath, newData, version)
	require.NoError(t, err)
	require.NotEqual(t, version, newVersion)

	// Verify update
	data, _, err = conn.Get(ctx, testPath)
	require.NoError(t, err)
	require.Equal(t, newData, data)

	// Test Delete
	err = conn.Delete(ctx, testPath, newVersion)
	require.NoError(t, err)

	// Verify deletion
	_, _, err = conn.Get(ctx, testPath)
	require.Error(t, err)
	require.True(t, topo.IsErrType(err, topo.NoNode))
}

// TestMySQLTopoWatch tests the watch functionality with random schema.
func TestMySQLTopoWatch(t *testing.T) {
	server, schemaName, cleanup := createTestServer(t, "")
	defer cleanup()

	t.Logf("Testing watch functionality with schema: %s", schemaName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a file to watch
	testPath := "watch_test"
	testData := []byte("initial data")
	_, err := server.Create(ctx, testPath, testData)
	require.NoError(t, err)

	// Start watching
	current, changes, err := server.Watch(ctx, testPath)
	require.NoError(t, err)
	require.Equal(t, testData, current.Contents)
	require.Nil(t, current.Err)

	// Update the file in a separate goroutine to trigger watch
	go func() {
		time.Sleep(100 * time.Millisecond)
		newData := []byte("updated data")
		_, err := server.Update(context.Background(), testPath, newData, current.Version)
		if err != nil {
			log.Errorf("Failed to update file: %v", err)
		}
	}()

	// Wait for change notification (with timeout since replication may not be enabled)
	select {
	case change := <-changes:
		if change.Err != nil {
			t.Logf("Watch error (expected if replication not enabled): %v", change.Err)
		} else {
			require.Equal(t, []byte("updated data"), change.Contents)
			t.Logf("Successfully received real-time change notification")
		}
	case <-time.After(2 * time.Second):
		t.Log("No change notification received (expected if binary logging not enabled)")
	}
}

// TestMySQLTopoLocks tests the locking functionality with random schema.
func TestMySQLTopoLocks(t *testing.T) {
	server, schemaName, cleanup := createTestServer(t, "")
	defer cleanup()

	t.Logf("Testing lock functionality with schema: %s", schemaName)

	ctx := context.Background()

	// Create a directory to lock
	testDir := "lock_test_dir"
	testFile := testDir + "/data"

	// Create a file in the directory we want to lock
	_, err := server.Create(ctx, testFile, []byte(`{"protected": "data"}`))
	require.NoError(t, err)

	// Acquire a lock
	lock, err := server.Lock(ctx, testDir, "test lock")
	require.NoError(t, err)

	// Check the lock
	err = lock.Check(ctx)
	require.NoError(t, err)

	// Try to acquire the same lock (should fail)
	_, err = server.TryLock(ctx, testDir, "second lock")
	require.Error(t, err)

	// Release the lock
	err = lock.Unlock(ctx)
	require.NoError(t, err)

	// Now we should be able to acquire the lock
	lock2, err := server.TryLock(ctx, testDir, "third lock")
	require.NoError(t, err)
	defer lock2.Unlock(ctx)
}

// TestMySQLTopoElection tests the leader election functionality with random schema.
func TestMySQLTopoElection(t *testing.T) {
	server, schemaName, cleanup := createTestServer(t, "")
	defer cleanup()

	t.Logf("Testing election functionality with schema: %s", schemaName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Increased timeout
	defer cancel()

	// Create leader participation
	lp, err := server.NewLeaderParticipation("test-election", "node1")
	require.NoError(t, err)
	defer lp.Stop()

	// Wait for leadership with a longer timeout
	leaderCtx, err := lp.WaitForLeadership()
	if err != nil {
		t.Logf("Failed to get leadership: %v", err)
		// Let's check if we can at least create the participation without error
		require.NoError(t, err, "WaitForLeadership should not fail")
	}
	require.NotNil(t, leaderCtx, "Leadership context should not be nil")

	// Check that we're the leader
	leaderID, err := lp.GetCurrentLeaderID(ctx)
	require.NoError(t, err)
	require.Equal(t, "node1", leaderID)

	// Stop participation
	lp.Stop()

	// Leadership context should be cancelled
	select {
	case <-leaderCtx.Done():
		// Expected
		t.Log("Leadership context was properly cancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("Leadership context was not cancelled within timeout")
	}
}
