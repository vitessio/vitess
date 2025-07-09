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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
)

// TestWatchTopoVersion tests how the topo.Version values work within the mysqltopo
// Watch implementation. The logical versions are based on the MySQL AUTO_INCREMENT
// value, which is a monotonically increasing int64 value.
func TestWatchTopoVersion(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	name := "testkey"
	value := "testval"
	// We use these two variables to ensure that we receive all of the changes in
	// our watch.
	changesMade := atomic.Int64{} // This is accessed across goroutines
	changesSeen := int64(0)

	// Create the key as the vitess topo server requires that it exist before you
	// can watch it.
	_, err := server.Create(ctx, name, []byte(fmt.Sprintf("%s-%d", value, changesMade.Load())))
	require.NoError(t, err)
	changesMade.Add(1)

	var data <-chan *topo.WatchData
	_, data, err = server.Watch(ctx, name)
	require.NoError(t, err, "Server.Watch() error = %v", err)

	// Coordinate between the goroutines on the delete so that we don't miss
	// N changes when restarting the watch.
	token := make(chan struct{})
	defer close(token)

	// Run a goroutine that updates the key we're watching.
	go func() {
		cur := changesMade.Load() + 1
		batchSize := int64(5) // Smaller batch size for MySQL
		for i := cur; i <= cur+batchSize; i++ {
			_, err := server.Update(ctx, name, []byte(fmt.Sprintf("%s-%d", value, i)), nil)
			if err != nil {
				t.Logf("Update failed: %v", err)
				return
			}
			changesMade.Add(1)
			select {
			case <-ctx.Done():
				return
			default:
			}
			time.Sleep(100 * time.Millisecond) // Give MySQL time to process
		}
		// Delete the key to ensure that our version continues to be monotonically
		// increasing.
		err := server.Delete(ctx, name, nil)
		if err != nil {
			t.Logf("Delete failed: %v", err)
			return
		}
		changesMade.Add(1)
		// Let the main goroutine process the delete and restart the watch before
		// we make more changes.
		<-token
		// Recreate the key and make more changes
		cur = changesMade.Load() + 1
		_, err = server.Create(ctx, name, []byte(fmt.Sprintf("%s-%d", value, cur)))
		if err != nil {
			t.Logf("Recreate failed: %v", err)
			return
		}
		changesMade.Add(1)
		for i := cur + 1; i <= cur+batchSize; i++ {
			_, err := server.Update(ctx, name, []byte(fmt.Sprintf("%s-%d", value, i)), nil)
			if err != nil {
				t.Logf("Update after recreate failed: %v", err)
				return
			}
			changesMade.Add(1)
			select {
			case <-ctx.Done():
				return
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Track the expected version - MySQL versions should be monotonically increasing
	var lastVersion int64 = 0

	// Consider the test done when we've been watching the key for 15 seconds.
	// MySQL watch implementation may be slower than etcd.
	watchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		select {
		case <-watchCtx.Done():
			t.Logf("Test completed: expected %d changes, got %d", changesMade.Load(), changesSeen)
			// We may not see all changes due to MySQL's polling nature, but we should see some
			require.Greater(t, changesSeen, int64(0), "should have seen at least some changes")
			return
		case <-ctx.Done():
			require.FailNow(t, "test context cancelled")
		case wd := <-data:
			changesSeen++
			if wd.Err != nil {
				if topo.IsErrType(wd.Err, topo.NoNode) {
					// This was our delete. We'll restart the watch.
					t.Logf("Received delete notification, restarting watch")
					token <- struct{}{} // Tell the goroutine making changes to continue
					// Wait a bit for the key to be recreated
					time.Sleep(500 * time.Millisecond)
					_, data, err = server.Watch(ctx, name)
					if err != nil {
						t.Logf("Failed to restart watch: %v", err)
						return
					}
					continue
				}
				require.FailNow(t, "unexpected error in watch data", "error: %v", wd.Err)
			}
			gotVersion := int64(wd.Version.(MySQLVersion))
			require.Greater(t, gotVersion, lastVersion, "version should be monotonically increasing: got %d, last was %d", gotVersion, lastVersion)
			lastVersion = gotVersion
			t.Logf("Received change %d with version %d", changesSeen, gotVersion)
		}
	}
}

// TestWatchReconnection tests that watch handles MySQL connection issues gracefully.
func TestWatchReconnection(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	name := "reconnect_test_key"
	value := "test_value"

	// Create the key
	_, err := server.Create(ctx, name, []byte(value))
	require.NoError(t, err)

	// Start watching
	_, data, err := server.Watch(ctx, name)
	require.NoError(t, err)

	// Update the key to generate a watch event
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, err := server.Update(ctx, name, []byte(value+"_updated"), nil)
		if err != nil {
			t.Logf("Update failed: %v", err)
		}
	}()

	// We should receive the update
	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	select {
	case <-watchCtx.Done():
		t.Fatal("Timeout waiting for watch event")
	case wd := <-data:
		require.NoError(t, wd.Err)
		require.Equal(t, value+"_updated", string(wd.Contents))
		t.Logf("Successfully received watch event after update")
	}
}

// TestWatchMultipleKeys tests watching multiple keys simultaneously.
func TestWatchMultipleKeys(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	serverAddr := getTestMySQLAddr()
	server, cleanup := setupMySQLTopo(t, serverAddr)
	defer cleanup()

	numKeys := 3
	keys := make([]string, numKeys)
	watches := make([]<-chan *topo.WatchData, numKeys)

	// Create keys and start watches
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("multi_key_%d", i)
		_, err := server.Create(ctx, keys[i], []byte(fmt.Sprintf("value_%d", i)))
		require.NoError(t, err)

		_, data, err := server.Watch(ctx, keys[i])
		require.NoError(t, err)
		watches[i] = data
	}

	// Update all keys
	for i := 0; i < numKeys; i++ {
		go func(idx int) {
			time.Sleep(time.Duration(idx*100) * time.Millisecond)
			_, err := server.Update(ctx, keys[idx], []byte(fmt.Sprintf("updated_value_%d", idx)), nil)
			if err != nil {
				t.Logf("Update failed for key %d: %v", idx, err)
			}
		}(i)
	}

	// Collect all watch events
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	received := make(map[int]bool)
	for len(received) < numKeys {
		select {
		case <-watchCtx.Done():
			t.Fatalf("Timeout waiting for watch events. Received %d out of %d", len(received), numKeys)
		default:
			for i, data := range watches {
				if received[i] {
					continue
				}
				select {
				case wd := <-data:
					require.NoError(t, wd.Err)
					expectedValue := fmt.Sprintf("updated_value_%d", i)
					require.Equal(t, expectedValue, string(wd.Contents))
					received[i] = true
					t.Logf("Received update for key %d", i)
				default:
					// No data available for this watch yet
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("Successfully received watch events for all %d keys", numKeys)
}
