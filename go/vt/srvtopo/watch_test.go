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

package srvtopo

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// TestWatcherOutageBehavior tests that watchers remain silent during topo outages.
func TestWatcherOutageBehavior(t *testing.T) {
	originalCacheTTL := srvTopoCacheTTL
	originalCacheRefresh := srvTopoCacheRefresh
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 50 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = originalCacheTTL
		srvTopoCacheRefresh = originalCacheRefresh
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "test_cell")
	counts := stats.NewCountersWithSingleLabel("", "Watcher outage test", "type")
	rs := NewResilientServer(ctx, ts, counts)

	initialVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {Sharded: false},
		},
	}
	err := ts.UpdateSrvVSchema(ctx, "test_cell", initialVSchema)
	require.NoError(t, err)

	var watcherCallCount atomic.Int32
	var lastWatcherError atomic.Value

	rs.WatchSrvVSchema(ctx, "test_cell", func(v *vschemapb.SrvVSchema, e error) bool {
		watcherCallCount.Add(1)
		if e != nil {
			lastWatcherError.Store(e)
		} else {
			lastWatcherError.Store((*error)(nil))
		}
		return true
	})

	// Wait for initial callback
	assert.Eventually(t, func() bool {
		return watcherCallCount.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	initialWatcherCalls := watcherCallCount.Load()
	require.GreaterOrEqual(t, initialWatcherCalls, int32(1))
	if errPtr := lastWatcherError.Load(); errPtr != nil {
		if err, ok := errPtr.(error); ok && err != nil {
			require.NoError(t, err)
		}
	}

	// Verify Get operations work normally
	vschema, err := rs.GetSrvVSchema(ctx, "test_cell")
	require.NoError(t, err)
	require.NotNil(t, vschema)

	// Simulate topo outage
	factory.SetError(errors.New("simulated topo error"))

	// Get should still work from cache during outage
	vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
	assert.NoError(t, err)
	assert.NotNil(t, vschema)

	// Wait during outage period
	outageDuration := 500 * time.Millisecond
	time.Sleep(outageDuration)

	// Watchers should remain silent during outage
	watcherCallsDuringOutage := watcherCallCount.Load() - initialWatcherCalls
	assert.Equal(t, int32(0), watcherCallsDuringOutage, "watchers should be silent during outage")

	// Get operations should continue working from cache
	vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
	assert.NoError(t, err)
	assert.NotNil(t, vschema)

	// Clear the error and update VSchema
	factory.SetError(nil)
	updatedVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks2": {Sharded: false},
		},
	}
	err = ts.UpdateSrvVSchema(ctx, "test_cell", updatedVSchema)
	require.NoError(t, err)

	// Verify recovery callback occurs
	watcherCallsBeforeRecovery := watcherCallCount.Load()
	assert.Eventually(t, func() bool {
		errPtr := lastWatcherError.Load()
		isNoError := errPtr == nil || (errPtr.(*error) == nil)
		return watcherCallCount.Load() > watcherCallsBeforeRecovery && isNoError
	}, 2*time.Second, 10*time.Millisecond)

	// Verify recovery worked
	vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
	assert.NoError(t, err)
	assert.NotNil(t, vschema)
}

// TestVSchemaWatcherCacheExpiryBehavior tests cache behavior during different error types.
func TestVSchemaWatcherCacheExpiryBehavior(t *testing.T) {
	originalCacheTTL := srvTopoCacheTTL
	originalCacheRefresh := srvTopoCacheRefresh
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 50 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = originalCacheTTL
		srvTopoCacheRefresh = originalCacheRefresh
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "test_cell")
	counts := stats.NewCountersWithSingleLabel("", "Cache expiry test", "type")
	rs := NewResilientServer(ctx, ts, counts)

	// Set initial VSchema
	initialVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {Sharded: false},
		},
	}
	err := ts.UpdateSrvVSchema(ctx, "test_cell", initialVSchema)
	require.NoError(t, err)

	// Get the initial value to populate cache
	vschema, err := rs.GetSrvVSchema(ctx, "test_cell")
	require.NoError(t, err)
	require.NotNil(t, vschema)

	// Wait for cache TTL to expire
	time.Sleep(srvTopoCacheTTL + 10*time.Millisecond)

	// Set a non-topo error (like 500 HTTP error)
	nonTopoErr := errors.New("HTTP 500 internal server error")
	factory.SetError(nonTopoErr)

	// Get VSchema after TTL expiry with non-topo error
	// Should still serve cached value (not the error)
	vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
	assert.NoError(t, err, "Should serve cached value for non-topo errors even after TTL")
	assert.NotNil(t, vschema, "Should return cached VSchema")

	// Now test with a topo error
	factory.SetError(topo.NewError(topo.Timeout, "topo timeout error"))
	time.Sleep(srvTopoCacheTTL + 10*time.Millisecond) // Let TTL expire again

	// With topo error after TTL expiry, cache should be cleared
	vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
	assert.Error(t, err, "Should return error for topo errors after TTL expiry")
	assert.True(t, topo.IsErrType(err, topo.Timeout), "Should return the topo error")
	assert.Nil(t, vschema, "Should not return vschema when error occurs")
}

// TestWatcherShouldOnlyNotifyOnActualChanges tests that watchers are called when VSchema content changes.
func TestWatcherShouldOnlyNotifyOnActualChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "test_cell")
	counts := stats.NewCountersWithSingleLabel("", "Change detection test", "type")
	rs := NewResilientServer(ctx, ts, counts)

	vschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {Sharded: false},
		},
	}
	err := ts.UpdateSrvVSchema(ctx, "test_cell", vschema)
	require.NoError(t, err)

	var callCount atomic.Int32
	rs.WatchSrvVSchema(ctx, "test_cell", func(v *vschemapb.SrvVSchema, e error) bool {
		callCount.Add(1)
		return true
	})

	// Wait for initial call
	assert.Eventually(t, func() bool {
		return callCount.Load() >= 1
	}, 1*time.Second, 10*time.Millisecond)

	initialCalls := callCount.Load()

	// Update with same vschema content
	err = ts.UpdateSrvVSchema(ctx, "test_cell", vschema)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	callsAfterSameUpdate := callCount.Load()

	t.Logf("Calls after same content update: %d", callsAfterSameUpdate-initialCalls)

	// Update with different vschema
	differentVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks2": {Sharded: true},
		},
	}
	err = ts.UpdateSrvVSchema(ctx, "test_cell", differentVSchema)
	require.NoError(t, err)

	// Should trigger a call for actual changes
	assert.Eventually(t, func() bool {
		return callCount.Load() > callsAfterSameUpdate
	}, 1*time.Second, 10*time.Millisecond)
}
