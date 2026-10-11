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
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

	ctx := t.Context()
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
	require.NoError(t, err)
	assert.NotNil(t, vschema)

	// Wait during outage period
	outageDuration := 500 * time.Millisecond
	time.Sleep(outageDuration)

	// Watchers should remain silent during outage
	watcherCallsDuringOutage := watcherCallCount.Load() - initialWatcherCalls
	assert.Equal(t, int32(0), watcherCallsDuringOutage, "watchers should be silent during outage")

	// Get operations should continue working from cache
	vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
	require.NoError(t, err)
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
	require.NoError(t, err)
	assert.NotNil(t, vschema)
}

// TestVSchemaWatcherKeepsCachedValueOnError tests that the cached value is kept
// for every error other than NoNode, even after the TTL has elapsed.
func TestVSchemaWatcherKeepsCachedValueOnError(t *testing.T) {
	originalCacheTTL := srvTopoCacheTTL
	originalCacheRefresh := srvTopoCacheRefresh
	srvTopoCacheTTL = 100 * time.Millisecond
	srvTopoCacheRefresh = 50 * time.Millisecond
	defer func() {
		srvTopoCacheTTL = originalCacheTTL
		srvTopoCacheRefresh = originalCacheRefresh
	}()

	ctx := t.Context()
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

	for _, forceErr := range []error{
		errors.New("HTTP 500 internal server error"),
		topo.NewError(topo.Timeout, "topo timeout error"),
	} {
		factory.SetError(forceErr)
		// Let the TTL expire and the watch fail to be re-established a few
		// times.
		time.Sleep(3 * srvTopoCacheTTL)

		vschema, err = rs.GetSrvVSchema(ctx, "test_cell")
		require.NoError(t, err, "should serve cached value for %v", forceErr)
		assert.True(t, proto.Equal(initialVSchema, vschema), "should return cached VSchema for %v", forceErr)
	}
	factory.SetError(nil)

	// When the node is deleted, the cached value is cleared.
	err = ts.DeleteSrvVSchema(ctx, "test_cell")
	require.NoError(t, err)
	assert.Eventually(t, func() bool {
		_, err := rs.GetSrvVSchema(ctx, "test_cell")
		return topo.IsErrType(err, topo.NoNode)
	}, 30*time.Second, 10*time.Millisecond)
}

// TestWatcherShouldOnlyNotifyOnActualChanges tests that watchers are called when VSchema content changes.
func TestWatcherShouldOnlyNotifyOnActualChanges(t *testing.T) {
	ctx := t.Context()
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

// setCacheFlags overrides the srvtopo cache flags for the duration of the test.
func setCacheFlags(t *testing.T, ttl, refresh time.Duration) {
	originalCacheTTL := srvTopoCacheTTL
	originalCacheRefresh := srvTopoCacheRefresh
	srvTopoCacheTTL = ttl
	srvTopoCacheRefresh = refresh
	t.Cleanup(func() {
		srvTopoCacheTTL = originalCacheTTL
		srvTopoCacheRefresh = originalCacheRefresh
	})
}

// TestGetSrvKeyspaceDoesNotWaitForWatchRestart tests that, once a watch has
// failed, GetSrvKeyspace serves the cached value instead of waiting for the
// watch to be re-established while the topo server does not answer.
func TestGetSrvKeyspaceDoesNotWaitForWatchRestart(t *testing.T) {
	setCacheFlags(t, 10*time.Millisecond, 10*time.Millisecond)

	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "test_cell")
	counts := stats.NewCountersWithSingleLabel("", "Watch restart test", "type")
	rs := NewResilientServer(ctx, ts, counts)

	want := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{ServedType: topodatapb.TabletType_PRIMARY}},
	}
	err := ts.UpdateSrvKeyspace(ctx, "test_cell", "test_ks", want)
	require.NoError(t, err)
	got, err := rs.GetSrvKeyspace(ctx, "test_cell", "test_ks")
	require.NoError(t, err)
	require.True(t, proto.Equal(want, got))

	entry := rs.SrvKeyspaceWatcher.rw.getEntry(&srvKeyspaceKey{"test_cell", "test_ks"})
	entryState := func() (watchState, time.Time) {
		entry.mutex.Lock()
		defer entry.mutex.Unlock()
		return entry.watchState, entry.lastErrorTime
	}

	// Break the watch, wait until it is due to be re-established, then make
	// the topo server hang.
	factory.SetError(errors.New("watch broken"))
	assert.Eventually(t, func() bool {
		state, lastErrorTime := entryState()
		return state == watchStateIdle && time.Since(lastErrorTime) > srvTopoCacheRefresh
	}, 30*time.Second, time.Millisecond)
	factory.SetError(nil)
	factory.Lock()
	t.Cleanup(factory.Unlock)

	// Start re-establishing the watch. It blocks on the hung topo server.
	entry.mutex.Lock()
	entry.ensureWatchingLocked(ctx)
	entry.mutex.Unlock()
	state, _ := entryState()
	require.Equal(t, watchStateStarting, state)

	// Let the TTL elapse; the cached value must still be served right away.
	time.Sleep(2 * srvTopoCacheTTL)
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	got, err = rs.GetSrvKeyspace(queryCtx, "test_cell", "test_ks")
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, got))
	state, _ = entryState()
	assert.Equal(t, watchStateStarting, state)
}

// TestSrvTopoWatchStaleSeconds tests that SrvTopoWatchStaleSeconds reports how
// long a cached value has been served without a running watch.
func TestSrvTopoWatchStaleSeconds(t *testing.T) {
	setCacheFlags(t, 10*time.Millisecond, 10*time.Millisecond)

	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "stale_cell")
	counts := stats.NewCountersWithSingleLabel("", "Stale seconds test", "type")
	rs := NewResilientServer(ctx, ts, counts)

	err := ts.UpdateSrvKeyspace(ctx, "stale_cell", "stale_ks", &topodatapb.SrvKeyspace{})
	require.NoError(t, err)
	_, err = rs.GetSrvKeyspace(ctx, "stale_cell", "stale_ks")
	require.NoError(t, err)

	const label = "SrvKeyspace.stale_cell.stale_ks"
	staleSeconds := func() (int64, bool) {
		// Querying the value also re-establishes the watch once the topo
		// server is back.
		_, err := rs.GetSrvKeyspace(ctx, "stale_cell", "stale_ks")
		require.NoError(t, err)
		v, ok := allWatchers.staleSeconds()[label]
		return v, ok
	}

	v, ok := staleSeconds()
	require.True(t, ok)
	assert.Zero(t, v)

	factory.SetError(errors.New("topo unavailable"))
	assert.Eventually(t, func() bool {
		v, _ := staleSeconds()
		return v >= 1
	}, 30*time.Second, 10*time.Millisecond)

	factory.SetError(nil)
	assert.Eventually(t, func() bool {
		v, ok := staleSeconds()
		return ok && v == 0
	}, 30*time.Second, 10*time.Millisecond)
}
